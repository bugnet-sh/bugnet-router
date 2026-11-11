package lium

import (
	"bytes"
	"cmp"
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const (
	// sshKeyLabel separates the pod key's derivation from any other use of the seed
	sshKeyLabel = "bugnet/lium-ssh/v1"
	seedBytes   = 32
	keyComment  = "bugnet"
	// sshUser is the user the router logs in as, root in every pod
	sshUser = "root"
	// maxHostKeyBytes bounds a host key in wire form, a 16384-bit RSA key being about 2 KiB
	maxHostKeyBytes = 4 << 10
	// maxHostKeyLine bounds the host_key line read from Ref.Meta
	maxHostKeyLine = 8 << 10
	// keepaliveRequest is the global request OpenSSH's own client sends to check a connection
	keepaliveRequest = "keepalive@openssh.com"
	// sshRefused is the reason of a pod whose sshd no longer takes the router's key
	sshRefused = "lium_ssh_refused"
)

// sshLimits bound each pod's pool and every SSH operation, all design 3.4's starting values
type sshLimits struct {
	// dial bounds one connection's dial, handshake and login
	dial time.Duration
	// keepalive spaces the keepalives, a connection that misses one answer being broken
	keepalive time.Duration
	// idle closes a connection that held no session this long
	idle time.Duration
	// op bounds an operation whose caller set no deadline
	op time.Duration
	// kill bounds the kill of a cancelled command's group
	kill time.Duration
	// stall drops a connection whose open or exec still waits this long after the caller left
	stall time.Duration
	// perConn is the sessions one connection takes before another opens, conns the connections
	perConn, conns int
}

var defaultSSHLimits = sshLimits{
	dial: 10 * time.Second, keepalive: 15 * time.Second, idle: 2 * time.Minute,
	op: 2 * time.Minute, kill: 30 * time.Second, stall: 5 * time.Second, perConn: 8, conns: 4,
}

var (
	errHostKeyChanged = errors.New("the pod's sshd showed another host key than the one pinned")
	errKeyRefused     = errors.New(sshRefused + ", the pod's sshd refused the router's key")
	errPoolClosed     = errors.New("the pod's SSH connections are closed")
	// plainHostKeyAlgos are the host keys a first connection takes, never a certificate
	plainHostKeyAlgos = []string{
		ssh.KeyAlgoED25519, ssh.KeyAlgoECDSA256, ssh.KeyAlgoECDSA384, ssh.KeyAlgoECDSA521,
		ssh.KeyAlgoRSASHA512, ssh.KeyAlgoRSASHA256,
	}
)

// podPrivateKey derives the router's key for one pod from the seed and the pod name
func (p *Provider) podPrivateKey(podName string) ed25519.PrivateKey {
	mac := hmac.New(sha256.New, p.sshSeed)
	mac.Write([]byte(sshKeyLabel))
	mac.Write([]byte{0})
	mac.Write([]byte(podName))
	return ed25519.NewKeyFromSeed(mac.Sum(nil))
}

// podPublicKey is the pod key's authorized_keys line, the only part that leaves the router
func (p *Provider) podPublicKey(podName string) string {
	pub, err := ssh.NewPublicKey(p.podPrivateKey(podName).Public())
	if err != nil {
		// An ed25519 public key always converts
		panic(err)
	}
	return strings.TrimSpace(string(ssh.MarshalAuthorizedKey(pub))) + " " + keyComment
}

// authorizedKey is a key's type and base64, as Ref.Meta keeps a pinned host key
func authorizedKey(key ssh.PublicKey) string {
	return strings.TrimSpace(string(ssh.MarshalAuthorizedKey(key)))
}

// parseHostKey reads a pinned host key line, refusing a certificate and an oversized line
func parseHostKey(line string) (ssh.PublicKey, error) {
	if len(line) > maxHostKeyLine {
		return nil, errors.New("the pinned host key is too long")
	}
	key, _, _, rest, err := ssh.ParseAuthorizedKey([]byte(line))
	if err != nil || len(bytes.TrimSpace(rest)) > 0 {
		return nil, errors.New("the pinned host key is not one key line")
	}
	if _, cert := key.(*ssh.Certificate); cert || len(key.Marshal()) > maxHostKeyBytes {
		return nil, errors.New("the pinned host key is not a plain key")
	}
	return key, nil
}

// hostCheck checks the host key a server shows, against the pin when there is one
type hostCheck struct {
	pin ssh.PublicKey
	// seen is the key the server showed once it passed, changed marks a key other than the pin
	seen    ssh.PublicKey
	changed bool
}

func (h *hostCheck) verify(_ string, _ net.Addr, key ssh.PublicKey) error {
	if _, cert := key.(*ssh.Certificate); cert {
		return errors.New("the pod's sshd showed a host certificate")
	}
	if len(key.Marshal()) > maxHostKeyBytes {
		return errors.New("the pod's sshd showed an oversized host key")
	}
	if h.pin != nil && !bytes.Equal(key.Marshal(), h.pin.Marshal()) {
		h.changed = true
		return errHostKeyChanged
	}
	h.seen = key
	return nil
}

// algorithms are the host key algorithms to ask for, only the pin's own once there is one
func (h *hostCheck) algorithms() []string {
	switch {
	case h.pin == nil:
		return plainHostKeyAlgos
	case h.pin.Type() == ssh.KeyAlgoRSA:
		return []string{ssh.KeyAlgoRSASHA512, ssh.KeyAlgoRSASHA256}
	}
	return []string{h.pin.Type()}
}

// dialPod logs in to a pod's sshd as root with the pod's key, within the dial limit
func (p *Provider) dialPod(
	ctx context.Context, name, addr string, h *hostCheck,
) (*ssh.Client, error) {
	signer, err := ssh.NewSignerFromKey(p.podPrivateKey(name))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, p.ssh.dial)
	defer cancel()
	raw, err := p.dial(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial the pod's sshd: %w", err)
	}
	// The handshake reads an untrusted server, so the deadline closes the socket under it
	stop := context.AfterFunc(ctx, func() { _ = raw.Close() })
	conn, chans, reqs, err := ssh.NewClientConn(raw, addr, &ssh.ClientConfig{
		User: sshUser, Auth: []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: h.verify, HostKeyAlgorithms: h.algorithms(),
	})
	if !stop() {
		if err == nil {
			_ = conn.Close()
		}
		return nil, fmt.Errorf("log in to the pod's sshd: %w", context.Cause(ctx))
	}
	switch {
	case h.changed:
		return nil, errHostKeyChanged
	case err != nil && h.seen != nil && strings.Contains(err.Error(), "unable to authenticate"):
		return nil, errKeyRefused
	case err != nil:
		return nil, fmt.Errorf("log in to the pod's sshd: %w", err)
	}
	return ssh.NewClient(conn, chans, reqs), nil
}

// pinHostKey logs in until the pod's sshd takes our key, pinning its host key if none is yet
func (p *Provider) pinHostKey(ctx context.Context, ref provider.Ref, addr string) (string, error) {
	id := ref.ID
	name, err := p.podNameOf(ctx, ref)
	if err != nil {
		return "", err
	}
	pin, err := p.knownPin(ref, name, addr)
	if err != nil {
		return "", err
	}
	last := errors.New("not dialled yet")
	for {
		h := &hostCheck{pin: pin}
		client, err := p.dialPod(ctx, name, addr, h)
		if err == nil {
			p.adopt(id, name, addr, h.seen, client)
			return authorizedKey(h.seen), nil
		}
		if errors.Is(err, errHostKeyChanged) {
			return "", p.sshError(ctx, id, err)
		}
		if ctx.Err() == nil {
			last = err
		}
		d, gerr := p.getPod(ctx, id)
		if e, ok := provider.AsError(gerr); ok && e.Code == provider.CodeNotFound {
			return "", gerr
		}
		if gerr == nil {
			if _, end := startOf(d); end != nil {
				return "", end
			}
		}
		if !pause(ctx, p.pollEvery) {
			return "", fmt.Errorf("lium pod %s let no SSH login in: %w, last %w", id, ctx.Err(),
				last)
		}
	}
}

// knownPin is the host key the ref pinned, else the one this process pinned at the same place
func (p *Provider) knownPin(ref provider.Ref, name, addr string) (ssh.PublicKey, error) {
	if line := ref.Meta[provider.MetaHostKey]; line != "" {
		key, err := parseHostKey(line)
		if err != nil {
			return nil, &provider.Error{Code: provider.CodeInternal,
				Err: fmt.Errorf("lium pod %s: %w", ref.ID, err)}
		}
		return key, nil
	}
	p.smu.Lock()
	defer p.smu.Unlock()
	if pl := p.pools[ref.ID]; pl != nil && pl.name == name && pl.addr == addr {
		return pl.pin, nil
	}
	return nil, nil
}

// podNameOf is the pod name the ref keeps, else the one Lium answers
func (p *Provider) podNameOf(ctx context.Context, ref provider.Ref) (string, error) {
	if name := ref.Meta[metaPodName]; name != "" {
		return name, nil
	}
	d, err := p.getPod(ctx, ref.ID)
	if err != nil {
		return "", err
	}
	if d.Name == "" {
		return "", &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("lium pod %s has no name", ref.ID)}
	}
	return d.Name, nil
}

// sshError classifies a failed login or session, not_found once Lium no longer has the pod
func (p *Provider) sshError(ctx context.Context, id string, err error) error {
	if e, ok := provider.AsError(err); ok {
		return e
	}
	if ctx.Err() == nil {
		rctx, cancel := context.WithTimeout(ctx, refuseTimeout)
		d, gerr := p.getPod(rctx, id)
		cancel()
		e, _ := provider.AsError(gerr)
		if e != nil && e.Code == provider.CodeNotFound || gerr == nil &&
			d.Status == statusDeleting {
			return &provider.Error{Code: provider.CodeNotFound,
				Err: fmt.Errorf("lium pod %s is gone: %w", id, err)}
		}
	}
	return &provider.Error{Code: provider.CodeInternal,
		Err: fmt.Errorf("ssh to lium pod %s: %w", id, err)}
}

// poolFor finds or makes the pod's pool, reading the pod once when the ref lacks its SSH entries
func (p *Provider) poolFor(ctx context.Context, ref provider.Ref) (*sshPool, error) {
	id := ref.ID
	p.smu.Lock()
	known := p.pools[id]
	p.smu.Unlock()
	name, addr, err := p.metaTarget(ref)
	if err != nil {
		return nil, err
	}
	if known != nil {
		name, addr = cmp.Or(name, known.name), cmp.Or(addr, known.addr)
	}
	if name == "" || addr == "" {
		if name, addr, err = p.readTarget(ctx, id, name, addr); err != nil {
			return nil, err
		}
	}
	pin, err := p.knownPin(ref, name, addr)
	if err != nil {
		return nil, err
	}
	if pin == nil {
		return nil, &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("lium pod %s has no pinned host key", id)}
	}
	return p.pool(id, name, addr, pin), nil
}

// metaTarget reads the pod name and the sshd's address from Ref.Meta, empty when it lacks them
func (p *Provider) metaTarget(ref provider.Ref) (string, string, error) {
	name, host, port := ref.Meta[metaPodName], ref.Meta[metaSSHHost], ref.Meta[metaSSHPort]
	if host == "" || port == "" {
		return name, "", nil
	}
	addr, err := p.sshAddr(host, port)
	if err != nil {
		return "", "", &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("lium pod %s: %w", ref.ID, err)}
	}
	return name, addr, nil
}

// sshAddr joins a public host and a port, refusing what netguard refuses
func (p *Provider) sshAddr(host, port string) (string, error) {
	ip, err := netip.ParseAddr(host)
	if err != nil || p.refuses(ip) {
		return "", fmt.Errorf("the SSH host %q is no public address", host)
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n >= 1<<16 {
		return "", fmt.Errorf("the SSH port %q is no port", port)
	}
	return net.JoinHostPort(ip.Unmap().String(), strconv.Itoa(n)), nil
}

// readTarget reads what the ref lacks of the pod name and the sshd's address from Lium
func (p *Provider) readTarget(ctx context.Context, id, name, addr string) (string, string, error) {
	d, err := p.getPod(ctx, id)
	if err != nil {
		return "", "", err
	}
	if d.Status == statusDeleting {
		return "", "", &provider.Error{Code: provider.CodeNotFound,
			Err: fmt.Errorf("lium pod %s is being deleted", id)}
	}
	name = cmp.Or(name, d.Name)
	if addr == "" {
		port, ok := d.sshPortOf()
		if !ok {
			return "", "", &provider.Error{Code: provider.CodeInternal,
				Err: fmt.Errorf("lium pod %s has no SSH port", id)}
		}
		if addr, err = p.sshAddr(d.Executor.Address, strconv.Itoa(port)); err != nil {
			return "", "", &provider.Error{Code: provider.CodeInternal,
				Err: fmt.Errorf("lium pod %s: %w", id, err)}
		}
	}
	if name == "" {
		return "", "", &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("lium pod %s has no name", id)}
	}
	return name, addr, nil
}

// pool returns the pod's pool for this name, address and pin, replacing one made for others
func (p *Provider) pool(id, name, addr string, pin ssh.PublicKey) *sshPool {
	p.smu.Lock()
	old := p.pools[id]
	if old != nil && old.serves(name, addr, pin) {
		p.smu.Unlock()
		return old
	}
	pl := &sshPool{p: p, name: name, addr: addr, pin: pin, freed: make(chan struct{})}
	p.pools[id] = pl
	p.smu.Unlock()
	if old != nil {
		old.close()
	}
	return pl
}

// adopt keeps the connection that pinned the host key in the pod's pool
func (p *Provider) adopt(id, name, addr string, pin ssh.PublicKey, client *ssh.Client) {
	pl := p.pool(id, name, addr, pin)
	pl.mu.Lock()
	defer pl.mu.Unlock()
	if pl.closed {
		_ = client.Close()
		return
	}
	pl.idleLocked(pl.addLocked(client))
}

// dropPool closes the pod's connections, as Destroy does
func (p *Provider) dropPool(id string) {
	p.smu.Lock()
	pl := p.pools[id]
	delete(p.pools, id)
	p.smu.Unlock()
	if pl != nil {
		pl.close()
	}
}

// closePools closes every pod's connections
func (p *Provider) closePools() {
	p.smu.Lock()
	pools := p.pools
	p.pools = map[string]*sshPool{}
	p.smu.Unlock()
	for _, pl := range pools {
		pl.close()
	}
}

// sshPool keeps one pod's connections, opening another only when each open one is full
type sshPool struct {
	p *Provider
	// name, addr and pin are the pod name its key derives from, the sshd's address and host key
	name, addr string
	pin        ssh.PublicKey

	mu      sync.Mutex
	conns   []*sshConn
	dialing int
	closed  bool
	// freed is closed and replaced each time a session or a connection slot frees
	freed chan struct{}
}

// sshConn is one pooled connection and the sessions it holds
type sshConn struct {
	client   *ssh.Client
	sessions int
	// room is the most sessions it takes, below perConn once the server refused one
	room int
	idle *time.Timer
	gone bool
}

func (pl *sshPool) serves(name, addr string, pin ssh.PublicKey) bool {
	return pl.name == name && pl.addr == addr && bytes.Equal(pl.pin.Marshal(), pin.Marshal())
}

// session opens a session on a pooled connection, redialling once when the connection broke
func (pl *sshPool) session(ctx context.Context) (*ssh.Session, *sshConn, error) {
	var last error
	redialled := false
	// Each refusal lowers one connection's room, so the tries end
	for range pl.p.ssh.conns*pl.p.ssh.perConn + 1 {
		c, err := pl.take(ctx)
		if err != nil {
			return nil, nil, err
		}
		stop := pl.guard(ctx, c)
		sess, err := c.client.NewSession()
		if !stop() {
			if err == nil {
				_ = sess.Close()
			}
			pl.release(c)
			return nil, nil, fmt.Errorf("open an SSH session: %w", context.Cause(ctx))
		}
		if err == nil {
			return sess, c, nil
		}
		last = fmt.Errorf("open an SSH session: %w", err)
		if refused := new(ssh.OpenChannelError); errors.As(err, &refused) {
			pl.refused(c)
			continue
		}
		pl.release(c)
		pl.drop(c)
		if redialled {
			break
		}
		redialled = true
	}
	return nil, nil, last
}

// guard drops c when ctx ends while the call it covers still waits a stall period later
func (pl *sshPool) guard(ctx context.Context, c *sshConn) (stop func() bool) {
	returned := make(chan struct{})
	after := context.AfterFunc(ctx, func() {
		t := time.NewTimer(pl.p.ssh.stall)
		defer t.Stop()
		select {
		case <-returned:
		case <-t.C:
			pl.drop(c)
		}
	})
	return func() bool {
		close(returned)
		return after()
	}
}

// take holds a session slot, a burst waiting on one dial at a time once every connection is full
func (pl *sshPool) take(ctx context.Context) (*sshConn, error) {
	for {
		pl.mu.Lock()
		if pl.closed {
			pl.mu.Unlock()
			return nil, errPoolClosed
		}
		for _, c := range pl.conns {
			if c.sessions < c.room {
				c.sessions++
				if c.idle != nil {
					c.idle.Stop()
				}
				pl.mu.Unlock()
				return c, nil
			}
		}
		if pl.dialing == 0 && len(pl.conns) < pl.p.ssh.conns {
			pl.dialing++
			pl.mu.Unlock()
			return pl.dialOne(ctx)
		}
		freed := pl.freed
		pl.mu.Unlock()
		select {
		case <-freed:
		case <-ctx.Done():
			return nil, fmt.Errorf("every SSH session to the pod is in use: %w",
				context.Cause(ctx))
		}
	}
}

// dialOne opens one more connection and holds a session slot on it
func (pl *sshPool) dialOne(ctx context.Context) (*sshConn, error) {
	client, err := pl.p.dialPod(ctx, pl.name, pl.addr, &hostCheck{pin: pl.pin})
	pl.mu.Lock()
	defer pl.mu.Unlock()
	pl.dialing--
	pl.wakeLocked()
	if err != nil {
		return nil, err
	}
	if pl.closed {
		_ = client.Close()
		return nil, errPoolClosed
	}
	c := pl.addLocked(client)
	c.sessions = 1
	return c, nil
}

// addLocked keeps a connection, watching it until it closes
func (pl *sshPool) addLocked(client *ssh.Client) *sshConn {
	c := &sshConn{client: client, room: pl.p.ssh.perConn}
	pl.conns = append(pl.conns, c)
	go pl.watch(c)
	return c
}

// watch sends keepalives until the connection closes, dropping it at the first one unanswered
func (pl *sshPool) watch(c *sshConn) {
	ended := make(chan struct{})
	go func() {
		_ = c.client.Wait()
		close(ended)
		pl.drop(c)
	}()
	t := time.NewTicker(pl.p.ssh.keepalive)
	defer t.Stop()
	for {
		select {
		case <-ended:
			return
		case <-t.C:
			if !pl.alive(c, ended) {
				pl.drop(c)
				return
			}
		}
	}
}

// alive sends a keepalive and reports whether the server answered within one interval
func (pl *sshPool) alive(c *sshConn, ended <-chan struct{}) bool {
	answered := make(chan error, 1)
	go func() {
		_, _, err := c.client.SendRequest(keepaliveRequest, true, nil)
		answered <- err
	}()
	t := time.NewTimer(pl.p.ssh.keepalive)
	defer t.Stop()
	select {
	case err := <-answered:
		return err == nil
	case <-ended:
		return false
	case <-t.C:
		return false
	}
}

// release frees a session slot, starting the idle clock of a connection left with none
func (pl *sshPool) release(c *sshConn) {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	c.sessions--
	if c.sessions == 0 && !c.gone {
		pl.idleLocked(c)
	}
	pl.wakeLocked()
}

// refused frees the slot of a session the server would not open, taking no more on c
func (pl *sshPool) refused(c *sshConn) {
	pl.mu.Lock()
	c.sessions--
	c.room = c.sessions
	empty := c.room == 0
	pl.wakeLocked()
	pl.mu.Unlock()
	// A connection that takes no session at all is of no use
	if empty {
		pl.drop(c)
	}
}

// idleLocked closes c after the idle time unless a session takes it first
func (pl *sshPool) idleLocked(c *sshConn) {
	if c.idle != nil {
		c.idle.Reset(pl.p.ssh.idle)
		return
	}
	c.idle = time.AfterFunc(pl.p.ssh.idle, func() {
		pl.mu.Lock()
		idle := c.sessions == 0 && !c.gone
		if idle {
			pl.forgetLocked(c)
		}
		pl.mu.Unlock()
		if idle {
			_ = c.client.Close()
		}
	})
}

// drop closes a broken connection and takes it out of the pool
func (pl *sshPool) drop(c *sshConn) {
	pl.mu.Lock()
	pl.forgetLocked(c)
	pl.mu.Unlock()
	_ = c.client.Close()
}

func (pl *sshPool) forgetLocked(c *sshConn) {
	if c.gone {
		return
	}
	c.gone = true
	pl.conns = slices.DeleteFunc(pl.conns, func(o *sshConn) bool { return o == c })
	if c.idle != nil {
		c.idle.Stop()
	}
	pl.wakeLocked()
}

func (pl *sshPool) wakeLocked() {
	close(pl.freed)
	pl.freed = make(chan struct{})
}

// close closes every connection and refuses new sessions
func (pl *sshPool) close() {
	pl.mu.Lock()
	pl.closed = true
	conns := slices.Clone(pl.conns)
	for _, c := range conns {
		pl.forgetLocked(c)
	}
	pl.mu.Unlock()
	for _, c := range conns {
		_ = c.client.Close()
	}
}
