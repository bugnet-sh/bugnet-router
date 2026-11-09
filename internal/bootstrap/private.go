package bootstrap

import (
	"archive/tar"
	"cmp"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/coder/websocket"

	"github.com/bugnet-sh/bugnet-router/internal/jobs"
	"github.com/bugnet-sh/bugnet-router/internal/netguard"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

const (
	tunnelProtocol = "bugnet-tunnel.v1"
	codeloadURL    = "https://codeload.github.com/"
	// maxFrame is the largest WebSocket message the proxy sends
	maxFrame = 1 << 20
	// wsChunk is the most bytes one WebSocket message carries toward the proxy
	wsChunk = 32 << 10
	// countEvery is how many streamed bytes pass between two counts against the budget
	countEvery = 1 << 20
	// fetchTimeout bounds one private fetch from the tunnel to the last byte spooled
	fetchTimeout = 15 * time.Minute
	dialTimeout  = 10 * time.Second
	// unpackScript extracts archive $1 into $2 without its top directory, then deletes it
	unpackScript = `command -v tar >/dev/null 2>&1 || exit 127
mkdir -p "$2" && tar -xzf "$1" -C "$2" --strip-components=1
rc=$?
rm -f "$1"
exit $rc`
)

// Upstream names the hosts a private fetch talks to, each zero field taking its production value
type Upstream struct {
	// APIHost is the host the tunnel's inner TLS and request name
	APIHost string
	// CodeloadURL is the only place a redirect may lead
	CodeloadURL string
	// Guard dials the redirect's target
	Guard netguard.Dialer
	// CodeloadRoots verify the redirect's target, the system roots when nil
	CodeloadRoots *x509.CertPool
}

func (u Upstream) orDefaults() Upstream {
	u.APIHost = cmp.Or(u.APIHost, githubHost)
	u.CodeloadURL = cmp.Or(u.CodeloadURL, codeloadURL)
	return u
}

// noRedirect hands a redirect to the caller, which decides whether it may be followed
func noRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

// failure is a fetch that ended with a stable reason
type failure struct {
	reason string
	err    error
}

func (f *failure) Error() string { return f.reason + ": " + f.err.Error() }
func (f *failure) Unwrap() error { return f.err }

func failed(reason string, format string, args ...any) *failure {
	return &failure{reason: reason, err: fmt.Errorf(format, args...)}
}

// startPrivate starts the private fetch in this replica when every guard of CP 4.9 passes
func (s *Service) startPrivate(
	ctx context.Context, p store.Placement, prov provider.Provider, sp sourceParts,
	row store.Bootstrap, secret store.Secret,
) (*routerv1.Bootstrap, error) {
	d := s.d
	if d.FetchToken == "" || d.EgressHost == "" || d.CAFile == "" || d.Fetch.EgressAddr == "" {
		return nil, ErrNotConfigured
	}
	spent := func(reason string) (*routerv1.Bootstrap, error) {
		s.mark(ctx, p, row, store.Bootstrap{
			State: store.BootstrapFailed, Reason: reason, Path: s.shown(sp)})
		return s.reread(ctx, p)
	}
	if row.PrivateAttempts >= d.Settings.MaxPrivateAttempts {
		return spent(ReasonAttemptsSpent)
	}
	left, err := d.Counter.Left(ctx, p)
	if err != nil {
		return nil, err
	}
	if left == 0 {
		return spent(ReasonBudgetSpent)
	}
	if s.running.Add(1) > int64(d.Settings.MaxConcurrent) {
		s.running.Add(-1)
		return nil, ErrRateLimited
	}
	to := row
	to.State, to.JobID, to.Reason, to.Revision = store.BootstrapPrivate, "", "", ""
	to.PrivateAttempts++
	to.Path = s.shown(sp)
	claimed, err := d.Store.ClaimBootstrap(ctx, p.ID, row, to)
	if err != nil || !claimed {
		s.running.Add(-1)
		if err != nil {
			return nil, err
		}
		return s.reread(ctx, p)
	}
	s.wg.Go(func() {
		defer s.running.Add(-1)
		s.runPrivate(p, prov, sp, secret, to)
	})
	return s.reread(ctx, p)
}

// runPrivate fetches the archive, writes it into the sandbox and starts the job that unpacks it
func (s *Service) runPrivate(
	p store.Placement, prov provider.Provider, sp sourceParts, secret store.Secret,
	at store.Bootstrap,
) {
	ctx, cancel := context.WithTimeout(s.life, fetchTimeout)
	defer cancel()
	stop := make(chan struct{})
	defer close(stop)
	go s.beat(ctx, p, at, stop)
	err := s.deliver(ctx, p, prov, sp, secret, at)
	if err == nil {
		return
	}
	reason := ReasonFetchFailed
	if f, ok := errors.AsType[*failure](err); ok {
		reason = f.reason
	}
	s.d.Log.Warn("bootstrap: private fetch failed", "placement", p.ID, "reason", reason,
		"err", err)
	fctx, done := context.WithTimeout(context.WithoutCancel(ctx), dialTimeout)
	defer done()
	to := store.Bootstrap{State: store.BootstrapFailed, Reason: reason}
	if _, uerr := s.d.Store.UpdateBootstrap(fctx, p.ID, at, to); uerr != nil {
		s.d.Log.Warn("bootstrap: record a failed fetch", "placement", p.ID, "err", uerr)
	}
}

// beat renews the row's heartbeat until stop closes or another fetch holds the row
func (s *Service) beat(
	ctx context.Context, p store.Placement, at store.Bootstrap, stop <-chan struct{},
) {
	t := time.NewTicker(beatEvery)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-t.C:
			held, err := s.d.Store.BeatBootstrap(ctx, p.ID, at.PrivateAttempts)
			if err == nil && !held {
				return
			}
		}
	}
}

// archive is a tarball spooled to a temporary file
type archive struct {
	file     *os.File
	size     int64
	sum      []byte
	revision string
}

func (a *archive) close() {
	_ = a.file.Close()
	_ = os.Remove(a.file.Name())
}

// deliver runs the whole private fetch and hands the unpack job its archive
func (s *Service) deliver(
	ctx context.Context, p store.Placement, prov provider.Provider, sp sourceParts,
	secret store.Secret, at store.Bootstrap,
) error {
	ph, err := s.d.Keys.Placeholder(cmp.Or(p.KeyID, s.d.Keys.Current().ID), p.ID, secret.Name)
	if err != nil {
		return failed(ReasonFetchFailed, "derive the placeholder: %w", err)
	}
	arc, err := s.download(ctx, p, sp, ph)
	if err != nil {
		return err
	}
	defer arc.close()
	h := home(prov, p)
	remote := path.Join(h, archiveDir, archiveName)
	if err := s.upload(ctx, p, prov, arc, remote); err != nil {
		return failed(ReasonFetchFailed, "write the archive into the sandbox: %w", err)
	}
	argv := []string{"sh", "-c", unpackScript, "sh", path.Join(archiveDir, archiveName), s.dest(sp)}
	shown := jobs.WithDisplay(ctx, []string{"unpack", sp.owner + "/" + sp.repo})
	res, err := s.d.Jobs.Exec(shown, p, argv, h, nil,
		reservedKey+"p"+strconv.Itoa(at.PrivateAttempts), startWait, jobs.Until{})
	if err != nil {
		return failed(ReasonCloneFailed, "start the unpack job: %w", err)
	}
	to := store.Bootstrap{State: store.BootstrapPrivate, JobID: res.JobID, Revision: arc.revision}
	if _, err := s.d.Store.UpdateBootstrap(ctx, p.ID, at, to); err != nil {
		return err
	}
	return nil
}

// download asks the proxy's tunnel for the repository's tarball and spools it, counting bytes
func (s *Service) download(
	ctx context.Context, p store.Placement, sp sourceParts, placeholder string,
) (*archive, error) {
	api, closeTunnel, err := s.tunnel(ctx, p)
	if err != nil {
		return nil, failed(ReasonFetchFailed, "open the tunnel: %w", err)
	}
	defer closeTunnel()
	target := "https://" + s.d.APIHost + "/repos/" + url.PathEscape(sp.owner) + "/" +
		url.PathEscape(sp.repo) + "/tarball"
	if t := sp.target(); t != "" {
		target += "/" + escapeRef(t)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, failed(ReasonFetchFailed, "build the request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+placeholder)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("User-Agent", "bugnet-router")
	resp, err := api.Do(req)
	if err != nil {
		return nil, failed(ReasonFetchFailed, "ask for the tarball: %w", err)
	}
	_ = resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther,
		http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
	default:
		return nil, failed(ReasonFetchFailed, "GitHub answered %d", resp.StatusCode)
	}
	next, err := s.redirect(resp)
	if err != nil {
		return nil, err
	}
	return s.spool(ctx, p, next)
}

// escapeRef path-escapes each segment of a ref
func escapeRef(ref string) string {
	parts := strings.Split(ref, "/")
	for i, x := range parts {
		parts[i] = url.PathEscape(x)
	}
	return strings.Join(parts, "/")
}

// redirect is the one place the answer may lead, the codeload host and nowhere else
func (s *Service) redirect(resp *http.Response) (*url.URL, error) {
	allowed, err := url.Parse(s.d.CodeloadURL)
	if err != nil {
		return nil, failed(ReasonFetchFailed, "parse the redirect setting: %w", err)
	}
	next, err := resp.Request.URL.Parse(resp.Header.Get("Location"))
	if err != nil || next.Scheme != allowed.Scheme || next.Host != allowed.Host ||
		next.User != nil {
		return nil, failed(ReasonFetchFailed, "GitHub redirected somewhere else")
	}
	return next, nil
}

// spool streams the archive at u into a temporary file, counting every byte
func (s *Service) spool(ctx context.Context, p store.Placement, u *url.URL) (*archive, error) {
	cl := &http.Client{
		Transport: &http.Transport{
			DialContext:     s.d.Guard.DialContext,
			TLSClientConfig: &tls.Config{RootCAs: s.d.CodeloadRoots, MinVersion: tls.VersionTLS12},
		},
		CheckRedirect: noRedirect,
	}
	defer cl.CloseIdleConnections()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, failed(ReasonFetchFailed, "build the download request: %w", err)
	}
	req.Header.Set("User-Agent", "bugnet-router")
	resp, err := cl.Do(req)
	if err != nil {
		return nil, failed(ReasonFetchFailed, "download the archive: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, failed(ReasonFetchFailed, "the download answered %d", resp.StatusCode)
	}
	if resp.ContentLength > s.d.Fetch.MaxBytes {
		return nil, failed(ReasonTooLarge, "the archive is %d bytes", resp.ContentLength)
	}
	f, err := os.CreateTemp("", "bugnet-src-*")
	if err != nil {
		return nil, failed(ReasonFetchFailed, "create the spool file: %w", err)
	}
	a := &archive{file: f}
	if err := s.copyCounted(ctx, p, a, resp.Body); err != nil {
		a.close()
		return nil, err
	}
	a.revision = revisionOf(f)
	return a, nil
}

// copyCounted copies body into a, adding every byte to the placement's egress counter
func (s *Service) copyCounted(
	ctx context.Context, p store.Placement, a *archive, body io.Reader,
) error {
	sum := sha256.New()
	buf := make([]byte, 64<<10)
	var pending uint64
	count := func(final bool) error {
		if pending == 0 {
			return nil
		}
		n := pending
		pending = 0
		left, err := s.d.Counter.Add(ctx, p, n)
		switch {
		case err != nil:
			return failed(ReasonFetchFailed, "count the bytes: %w", err)
		case left == 0 && !final:
			return failed(ReasonBudgetSpent, "the egress budget ran out")
		}
		return nil
	}
	for {
		n, rerr := body.Read(buf)
		if n > 0 {
			if _, err := a.file.Write(buf[:n]); err != nil {
				return failed(ReasonFetchFailed, "spool the archive: %w", err)
			}
			_, _ = sum.Write(buf[:n])
			a.size += int64(n)
			pending += uint64(n)
			if a.size > s.d.Fetch.MaxBytes {
				_ = count(true)
				return failed(ReasonTooLarge, "the archive is over %d bytes", s.d.Fetch.MaxBytes)
			}
			if pending >= countEvery {
				if err := count(false); err != nil {
					return err
				}
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			_ = count(true)
			return failed(ReasonFetchFailed, "read the archive: %w", rerr)
		}
	}
	a.sum = sum.Sum(nil)
	return count(true)
}

// revisionOf reads the commit from the archive's first entries, empty when they name none
func revisionOf(f *os.File) string {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return ""
	}
	zr, err := gzip.NewReader(f)
	if err != nil {
		return ""
	}
	tr := tar.NewReader(zr)
	for range 3 {
		h, err := tr.Next()
		if err != nil {
			return ""
		}
		if h.Typeflag == tar.TypeXGlobalHeader {
			if c := h.PAXRecords["comment"]; shaRE.MatchString(c) {
				return c[:7]
			}
			continue
		}
		top, _, _ := strings.Cut(strings.TrimPrefix(h.Name, "./"), "/")
		if i := strings.LastIndexByte(top, '-'); i >= 0 && len(top)-i-1 >= 7 {
			return top[i+1:]
		}
		return ""
	}
	return ""
}

// upload writes the spooled archive into the sandbox at remote
func (s *Service) upload(
	ctx context.Context, p store.Placement, prov provider.Provider, a *archive, remote string,
) error {
	if _, err := a.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	w, err := prov.OpenWriter(ctx, *p.ProviderRef, remote,
		provider.WriteSpec{Size: a.size, Mode: 0o600, SHA256: a.sum})
	if err != nil {
		return err
	}
	if _, err := io.Copy(w, a.file); err != nil {
		_ = w.Abort()
		return err
	}
	if _, err := w.Commit(ctx, a.sum); err != nil {
		_ = w.Abort()
		return err
	}
	return nil
}

/* The tunnel */

// tunnel opens the proxy's tunnel as the fetcher and returns an HTTP client whose one
// connection runs TLS to the API host inside it
func (s *Service) tunnel(ctx context.Context, p store.Placement) (*http.Client, func(), error) {
	roots, err := s.outerRoots()
	if err != nil {
		return nil, nil, err
	}
	inner, err := s.caRoots()
	if err != nil {
		return nil, nil, err
	}
	raw, err := (&net.Dialer{Timeout: dialTimeout}).DialContext(ctx, "tcp", s.d.Fetch.EgressAddr)
	if err != nil {
		return nil, nil, err
	}
	if err := writeProxyHeader(raw); err != nil {
		_ = raw.Close()
		return nil, nil, err
	}
	outer := tls.Client(raw, &tls.Config{
		ServerName: s.d.EgressHost, RootCAs: roots, NextProtos: []string{"http/1.1"},
		MinVersion: tls.VersionTLS12,
	})
	if err := outer.HandshakeContext(ctx); err != nil {
		_ = raw.Close()
		return nil, nil, fmt.Errorf("outer TLS: %w", err)
	}
	token := s.d.FetchToken
	if !strings.HasPrefix(token, "bnf_") {
		token = "bnf_" + token
	}
	ws, _, err := websocket.Dial(ctx, "https://"+s.d.EgressHost+"/v1/tunnel",
		&websocket.DialOptions{
			HTTPClient: &http.Client{Transport: &http.Transport{
				DialTLSContext: handOver(outer),
			}},
			HTTPHeader: http.Header{
				"Authorization":    {"Bearer " + token},
				"Bugnet-Placement": {p.ID.String()},
			},
			Subprotocols:    []string{tunnelProtocol},
			CompressionMode: websocket.CompressionDisabled,
		})
	if err != nil {
		_ = raw.Close()
		return nil, nil, fmt.Errorf("upgrade: %w", err)
	}
	ws.SetReadLimit(maxFrame)
	wctx, stop := context.WithCancel(ctx)
	conn := wsNetConn(wctx, ws)
	in := tls.Client(conn, &tls.Config{
		ServerName: s.d.APIHost, RootCAs: inner, NextProtos: []string{"http/1.1"},
		MinVersion: tls.VersionTLS12,
	})
	if err := in.HandshakeContext(ctx); err != nil {
		stop()
		_ = raw.Close()
		return nil, nil, fmt.Errorf("inner TLS: %w", err)
	}
	api := &http.Client{
		Transport:     &http.Transport{DialTLSContext: handOver(in)},
		CheckRedirect: noRedirect,
	}
	return api, func() { api.CloseIdleConnections(); stop(); _ = raw.Close() }, nil
}

// handOver gives a transport its one prepared connection and no other
func handOver(c net.Conn) func(context.Context, string, string) (net.Conn, error) {
	used := false
	return func(context.Context, string, string) (net.Conn, error) {
		if used {
			return nil, errors.New("bootstrap: the tunnel carries one connection")
		}
		used = true
		return c, nil
	}
}

// outerRoots are the roots the proxy's outer certificate is checked against, nil for the system's
func (s *Service) outerRoots() (*x509.CertPool, error) {
	if s.d.Fetch.EgressRootsFile == "" {
		return nil, nil //nolint:nilnil // nil roots mean the system's
	}
	return poolOf(s.d.Fetch.EgressRootsFile)
}

// caRoots hold the Bugnet CA that signs the inner certificates
func (s *Service) caRoots() (*x509.CertPool, error) { return poolOf(s.d.CAFile) }

func poolOf(file string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", file, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("%s holds no certificate", file)
	}
	return pool, nil
}

// proxySignature opens every PROXY protocol version 2 header
var proxySignature = []byte("\r\n\r\n\x00\r\nQUIT\n")

// writeProxyHeader sends a PROXY v2 header naming the connection's own addresses, which the
// proxy reads as the fetcher's loopback source
func writeProxyHeader(c net.Conn) error {
	src, err := netip.ParseAddrPort(c.LocalAddr().String())
	if err != nil {
		return err
	}
	dst, err := netip.ParseAddrPort(c.RemoteAddr().String())
	if err != nil {
		return err
	}
	h := append([]byte(nil), proxySignature...)
	family, size := byte(0x11), uint16(12)
	if src.Addr().Unmap().Is6() || dst.Addr().Unmap().Is6() {
		family, size = 0x21, 36
	}
	h = append(h, 0x21, family)
	h = binary.BigEndian.AppendUint16(h, size)
	if size == 12 {
		s, d := src.Addr().Unmap().As4(), dst.Addr().Unmap().As4()
		h = append(h, s[:]...)
		h = append(h, d[:]...)
	} else {
		s, d := src.Addr().As16(), dst.Addr().As16()
		h = append(h, s[:]...)
		h = append(h, d[:]...)
	}
	h = binary.BigEndian.AppendUint16(h, src.Port())
	h = binary.BigEndian.AppendUint16(h, dst.Port())
	_, err = c.Write(h)
	return err
}

// wsNetConn is the WebSocket as a net.Conn of binary messages with working deadlines
func wsNetConn(ctx context.Context, ws *websocket.Conn) net.Conn {
	// The library's own net.Conn closes the socket on a past deadline, which net/http sets
	near, far := net.Pipe()
	go func() {
		defer func() { _ = far.Close() }()
		for {
			_, r, err := ws.Reader(ctx)
			if err != nil {
				return
			}
			if _, err := io.Copy(far, r); err != nil {
				return
			}
		}
	}()
	go func() {
		defer func() { _ = ws.Close(websocket.StatusNormalClosure, "") }()
		buf := make([]byte, wsChunk)
		for {
			n, err := far.Read(buf)
			if n > 0 {
				if werr := ws.Write(ctx, websocket.MessageBinary, buf[:n]); werr != nil {
					_ = far.Close()
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()
	go func() {
		<-ctx.Done()
		_ = ws.CloseNow()
		_ = far.Close()
	}()
	return near
}
