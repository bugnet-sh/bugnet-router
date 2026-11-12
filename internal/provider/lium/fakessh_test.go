package lium

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/bugnet-sh/bugnet-router/internal/provider/shell"
)

const (
	// fakeSetsidEnv makes the test binary act as setsid -w, which macOS lacks
	fakeSetsidEnv = "LIUM_FAKE_SETSID"
	// fakeMaxSessions is OpenSSH's MaxSessions default
	fakeMaxSessions = 10
)

func TestMain(m *testing.M) {
	if os.Getenv(fakeSetsidEnv) == "1" {
		os.Exit(fakeSetsid(os.Args[1:]))
	}
	os.Exit(m.Run())
}

// fakeSetsid starts a session and runs argv in it, as util-linux setsid -w does
func fakeSetsid(args []string) int {
	if len(args) > 0 && args[0] == "-w" {
		args = args[1:]
	}
	if len(args) == 0 {
		return 1
	}
	env := slices.DeleteFunc(os.Environ(), func(kv string) bool {
		return strings.HasPrefix(kv, fakeSetsidEnv+"=")
	})
	path, err := exec.LookPath(args[0])
	if err != nil {
		return 127
	}
	if _, err := syscall.Setsid(); err == nil {
		_ = syscall.Exec(path, args, env)
		return 126
	}
	// A group leader cannot start a session, so setsid forks and waits as -w asks
	cmd := exec.Command(path)
	cmd.Args, cmd.Env = args, env
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	err = cmd.Run()
	var ee *exec.ExitError
	switch {
	case err == nil:
		return 0
	case errors.As(err, &ee) && ee.ExitCode() >= 0:
		return ee.ExitCode()
	}
	return 1
}

// fakeShell is where the fake's pods run commands, the host's own shell standing in for the pod's
type fakeShell struct {
	// home is the login directory, execDir the adapter's pid files, setsid the helper script
	home, execDir, setsid string
}

func newFakeShell(t *testing.T) fakeShell {
	t.Helper()
	dir := t.TempDir()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	sh := fakeShell{
		home: filepath.Join(dir, "root"), execDir: filepath.Join(dir, "exec"),
		setsid: filepath.Join(dir, "bin", "setsid"),
	}
	for _, d := range []string{sh.home, filepath.Dir(sh.setsid)} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	script := "#!/bin/sh\n" + fakeSetsidEnv + "=1 exec " + shell.Quote(exe) + " \"$@\"\n"
	if err := os.WriteFile(sh.setsid, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return sh
}

// env is what the pod's login shell starts with
func (sh fakeShell) env() []string {
	return []string{"PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin", "HOME=" + sh.home,
		"LANG=C"}
}

// fakeSSHD is one pod's sshd on a loopback port, which the adapter reaches through dialSSH
type fakeSSHD struct {
	f   *fakeLium
	pod string
	ln  net.Listener

	mu      sync.Mutex
	hostKey ssh.Signer
	// conns counts the sessions open on each connection, running the commands not exited yet
	conns, running map[*ssh.ServerConn]int
	// sockets are the accepted connections, kept from before their handshake so a drop finds them
	sockets map[net.Conn]bool
	// shells are the login shells running commands, each the leader of its own session
	shells map[*os.Process]bool
	// accepted counts the connections that logged in, peak the most commands one ran at once
	accepted, peak int
	// silent leaves global requests such as keepalives unanswered
	silent bool
	// maxSessions is the sessions one connection takes, MaxSessions in sshd_config
	maxSessions int
	// holdOpens leaves every session open unanswered, as a hostile sshd may
	holdOpens bool
	// execGate holds each exec reply until it closes, the command already started
	execGate chan struct{}
	// hostile serves SFTP as a hostile pod would, the host's files when nil
	hostile *hostileFS
	// broken names the answer a raw sftp-server cuts short, none when empty
	broken string
	// sftpOut counts the bytes every SFTP session sent the adapter
	sftpOut atomic.Int64
	// done closes when the sshd closes
	done   chan struct{}
	closed bool
}

func newHostKey(t *testing.T) ssh.Signer {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	s, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// sshdLocked returns the pod's sshd, starting it on first use, with f.mu held
func (f *fakeLium) sshdLocked(pod string) *fakeSSHD {
	if s := f.sshds[pod]; s != nil {
		return s
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		f.t.Fatal(err)
	}
	s := &fakeSSHD{
		f: f, pod: pod, ln: ln, hostKey: newHostKey(f.t), conns: map[*ssh.ServerConn]int{},
		running: map[*ssh.ServerConn]int{}, shells: map[*os.Process]bool{},
		maxSessions: fakeMaxSessions, done: make(chan struct{}), sockets: map[net.Conn]bool{},
	}
	f.sshds[pod] = s
	go s.serve()
	return s
}

// sshd returns the pod's sshd, starting it on first use
func (f *fakeLium) sshd(pod string) *fakeSSHD {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sshdLocked(pod)
}

// dialSSH reaches the sshd of the pod at addr, refusing as a closed port does when none is there
func (f *fakeLium) dialSSH(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.dialed = append(f.dialed, addr)
	var s *fakeSSHD
	for _, p := range f.pods {
		if p.ip == host && strconv.Itoa(p.sshPort) == port && !f.sshDown {
			s = f.sshdLocked(p.id)
		}
	}
	f.mu.Unlock()
	if s == nil {
		return nil, &net.OpError{Op: "dial", Net: network, Err: syscall.ECONNREFUSED}
	}
	var d net.Dialer
	return d.DialContext(ctx, "tcp", s.ln.Addr().String())
}

// dials is how many times the adapter dialled any pod
func (f *fakeLium) dials() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.dialed)
}

// authorized reports a key the pod's authorized_keys holds
func (f *fakeLium) authorized(pod string, key ssh.PublicKey) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.pods[pod]
	if !ok {
		return false
	}
	for _, line := range p.keys {
		k, _, _, _, err := ssh.ParseAuthorizedKey([]byte(line))
		if err == nil && string(k.Marshal()) == string(key.Marshal()) {
			return true
		}
	}
	return false
}

// recordExec keeps a command line the pods ran, for the secret check
func (f *fakeLium) recordExec(line string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.execs = append(f.execs, line)
}

func (f *fakeLium) execLines() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.execs)
}

func (f *fakeLium) recordSignal(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.signals = append(f.signals, name)
}

func (f *fakeLium) signalsSent() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.signals)
}

// closeSSHDs ends every pod's sshd and kills what its commands left running
func (f *fakeLium) closeSSHDs() {
	f.mu.Lock()
	all := make([]*fakeSSHD, 0, len(f.sshds))
	for _, s := range f.sshds {
		all = append(all, s)
	}
	f.mu.Unlock()
	for _, s := range all {
		s.close()
	}
	pids, _ := filepath.Glob(filepath.Join(f.sh.execDir, "*.pid"))
	for _, file := range pids {
		raw, _ := os.ReadFile(file)
		if pid, err := strconv.Atoi(strings.TrimSpace(string(raw))); err == nil && pid > 1 {
			_ = syscall.Kill(-pid, syscall.SIGKILL)
		}
	}
}

// hostPublicKey is the pod's host key as an authorized_keys line
func (s *fakeSSHD) hostPublicKey() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.TrimSpace(string(ssh.MarshalAuthorizedKey(s.hostKey.PublicKey())))
}

// rekey gives the sshd a new host key for the connections that follow
func (s *fakeSSHD) rekey(t *testing.T) {
	key := newHostKey(t)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hostKey = key
}

// dropConns breaks every connection, as a network fault would
func (s *fakeSSHD) dropConns() {
	s.mu.Lock()
	conns := make([]net.Conn, 0, len(s.sockets))
	for c := range s.sockets {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

func (s *fakeSSHD) setMaxSessions(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxSessions = n
}

func (s *fakeSSHD) setHoldOpens(hold bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.holdOpens = hold
}

// holdExecs holds the exec replies until gate closes
func (s *fakeSSHD) holdExecs(gate chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.execGate = gate
}

// loginShells are the pids of the login shells running commands now
func (s *fakeSSHD) loginShells() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var pids []int
	for p := range s.shells {
		pids = append(pids, p.Pid)
	}
	return pids
}

func (s *fakeSSHD) setSilent(silent bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.silent = silent
}

// stats are the logins so far, the connections open now and the most sessions one held
func (s *fakeSSHD) stats() (accepted, open, peak int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.accepted, len(s.conns), s.peak
}

func (s *fakeSSHD) close() {
	s.mu.Lock()
	if !s.closed {
		close(s.done)
	}
	s.closed = true
	shells := make([]*os.Process, 0, len(s.shells))
	for p := range s.shells {
		shells = append(shells, p)
	}
	s.mu.Unlock()
	_ = s.ln.Close()
	s.dropConns()
	for _, p := range shells {
		_ = syscall.Kill(-p.Pid, syscall.SIGKILL)
	}
}

func (s *fakeSSHD) serve() {
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.sockets[c] = true
		s.mu.Unlock()
		go s.handle(c)
	}
}

// handle logs a connection in by the pod's authorized keys and serves its sessions
func (s *fakeSSHD) handle(c net.Conn) {
	defer func() {
		s.mu.Lock()
		delete(s.sockets, c)
		s.mu.Unlock()
	}()
	login := func(meta ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
		if meta.User() == "root" && s.f.authorized(s.pod, key) {
			return nil, nil
		}
		return nil, errors.New("the key is not in authorized_keys")
	}
	cfg := &ssh.ServerConfig{PublicKeyCallback: login}
	s.mu.Lock()
	cfg.AddHostKey(s.hostKey)
	s.mu.Unlock()
	sc, chans, reqs, err := ssh.NewServerConn(c, cfg)
	if err != nil {
		_ = c.Close()
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = sc.Close()
		return
	}
	s.conns[sc], s.accepted = 0, s.accepted+1
	s.mu.Unlock()
	go s.global(reqs)
	for nc := range chans {
		if nc.ChannelType() != "session" {
			_ = nc.Reject(ssh.UnknownChannelType, "only sessions")
			continue
		}
		s.mu.Lock()
		hold := s.holdOpens
		s.mu.Unlock()
		if hold {
			continue
		}
		if !s.openSession(sc) {
			_ = nc.Reject(ssh.Prohibited, "no more sessions")
			continue
		}
		ch, creqs, err := nc.Accept()
		if err != nil {
			s.endSession(sc)
			continue
		}
		go func() {
			defer s.endSession(sc)
			s.session(sc, ch, creqs)
		}()
	}
	s.mu.Lock()
	delete(s.conns, sc)
	delete(s.running, sc)
	s.mu.Unlock()
}

// global answers global requests as OpenSSH does an unknown one, unless the sshd is silent
func (s *fakeSSHD) global(reqs <-chan *ssh.Request) {
	for r := range reqs {
		s.mu.Lock()
		silent := s.silent
		s.mu.Unlock()
		if !silent && r.WantReply {
			_ = r.Reply(false, nil)
		}
	}
}

// openSession counts a session, waiting a moment for closes OpenSSH would have read before it
func (s *fakeSSHD) openSession(sc *ssh.ServerConn) bool {
	for range 40 {
		s.mu.Lock()
		if s.conns[sc] < s.maxSessions {
			s.conns[sc]++
			s.mu.Unlock()
			return true
		}
		s.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// started counts a running command, keeping the most one connection ran at once
func (s *fakeSSHD) started(sc *ssh.ServerConn, shell *os.Process) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.shells[shell] = true
	s.running[sc]++
	s.peak = max(s.peak, s.running[sc])
}

func (s *fakeSSHD) endSession(sc *ssh.ServerConn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.conns[sc]; ok {
		s.conns[sc]--
	}
}

// session runs one exec request through the login shell, or serves SFTP, as OpenSSH does
func (s *fakeSSHD) session(sc *ssh.ServerConn, ch ssh.Channel, reqs <-chan *ssh.Request) {
	var shell *os.Process
	sftpOn := false
	for r := range reqs {
		switch r.Type {
		case "subsystem":
			var req struct{ Name string }
			if shell != nil || sftpOn || ssh.Unmarshal(r.Payload, &req) != nil ||
				req.Name != "sftp" {
				_ = r.Reply(false, nil)
				continue
			}
			sftpOn = true
			_ = r.Reply(true, nil)
			go s.serveSFTP(ch)
		case "exec":
			var req struct{ Command string }
			if shell != nil || sftpOn || ssh.Unmarshal(r.Payload, &req) != nil {
				_ = r.Reply(false, nil)
				continue
			}
			s.f.recordExec(req.Command)
			cmd := exec.Command("/bin/sh", "-c", req.Command)
			cmd.Dir, cmd.Env = s.f.sh.home, s.f.sh.env()
			cmd.Stdout, cmd.Stderr = ch, ch.Stderr()
			cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
			if err := cmd.Start(); err != nil {
				_ = r.Reply(false, nil)
				continue
			}
			shell = cmd.Process
			s.started(sc, shell)
			s.mu.Lock()
			gate := s.execGate
			s.mu.Unlock()
			if gate != nil {
				select {
				case <-gate:
				case <-s.done:
				}
			}
			_ = r.Reply(true, nil)
			go s.exit(sc, ch, cmd)
		case "signal":
			var req struct{ Signal string }
			if ssh.Unmarshal(r.Payload, &req) != nil {
				continue
			}
			s.f.recordSignal(req.Signal)
			if sig, ok := fakeSignals[req.Signal]; ok && shell != nil {
				_ = syscall.Kill(-shell.Pid, sig)
			}
		default:
			if r.WantReply {
				_ = r.Reply(false, nil)
			}
		}
	}
}

// exit sends the shell's exit status, or the signal that ended it, then closes the channel
func (s *fakeSSHD) exit(sc *ssh.ServerConn, ch ssh.Channel, cmd *exec.Cmd) {
	_ = cmd.Wait()
	s.mu.Lock()
	delete(s.shells, cmd.Process)
	if _, ok := s.running[sc]; ok {
		s.running[sc]--
	}
	s.mu.Unlock()
	_ = ch.CloseWrite()
	ws, _ := cmd.ProcessState.Sys().(syscall.WaitStatus)
	if ws.Signaled() {
		name := strings.TrimPrefix(fakeSignalName(ws.Signal()), "SIG")
		_, _ = ch.SendRequest("exit-signal", false, ssh.Marshal(struct {
			Signal     string
			CoreDumped bool
			Error      string
			Lang       string
		}{Signal: name}))
	} else {
		_, _ = ch.SendRequest("exit-status", false,
			ssh.Marshal(struct{ Status uint32 }{uint32(ws.ExitStatus())}))
	}
	_ = ch.Close()
}

// fakeSignals are the signals a signal request may name
var fakeSignals = map[string]syscall.Signal{
	"KILL": syscall.SIGKILL, "TERM": syscall.SIGTERM, "INT": syscall.SIGINT,
	"HUP": syscall.SIGHUP,
}

func fakeSignalName(sig syscall.Signal) string {
	for name, s := range fakeSignals {
		if s == sig {
			return name
		}
	}
	return strconv.Itoa(int(sig))
}
