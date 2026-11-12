package lium

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/shell"
)

const (
	// defaultExecDir holds the pid file of each running command's group in the pod
	defaultExecDir = "/tmp/bugnet/exec"
	// podSetsid is util-linux's setsid in our Ubuntu image, named whole so a user PATH misses it
	podSetsid = "/usr/bin/setsid"
)

// errStreamClosed ends the stream of a command whose caller closed it
var errStreamClosed = errors.New("the caller closed the stream")

// linuxSignals numbers signals by name as Linux does, since every Lium pod runs Linux
var linuxSignals = map[string]int{
	"HUP": 1, "INT": 2, "QUIT": 3, "ILL": 4, "TRAP": 5, "ABRT": 6, "BUS": 7, "FPE": 8,
	"KILL": 9, "USR1": 10, "SEGV": 11, "USR2": 12, "PIPE": 13, "ALRM": 14, "TERM": 15,
	"STKFLT": 16, "CHLD": 17, "CONT": 18, "STOP": 19, "TSTP": 20, "TTIN": 21, "TTOU": 22,
	"URG": 23, "XCPU": 24, "XFSZ": 25, "VTALRM": 26, "PROF": 27, "WINCH": 28, "IO": 29,
	"PWR": 30, "SYS": 31,
}

// Exec runs argv on the pod in a process group of its own, which a cancel kills whole
func (p *Provider) Exec(
	ctx context.Context, ref provider.Ref, spec provider.ExecSpec,
) (provider.ExecStream, error) {
	return p.exec(ctx, ref, spec, newCallID())
}

// newCallID names one call's pid file
func newCallID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// exec runs one call under the given id
func (p *Provider) exec(
	ctx context.Context, ref provider.Ref, spec provider.ExecSpec, call string,
) (provider.ExecStream, error) {
	id, ok := podID(ref)
	if !ok {
		return nil, notLium(ref)
	}
	line, err := p.groupCommand(spec, call)
	if err != nil {
		return nil, err
	}
	sctx, cancel := p.opContext(ctx)
	defer cancel()
	pl, err := p.poolFor(sctx, ref)
	if err != nil {
		return nil, p.sshError(ctx, id, err)
	}
	sess, c, err := pl.session(sctx)
	if err != nil {
		return nil, p.sshError(ctx, id, err)
	}
	stdout, err := sess.StdoutPipe()
	if err == nil {
		var left bool
		left, err = pl.await(sctx, c, func() error { return sess.Start(line) }, func(error) {
			_ = sess.Close()
			pl.release(c)
		})
		switch {
		case left:
			// The exec request may have reached the pod, so its group is killed as on a cancel
			go p.killLate(pl, id, call)
			return nil, p.sshError(ctx, id, fmt.Errorf("start a command: %w", err))
		case err != nil:
			// x/crypto's error quotes the whole command line, which may hold the user's values
			err = errors.New("the pod's sshd refused the command")
		}
	}
	if err != nil {
		_ = sess.Close()
		pl.release(c)
		return nil, p.sshError(ctx, id, fmt.Errorf("start a command: %w", err))
	}
	r, w := io.Pipe()
	s := &execStream{
		p: p, pl: pl, c: c, sess: sess, pod: id, call: call, r: r, w: w,
		eof: make(chan struct{}), done: make(chan struct{}), aborted: make(chan struct{}),
	}
	go s.pump(stdout)
	go s.watch(ctx)
	return s, nil
}

// opContext is ctx, bounded by the SSH operation limit when the caller set no deadline
func (p *Provider) opContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, p.ssh.op)
}

// groupCommand runs argv under a new session's leader, which keeps its pid file while argv runs
func (p *Provider) groupCommand(spec provider.ExecSpec, call string) (string, error) {
	if len(spec.Argv) == 0 {
		return "", invalid(errors.New("exec needs a command"), false)
	}
	pid, mark := p.callFiles(call)
	// "$@" runs argv as a plain command, so no shell reads its first word as an option
	script := fmt.Sprintf(`/bin/mkdir -p %[1]s 2>/dev/null; echo $$ >%[2]s 2>/dev/null; `+
		`if [ -e %[3]s ]; then /bin/rm -f %[2]s %[3]s; exit 137; fi; `+
		`"$@"; s=$?; /bin/rm -f %[2]s; exit $s`,
		shell.Quote(p.execDir), shell.Quote(pid), shell.Quote(mark))
	argv := append([]string{p.setsid, "-w", "/bin/sh", "-c", script, "sh"}, spec.Argv...)
	return shell.Command(provider.ExecSpec{Argv: argv, Cwd: spec.Cwd, Env: spec.Env})
}

// killCommand kills a call's group by its pid file, or marks the call so argv never starts
func (p *Provider) killCommand(call string) (string, error) {
	pid, mark := p.callFiles(call)
	script := fmt.Sprintf(`/bin/mkdir -p %[1]s 2>/dev/null; : >%[3]s; `+
		`p=$(/bin/cat %[2]s 2>/dev/null); case $p in ''|*[!0-9]*) exit 0;; esac; `+
		// dash's kill reads a double dash as a pid, so the group goes as a bare negative number
		`[ "$p" -gt 1 ] && kill -KILL "-$p" 2>/dev/null; /bin/rm -f %[2]s %[3]s`,
		shell.Quote(p.execDir), shell.Quote(pid), shell.Quote(mark))
	return shell.Command(provider.ExecSpec{Argv: []string{"/bin/sh", "-c", script}})
}

// callFiles are a call's pid file and the mark a kill leaves when the pid file is not there yet
func (p *Provider) callFiles(call string) (pid, mark string) {
	dir := strings.TrimRight(p.execDir, "/")
	return dir + "/" + call + ".pid", dir + "/" + call + ".cancel"
}

// killGroup kills a cancelled call's group on a fresh session of the pod's pool
func (p *Provider) killGroup(ctx context.Context, pl *sshPool, call string) error {
	line, err := p.killCommand(call)
	if err != nil {
		return err
	}
	sess, c, err := pl.session(ctx)
	if err != nil {
		return err
	}
	defer pl.release(c)
	defer func() { _ = sess.Close() }()
	stop := pl.guard(ctx, c)
	err = sess.Run(line)
	if !stop() {
		return context.Cause(ctx)
	}
	return err
}

// killLate kills the group of a call whose start the caller gave up on
func (p *Provider) killLate(pl *sshPool, pod, call string) {
	ctx, cancel := context.WithTimeout(context.Background(), p.ssh.kill)
	defer cancel()
	if err := p.killGroup(ctx, pl, call); err != nil {
		p.log.Warn("lium: the group of a command given up on was not killed", "pod", pod,
			"err", err)
	}
}

// execStream is one command's stdout, its session holding a slot of the pod's pool
type execStream struct {
	p    *Provider
	pl   *sshPool
	c    *sshConn
	sess *ssh.Session
	// pod is the pod id, call the id of the call's pid file
	pod, call string
	// r is what Read reads, w what pump copies stdout to
	r *io.PipeReader
	w *io.PipeWriter
	// eof closes once stdout ended, done once the session ended and code and err hold its exit
	eof, done chan struct{}
	// aborted closes once a cancel or Close ended the stream before its exit
	aborted chan struct{}
	mu      sync.Mutex
	code    int
	err     error
	abort   error
	once    sync.Once
}

func (s *execStream) Read(b []byte) (int, error) { return s.r.Read(b) }

// pump copies stdout until it ends, then reads the exit and frees the session's slot
func (s *execStream) pump(stdout io.Reader) {
	if _, err := io.Copy(s.w, stdout); err == nil {
		close(s.eof)
	}
	_ = s.w.Close()
	werr := s.sess.Wait()
	_ = s.sess.Close()
	var code int
	var err error
	select {
	case <-s.aborted:
	default:
		code, err = s.p.exitOf(s.pod, werr)
	}
	s.mu.Lock()
	s.code, s.err = code, err
	s.mu.Unlock()
	s.pl.release(s.c)
	close(s.done)
}

// watch ends the stream when the caller's context ends before the command does
func (s *execStream) watch(ctx context.Context) {
	select {
	case <-ctx.Done():
		s.stop(context.Cause(ctx))
	case <-s.done:
	case <-s.aborted:
	}
}

// stop ends Read at once and kills the command's group out of the caller's way
func (s *execStream) stop(why error) {
	s.once.Do(func() {
		select {
		case <-s.done:
			return
		default:
		}
		err := &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("lium command on pod %s ended early: %w", s.pod, why)}
		s.mu.Lock()
		s.abort = err
		s.mu.Unlock()
		close(s.aborted)
		_ = s.w.CloseWithError(err)
		go s.killGroup()
	})
}

// killGroup signals and closes the session, kills the call's group and drops a stuck connection
func (s *execStream) killGroup() {
	if s.exiting() {
		return
	}
	// Both writes may wait on a stalled connection, so they run here and not in Close
	_ = s.sess.Signal(ssh.SIGKILL)
	_ = s.sess.Close()
	ctx, cancel := context.WithTimeout(context.Background(), s.p.ssh.kill)
	defer cancel()
	if err := s.p.killGroup(ctx, s.pl, s.call); err != nil {
		s.p.log.Warn("lium: the group of a cancelled command was not killed", "pod", s.pod,
			"err", err)
	}
	select {
	case <-s.done:
	case <-ctx.Done():
		s.pl.drop(s.c)
	}
}

// exiting reports a command whose stdout ended and whose exit came within the stall period
func (s *execStream) exiting() bool {
	select {
	case <-s.eof:
	default:
		return false
	}
	t := time.NewTimer(s.p.ssh.stall)
	defer t.Stop()
	select {
	case <-s.done:
		return true
	case <-t.C:
		return false
	}
}

// Wait drains stdout and returns the exit status, 128 plus the signal for a signal exit
func (s *execStream) Wait() (int, error) {
	_, _ = io.Copy(io.Discard, s.r)
	select {
	case <-s.done:
	case <-s.aborted:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.abort != nil:
		return -1, s.abort
	case s.err != nil:
		return -1, s.err
	}
	return s.code, nil
}

// Close ends the stream, killing the command's group when it has not exited yet
func (s *execStream) Close() error {
	s.stop(errStreamClosed)
	return nil
}

// exitOf reads a session's end, a lost one being not_found once Lium no longer has the pod
func (p *Provider) exitOf(pod string, err error) (int, error) {
	var exit *ssh.ExitError
	switch {
	case err == nil:
		return 0, nil
	case errors.As(err, &exit) && exit.Signal() != "":
		if n, ok := linuxSignals[strings.TrimPrefix(exit.Signal(), "SIG")]; ok {
			return 128 + n, nil
		}
		return -1, &provider.Error{Code: provider.CodeOutcomeUnknown,
			Err: fmt.Errorf("lium command on pod %s ended by an unknown signal", pod)}
	case errors.As(err, &exit) && exit.ExitStatus() > 0 && exit.ExitStatus() < 256:
		return exit.ExitStatus(), nil
	case errors.As(err, &exit):
		return -1, &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("lium command on pod %s gave exit status %d", pod, exit.ExitStatus())}
	}
	ctx, cancel := context.WithTimeout(context.Background(), refuseTimeout)
	defer cancel()
	lost := p.sshError(ctx, pod, fmt.Errorf("the command's session ended without an exit: %w",
		err))
	if e, ok := provider.AsError(lost); ok && e.Code == provider.CodeInternal {
		e.Code = provider.CodeOutcomeUnknown
	}
	return -1, lost
}
