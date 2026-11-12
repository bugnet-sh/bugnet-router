package hf

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/shell"
)

const (
	// defaultExecDir holds the pid file of each running command's group in the Job
	defaultExecDir = "/tmp/bugnet/exec"
	// jobSetsid is util-linux's setsid in our Ubuntu image, named whole so a user PATH misses it
	jobSetsid = "/usr/bin/setsid"
	// staleMarkMinutes is the age at which a cancel mark no wrapper read is removed
	staleMarkMinutes = 10
	// maxEventBytes bounds one NDJSON line, a 32 KiB output chunk escaped six times over fits
	maxEventBytes = 1 << 20
	// killTimeout bounds the exec that kills a cancelled command's group
	killTimeout = 30 * time.Second
	// maxSignal is the highest signal number Linux has
	maxSignal = 64
)

var (
	// errStreamClosed ends the stream of a command whose caller closed it
	errStreamClosed = errors.New("the caller closed the stream")
	// envName is a variable name every shell takes
	envName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	// safeWord matches the words a shell reads as they are, which need no quotes
	safeWord = regexp.MustCompile(`^[A-Za-z0-9@%+:,./_-]+$`)
)

// quote makes a word one shell word, leaving plain words as they are
func quote(s string) string {
	if safeWord.MatchString(s) {
		return s
	}
	return shell.Quote(s)
}

// Exec runs argv in the Job in a session of its own, whose group a cancel kills whole
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
	if _, ok := jobOf(ref); !ok {
		return nil, notHF(ref)
	}
	if err := checkSpec(spec); err != nil {
		return nil, err
	}
	sb, err := p.sandbox(ctx, ref)
	if err != nil {
		return nil, err
	}
	body, err := p.execBody(sb, p.groupArgv(spec.Argv, call), spec.Env, spec.Cwd)
	if err != nil {
		return nil, err
	}
	return p.startExec(ctx, sb, body, call)
}

// checkSpec refuses what sbx-server's JSON or execve cannot carry
func checkSpec(spec provider.ExecSpec) error {
	if len(spec.Argv) == 0 {
		return invalid(errors.New("exec needs a command"), false)
	}
	for i, a := range spec.Argv {
		if !plainText(a) {
			return invalid(fmt.Errorf("argument %d is not UTF-8 text without NUL bytes", i),
				false)
		}
	}
	for k, v := range spec.Env {
		if !envName.MatchString(k) || !plainText(v) {
			return invalid(fmt.Errorf("environment variable %.40q is not a name with UTF-8 "+
				"text without NUL bytes", k), false)
		}
	}
	if !plainText(spec.Cwd) {
		return invalid(errors.New("the working directory is not UTF-8 text without NUL bytes"),
			false)
	}
	return nil
}

// plainText reports UTF-8 text without NUL bytes, which JSON keeps whole and execve takes
func plainText(s string) bool { return utf8.ValidString(s) && !strings.ContainsRune(s, 0) }

// execRequest is the body of POST /v1/exec, design 4.4
type execRequest struct {
	Cmd     []string          `json:"cmd"`
	Shell   bool              `json:"shell"`
	Env     map[string]string `json:"env,omitempty"`
	Cwd     string            `json:"cwd,omitempty"`
	Timeout int64             `json:"timeout"`
}

// execBody is an exec's body, its timeout the time left until the Job's own end
func (p *Provider) execBody(
	sb sandbox, argv []string, env map[string]string, cwd string,
) ([]byte, error) {
	left := time.Duration(p.timeoutFor(p.maxLifetime)) * time.Second
	if !sb.end.IsZero() {
		left = sb.end.Sub(p.now())
	}
	return p.execBodyFor(argv, env, cwd, left)
}

// execBodyFor is an exec's body with a timeout of d, at least a second
func (p *Provider) execBodyFor(
	argv []string, env map[string]string, cwd string, d time.Duration,
) ([]byte, error) {
	secs := int64((d + time.Second - 1) / time.Second)
	body, err := json.Marshal(execRequest{
		Cmd: argv, Env: env, Cwd: cwd, Timeout: min(max(secs, 1), maxTimeoutSeconds-1),
	})
	switch {
	case err != nil:
		return nil, invalid(err, false)
	case len(body) > maxBodyBytes:
		return nil, invalid(fmt.Errorf("an exec of %d bytes is over sbx-server's %d",
			len(body), maxBodyBytes), false)
	}
	return body, nil
}

// groupArgv runs argv under a new session's leader, whose pid file stays while the group lives
func (p *Provider) groupArgv(argv []string, call string) []string {
	pid, mark := p.callFiles(call)
	// "$@" runs argv as a plain command, so no shell reads its first word as an option
	inner := fmt.Sprintf(`/bin/mkdir -p %[1]s 2>/dev/null; echo $$ >%[2]s 2>/dev/null; `+
		`if [ -e %[3]s ]; then /bin/rm -f %[2]s %[3]s; exit 137; fi; "$@" </dev/null; exit $?`,
		quote(p.execDir), quote(pid), quote(mark))
	// The outer shell sweeps after setsid returns, from outside the group it checks
	outer := quote(p.setsid) + " -w /bin/sh -c " + shell.Quote(inner) + ` sh "$@"; s=$?; ` +
		p.sweepCommand() + "; exit $s"
	return append([]string{"/bin/sh", "-c", outer, "sh"}, argv...)
}

// sweepCommand removes the pid files of groups that ended and the cancel marks gone stale
func (p *Provider) sweepCommand() string {
	dir := quote(strings.TrimRight(p.execDir, "/"))
	// An empty pid file is one a wrapper is still writing
	return fmt.Sprintf(`for f in %[1]s/*.pid; do p=; read -r p 2>/dev/null <"$f"; `+
		`case $p in ''|*[!0-9]*) continue;; esac; `+
		`kill -0 "-$p" 2>/dev/null || /bin/rm -f "$f"; done; `+
		`for m in %[1]s/*.cancel; do [ -e "$m" ] && /usr/bin/find %[1]s -type f `+
		`-name '*.cancel' -mmin +%[2]d -exec /bin/rm -f {} +; break; done`,
		dir, staleMarkMinutes)
}

// killArgv kills a call's group by its pid file, or marks the call so argv never starts
func (p *Provider) killArgv(call string) []string {
	pid, mark := p.callFiles(call)
	script := fmt.Sprintf(`/bin/mkdir -p %[1]s 2>/dev/null; : >%[3]s; `+
		`p=$(/bin/cat %[2]s 2>/dev/null); case $p in ''|*[!0-9]*) exit 0;; esac; `+
		// dash's kill reads a double dash as a pid, so the group goes as a bare negative number
		`[ "$p" -gt 1 ] && kill -KILL "-$p" 2>/dev/null; /bin/rm -f %[2]s %[3]s`,
		quote(p.execDir), quote(pid), quote(mark))
	return []string{"/bin/sh", "-c", script, "sh"}
}

// callFiles are a call's pid file and the mark a kill leaves when the pid file is not there yet
func (p *Provider) callFiles(call string) (pid, mark string) {
	dir := strings.TrimRight(p.execDir, "/")
	return dir + "/" + call + ".pid", dir + "/" + call + ".cancel"
}

// startExec sends an exec and streams its answer, killing call's group when no answer came
func (p *Provider) startExec(
	ctx context.Context, sb sandbox, body []byte, call string,
) (*execStream, error) {
	rctx, cancel := context.WithCancel(ctx)
	// sent is set once the transport wrote the request, after which the command may run
	var sent atomic.Bool
	trace := &httptrace.ClientTrace{WroteHeaders: func() { sent.Store(true) }}
	req, err := p.sbx.request(httptrace.WithClientTrace(rctx, trace), sb, http.MethodPost,
		"/v1/exec", nil, body)
	if err != nil {
		cancel()
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/x-ndjson")
	resp, err := p.sbx.client.Do(req)
	if err != nil {
		cancel()
		if sent.Load() && call != "" {
			go p.killLate(sb, call)
		}
		return nil, p.lost(ctx, sb, &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("hf Job %s sbx-server %s: %w", sb.id, routeName(req), err)})
	}
	if resp.StatusCode != http.StatusOK {
		defer drainClose(resp)
		cancel()
		return nil, p.answerError(ctx, sb, req, resp)
	}
	lines := bufio.NewScanner(resp.Body)
	lines.Buffer(make([]byte, 0, 64<<10), maxEventBytes)
	s := &execStream{
		p: p, ctx: ctx, sb: sb, call: call, body: resp.Body, lines: lines, cancel: cancel,
	}
	s.stopWatch = context.AfterFunc(ctx, func() { s.stop(context.Cause(ctx)) })
	return s, nil
}

// killLate kills the group of a call given up on, its start or its stream's end unseen
func (p *Provider) killLate(sb sandbox, call string) {
	ctx, cancel := context.WithTimeout(context.Background(), killTimeout)
	defer cancel()
	// A Job gone has nothing left to kill
	if err := p.killGroup(ctx, sb, call); err != nil && !isCode(err, provider.CodeNotFound) {
		p.log.Warn("hf: the group of a command given up on was not killed", "job", sb.id,
			"err", err)
	}
}

// killGroup kills a call's group through an exec of its own, never one wrapped in a session
func (p *Provider) killGroup(ctx context.Context, sb sandbox, call string) error {
	body, err := p.execBodyFor(p.killArgv(call), nil, "", killTimeout)
	if err != nil {
		return err
	}
	s, err := p.startExec(ctx, sb, body, "")
	if err != nil {
		return err
	}
	defer func() { _ = s.Close() }()
	code, err := s.Wait()
	if err == nil && code != 0 {
		err = &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("the kill of a group in hf Job %s exited %d", sb.id, code)}
	}
	return err
}

// execStream is one command's stdout, read from its NDJSON events
type execStream struct {
	p    *Provider
	ctx  context.Context
	sb   sandbox
	call string
	body io.ReadCloser
	// lines, pending and the end are read and written by the caller's goroutine alone
	lines     *bufio.Scanner
	cancel    context.CancelFunc
	stopWatch func() bool
	pending   []byte
	done      bool
	code      int
	err       error
	// exited is set once the exit event came, after which nothing is killed
	exited atomic.Bool
	mu     sync.Mutex
	abort  error
	once   sync.Once
}

// event is one NDJSON line of an exec's answer
type event struct {
	Event    string `json:"event"`
	Data     string `json:"data"`
	ExitCode *int   `json:"exit_code"`
	Signal   *int   `json:"signal"`
	TimedOut bool   `json:"timed_out"`
}

func (s *execStream) Read(b []byte) (int, error) {
	if err := s.aborted(); err != nil {
		return 0, err
	}
	for len(s.pending) == 0 {
		if s.done {
			if s.err != nil {
				return 0, s.err
			}
			return 0, io.EOF
		}
		s.next()
	}
	n := copy(b, s.pending)
	s.pending = s.pending[n:]
	return n, nil
}

// next reads one event, keeping stdout and the exit and skipping stderr, pings and the rest
func (s *execStream) next() {
	if !s.lines.Scan() {
		err := s.lines.Err()
		if errors.Is(err, bufio.ErrTooLong) {
			err = fmt.Errorf("an event line is over %d bytes", maxEventBytes)
		}
		s.lostEnd(cmpErr(err, io.ErrUnexpectedEOF))
		return
	}
	var ev event
	if err := json.Unmarshal(s.lines.Bytes(), &ev); err != nil {
		s.lostEnd(fmt.Errorf("an event that is not JSON: %w", err))
		return
	}
	switch ev.Event {
	case "stdout":
		s.pending = append(s.pending, ev.Data...)
	case "exit":
		s.exit(ev)
	}
}

// cmpErr is err, or alt when err is nil
func cmpErr(err, alt error) error {
	if err != nil {
		return err
	}
	return alt
}

// exit reads the exit event, the code or 128 plus the signal, an error on a timeout
func (s *execStream) exit(ev event) {
	s.exited.Store(true)
	switch {
	case ev.TimedOut:
		s.finish(-1, &provider.Error{Code: provider.CodeInternal, Err: fmt.Errorf(
			"hf Job %s stopped a command at its timeout", s.sb.id)})
	case ev.Signal != nil && *ev.Signal > 0 && *ev.Signal <= maxSignal:
		s.finish(128+*ev.Signal, nil)
	case ev.Signal == nil && ev.ExitCode != nil && *ev.ExitCode >= 0 && *ev.ExitCode < 256:
		s.finish(*ev.ExitCode, nil)
	default:
		s.finish(-1, &provider.Error{Code: provider.CodeInternal, Err: fmt.Errorf(
			"hf Job %s ended a command with no exit status it could read", s.sb.id)})
	}
}

// lostEnd ends a stream that stopped before its exit, killing its group as on a cancel
func (s *execStream) lostEnd(cause error) {
	if err := s.aborted(); err != nil {
		s.finish(-1, err)
		return
	}
	// No one reads the command any more, so it must not run on to the Job's end
	go s.p.killLate(s.sb, s.call)
	ctx, cancel := context.WithTimeout(context.WithoutCancel(s.ctx), lostTimeout)
	defer cancel()
	s.finish(-1, s.p.lost(ctx, s.sb, &provider.Error{Code: provider.CodeOutcomeUnknown,
		Err: fmt.Errorf("hf Job %s ended a command's stream before its exit: %w", s.sb.id,
			cause)}))
}

// finish records the end and lets the answer go
func (s *execStream) finish(code int, err error) {
	s.done, s.code, s.err = true, code, err
	s.stopWatch()
	s.cancel()
	_ = s.body.Close()
}

// aborted is the error a cancel or Close ended the stream with, nil before one
func (s *execStream) aborted() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.abort
}

// stop ends the stream at once and kills the command's group out of the caller's way
func (s *execStream) stop(why error) {
	s.once.Do(func() {
		if s.exited.Load() {
			return
		}
		s.mu.Lock()
		s.abort = &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("hf command in Job %s ended early: %w", s.sb.id, why)}
		s.mu.Unlock()
		s.cancel()
		_ = s.body.Close()
		if s.call != "" {
			go s.p.killLate(s.sb, s.call)
		}
	})
}

// Wait drains stdout and returns the exit status, 128 plus the signal for a signal exit
func (s *execStream) Wait() (int, error) {
	for !s.done {
		if err := s.aborted(); err != nil {
			s.finish(-1, err)
			break
		}
		s.pending = nil
		s.next()
	}
	s.pending = nil
	if s.err != nil {
		return -1, s.err
	}
	return s.code, nil
}

// Close ends the stream, killing the command's group when it has not exited yet
func (s *execStream) Close() error {
	s.stop(errStreamClosed)
	return nil
}
