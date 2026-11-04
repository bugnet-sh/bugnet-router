package boat

import (
	"bufio"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/provider/boat/api"
	"github.com/oynozan/bugnet-router/internal/provider/shell"
)

const (
	// maxReattach bounds the failed attaches in a row before the stream gives up
	maxReattach = 5
	// maxLineBytes fits one stream line holding a large output chunk
	maxLineBytes = 16 << 20
	// defaultIdle is four missed heartbeats, which Boat sends after 15 s without output
	defaultIdle = time.Minute
	killTimeout = 10 * time.Second
)

// linuxSignals numbers signals by name as Linux does, since every Boat machine runs Linux
var linuxSignals = map[string]int{
	"HUP": 1, "INT": 2, "QUIT": 3, "ILL": 4, "TRAP": 5, "ABRT": 6, "BUS": 7, "FPE": 8,
	"KILL": 9, "USR1": 10, "SEGV": 11, "USR2": 12, "PIPE": 13, "ALRM": 14, "TERM": 15,
	"STKFLT": 16, "CHLD": 17, "CONT": 18, "STOP": 19, "TSTP": 20, "TTIN": 21, "TTOU": 22,
	"URG": 23, "XCPU": 24, "XFSZ": 25, "VTALRM": 26, "PROF": 27, "WINCH": 28, "IO": 29,
	"PWR": 30, "SYS": 31,
}

// Exec starts argv detached through Boat's shell and follows its output on the attach stream
func (p *Provider) Exec(
	ctx context.Context, ref provider.Ref, spec provider.ExecSpec,
) (provider.ExecStream, error) {
	id, err := sandboxID(ref)
	if err != nil {
		return nil, err
	}
	line, err := shell.Command(spec)
	if err != nil {
		return nil, err
	}
	// A detached command has no time limit, while a waited or streamed one stops at 600 s
	body := api.CommandRequest{Command: line, Detached: new(true)}
	raw, err := p.send(ctx, true, func(ctx context.Context) (*http.Response, error) {
		return p.api.Command(ctx, id, body)
	})
	if err != nil {
		return nil, err
	}
	var started api.CommandStartedResponse
	if err := json.Unmarshal(raw, &started); err != nil || started.ProcessId < 1 {
		return nil, unknown(fmt.Errorf("boat command reply names no process: %.200s", raw))
	}
	sctx, cancel := context.WithCancel(ctx)
	s := &execStream{
		p: p, ctx: ctx, sctx: sctx, cancel: cancel, sandbox: id, process: started.ProcessId,
	}
	if err := s.reopen(); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

// execStream follows one detached command, reattaching from the offsets it has delivered
type execStream struct {
	p   *Provider
	ctx context.Context
	// sctx ends with Close and carries every attach of the stream
	sctx    context.Context
	cancel  context.CancelFunc
	sandbox string
	process int
	body    io.ReadCloser
	lines   *bufio.Scanner
	// idle ends the current attach through stop when no line comes for a while
	idle *time.Timer
	stop context.CancelFunc
	// stdout and stderr are the offsets of the first bytes not delivered yet
	stdout, stderr int
	failures       int
	pending        []byte
	exited         atomic.Bool
	done           bool
	code           int
	err            error
	closed         sync.Once
}

func (s *execStream) Read(p []byte) (int, error) {
	for len(s.pending) == 0 {
		if s.done {
			if s.err != nil {
				return 0, s.err
			}
			return 0, io.EOF
		}
		s.next()
	}
	n := copy(p, s.pending)
	s.pending = s.pending[n:]
	return n, nil
}

// next handles one line, or reattaches when the stream ended before its last line
func (s *execStream) next() {
	if s.lines == nil {
		if err := s.reopen(); err != nil {
			s.finish(err)
		}
		return
	}
	if !s.lines.Scan() {
		err := s.lines.Err()
		s.drop()
		if s.sctx.Err() != nil {
			s.finish(s.ended())
			return
		}
		s.failures++
		if s.failures > maxReattach {
			s.finish(internal(fmt.Errorf("boat stream of command %d broke %d times in a row: %v",
				s.process, s.failures, cmp.Or(err, io.ErrUnexpectedEOF))))
			return
		}
		if !sleep(s.sctx, s.p.reattachWait) {
			s.finish(s.ended())
		}
		return
	}
	s.failures = 0
	s.idle.Reset(s.p.idleTimeout)
	var f api.CommandAttachFrame
	if err := json.Unmarshal(s.lines.Bytes(), &f); err != nil {
		s.finish(internal(fmt.Errorf("boat stream line %.200q: %w", s.lines.Bytes(), err)))
		return
	}
	switch f.Type {
	case api.CommandAttachFrameTypeStdout:
		s.take(f)
	case api.CommandAttachFrameTypeStderr:
		s.stderr = max(s.stderr, deref(f.NextOffset))
	case api.CommandAttachFrameTypeReconnect:
		s.stderr = max(s.stderr, deref(f.StderrOffset))
		s.drop()
	case api.CommandAttachFrameTypeExit:
		s.exit(f)
	case api.CommandAttachFrameTypeError:
		s.finish(internal(fmt.Errorf("boat stream of command %d failed: %s: %s", s.process,
			deref(f.Error), deref(f.Message))))
	}
}

// take keeps what a stdout chunk holds past the delivered offset, so a replay adds nothing twice
func (s *execStream) take(f api.CommandAttachFrame) {
	data := deref(f.Data)
	end := s.stdout + len(data)
	if f.NextOffset != nil {
		end = *f.NextOffset
	}
	if skip := s.stdout - (end - len(data)); skip > 0 {
		data = data[min(skip, len(data)):]
	}
	s.pending = append(s.pending, data...)
	s.stdout = max(s.stdout, end)
}

// exit reads the last line, a signal becoming 128 plus its number as a shell reports it
func (s *execStream) exit(f api.CommandAttachFrame) {
	status := deref(f.Status)
	signal := deref(f.Signal)
	switch {
	case status != "" && status != api.CommandAttachFrameStatusExited:
		s.finish(unknown(fmt.Errorf("boat command %d is %s, its exit is unknown", s.process,
			status)))
	case f.ExitCode != nil:
		s.exited.Store(true)
		s.code = *f.ExitCode
		s.finish(nil)
	case signal != "":
		n, ok := signalNumber(signal)
		if !ok {
			s.finish(unknown(fmt.Errorf("boat command %d ended by signal %s", s.process,
				signal)))
			return
		}
		s.exited.Store(true)
		s.code = 128 + n
		s.finish(nil)
	default:
		s.finish(unknown(fmt.Errorf("boat command %d ended without an exit code", s.process)))
	}
}

func signalNumber(name string) (int, bool) {
	if n, err := strconv.Atoi(name); err == nil {
		return n, n > 0 && n < 128
	}
	n, ok := linuxSignals[strings.TrimPrefix(strings.ToUpper(name), "SIG")]
	return n, ok
}

// reopen attaches from the delivered offsets, retrying failures that may pass
func (s *execStream) reopen() error {
	for {
		err := s.attach()
		if err == nil {
			return nil
		}
		if s.sctx.Err() != nil {
			return s.ended()
		}
		s.failures++
		e, ok := provider.AsError(err)
		passing := !ok || e.Code == provider.CodeInternal || e.Code == provider.CodeRateLimited
		if !passing || s.failures > maxReattach {
			return err
		}
		if !sleep(s.sctx, s.p.reattachWait) {
			return s.ended()
		}
	}
}

// attach opens the stream at the delivered offsets, a stream gone quiet counting as broken
func (s *execStream) attach() error {
	params := &api.AttachCommandParams{StdoutOffset: new(s.stdout), StderrOffset: new(s.stderr)}
	actx, stop := context.WithCancel(s.sctx)
	idle := time.AfterFunc(s.p.idleTimeout, stop)
	resp, err := s.p.open(actx, false, func(ctx context.Context) (*http.Response, error) {
		return s.p.api.AttachCommand(ctx, s.sandbox, s.process, params,
			func(_ context.Context, req *http.Request) error {
				req.Header.Set("Accept", "application/x-ndjson")
				return nil
			})
	})
	if err != nil {
		idle.Stop()
		stop()
		return err
	}
	s.body, s.idle, s.stop = resp.Body, idle, stop
	s.lines = bufio.NewScanner(resp.Body)
	s.lines.Buffer(make([]byte, 0, 64<<10), maxLineBytes)
	return nil
}

func (s *execStream) drop() {
	if s.body != nil {
		s.idle.Stop()
		s.stop()
		_ = s.body.Close()
	}
	s.body, s.lines, s.idle, s.stop = nil, nil, nil, nil
}

// ended is the error of a stream whose context ended, by Close or by the caller's deadline
func (s *execStream) ended() error {
	return internal(fmt.Errorf("boat stream of command %d ended: %w", s.process,
		context.Cause(s.sctx)))
}

func (s *execStream) finish(err error) {
	s.done, s.err = true, err
	s.drop()
	s.cancel()
}

// Wait drains the stream and returns the exit code its last line carries
func (s *execStream) Wait() (int, error) {
	for !s.done {
		s.next()
	}
	s.pending = nil
	if s.err != nil {
		return -1, s.err
	}
	return s.code, nil
}

// Close stops following and stops a command that has not exited
func (s *execStream) Close() error {
	s.closed.Do(func() {
		s.cancel()
		if !s.exited.Load() {
			s.p.kill(s.ctx, s.sandbox, s.process)
		}
	})
	return nil
}

// kill stops a detached command, best effort since the sandbox may already be gone
func (p *Provider) kill(ctx context.Context, sandbox string, process int) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), killTimeout)
	defer cancel()
	_, err := p.send(ctx, false, func(ctx context.Context) (*http.Response, error) {
		return p.api.StopCommand(ctx, sandbox, process)
	})
	if err != nil && !isCode(err, provider.CodeNotFound) {
		p.log.Warn("boat could not stop a command whose stream closed early", "sandbox", sandbox,
			"process", process, "err", err)
	}
}
