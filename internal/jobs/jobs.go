// Package jobs runs commands inside placements through bugnet-run and serves their output.
//
// Exec derives the job id from the placement and idempotency key and takes the job's lease
// before recording the job, so exactly one replica starts it. The lease holder runs an owner
// loop that executes bugnet-run start, or attach when the job already runs, through the
// provider and appends every frame to the job's Valkey stream, renewing the lease as it goes.
// Readers on any replica follow that stream from a cursor and never see a byte twice. When an
// owner stops, crashes or loses its lease, the scanner of some replica takes the job over and
// attaches from the offsets the stream already holds. The exit frame, or an error frame for a
// job that cannot be reached, ends the stream, and its owner records the outcome in the store.
// Output is bounded per job and per placement. Past either budget the owner follows the job by
// its newest 64 KiB, stores that tail every 10 s while an allowance lasts and once more at the
// end, and marks the output it left out with skip frames, the whole output staying in the
// sandbox's job files. A job's final 8 KiB per output is kept even past the placement's budget,
// from a tail reserve of the placement's own, so a later small command still shows its output.
package jobs

import (
	"cmp"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bugnet-sh/bugnet-run/frame"
	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/sessions"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/streams"
)

const (
	runnerPath    = "/tmp/bugnet/bin/bugnet-run"
	defaultWait   = 25 * time.Second
	maxWait       = 45 * time.Second
	tailSize      = 8 << 10
	streamGrace   = 10 * time.Minute
	renewEvery    = 1500 * time.Millisecond
	refreshEvery  = time.Minute
	scanEvery     = time.Second
	adoptEvery    = 10 * time.Second
	attachTries   = 5
	attachBackoff = 200 * time.Millisecond
	readerPoll    = 5 * time.Second
	cancelWait    = 10 * time.Second
	finishPoll    = 50 * time.Millisecond
	holderPoll    = 50 * time.Millisecond
	callTimeout   = 10 * time.Second
	retryAfter    = 5 * time.Second
	// floodTail is the newest output of each stream a flooding job is followed by
	floodTail = 64 << 10
)

// floodEvery paces the snapshots that store a flooding job's newest output
var floodEvery = 10 * time.Second

// Defaults of the output budgets and the running job limit
const (
	DefaultJobBudget       = 8 << 20
	DefaultPlacementBudget = 32 << 20
	DefaultTailReserve     = 8 << 20
	DefaultMaxRunning      = 16
)

// idleAfter gives up a runner stream that sent nothing, not even a heartbeat, for three beats
var idleAfter = 30 * time.Second

// reasonCancelled marks the exit frame of a job that ended after a cancel was asked for
const reasonCancelled = "cancelled"

// Errors the API maps to status codes
var (
	ErrInvalid  = errors.New("jobs: invalid command")
	ErrNotReady = errors.New("jobs: placement is not ready")
	ErrNotFound = errors.New("jobs: job not found")
	// ErrShuttingDown means this replica stopped taking jobs, so the caller should try a peer
	ErrShuttingDown = errors.New("jobs: service is shutting down")
	// ErrTooManyJobs means the placement already runs as many jobs as it may
	ErrTooManyJobs = errors.New("jobs: the placement runs as many jobs as it may")
	// ErrNotRecorded means the call holding the idempotency key recorded no job, so retry it
	ErrNotRecorded = errors.New("jobs: the call holding this idempotency key recorded no job")
)

var errNoProvider = errors.New("jobs: provider is not configured")

// Deps are the services and settings a Service works with
type Deps struct {
	Store     *store.Store
	Bus       *streams.Bus
	Registry  *provider.Registry
	ReplicaID string       // the replica part of every lease this service takes
	Log       *slog.Logger // slog.Default when nil
	// JobBudget is what one job's output may cost its stream, DefaultJobBudget when zero
	JobBudget int64
	// PlacementBudget is what a placement's job streams may cost together, a default when zero
	PlacementBudget int64
	// TailReserve is what final tails may cost a placement past its budget, a default when zero
	TailReserve int64
	// MaxRunning bounds the jobs one placement runs at once, DefaultMaxRunning when zero
	MaxRunning int
	// Sessions gives each new job its session record row and events, none when nil
	Sessions *sessions.Writer
}

// Service runs jobs and serves their streams
type Service struct {
	d      Deps
	ctx    context.Context
	stop   context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.Mutex
	closed bool
	owners map[string]*owner
}

// New builds a service over deps
func New(deps Deps) *Service {
	if deps.Log == nil {
		deps.Log = slog.Default()
	}
	deps.JobBudget = cmp.Or(deps.JobBudget, DefaultJobBudget)
	deps.PlacementBudget = cmp.Or(deps.PlacementBudget, DefaultPlacementBudget)
	deps.TailReserve = cmp.Or(deps.TailReserve, DefaultTailReserve)
	deps.MaxRunning = cmp.Or(deps.MaxRunning, DefaultMaxRunning)
	ctx, stop := context.WithCancel(context.Background())
	return &Service{d: deps, ctx: ctx, stop: stop, owners: map[string]*owner{}}
}

// ExecResult is what Exec saw of a job up to the moment it returned
type ExecResult struct {
	JobID      string
	State      string // running, exited, cancelled, lost
	ExitCode   *int
	StdoutTail []byte // last 8 KiB
	StderrTail []byte // last 8 KiB
	Cursor     string
	Truncated  bool // the tails or the job's stream leave output out
	// OOMKilled is read from the exit frame, so a job ended before Exec saw its stream has none
	OOMKilled bool
}

var idEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

// newJobID derives the id from the key so a re-sent Exec names the job it already started
func newJobID(placementID uuid.UUID, key string) string {
	if key == "" {
		return "j_" + strings.ToLower(rand.Text())
	}
	sum := sha256.Sum256([]byte(placementID.String() + "\x00" + key))
	return "j_" + strings.ToLower(idEncoding.EncodeToString(sum[:])[:26])
}

func (s *Service) provider(p store.Placement) (provider.Provider, error) {
	if p.State != store.PlacementReady || p.ProviderRef == nil {
		return nil, fmt.Errorf("%w: placement %s is %s", ErrNotReady, p.ID, p.State)
	}
	prov, ok := s.d.Registry.Get(p.ProviderRef.Provider)
	if !ok {
		return nil, fmt.Errorf("%w: %q", errNoProvider, p.ProviderRef.Provider)
	}
	return prov, nil
}

func (s *Service) closing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// expiry keeps a job's keys 10 minutes past the deadline, or past now for a job outliving it
func expiry(deadline *time.Time) time.Time {
	at := time.Now()
	if deadline != nil && deadline.After(at) {
		at = *deadline
	}
	return at.Add(streamGrace)
}

func detached(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), callTimeout)
}

/* Exec */

type command struct {
	argv []string
	cwd  string
	env  map[string]string
	// arrived is when Exec arrived, which first_frame_ms counts from
	arrived time.Time
}

// Exec starts argv in the placement, or attaches to the job a repeated key already started
func (s *Service) Exec(
	ctx context.Context, p store.Placement, argv []string, cwd string,
	env map[string]string, idemKey string, wait time.Duration, until Until,
) (ExecResult, error) {
	arrived := time.Now()
	if len(argv) == 0 {
		return ExecResult{}, fmt.Errorf("%w: no argv", ErrInvalid)
	}
	if err := CheckEnv(env); err != nil {
		return ExecResult{}, err
	}
	prov, err := s.provider(p)
	if err != nil {
		return ExecResult{}, err
	}
	if wait <= 0 {
		wait = defaultWait
	}
	deadline := time.Now().Add(min(wait, maxWait))
	if s.closing() {
		return ExecResult{}, ErrShuttingDown
	}
	nj := store.NewJob{
		ID: newJobID(p.ID, idemKey), PlacementID: p.ID, IdempotencyKey: idemKey,
		Limit: s.d.MaxRunning, Install: IsInstall(argv),
	}
	c := &command{argv: argv, cwd: cwd, env: env, arrived: arrived}
	if err := s.begin(ctx, p, prov, nj, c, deadline); err != nil {
		return ExecResult{}, err
	}
	return s.collect(ctx, nj.ID, until, deadline)
}

// CheckEnv refuses a command environment with an empty key or a key holding an equals sign
func CheckEnv(env map[string]string) error {
	for k := range env {
		if k == "" || strings.Contains(k, "=") {
			return fmt.Errorf("%w: env key %q", ErrInvalid, k)
		}
	}
	return nil
}

// begin records and starts the job, or waits until the Exec holding its lease has recorded it
func (s *Service) begin(
	ctx context.Context, p store.Placement, prov provider.Provider, nj store.NewJob, c *command,
	deadline time.Time,
) error {
	for {
		// The lease comes first so no scanner adopts the job before its command starts
		lease, acquired, err := s.d.Bus.AcquireLease(ctx, nj.ID, s.d.ReplicaID)
		if err != nil {
			return err
		}
		if acquired {
			return s.start(ctx, lease, p, prov, nj, c)
		}
		// The holder records the job at once unless it failed, and then its lease ends or expires
		if _, err := s.d.Store.JobByID(ctx, nj.ID); !errors.Is(err, store.ErrNotFound) {
			return err
		}
		left := time.Until(deadline)
		if left <= 0 {
			return ErrNotRecorded
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(min(holderPoll, left)):
		}
	}
}

func (s *Service) start(
	ctx context.Context, lease streams.Lease, p store.Placement, prov provider.Provider,
	nj store.NewJob, c *command,
) error {
	if s.closing() {
		s.giveBack(lease, false)
		return ErrShuttingDown
	}
	job, created, err := s.d.Store.CreateJob(ctx, nj)
	if errors.Is(err, store.ErrJobLimit) {
		s.giveBack(lease, true)
		return fmt.Errorf("%w, %d at once", ErrTooManyJobs, nj.Limit)
	}
	if err != nil {
		s.giveBack(lease, true)
		return err
	}
	if created && c != nil {
		s.d.Sessions.StartJob(ctx, p, nj.ID, shownArgv(ctx, c.argv))
	}
	if job.State == store.JobRunning {
		s.own(lease, p, prov, c)
	} else {
		s.giveBack(lease, true)
	}
	return nil
}

type tails struct {
	data    [2][]byte
	size    [2]int64
	skipped bool
}

func (t *tails) add(f frame.Frame) {
	if f.Kind == frame.KindSkip {
		t.skipped = true
		// Bytes were left out, so a tail starts over after them rather than join across the gap
		for i, at := range [2]int64{f.Out, f.Err} {
			if at > t.size[i] {
				t.data[i], t.size[i] = nil, at
			}
		}
		return
	}
	i, ok := streamOf(f.Kind)
	if !ok {
		return
	}
	t.data[i] = keepLast(t.data[i], f.Data, tailSize)
	t.size[i] = max(t.size[i], f.Offset+int64(len(f.Data)))
}

// keepLast appends data to buf and returns at most the last n bytes of the result
func keepLast(buf, data []byte, n int) []byte {
	b := append(buf, data...)
	if len(b) > n {
		b = b[len(b)-n:]
	}
	return b
}

func (s *Service) collect(
	ctx context.Context, id string, until Until, deadline time.Time,
) (ExecResult, error) {
	r := s.reader(id, "", until)
	var t tails
	end, err := r.follow(ctx, deadline, func(e streams.Entry) error {
		t.add(e.Frame)
		return nil
	})
	if err != nil {
		return ExecResult{}, err
	}
	res := ExecResult{
		JobID:      id,
		State:      store.JobRunning,
		StdoutTail: slices.Clone(t.data[0]),
		StderrTail: slices.Clone(t.data[1]),
		Cursor:     r.cursor,
		Truncated:  t.skipped || t.size[0] > tailSize || t.size[1] > tailSize,
	}
	switch {
	case end.final != nil:
		res.State, res.ExitCode = outcome(*end.final)
		res.OOMKilled = end.final.Kind == frame.KindExit && end.final.OOM
	case end.job != nil:
		res.State, res.ExitCode = end.job.State, end.job.ExitCode
		res.Truncated = res.Truncated || end.job.OutputTruncated
	}
	return res, nil
}

// outcome returns the job state and exit code a final frame stands for
func outcome(f frame.Frame) (string, *int) {
	if f.Kind != frame.KindExit {
		return store.JobLost, nil
	}
	code := f.Code
	if f.Reason == reasonCancelled {
		return store.JobCancelled, &code
	}
	return store.JobExited, &code
}

func streamOf(k frame.Kind) (int, bool) {
	switch k {
	case frame.KindStdout:
		return 0, true
	case frame.KindStderr:
		return 1, true
	}
	return 0, false
}

/* Reading */

// Watch hands send every entry after cursor until the job ends, until is met or ctx ends
func (s *Service) Watch(
	ctx context.Context, jobID, cursor string, until Until, send func(streams.Entry) error,
) error {
	_, err := s.reader(jobID, cursor, until).follow(ctx, time.Time{}, send)
	return err
}

type reader struct {
	s        *Service
	job      string
	cursor   string
	next     [2]int64
	progress progress
}

// ending says why follow stopped, both fields nil when until was met or the deadline passed
type ending struct {
	final *frame.Frame
	job   *store.Job // a finished job whose stream holds no final frame
}

func (s *Service) reader(jobID, cursor string, until Until) *reader {
	return &reader{s: s, job: jobID, cursor: cursor, progress: progress{until: until}}
}

// follow hands fn each new entry until the job ends, until is met, deadline passes or ctx ends
func (r *reader) follow(
	ctx context.Context, deadline time.Time, fn func(streams.Entry) error,
) (ending, error) {
	var finished *store.Job
	var block time.Duration
	for {
		entries, err := r.s.d.Bus.Read(ctx, r.job, r.cursor, block)
		if err != nil {
			return ending{}, err
		}
		for _, e := range entries {
			r.cursor = e.ID
			f, ok := r.fresh(e.Frame)
			if !ok {
				continue
			}
			e.Frame = f
			if err := fn(e); err != nil {
				return ending{}, err
			}
			if streams.Terminal(f) {
				return ending{final: &f}, nil
			}
			i, isOutput := streamOf(f.Kind)
			if isOutput && r.progress.output(i, f.Data, time.Now()) {
				return ending{}, nil
			}
		}
		if len(entries) == 0 {
			if finished != nil {
				return ending{job: finished}, nil
			}
			job, err := r.s.d.Store.JobByID(ctx, r.job)
			switch {
			case errors.Is(err, store.ErrNotFound):
				return ending{}, ErrNotFound
			case err != nil:
				return ending{}, err
			case job.State != store.JobRunning:
				finished = &job
			}
		}
		if finished != nil {
			block = 0
			continue
		}
		var more bool
		if block, more = r.nextBlock(deadline); !more {
			return ending{}, nil
		}
	}
}

// nextBlock returns how long the next read may wait, reporting false once Idle or deadline is up
func (r *reader) nextBlock(deadline time.Time) (time.Duration, bool) {
	now := time.Now()
	block := readerPoll
	if left, ok := r.progress.idleLeft(now); ok {
		if left <= 0 {
			return 0, false
		}
		block = min(block, left)
	}
	if !deadline.IsZero() {
		left := deadline.Sub(now)
		if left <= 0 {
			return 0, false
		}
		block = min(block, left)
	}
	return block, true
}

// fresh drops output this reader already delivered and trims a frame that overlaps it
func (r *reader) fresh(f frame.Frame) (frame.Frame, bool) {
	if f.Kind == frame.KindSkip {
		r.next = [2]int64{max(r.next[0], f.Out), max(r.next[1], f.Err)}
		return f, true
	}
	i, ok := streamOf(f.Kind)
	if !ok {
		return f, true
	}
	end := f.Offset + int64(len(f.Data))
	if next := r.next[i]; next > f.Offset {
		if end <= next {
			return f, false
		}
		f.Data = f.Data[next-f.Offset:]
		f.Offset = next
	}
	r.next[i] = end
	return f, true
}

/* Cancel */

// Cancel stops a running job and returns its state once it ends or 10 s have passed
func (s *Service) Cancel(ctx context.Context, p store.Placement, jobID string) (string, error) {
	job, err := s.d.Store.JobByID(ctx, jobID)
	if errors.Is(err, store.ErrNotFound) || (err == nil && job.PlacementID != p.ID) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", err
	}
	if job.State != store.JobRunning {
		return job.State, nil
	}
	prov, err := s.provider(p)
	if err != nil {
		return "", err
	}
	if err := s.d.Bus.RequestCancel(ctx, jobID, expiry(p.Deadline)); err != nil {
		return "", err
	}
	if err := runCancel(ctx, prov, *p.ProviderRef, jobID); err != nil {
		// A cancel that never reached the job must not mark its natural exit as cancelled
		clearCtx, done := detached(ctx)
		defer done()
		return "", errors.Join(err, s.d.Bus.ClearCancel(clearCtx, jobID))
	}
	return s.awaitFinish(ctx, jobID)
}

// runCancel runs bugnet-run cancel, which exits 0 once the command is signalled or had ended
func runCancel(ctx context.Context, prov provider.Provider, ref provider.Ref, jobID string) error {
	argv := []string{runnerPath, "cancel", "--job", jobID}
	es, err := prov.Exec(ctx, ref, provider.ExecSpec{Argv: argv})
	if err != nil {
		return fmt.Errorf("run bugnet-run cancel: %w", err)
	}
	_, err = io.Copy(io.Discard, es)
	code, waitErr := es.Wait()
	_ = es.Close()
	if err = errors.Join(err, waitErr); err != nil {
		return fmt.Errorf("run bugnet-run cancel: %w", err)
	}
	if code != 0 {
		return fmt.Errorf("run bugnet-run cancel: exit status %d", code)
	}
	return nil
}

func (s *Service) awaitFinish(ctx context.Context, jobID string) (string, error) {
	tick := time.NewTicker(finishPoll)
	defer tick.Stop()
	deadline := time.Now().Add(cancelWait)
	for {
		job, err := s.d.Store.JobByID(ctx, jobID)
		if err != nil {
			return "", err
		}
		if job.State != store.JobRunning || time.Now().After(deadline) {
			return job.State, nil
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-tick.C:
		}
	}
}

/* Takeover */

// RunScanner takes over jobs whose lease expired, and every 10 s running jobs with no owner
func (s *Service) RunScanner(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(s.ctx, cancel)()
	tick := time.NewTicker(scanEvery)
	defer tick.Stop()
	var adopted time.Time
	for {
		s.takeOverExpired(ctx)
		if time.Since(adopted) >= adoptEvery {
			s.adopt(ctx)
			adopted = time.Now()
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (s *Service) takeOverExpired(ctx context.Context) {
	ids, err := s.d.Bus.ExpiredLeases(ctx, time.Now())
	if err != nil {
		s.scanFailed(ctx, "list expired leases", err)
		return
	}
	for _, id := range ids {
		s.takeover(ctx, id)
	}
}

// adopt covers running jobs that lost every Valkey key, as after a flush
func (s *Service) adopt(ctx context.Context) {
	running, err := s.d.Store.RunningJobs(ctx)
	if err != nil {
		s.scanFailed(ctx, "list running jobs", err)
		return
	}
	for _, j := range running {
		s.takeover(ctx, j.ID)
	}
}

func (s *Service) scanFailed(ctx context.Context, what string, err error) {
	if ctx.Err() == nil {
		s.d.Log.Warn("job scanner failed to "+what, "err", err)
	}
}

func (s *Service) takeover(ctx context.Context, id string) {
	if s.owns(id) {
		return
	}
	lease, ok, err := s.d.Bus.AcquireLease(ctx, id, s.d.ReplicaID)
	if err != nil || !ok {
		if err != nil {
			s.scanFailed(ctx, "acquire a lease", err)
		}
		return
	}
	job, err := s.d.Store.JobByID(ctx, id)
	if errors.Is(err, store.ErrNotFound) || (err == nil && job.State != store.JobRunning) {
		s.giveBack(lease, true)
		return
	}
	var p store.Placement
	if err == nil {
		p, err = s.d.Store.PlacementByID(ctx, job.PlacementID)
	}
	if errors.Is(err, store.ErrNotFound) {
		// A placement with no row is not ready, so the owner records its job lost
		p, err = store.Placement{ID: job.PlacementID}, nil
	}
	if err != nil {
		s.scanFailed(ctx, "take a job over", err)
		s.retryLater(lease)
		return
	}
	// The owner records the job lost when this replica has no provider able to reach it
	prov, err := s.provider(p)
	if errors.Is(err, errNoProvider) {
		s.d.Log.Warn("taking over a job this replica cannot reach", "job", id, "err", err)
	}
	s.d.Log.Info("taking a job over", "job", id, "replica", s.d.ReplicaID)
	s.own(lease, p, prov, nil)
}

// Shutdown stops every owner loop and releases its lease so peers take the jobs over at once
func (s *Service) Shutdown(ctx context.Context) {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.stop()
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

/* Owner loop */

type owner struct {
	s      *Service
	lease  streams.Lease
	place  store.Placement
	prov   provider.Provider // nil once the placement can no longer run anything
	cmd    *command          // nil when the job only needs attaching
	ctx    context.Context
	cancel context.CancelFunc
	// The fields up to mu are kept by the run goroutine
	offsets     [2]int64  // the next stdout and stderr bytes the follower sends
	stream      [2]int64  // the offsets the stream's output and skip frames reach
	stored      int64     // what the stream's output entries cost
	flood       bool      // output passed a budget, so the job is followed by its tail
	noTail      bool      // the sandbox's bugnet-run has no attach --tail
	spent       bool      // the placement's budget refused a snapshot
	skipped     bool      // the stream left output out
	framed      bool      // the job's first frame has been seen and timed
	held        [2][]byte // the newest output between stream and offsets while flooding
	snapped     time.Time // when held output was last stored or passed over
	mu          sync.Mutex
	deadline    *time.Time
	renewPaused atomic.Bool // set by tests to let the lease expire while the loop streams
}

func (s *Service) owns(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.owners[id]
	return ok
}

// own runs an owner loop for the lease's job, or gives the lease back once Shutdown began
func (s *Service) own(lease streams.Lease, p store.Placement, prov provider.Provider, c *command) {
	ctx, cancel := context.WithCancel(s.ctx)
	o := &owner{
		s: s, lease: lease, place: p, prov: prov, cmd: c, ctx: ctx, cancel: cancel,
		deadline: p.Deadline,
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		cancel()
		s.giveBack(lease, false)
		return
	}
	s.owners[lease.JobID()] = o
	s.wg.Add(1)
	s.mu.Unlock()
	go o.run()
}

func (s *Service) forget(o *owner) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.owners[o.lease.JobID()] == o {
		delete(s.owners, o.lease.JobID())
	}
}

// giveBack ends the lease of a finished job, or releases it for a peer to continue the job
func (s *Service) giveBack(l streams.Lease, finished bool) {
	if finished {
		s.leaseCall(l, l.End)
	} else {
		s.leaseCall(l, l.Release)
	}
}

// retryLater hands the lease back for a later takeover, so scanners do not retry every pass
func (s *Service) retryLater(l streams.Lease) {
	s.leaseCall(l, func(ctx context.Context) error {
		return l.Postpone(ctx, time.Now().Add(retryAfter))
	})
}

func (s *Service) leaseCall(l streams.Lease, call func(context.Context) error) {
	ctx, cancel := detached(s.ctx)
	defer cancel()
	if err := call(ctx); err != nil && !errors.Is(err, streams.ErrLeaseLost) {
		s.d.Log.Warn("give a job lease back", "job", l.JobID(), "err", err)
	}
}

func (o *owner) run() {
	defer o.s.wg.Done()
	var renew sync.WaitGroup
	renew.Go(o.renew)
	finished := o.follow()
	o.cancel()
	renew.Wait()
	o.s.forget(o)
	o.s.giveBack(o.lease, finished)
}

func (o *owner) job() string { return o.lease.JobID() }

func (o *owner) warn(what string, err error) {
	if o.ctx.Err() == nil {
		o.s.d.Log.Warn(what, "job", o.job(), "err", err)
	}
}

// renew keeps the lease alive and cancels the loop once the lease is lost
func (o *owner) renew() {
	tick := time.NewTicker(renewEvery)
	defer tick.Stop()
	refresh := time.Now().Add(refreshEvery)
	for {
		select {
		case <-o.ctx.Done():
			return
		case <-tick.C:
		}
		if o.renewPaused.Load() {
			continue
		}
		err := o.lease.Renew(o.ctx)
		if errors.Is(err, streams.ErrLeaseLost) {
			o.cancel()
			return
		}
		if err != nil {
			o.warn("renew job lease", err)
			continue
		}
		if time.Now().After(refresh) {
			o.refreshDeadline()
			refresh = time.Now().Add(refreshEvery)
		}
	}
}

// refreshDeadline follows extensions so the stream outlives the placement's new deadline
func (o *owner) refreshDeadline() {
	p, err := o.s.d.Store.PlacementByID(o.ctx, o.place.ID)
	if err != nil {
		o.warn("reload placement deadline", err)
		return
	}
	o.mu.Lock()
	o.deadline = p.Deadline
	o.mu.Unlock()
}

func (o *owner) leaseNow() streams.Lease {
	l := o.lease
	o.mu.Lock()
	l.StreamExpiry = expiry(o.deadline)
	o.mu.Unlock()
	l.Placement, l.PlacementBudget = o.place.ID.String(), o.s.d.PlacementBudget
	l.PlacementReserve = o.s.d.TailReserve
	return l
}

type pumpResult int

const (
	pumpBroken     pumpResult = iota // the runner stream ended early without delivering a frame
	pumpProgressed                   // the runner stream ended early after delivering frames
	pumpExists                       // start found the job already started
	pumpGap                          // the runner stream skipped output before delivering a frame
	pumpFinished                     // the job ended and is recorded
	pumpStopped                      // the lease is gone, the service stopped or Valkey failed
	pumpFlood                        // the output passed a budget, so the job is followed anew
)

// follow streams the job's frames into its stream and reports whether the job finished
func (o *owner) follow() bool {
	st, err := o.s.d.Bus.LastOffsets(o.ctx, o.job())
	if err != nil {
		o.warn("read job stream offsets", err)
		return false
	}
	o.offsets = [2]int64{st.Out, st.Err}
	o.stream, o.stored, o.skipped = o.offsets, st.Stored, st.Skipped
	o.flood, o.snapped = st.Stored >= o.s.d.JobBudget, time.Now()
	if st.Done {
		return o.finishFromStream()
	}
	if o.prov == nil {
		return o.end(frame.Frame{Kind: frame.KindError, Reason: frame.ReasonJobLost}) ==
			pumpFinished
	}
	start := o.cmd != nil && st.Out == 0 && st.Err == 0
	failures := 0
	for {
		argv := o.attachArgv()
		if start {
			argv = o.startArgv()
		}
		switch r := o.pump(argv); r {
		case pumpFinished:
			return true
		case pumpStopped:
			return false
		case pumpExists, pumpFlood:
			start = false
			continue
		case pumpProgressed:
			start, failures = false, 0
		case pumpGap:
			start = false
		}
		if o.ctx.Err() != nil {
			return false
		}
		if failures++; failures > attachTries {
			lost := frame.Frame{Kind: frame.KindError, Reason: frame.ReasonJobLost}
			return o.end(lost) == pumpFinished
		}
		select {
		case <-o.ctx.Done():
			return false
		case <-time.After(attachBackoff << (failures - 1)):
		}
	}
}

func (o *owner) startArgv() []string {
	argv := []string{runnerPath, "start", "--job", o.job()}
	if o.cmd.cwd != "" {
		argv = append(argv, "--cwd", o.cmd.cwd)
	}
	for _, k := range slices.Sorted(maps.Keys(o.cmd.env)) {
		argv = append(argv, "--env", k+"="+o.cmd.env[k])
	}
	return append(append(argv, "--"), o.cmd.argv...)
}

func (o *owner) attachArgv() []string {
	argv := []string{
		runnerPath, "attach", "--job", o.job(),
		"--out", strconv.FormatInt(o.offsets[0], 10), "--err", strconv.FormatInt(o.offsets[1], 10),
	}
	if o.flood && !o.noTail {
		argv = append(argv, "--tail", strconv.Itoa(floodTail))
	}
	return argv
}

// pump runs one bugnet-run follower and appends its frames until it ends
func (o *owner) pump(argv []string) pumpResult {
	// Cancelling the call ends its stream in every adapter, which Close may not do mid-read
	pctx, pcancel := context.WithCancel(o.ctx)
	defer pcancel()
	idle := time.AfterFunc(idleAfter, pcancel)
	defer idle.Stop()
	es, err := o.prov.Exec(pctx, *o.place.ProviderRef, provider.ExecSpec{Argv: argv})
	if err != nil {
		o.warn("run bugnet-run", err)
		return pumpBroken
	}
	defer func() { _ = es.Close() }()
	result := pumpBroken
	dec := frame.NewDecoder(es)
	for {
		f, err := dec.Next()
		if err != nil {
			switch {
			case pctx.Err() != nil && o.ctx.Err() == nil:
				o.warn("bugnet-run went silent, following it again", err)
			case !errors.Is(err, io.EOF):
				o.warn("decode bugnet-run frames", err)
			}
			return result
		}
		idle.Reset(idleAfter)
		if f.Kind != frame.KindPong {
			o.firstFrame()
		}
		switch {
		case f.Kind == frame.KindPong:
			continue
		case f.Kind == frame.KindError && f.Reason == frame.ReasonJobExists:
			return pumpExists
		case f.Kind == frame.KindError && f.Reason == frame.ReasonBadArgs && o.flood && !o.noTail:
			// A bugnet-run without attach --tail is followed whole and its tail cut here
			o.noTail = true
			return pumpFlood
		case o.flood:
			if r, ended := o.floodFrame(f); ended {
				return r
			}
			result = pumpProgressed
			continue
		case f.Kind == frame.KindSkip:
			continue
		case o.skips(f):
			// A lossy transport dropped output, so the next follower resumes from the stream's end
			if result == pumpProgressed {
				return result
			}
			return pumpGap
		case streams.Terminal(f):
			return o.end(f)
		}
		f, fresh := o.unseen(f)
		if !fresh {
			continue
		}
		if r, kept := o.keep(f); !kept {
			return r
		}
		result = pumpProgressed
	}
}

// firstFrame times the job's first frame from Exec's arrival and stores it, once, off the exec path
// with a commit that does not wait for the disk
//
// A job this owner only attaches to has no arrival to count from and stores nothing.
func (o *owner) firstFrame() {
	if o.framed || o.cmd == nil || o.cmd.arrived.IsZero() {
		return
	}
	o.framed = true
	ms := max(1, int(time.Since(o.cmd.arrived).Milliseconds()))
	id, st := o.job(), o.s.d.Store
	o.s.wg.Go(func() {
		ctx, cancel := detached(o.s.ctx)
		defer cancel()
		if err := st.SetJobFirstFrame(ctx, id, ms); err != nil {
			o.s.d.Log.Warn("store a job's first frame time", "job", id, "err", err)
		}
	})
}

// keep appends a frame of a job within its budget, reporting the pump's result when it cannot
func (o *owner) keep(f frame.Frame) (pumpResult, bool) {
	_, output := streamOf(f.Kind)
	err := streams.ErrOverBudget
	if !output || o.stored+streams.Cost(f) <= o.s.d.JobBudget {
		err = o.append(f)
	}
	switch {
	case errors.Is(err, streams.ErrOverBudget) && output:
		o.startFlood(f)
		return pumpFlood, false
	case errors.Is(err, streams.ErrOverBudget):
		// A heartbeat past the placement's budget is left out, as nothing depends on it
	case err != nil:
		return pumpStopped, false
	case output:
		o.stored += streams.Cost(f)
	}
	return 0, true
}

/* Floods */

// startFlood keeps f out of the stream and follows the job by its tail from now on
func (o *owner) startFlood(f frame.Frame) {
	o.s.d.Log.Info("job output passed its budget, following its tail", "job", o.job(),
		"stored", o.stored)
	o.flood, o.stream, o.snapped = true, o.offsets, time.Now()
	o.hold(f)
}

// floodFrame takes one frame of a flooding job, reporting the pump's result once it must end
func (o *owner) floodFrame(f frame.Frame) (pumpResult, bool) {
	if streams.Terminal(f) {
		if f.Kind == frame.KindExit {
			o.skipTo(f.Out, f.Err)
		}
		return o.end(f), true
	}
	// The held tail is stored before f, since a skip in f would drop it
	if time.Since(o.snapped) >= floodEvery && !o.snapshot(false) {
		return pumpStopped, true
	}
	switch f.Kind {
	case frame.KindSkip:
		o.skipTo(f.Out, f.Err)
	case frame.KindHeartbeat:
		o.skipTo(f.Out, f.Err)
		if !o.beat() {
			return pumpStopped, true
		}
	case frame.KindStdout, frame.KindStderr:
		i, _ := streamOf(f.Kind)
		// A gap is no loss here, since the tail is all that is kept of a flood
		if f.Offset > o.offsets[i] {
			o.held[i], o.offsets[i] = nil, f.Offset
		}
		if f, fresh := o.unseen(f); fresh {
			o.hold(f)
		}
	}
	return 0, false
}

// beat appends a heartbeat naming only offsets the stream covers, which a takeover resumes from
func (o *owner) beat() bool {
	err := o.append(frame.Frame{Kind: frame.KindHeartbeat, Out: o.stream[0], Err: o.stream[1]})
	return err == nil || errors.Is(err, streams.ErrOverBudget)
}

// hold keeps the newest floodTail bytes of an output frame the stream does not have
func (o *owner) hold(f frame.Frame) {
	i, ok := streamOf(f.Kind)
	if !ok {
		return
	}
	o.held[i] = keepLast(o.held[i], f.Data, floodTail)
	o.offsets[i] = f.Offset + int64(len(f.Data))
}

// skipTo moves the outputs to offsets the follower reports, dropping held bytes before a jump
func (o *owner) skipTo(out, errOff int64) {
	for i, at := range [2]int64{out, errOff} {
		if at > o.offsets[i] {
			o.held[i], o.offsets[i] = nil, at
		}
	}
}

// snapshot stores the held tails behind a skip frame, reporting false once the stream fails
func (o *owner) snapshot(final bool) bool {
	if !final && len(o.held[0])+len(o.held[1]) == 0 {
		// With no new output held the next frame that brings some is stored instead
		return true
	}
	o.snapped = time.Now()
	// Before the end, only an allowance of a quarter past the job's budget goes to snapshots
	allowed := final || (!o.spent && o.stored < o.s.d.JobBudget+o.s.d.JobBudget/4)
	if o.offsets == o.stream || !allowed {
		return true
	}
	from := [2]int64{
		o.offsets[0] - int64(len(o.held[0])), o.offsets[1] - int64(len(o.held[1])),
	}
	if o.skipStream(from) != nil {
		return false
	}
	for i, kind := range []frame.Kind{frame.KindStdout, frame.KindStderr} {
		if len(o.held[i]) == 0 {
			continue
		}
		f := frame.Frame{Kind: kind, Offset: from[i], Data: o.held[i]}
		err := o.append(f)
		if errors.Is(err, streams.ErrOverBudget) && final {
			f, err = o.finalTail(i, f)
		}
		if errors.Is(err, streams.ErrOverBudget) {
			// The placement's budget is spent, so the stream only says what it left out
			o.spent = true
			if final {
				continue
			}
			// Unstored output stays held for the final tail, which may draw on the reserve
			return true
		}
		if err != nil {
			return false
		}
		o.stored += streams.Cost(f)
		o.stream[i] = o.offsets[i]
		o.held[i] = nil
	}
	o.held = [2][]byte{}
	return o.skipStream(o.offsets) == nil
}

// finalTail stores f's newest tailSize bytes, the tail Exec returns, from the budget or reserve
func (o *owner) finalTail(i int, f frame.Frame) (frame.Frame, error) {
	if cut := len(f.Data) - tailSize; cut > 0 {
		f.Data, f.Offset = f.Data[cut:], f.Offset+int64(cut)
		to := o.stream
		to[i] = f.Offset
		if err := o.skipStream(to); err != nil {
			return f, err
		}
	}
	return f, o.appendTo(f, true)
}

// skipStream appends a skip frame moving the stream's offsets to to, when they lie behind it
func (o *owner) skipStream(to [2]int64) error {
	if to[0] <= o.stream[0] && to[1] <= o.stream[1] {
		return nil
	}
	to = [2]int64{max(to[0], o.stream[0]), max(to[1], o.stream[1])}
	if err := o.append(frame.Frame{Kind: frame.KindSkip, Out: to[0], Err: to[1]}); err != nil {
		return err
	}
	o.stream, o.skipped = to, true
	return nil
}

// skips reports whether f lies past output the stream lacks
func (o *owner) skips(f frame.Frame) bool {
	switch f.Kind {
	case frame.KindStdout:
		return f.Offset > o.offsets[0]
	case frame.KindStderr:
		return f.Offset > o.offsets[1]
	case frame.KindHeartbeat, frame.KindExit:
		return f.Out > o.offsets[0] || f.Err > o.offsets[1]
	}
	return false
}

// unseen drops output and heartbeats behind the stream's offsets and trims a frame overlapping them
func (o *owner) unseen(f frame.Frame) (frame.Frame, bool) {
	if f.Kind == frame.KindHeartbeat {
		return f, f.Out >= o.offsets[0] && f.Err >= o.offsets[1]
	}
	i, ok := streamOf(f.Kind)
	if !ok {
		return f, true
	}
	next := o.offsets[i]
	if f.Offset+int64(len(f.Data)) <= next {
		return f, false
	}
	if f.Offset < next {
		f.Data = f.Data[next-f.Offset:]
		f.Offset = next
	}
	return f, true
}

// append adds f to the stream, ErrOverBudget refusing output or a heartbeat past the budget
func (o *owner) append(f frame.Frame) error { return o.appendTo(f, false) }

// appendTo adds f, letting the placement's tail reserve take it when it is output of a final tail
func (o *owner) appendTo(f frame.Frame, tail bool) error {
	l := o.leaseNow()
	add := l.Append
	if tail {
		add = l.AppendTail
	}
	if _, err := add(o.ctx, f); err != nil {
		if !errors.Is(err, streams.ErrLeaseLost) && !errors.Is(err, streams.ErrOverBudget) {
			o.warn("append job frame", err)
		}
		return err
	}
	if i, ok := streamOf(f.Kind); ok && !o.flood {
		o.offsets[i] = f.Offset + int64(len(f.Data))
		o.stream[i] = o.offsets[i]
	}
	return nil
}

// end stores what a flood still holds, then appends the job's final frame and records its outcome
func (o *owner) end(f frame.Frame) pumpResult {
	if o.flood && !o.snapshot(true) {
		return pumpStopped
	}
	if f.Kind == frame.KindExit {
		cancelled, err := o.s.d.Bus.CancelRequested(o.ctx, o.job())
		if err != nil {
			o.warn("check job cancel", err)
			return pumpStopped
		}
		if cancelled {
			f.Reason = reasonCancelled
		}
	}
	if o.append(f) != nil || !o.record(f) {
		return pumpStopped
	}
	return pumpFinished
}

// finishFromStream records a job whose final frame a previous owner appended
func (o *owner) finishFromStream() bool {
	last, ok, err := o.s.d.Bus.Last(o.ctx, o.job())
	if err == nil && (!ok || !streams.Terminal(last.Frame)) {
		err = errors.New("the stream holds no final frame")
	}
	if err != nil {
		o.warn("read final job frame", err)
		return false
	}
	return o.record(last.Frame)
}

func (o *owner) record(f frame.Frame) bool {
	state, code := outcome(f)
	out, errBytes := o.offsets[0], o.offsets[1]
	if f.Kind == frame.KindExit {
		out, errBytes = f.Out, f.Err
	}
	ctx, cancel := detached(o.ctx)
	defer cancel()
	err := o.s.d.Store.FinishJob(ctx, o.job(), store.JobOutcome{
		State: state, Code: deref(code), StdoutBytes: out, StderrBytes: errBytes,
		Truncated: o.skipped, OOMKilled: f.Kind == frame.KindExit && f.OOM,
		PeakMemoryMiB: int(f.PeakMiB),
	})
	if err != nil && !errors.Is(err, store.ErrStateConflict) {
		o.s.d.Log.Warn("record job outcome", "job", o.job(), "err", err)
		return false
	}
	if err == nil {
		o.s.d.Sessions.EndJob(ctx, o.place.ID, o.job(), sessions.JobEnd{
			ExitCode: code, OOM: f.Kind == frame.KindExit && f.OOM, PeakMiB: f.PeakMiB,
			OutBytes: out, ErrBytes: errBytes,
		})
	}
	return true
}

func deref(p *int) int {
	if p == nil {
		return 0
	}
	return *p
}

// displayKey holds the argv a session record shows in place of the command a job runs
type displayKey struct{}

// WithDisplay makes the job Exec starts under ctx show argv in the session record
func WithDisplay(ctx context.Context, argv []string) context.Context {
	return context.WithValue(ctx, displayKey{}, argv)
}

// shownArgv is the argv the session record shows, the display argv when ctx carries one
func shownArgv(ctx context.Context, argv []string) []string {
	if d, ok := ctx.Value(displayKey{}).([]string); ok && len(d) > 0 {
		return d
	}
	return argv
}
