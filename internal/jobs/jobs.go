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
package jobs

import (
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

	"github.com/google/uuid"
	"github.com/oynozan/bugnet-run/frame"

	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/store"
	"github.com/oynozan/bugnet-router/internal/streams"
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
)

var errNoProvider = errors.New("jobs: provider is not configured")

// Deps are the services and settings a Service works with
type Deps struct {
	Store     *store.Store
	Bus       *streams.Bus
	Registry  *provider.Registry
	ReplicaID string       // the replica part of every lease this service takes
	Log       *slog.Logger // slog.Default when nil
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
	Truncated  bool
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
}

// Exec starts argv in the placement, or attaches to the job a repeated key already started
func (s *Service) Exec(
	ctx context.Context, p store.Placement, argv []string, cwd string,
	env map[string]string, idemKey string, wait time.Duration, until Until,
) (ExecResult, error) {
	if len(argv) == 0 {
		return ExecResult{}, fmt.Errorf("%w: no argv", ErrInvalid)
	}
	for k := range env {
		if k == "" || strings.Contains(k, "=") {
			return ExecResult{}, fmt.Errorf("%w: env key %q", ErrInvalid, k)
		}
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
	nj := store.NewJob{ID: newJobID(p.ID, idemKey), PlacementID: p.ID, IdempotencyKey: idemKey}
	c := &command{argv: argv, cwd: cwd, env: env}
	if err := s.begin(ctx, p, prov, nj, c, deadline); err != nil {
		return ExecResult{}, err
	}
	return s.collect(ctx, nj.ID, until, deadline)
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
			return fmt.Errorf("%w: %s was never recorded", ErrNotFound, nj.ID)
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
	job, _, err := s.d.Store.CreateJob(ctx, nj)
	if err != nil {
		s.giveBack(lease, true)
		return err
	}
	if job.State == store.JobRunning {
		s.own(lease, p, prov, c)
	} else {
		s.giveBack(lease, true)
	}
	return nil
}

type tails struct {
	data [2][]byte
	size [2]int64
}

func (t *tails) add(f frame.Frame) {
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
		Truncated:  t.size[0] > tailSize || t.size[1] > tailSize,
	}
	switch {
	case end.final != nil:
		res.State, res.ExitCode = outcome(*end.final)
	case end.job != nil:
		res.State, res.ExitCode = end.job.State, end.job.ExitCode
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
				return ending{}, fmt.Errorf("%w: %s", ErrNotFound, r.job)
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
		return "", fmt.Errorf("%w: %s", ErrNotFound, jobID)
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
	// offsets are the next stdout and stderr bytes the stream lacks, kept by the run goroutine
	offsets     [2]int64
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
)

// follow streams the job's frames into its stream and reports whether the job finished
func (o *owner) follow() bool {
	out, errOff, done, err := o.s.d.Bus.LastOffsets(o.ctx, o.job())
	if err != nil {
		o.warn("read job stream offsets", err)
		return false
	}
	o.offsets = [2]int64{out, errOff}
	if done {
		return o.finishFromStream()
	}
	if o.prov == nil {
		return o.end(frame.Frame{Kind: frame.KindError, Reason: frame.ReasonJobLost}) ==
			pumpFinished
	}
	start := o.cmd != nil && out == 0 && errOff == 0
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
		case pumpExists:
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
	return []string{
		runnerPath, "attach", "--job", o.job(),
		"--out", strconv.FormatInt(o.offsets[0], 10), "--err", strconv.FormatInt(o.offsets[1], 10),
	}
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
		switch {
		case f.Kind == frame.KindPong:
			continue
		case f.Kind == frame.KindError && f.Reason == frame.ReasonJobExists:
			return pumpExists
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
		if !o.append(f) {
			return pumpStopped
		}
		result = pumpProgressed
	}
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

func (o *owner) append(f frame.Frame) bool {
	if _, err := o.leaseNow().Append(o.ctx, f); err != nil {
		if !errors.Is(err, streams.ErrLeaseLost) {
			o.warn("append job frame", err)
		}
		return false
	}
	if i, ok := streamOf(f.Kind); ok {
		o.offsets[i] = f.Offset + int64(len(f.Data))
	}
	return true
}

// end appends the job's final frame and records its outcome
func (o *owner) end(f frame.Frame) pumpResult {
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
	if !o.append(f) || !o.record(f) {
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
	err := o.s.d.Store.FinishJob(ctx, o.job(), state, deref(code), out, errBytes)
	if err != nil && !errors.Is(err, store.ErrStateConflict) {
		o.s.d.Log.Warn("record job outcome", "job", o.job(), "err", err)
		return false
	}
	return true
}

func deref(p *int) int {
	if p == nil {
		return 0
	}
	return *p
}
