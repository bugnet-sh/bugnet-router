// Package api serves the SandboxRouterService over Connect, plus health and readiness checks.
//
// Placements go through the saga, commands through the jobs service and file operations
// straight to the placement's provider. Exec, CancelJob and each file operation first spend a
// token of the placement's operation bucket, which every replica shares in Valkey. GetPlacement
// and WatchPlacement wait on the placement's Valkey channel, which one pattern subscription per
// server fans out to the waiting requests. Each wake-up re-reads the store, so a lost message
// only delays a reader until its next read. Close ends long polls and streams so an HTTP server
// shutting down can drain its connections.
package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math"
	"net/http"
	"regexp"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"github.com/oynozan/bugnet-run/frame"
	"github.com/valkey-io/valkey-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/oynozan/bugnet-router/internal/catalog"
	"github.com/oynozan/bugnet-router/internal/handle"
	"github.com/oynozan/bugnet-router/internal/jobs"
	"github.com/oynozan/bugnet-router/internal/placement"
	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/ratelimit"
	"github.com/oynozan/bugnet-router/internal/saga"
	"github.com/oynozan/bugnet-router/internal/store"
	"github.com/oynozan/bugnet-router/internal/streams"
)

const (
	maxPollWait     = 25 * time.Second
	maxFileBytes    = 256 << 10
	maxListDepth    = 5
	maxListEntries  = 10000
	defaultFileMode = 0o644
	maxRequestBytes = 4 << 20
	readyTimeout    = 2 * time.Second
	// opsTimeout is how long an operation waits on its limit before going through unchecked
	opsTimeout = 500 * time.Millisecond
)

// Deps are the services and settings a Server works with
type Deps struct {
	Store       *store.Store
	Saga        *saga.Saga
	Jobs        *jobs.Service
	Engine      *placement.Engine
	Catalog     *catalog.Catalog
	Registry    *provider.Registry
	Valkey      valkey.Client
	Prefix      string
	MarginFloor float64
	Log         *slog.Logger // slog.Default when nil
	// SandboxOpsPerSecond refills each placement's operation bucket, a default when zero
	SandboxOpsPerSecond int
	// SandboxOpsBurst is what a placement's full operation bucket holds, a default when zero
	SandboxOpsBurst int
}

// Server implements routerv1connect.SandboxRouterServiceHandler
type Server struct {
	d      Deps
	notes  *notifier
	ops    *ratelimit.Limiter
	ctx    context.Context // ends when Close is called
	cancel context.CancelFunc
	done   chan struct{}
}

var _ routerv1connect.SandboxRouterServiceHandler = (*Server)(nil)

// New builds a server over deps and starts its placement subscription
func New(deps Deps) *Server {
	if deps.Log == nil {
		deps.Log = slog.Default()
	}
	ops := ratelimit.New(deps.Valkey, deps.Prefix, deps.SandboxOpsPerSecond, deps.SandboxOpsBurst)
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		d:      deps,
		notes:  newNotifier(deps.Valkey, saga.ChannelPattern(deps.Prefix), deps.Log),
		ops:    ops,
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go func() {
		defer close(s.done)
		s.notes.run(ctx)
	}()
	return s
}

// Close ends long polls and streams and stops the subscription, safe to call more than once
func (s *Server) Close() {
	s.cancel()
	<-s.done
}

// Mount serves s on mux behind the internal token, with /healthz and /readyz left open
func Mount(mux *http.ServeMux, s *Server, internalToken string) {
	path, h := routerv1connect.NewSandboxRouterServiceHandler(s,
		connect.WithInterceptors(auth{token: []byte(internalToken)}),
		connect.WithReadMaxBytes(maxRequestBytes))
	mux.Handle(path, h)
	mux.HandleFunc("GET /healthz", healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
}

/* Health */

func healthz(w http.ResponseWriter, _ *http.Request) {
	_, _ = io.WriteString(w, "ok\n")
}

// readyz reports whether Postgres and Valkey both answer
func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
	defer cancel()
	checks := []struct {
		name string
		ping func(context.Context) error
	}{
		{"postgres", s.d.Store.Ping},
		{"valkey", func(ctx context.Context) error {
			return s.d.Valkey.Do(ctx, s.d.Valkey.B().Ping().Build()).Error()
		}},
	}
	for _, c := range checks {
		if err := c.ping(ctx); err != nil {
			s.d.Log.Warn("not ready", "dependency", c.name, "err", err)
			http.Error(w, c.name+" is unreachable", http.StatusServiceUnavailable)
			return
		}
	}
	_, _ = io.WriteString(w, "ready\n")
}

/* Placements */

// Estimate lists the start packages and whether a provider can host each one now
func (s *Server) Estimate(
	_ context.Context, req *connect.Request[routerv1.EstimateRequest],
) (*connect.Response[routerv1.EstimateResponse], error) {
	need := requirements(req.Msg.GetRequirements())
	var opts []*routerv1.PackageOption
	for _, pkg := range s.d.Catalog.All() {
		if pkg.ExtensionOf != "" {
			continue
		}
		r := pkg.Resources
		opt := &routerv1.PackageOption{
			PackageId: pkg.ID,
			Resources: &routerv1.Resources{
				Vcpu: uint32(r.VCPU), MemoryMib: uint32(r.MemoryMiB), DiskGib: uint32(r.DiskGiB),
				GpuCount: uint32(r.GPUCount), GpuMemoryGib: uint32(r.GPUMemoryGiB),
			},
			Minutes:     uint32(pkg.Minutes),
			PriceMicros: pkg.PriceMicros,
		}
		_, err := s.d.Engine.Candidates(placement.Inputs{
			Package: pkg, Requirements: need, MarginFloor: s.d.MarginFloor,
		})
		switch {
		case err == nil:
			opt.Available = true
		case errors.Is(err, placement.ErrNoCandidate):
			opt.UnavailableReason = saga.ReasonNoCandidate
		default:
			opt.UnavailableReason = err.Error()
		}
		opts = append(opts, opt)
	}
	return connect.NewResponse(&routerv1.EstimateResponse{Options: opts}), nil
}

// CreatePlacement starts placing a paid order, a repeated order returning the same placement
func (s *Server) CreatePlacement(
	ctx context.Context, req *connect.Request[routerv1.CreatePlacementRequest],
) (*connect.Response[routerv1.CreatePlacementResponse], error) {
	p, h, err := s.d.Saga.Create(ctx, req.Msg.GetOrderToken(),
		requirements(req.Msg.GetRequirements()))
	if err != nil {
		return nil, s.status(err)
	}
	return connect.NewResponse(&routerv1.CreatePlacementResponse{
		Placement: placementMsg(p), Handle: h,
	}), nil
}

// GetPlacement waits up to wait_ms, at most 25 s, while a placement not yet ended is in known_state
func (s *Server) GetPlacement(
	ctx context.Context, req *connect.Request[routerv1.GetPlacementRequest],
) (*connect.Response[routerv1.GetPlacementResponse], error) {
	p, err := s.placement(ctx, req.Msg.GetHandle())
	if err != nil {
		return nil, s.status(err)
	}
	known := req.Msg.GetKnownState()
	wait := min(time.Duration(req.Msg.GetWaitMs())*time.Millisecond, maxPollWait)
	if known != routerv1.PlacementState_PLACEMENT_STATE_UNSPECIFIED &&
		placementState(p.State) == known && !ended(p.State) && wait > 0 {
		if p, err = s.awaitChange(ctx, p, known, wait); err != nil {
			return nil, s.status(err)
		}
	}
	return connect.NewResponse(&routerv1.GetPlacementResponse{Placement: placementMsg(p)}), nil
}

// awaitChange re-reads the placement on every message until it leaves known or wait passes
func (s *Server) awaitChange(
	ctx context.Context, p store.Placement, known routerv1.PlacementState, wait time.Duration,
) (store.Placement, error) {
	// Subscribing before the read means no change can slip in between the two
	wake, stop := s.notes.watch(saga.Channel(s.d.Prefix, p.ID))
	defer stop()
	timer := time.NewTimer(wait)
	defer timer.Stop()
	last := false
	for {
		cur, err := s.d.Store.PlacementByID(ctx, p.ID)
		if err != nil || last || placementState(cur.State) != known {
			return cur, err
		}
		select {
		case <-wake:
		case <-timer.C:
			last = true
		case <-s.ctx.Done():
			last = true
		case <-ctx.Done():
			return store.Placement{}, ctx.Err()
		}
	}
}

// WatchPlacement sends the placement, then every change, until it is terminated or failed
func (s *Server) WatchPlacement(
	ctx context.Context, req *connect.Request[routerv1.WatchPlacementRequest],
	stream *connect.ServerStream[routerv1.WatchPlacementResponse],
) error {
	p, err := s.placement(ctx, req.Msg.GetHandle())
	if err != nil {
		return s.status(err)
	}
	wake, stop := s.notes.watch(saga.Channel(s.d.Prefix, p.ID))
	defer stop()
	// A lost message delays the stream only until the next periodic read
	tick := time.NewTicker(maxPollWait)
	defer tick.Stop()
	var sent *routerv1.Placement
	for {
		cur, err := s.d.Store.PlacementByID(ctx, p.ID)
		if err != nil {
			return s.status(err)
		}
		if msg := placementMsg(cur); sent == nil || !proto.Equal(msg, sent) {
			if err := stream.Send(&routerv1.WatchPlacementResponse{Placement: msg}); err != nil {
				return err
			}
			sent = msg
		}
		if ended(cur.State) {
			return nil
		}
		select {
		case <-wake:
		case <-tick.C:
		case <-s.ctx.Done():
			return s.status(errShuttingDown)
		case <-ctx.Done():
			return s.status(ctx.Err())
		}
	}
}

// ExtendPlacement moves a ready placement's deadline once per extension order
func (s *Server) ExtendPlacement(
	ctx context.Context, req *connect.Request[routerv1.ExtendPlacementRequest],
) (*connect.Response[routerv1.ExtendPlacementResponse], error) {
	p, err := s.d.Saga.Extend(ctx, req.Msg.GetHandle(), req.Msg.GetOrderToken())
	if err != nil {
		return nil, s.status(err)
	}
	return connect.NewResponse(&routerv1.ExtendPlacementResponse{Placement: placementMsg(p)}), nil
}

// CheckExtendPlacement answers as ExtendPlacement would for a fresh order, with the new deadline
func (s *Server) CheckExtendPlacement(
	ctx context.Context, req *connect.Request[routerv1.CheckExtendPlacementRequest],
) (*connect.Response[routerv1.CheckExtendPlacementResponse], error) {
	p, deadline, err := s.d.Saga.CheckExtend(ctx, req.Msg.GetHandle(), req.Msg.GetPackageId())
	if err != nil {
		return nil, s.status(err)
	}
	return connect.NewResponse(&routerv1.CheckExtendPlacementResponse{
		Placement: placementMsg(p), Deadline: timestamppb.New(deadline),
	}), nil
}

// StopPlacement destroys the sandbox and ends the placement, a stopped one staying as it is
func (s *Server) StopPlacement(
	ctx context.Context, req *connect.Request[routerv1.StopPlacementRequest],
) (*connect.Response[routerv1.StopPlacementResponse], error) {
	p, err := s.d.Saga.Stop(ctx, req.Msg.GetHandle())
	if err != nil {
		return nil, s.status(err)
	}
	return connect.NewResponse(&routerv1.StopPlacementResponse{Placement: placementMsg(p)}), nil
}

// ended reports whether a placement is terminated or failed, which no transition leaves
func ended(state string) bool {
	return state == store.PlacementTerminated || state == store.PlacementFailed
}

// placement looks a handle up, reporting a malformed one as not found like an unknown one
func (s *Server) placement(ctx context.Context, h string) (store.Placement, error) {
	sha, err := handle.Hash(h)
	if err != nil {
		return store.Placement{}, fmt.Errorf("%w: %w", store.ErrNotFound, err)
	}
	return s.d.Store.PlacementByHandle(ctx, sha)
}

// sandbox returns a ready placement and the provider its sandbox runs on, spending an operation
func (s *Server) sandbox(
	ctx context.Context, h string,
) (store.Placement, provider.Provider, error) {
	p, err := s.placement(ctx, h)
	if err != nil {
		return store.Placement{}, nil, err
	}
	if p.State != store.PlacementReady || p.ProviderRef == nil {
		return store.Placement{}, nil, fmt.Errorf("%w: placement is %s", errNotReady, p.State)
	}
	prov, ok := s.d.Registry.Get(p.ProviderRef.Provider)
	if !ok {
		return store.Placement{}, nil, fmt.Errorf("provider %q is not configured",
			p.ProviderRef.Provider)
	}
	if err := s.spend(ctx, p); err != nil {
		return store.Placement{}, nil, err
	}
	return p, prov, nil
}

// spend takes one of the placement's operation tokens, refusing the operation when none is left
func (s *Server) spend(ctx context.Context, p store.Placement) error {
	tctx, cancel := context.WithTimeout(ctx, opsTimeout)
	defer cancel()
	ok, wait, err := s.ops.Take(tctx, p.ID)
	switch {
	case err != nil && ctx.Err() != nil:
		return ctx.Err()
	case err != nil:
		// The limit protects the router, so an operation never fails for want of reading it
		s.d.Log.Warn("api: sandbox operation limit unread, letting the operation through",
			"err", err)
	case !ok:
		return rateLimited(wait)
	}
	return nil
}

/* Jobs */

// Exec runs a command in a ready placement and returns what it printed by the time Until is met
func (s *Server) Exec(
	ctx context.Context, req *connect.Request[routerv1.ExecRequest],
) (*connect.Response[routerv1.ExecResponse], error) {
	m := req.Msg
	if len(m.GetArgv()) == 0 {
		return nil, invalid("argv is required")
	}
	if err := jobs.CheckEnv(m.GetEnv()); err != nil {
		return nil, s.status(err)
	}
	until, err := untilOf(m.GetUntil())
	if err != nil {
		return nil, err
	}
	p, _, err := s.sandbox(ctx, m.GetHandle())
	if err != nil {
		return nil, s.status(err)
	}
	// The jobs service waits 25 s when wait_ms is 0 and never more than 45 s
	wait := time.Duration(m.GetWaitMs()) * time.Millisecond
	res, err := s.d.Jobs.Exec(ctx, p, m.GetArgv(), m.GetCwd(), m.GetEnv(),
		m.GetIdempotencyKey(), wait, until)
	if err != nil {
		return nil, s.status(err)
	}
	out := &routerv1.ExecResponse{
		JobId:      res.JobID,
		State:      jobState(res.State),
		StdoutTail: res.StdoutTail,
		StderrTail: res.StderrTail,
		Cursor:     res.Cursor,
		Truncated:  res.Truncated,
	}
	if res.ExitCode != nil {
		code := int32(*res.ExitCode)
		out.ExitCode = &code
	}
	return connect.NewResponse(out), nil
}

// WatchJob streams a job's output after cursor, each message carrying its own cursor
func (s *Server) WatchJob(
	ctx context.Context, req *connect.Request[routerv1.WatchJobRequest],
	stream *connect.ServerStream[routerv1.WatchJobResponse],
) error {
	m := req.Msg
	until, err := untilOf(m.GetUntil())
	if err != nil {
		return err
	}
	job, err := s.job(ctx, m.GetHandle(), m.GetJobId())
	if err != nil {
		return s.status(err)
	}
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(s.ctx, cancel)()
	err = s.d.Jobs.Watch(wctx, job.ID, m.GetCursor(), until, func(e streams.Entry) error {
		f := e.Frame
		resp := &routerv1.WatchJobResponse{Cursor: e.ID}
		switch f.Kind {
		case frame.KindStdout, frame.KindStderr:
			out := routerv1.OutputStream_OUTPUT_STREAM_STDOUT
			if f.Kind == frame.KindStderr {
				out = routerv1.OutputStream_OUTPUT_STREAM_STDERR
			}
			resp.Frame = &routerv1.WatchJobResponse_Output{Output: &routerv1.JobOutput{
				Stream: out, Offset: uint64(f.Offset), Data: f.Data,
			}}
		case frame.KindExit:
			resp.Frame = &routerv1.WatchJobResponse_Exit{Exit: &routerv1.JobExit{
				ExitCode: int32(f.Code), StdoutBytes: uint64(f.Out), StderrBytes: uint64(f.Err),
			}}
		case frame.KindHeartbeat:
			resp.Frame = &routerv1.WatchJobResponse_Heartbeat{Heartbeat: true}
		case frame.KindSkip:
			resp.Frame = &routerv1.WatchJobResponse_Skip{Skip: &routerv1.JobSkip{
				StdoutOffset: uint64(f.Out), StderrOffset: uint64(f.Err),
			}}
		case frame.KindError:
			if streams.Terminal(f) {
				ce := connect.NewError(connect.CodeAborted,
					fmt.Errorf("job %s ended with %s", job.ID, f.Reason))
				return withInfo(ce, f.Reason)
			}
			return nil
		default:
			return nil
		}
		return stream.Send(resp)
	})
	switch {
	case err == nil:
		return nil
	case ctx.Err() != nil:
		return s.status(ctx.Err())
	case s.ctx.Err() != nil:
		return s.status(errShuttingDown)
	}
	return s.status(err)
}

// CancelJob stops a running job and returns its state once it ends or 10 s have passed
func (s *Server) CancelJob(
	ctx context.Context, req *connect.Request[routerv1.CancelJobRequest],
) (*connect.Response[routerv1.CancelJobResponse], error) {
	p, err := s.placement(ctx, req.Msg.GetHandle())
	if err == nil {
		err = s.spend(ctx, p)
	}
	if err != nil {
		return nil, s.status(err)
	}
	state, err := s.d.Jobs.Cancel(ctx, p, req.Msg.GetJobId())
	if err != nil {
		return nil, s.status(err)
	}
	return connect.NewResponse(&routerv1.CancelJobResponse{State: jobState(state)}), nil
}

// job returns a job of the handle's placement, a job of any other placement being not found
func (s *Server) job(ctx context.Context, h, jobID string) (store.Job, error) {
	p, err := s.placement(ctx, h)
	if err != nil {
		return store.Job{}, err
	}
	job, err := s.d.Store.JobByID(ctx, jobID)
	if errors.Is(err, store.ErrNotFound) || (err == nil && job.PlacementID != p.ID) {
		return store.Job{}, fmt.Errorf("%w: %s", jobs.ErrNotFound, jobID)
	}
	return job, err
}

func untilOf(u *routerv1.Until) (jobs.Until, error) {
	out := jobs.Until{Exit: u.GetExit(), Idle: time.Duration(u.GetIdleMs()) * time.Millisecond}
	if pattern := u.GetOutputPattern(); pattern != "" {
		re, err := regexp.Compile(pattern)
		if err != nil {
			return jobs.Until{}, invalid("output_pattern: %w", err)
		}
		out.Pattern = re
	}
	return out, nil
}

/* Files */

// ReadFile reads up to length bytes, 256 KiB when length is 0
func (s *Server) ReadFile(
	ctx context.Context, req *connect.Request[routerv1.ReadFileRequest],
) (*connect.Response[routerv1.ReadFileResponse], error) {
	m := req.Msg
	length := m.GetLength()
	switch {
	case length == 0:
		length = maxFileBytes
	case length > maxFileBytes:
		return nil, invalid("length %d is over %d bytes", length, maxFileBytes)
	}
	if m.GetOffset() > math.MaxInt64 {
		return nil, invalid("offset %d is too large", m.GetOffset())
	}
	p, prov, err := s.sandbox(ctx, m.GetHandle())
	if err != nil {
		return nil, s.status(err)
	}
	data, eof, err := prov.ReadFile(ctx, *p.ProviderRef, m.GetPath(), int64(m.GetOffset()),
		int(length))
	if err != nil {
		return nil, s.fileStatus(err)
	}
	return connect.NewResponse(&routerv1.ReadFileResponse{Data: data, Eof: eof}), nil
}

// WriteFile replaces a file of at most 256 KiB, with mode 0644 when mode is 0
func (s *Server) WriteFile(
	ctx context.Context, req *connect.Request[routerv1.WriteFileRequest],
) (*connect.Response[routerv1.WriteFileResponse], error) {
	m := req.Msg
	if len(m.GetData()) > maxFileBytes {
		return nil, invalid("data of %d bytes is over %d", len(m.GetData()), maxFileBytes)
	}
	mode := fs.FileMode(m.GetMode())
	switch {
	case mode == 0:
		mode = defaultFileMode
	case mode&^fs.ModePerm != 0:
		return nil, invalid("mode %#o has bits beyond the permission bits", m.GetMode())
	}
	p, prov, err := s.sandbox(ctx, m.GetHandle())
	if err != nil {
		return nil, s.status(err)
	}
	if err := prov.WriteFile(ctx, *p.ProviderRef, m.GetPath(), m.GetData(), mode); err != nil {
		return nil, s.fileStatus(err)
	}
	return connect.NewResponse(&routerv1.WriteFileResponse{Size: uint64(len(m.GetData()))}), nil
}

// ListFiles lists at most 10,000 entries down to depth levels, 1 when depth is 0 and at most 5
func (s *Server) ListFiles(
	ctx context.Context, req *connect.Request[routerv1.ListFilesRequest],
) (*connect.Response[routerv1.ListFilesResponse], error) {
	m := req.Msg
	depth := max(m.GetDepth(), 1)
	if depth > maxListDepth {
		return nil, invalid("depth %d is over %d", depth, maxListDepth)
	}
	p, prov, err := s.sandbox(ctx, m.GetHandle())
	if err != nil {
		return nil, s.status(err)
	}
	listing := provider.NewListing(maxListEntries)
	if err := prov.ListFiles(ctx, *p.ProviderRef, m.GetPath(), int(depth), listing); err != nil {
		return nil, s.fileStatus(err)
	}
	out := make([]*routerv1.FileEntry, 0, len(listing.Entries))
	for _, e := range listing.Entries {
		out = append(out, &routerv1.FileEntry{
			Path:       e.Path,
			Size:       uint64(max(e.Size, 0)),
			IsDir:      e.IsDir,
			Mode:       uint32(e.Mode.Perm()),
			ModifiedAt: timestamppb.New(e.Modified),
		})
	}
	return connect.NewResponse(&routerv1.ListFilesResponse{
		Entries: out, Truncated: listing.Truncated,
	}), nil
}

/* Messages */

func requirements(r *routerv1.Requirements) placement.Requirements {
	return placement.Requirements{SSH: r.GetSsh(), Preview: r.GetPreview(), GPU: r.GetGpu()}
}

func placementMsg(p store.Placement) *routerv1.Placement {
	out := &routerv1.Placement{
		PlacementId:   p.ID.String(),
		State:         placementState(p.State),
		PackageId:     p.PackageID,
		CreatedAt:     timestamppb.New(p.CreatedAt),
		FailureReason: p.FailureReason,
		Refundable:    p.Refundable,
	}
	if p.Deadline != nil {
		out.Deadline = timestamppb.New(*p.Deadline)
	}
	if p.ReadyAt != nil {
		out.ReadyAt = timestamppb.New(*p.ReadyAt)
	}
	return out
}

var placementStates = map[string]routerv1.PlacementState{
	store.PlacementPlacing:     routerv1.PlacementState_PLACEMENT_STATE_PLACING,
	store.PlacementReady:       routerv1.PlacementState_PLACEMENT_STATE_READY,
	store.PlacementTerminating: routerv1.PlacementState_PLACEMENT_STATE_TERMINATING,
	store.PlacementTerminated:  routerv1.PlacementState_PLACEMENT_STATE_TERMINATED,
	store.PlacementFailed:      routerv1.PlacementState_PLACEMENT_STATE_FAILED,
}

func placementState(state string) routerv1.PlacementState { return placementStates[state] }

var jobStates = map[string]routerv1.JobState{
	store.JobRunning:   routerv1.JobState_JOB_STATE_RUNNING,
	store.JobExited:    routerv1.JobState_JOB_STATE_EXITED,
	store.JobCancelled: routerv1.JobState_JOB_STATE_CANCELLED,
	store.JobLost:      routerv1.JobState_JOB_STATE_LOST,
}

func jobState(state string) routerv1.JobState { return jobStates[state] }
