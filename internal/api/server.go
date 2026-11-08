// Package api serves the router's Connect services, plus health and readiness checks.
//
// Each caller has its own service and bearer token. The gateway calls SandboxRouterService,
// bugnet-edge EdgeService, bugnet-secrets SecretsRouteService, bugnet-ssh SshGatewayService,
// bugnet-egress EgressService, bugnet-dev's operator commands OperatorService, Payments
// PaymentsService and the sessions role SessionPageService. A service admits only its own token,
// and one whose token is empty admits no calls. RPCs not built yet answer Unimplemented.
//
// Placements go through the saga, commands through the jobs service and file operations
// straight to the placement's provider. Exec, CancelJob and each file operation first spend a
// token of the placement's operation bucket, which every replica shares in Valkey. GetPlacement
// and WatchPlacement wait on the placement's Valkey channel, which one pattern subscription per
// server fans out to the waiting requests. Each wake-up re-reads the store, so a lost message
// only delays a reader until its next read. The edge-side services meter usage against each
// placement's budgets, answer lease checks and serve the edge event stream.
//
// CreatePreview answers the derived link of one port, opening or repairing the placement's gate
// first, and the edge resolves it with ResolvePreview and asks for a repair with RepairPreview,
// which runs at most once a minute per placement. OperatorService's RevokePreview moves a
// preview's generation, so every earlier link dies and its port gets no new one.
//
// Every Placement answer carries its session page link until the record expires. The page reads
// its record through SessionPageService, which checks the link's key in constant time, and
// streams a running job's cleaned output on a Valkey client and byte budget of its own.
//
// CreateTransfer mints a transfer URL, and the edge serves it through OpenTransfer, ReadTransfer
// and WriteTransfer, each stream ending when its placement ends or spends its budget, which one
// edge event reader per server fans out, or when it goes idle or reaches its hard stop. Drain
// ends long polls and watch streams so an HTTP server shutting down can drain its connections
// while transfer streams finish, and Close ends everything.
package api

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math"
	"net/http"
	"regexp"
	"sync"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"github.com/bugnet-sh/bugnet-run/frame"
	"github.com/valkey-io/valkey-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bugnet-sh/bugnet-router/internal/budget"
	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/dpkeys"
	"github.com/bugnet-sh/bugnet-router/internal/edgeevents"
	"github.com/bugnet-sh/bugnet-router/internal/handle"
	"github.com/bugnet-sh/bugnet-router/internal/jobs"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/ratelimit"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/sessions"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/streams"
	"github.com/bugnet-sh/bugnet-router/internal/transfer"
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

// Engine finds the providers that may host a package now
type Engine interface {
	Candidates(in placement.Inputs) ([]placement.Candidate, error)
}

// Deps are the services and settings a Server works with
type Deps struct {
	Store       *store.Store
	Saga        *saga.Saga
	Jobs        *jobs.Service
	Engine      Engine
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
	// Budget sizes the paces the usage reports answer
	Budget config.EdgeBudget
	// Keys derive the tokens of transfers minted under an idempotency key
	Keys dpkeys.Keys
	// Transfers are the transfer settings, a zero field taking its default
	Transfers config.Transfers
	// TransferHost names transfer URLs, and CreateTransfer refuses every call while it is empty
	TransferHost string
	// PreviewDomain names preview links, and CreatePreview refuses every call while it is empty
	PreviewDomain string
	// Previews are the preview settings, a zero max_per_placement taking its default
	Previews config.Previews
	// Sessions are the session page settings, an empty base URL giving no placement a link
	Sessions config.Sessions
	// SessionWriter writes the session page's events, none when nil
	SessionWriter *sessions.Writer
	// LiveValkey is the live view's own client, Valkey when nil
	LiveValkey valkey.Client
}

// Server implements routerv1connect.SandboxRouterServiceHandler
type Server struct {
	// Answers Unimplemented for every RPC of the contract without a method here
	routerv1connect.UnimplementedSandboxRouterServiceHandler
	d        Deps
	notes    *notifier
	ops      *ratelimit.Limiter
	meter    *budget.Meter
	events   *edgeevents.Bus
	hub      *hub
	xfers    *transfer.Store
	x        config.Transfers
	livePage *liveView
	ctx      context.Context // ends when Drain or Close is called
	cancel   context.CancelFunc
	life     context.Context // ends when Close is called, transfer streams run on it
	end      context.CancelFunc
	done     chan struct{}
}

var _ routerv1connect.SandboxRouterServiceHandler = (*Server)(nil)

// New builds a server over deps and starts its placement subscription
func New(deps Deps) *Server {
	if deps.Log == nil {
		deps.Log = slog.Default()
	}
	ops := ratelimit.New(deps.Valkey, deps.Prefix, deps.SandboxOpsPerSecond, deps.SandboxOpsBurst)
	life, end := context.WithCancel(context.Background())
	ctx, cancel := context.WithCancel(life)
	x := transferSettings(deps.Transfers)
	events := edgeevents.New(deps.Valkey, deps.Prefix, deps.Log)
	liveVK := cmp.Or(deps.LiveValkey, deps.Valkey)
	s := &Server{
		d:      deps,
		notes:  newNotifier(deps.Valkey, saga.ChannelPattern(deps.Prefix), deps.Log),
		ops:    ops,
		meter:  budget.New(deps.Valkey, deps.Prefix, deps.Budget),
		events: events,
		hub:    newHub(events, deps.Log),
		xfers: transfer.NewStore(deps.Valkey, deps.Prefix, transfer.Limits{
			MaxTokens: x.MaxTokensPerPlacement, MaxActive: x.MaxActivePerPlacement,
			MaxClaims: x.MaxClaims, TTL: x.URLTTL + x.MaxDuration, Lease: xferLease,
		}),
		x:        x,
		livePage: newLiveView(liveVK, deps.Prefix, deps.Sessions.Live),
		ctx:      ctx,
		cancel:   cancel,
		life:     life,
		end:      end,
		done:     make(chan struct{}),
	}
	var wg sync.WaitGroup
	wg.Go(func() { s.notes.run(life) })
	wg.Go(func() { s.hub.run(life) })
	go func() {
		wg.Wait()
		close(s.done)
	}()
	return s
}

// Drain ends long polls and watch streams, leaving transfer streams to finish
func (s *Server) Drain() { s.cancel() }

// Close ends long polls and every stream and stops the subscriptions, safe to call more than once
func (s *Server) Close() {
	s.cancel()
	s.end()
	<-s.done
}

// Tokens are the callers' bearer tokens, one per service, an empty one admitting no calls
type Tokens struct {
	Internal, Edge, Secrets, SSH, Egress, Operator, Payments, Sessions string
}

// Servers are the handlers of every caller service
type Servers struct {
	Router   *Server
	Edge     *EdgeServer
	Secrets  *SecretsRouteServer
	SSH      *SSHGatewayServer
	Egress   *EgressServer
	Operator *OperatorServer
	Payments *PaymentsServer
	Sessions *SessionPageServer
}

// Servers returns s and one server per other caller service, all sharing s's deps and Close
func (s *Server) Servers() Servers {
	return Servers{
		Router: s, Edge: &EdgeServer{s: s}, Secrets: &SecretsRouteServer{},
		SSH: &SSHGatewayServer{s: s}, Egress: &EgressServer{s: s}, Operator: &OperatorServer{s: s},
		Payments: &PaymentsServer{s: s}, Sessions: &SessionPageServer{s: s, live: s.livePage},
	}
}

// Mount serves each service on mux behind its own token, with /healthz and /readyz left open
func Mount(mux *http.ServeMux, s Servers, t Tokens) {
	opts := func(token string) []connect.HandlerOption {
		return []connect.HandlerOption{
			connect.WithInterceptors(auth{token: []byte(token)}),
			connect.WithReadMaxBytes(maxRequestBytes),
		}
	}
	mux.Handle(routerv1connect.NewSandboxRouterServiceHandler(s.Router, opts(t.Internal)...))
	mux.Handle(withWriter(routerv1connect.NewEdgeServiceHandler(s.Edge, opts(t.Edge)...)))
	mux.Handle(routerv1connect.NewSecretsRouteServiceHandler(s.Secrets, opts(t.Secrets)...))
	mux.Handle(withWriter(routerv1connect.NewSshGatewayServiceHandler(s.SSH, opts(t.SSH)...)))
	mux.Handle(withWriter(routerv1connect.NewEgressServiceHandler(s.Egress, opts(t.Egress)...)))
	mux.Handle(routerv1connect.NewOperatorServiceHandler(s.Operator, opts(t.Operator)...))
	mux.Handle(routerv1connect.NewPaymentsServiceHandler(s.Payments, opts(t.Payments)...))
	mux.Handle(withWriter(routerv1connect.NewSessionPageServiceHandler(s.Sessions,
		opts(t.Sessions)...)))
	mux.HandleFunc("GET /healthz", healthz)
	mux.HandleFunc("GET /readyz", s.Router.readyz)
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
		opts = append(opts, s.startOption(pkg, need))
	}
	return connect.NewResponse(&routerv1.EstimateResponse{Options: opts}), nil
}

// PricePackage answers with one start package as Estimate lists it, writing and spending nothing
func (s *Server) PricePackage(
	_ context.Context, req *connect.Request[routerv1.PricePackageRequest],
) (*connect.Response[routerv1.PricePackageResponse], error) {
	id := req.Msg.GetPackageId()
	pkg, ok := s.d.Catalog.Get(id)
	switch {
	case !ok:
		return nil, withInfo(invalid("unknown package %q", id), reasonPackageUnknown)
	case pkg.ExtensionOf != "":
		ce := invalid("package %q is an extension, a start needs a start package", id)
		return nil, withInfo(ce, reasonNotStartPackage)
	}
	return connect.NewResponse(&routerv1.PricePackageResponse{
		Option: s.startOption(pkg, requirements(req.Msg.GetRequirements())),
	}), nil
}

// startOption prices a start package and says whether a provider can host it now
func (s *Server) startOption(
	pkg catalog.Package, need placement.Requirements,
) *routerv1.PackageOption {
	_, err := s.d.Engine.Candidates(placement.Inputs{
		Package: pkg, Requirements: need, MarginFloor: s.d.MarginFloor,
	})
	reason := ""
	switch {
	case err == nil:
	case errors.Is(err, placement.ErrNoCapableProvider):
		reason = unavailableFeature
	case errors.Is(err, placement.ErrNoCandidate):
		reason = unavailableNoCandidate
	default:
		s.d.Log.Error("api: find candidates", "package", pkg.ID, "err", err)
		reason = unavailableInternal
	}
	return packageOption(pkg, err == nil, reason)
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
		Placement: s.answer(ctx, p), Handle: h,
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
	return connect.NewResponse(&routerv1.GetPlacementResponse{Placement: s.answer(ctx, p)}), nil
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
		if msg := s.answer(ctx, cur); sent == nil || !proto.Equal(msg, sent) {
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
	return connect.NewResponse(&routerv1.ExtendPlacementResponse{Placement: s.answer(ctx, p)}), nil
}

// CheckExtendPlacement answers as a fresh order's extension would, with its deadline and package
func (s *Server) CheckExtendPlacement(
	ctx context.Context, req *connect.Request[routerv1.CheckExtendPlacementRequest],
) (*connect.Response[routerv1.CheckExtendPlacementResponse], error) {
	p, deadline, err := s.d.Saga.CheckExtend(ctx, req.Msg.GetHandle(), req.Msg.GetPackageId())
	if err != nil {
		return nil, s.status(err)
	}
	pkg, ok := s.d.Catalog.Get(req.Msg.GetPackageId())
	if !ok {
		return nil, s.status(errors.New("api: the saga checked a package the catalog lacks"))
	}
	return connect.NewResponse(&routerv1.CheckExtendPlacementResponse{
		Placement: s.answer(ctx, p), Deadline: timestamppb.New(deadline),
		Package: packageOption(pkg, true, ""),
	}), nil
}

// ListExtensions lists the packages that can extend a placement, each as CheckExtendPlacement would
func (s *Server) ListExtensions(
	ctx context.Context, req *connect.Request[routerv1.ListExtensionsRequest],
) (*connect.Response[routerv1.ListExtensionsResponse], error) {
	exts, err := s.d.Saga.ListExtensions(ctx, req.Msg.GetHandle())
	if err != nil {
		return nil, s.status(err)
	}
	opts := make([]*routerv1.PackageOption, 0, len(exts))
	for _, x := range exts {
		opts = append(opts, packageOption(x.Package, x.Refusal == nil, unavailable(x.Refusal)))
	}
	return connect.NewResponse(&routerv1.ListExtensionsResponse{Options: opts}), nil
}

// StopPlacement destroys the sandbox and ends the placement, a stopped one staying as it is
func (s *Server) StopPlacement(
	ctx context.Context, req *connect.Request[routerv1.StopPlacementRequest],
) (*connect.Response[routerv1.StopPlacementResponse], error) {
	p, err := s.d.Saga.Stop(ctx, req.Msg.GetHandle())
	if err != nil {
		return nil, s.status(err)
	}
	return connect.NewResponse(&routerv1.StopPlacementResponse{Placement: s.answer(ctx, p)}), nil
}

// ended reports whether a placement is terminated or failed, which no transition leaves
func ended(state string) bool {
	return state == store.PlacementTerminated || state == store.PlacementFailed
}

// placement looks a handle up, reporting a malformed one as unknown like one never issued
func (s *Server) placement(ctx context.Context, h string) (store.Placement, error) {
	sha, err := handle.Hash(h)
	if err != nil {
		return store.Placement{}, fmt.Errorf("%w: %w", store.ErrUnknownHandle, err)
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
	prov, err := s.home(p)
	if err != nil {
		return store.Placement{}, nil, err
	}
	if err := s.spend(ctx, p); err != nil {
		return store.Placement{}, nil, err
	}
	return p, prov, nil
}

// home is the provider a ready placement's sandbox runs on
func (s *Server) home(p store.Placement) (provider.Provider, error) {
	if p.State != store.PlacementReady || p.ProviderRef == nil || p.Deadline == nil {
		return nil, fmt.Errorf("%w: placement is %s", errNotReady, p.State)
	}
	prov, ok := s.d.Registry.Get(p.ProviderRef.Provider)
	if !ok {
		return nil, fmt.Errorf("provider %q is not configured", p.ProviderRef.Provider)
	}
	return prov, nil
}

// spend takes one of the placement's operation tokens, refusing the operation when none is left
func (s *Server) spend(ctx context.Context, p store.Placement) error {
	tctx, cancel := context.WithTimeout(ctx, opsTimeout)
	defer cancel()
	ok, wait, err := s.ops.Take(tctx, p.ID)
	switch {
	case err != nil && gaveUp(ctx):
		return cmp.Or(ctx.Err(), context.DeadlineExceeded)
	case err != nil:
		// The limit protects the router, so an operation never fails for want of reading it
		s.d.Log.Warn("api: sandbox operation limit unread, letting the operation through",
			"err", err)
	case !ok:
		return rateLimited(wait)
	}
	return nil
}

// gaveUp reports whether ctx ended or passed its deadline, which a socket deadline can see first
func gaveUp(ctx context.Context) bool {
	dl, ok := ctx.Deadline()
	return ctx.Err() != nil || ok && !time.Now().Before(dl)
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
		OomKilled:  res.OOMKilled,
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
				OomKilled: f.OOM, PeakMemoryMib: f.PeakMiB,
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
					fmt.Errorf("job ended with %s", f.Reason))
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
		return store.Job{}, jobs.ErrNotFound
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
	s.d.SessionWriter.File(ctx, p, "read", m.GetPath(), int64(len(data)))
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
	s.d.SessionWriter.File(ctx, p, "write", m.GetPath(), int64(len(m.GetData())))
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
	s.d.SessionWriter.File(ctx, p, "list", m.GetPath(), 0)
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
	return placement.Requirements{
		SSH: r.GetSsh(), Preview: r.GetPreview(), GPU: r.GetGpu(), Secrets: r.GetSecrets(),
	}
}

// packageOption describes a package and whether it is available, reason saying why when not
func packageOption(pkg catalog.Package, available bool, reason string) *routerv1.PackageOption {
	r := pkg.Resources
	return &routerv1.PackageOption{
		PackageId: pkg.ID,
		Resources: &routerv1.Resources{
			Vcpu: uint32(r.VCPU), MemoryMib: uint32(r.MemoryMiB), DiskGib: uint32(r.DiskGiB),
			GpuCount: uint32(r.GPUCount), GpuMemoryGib: uint32(r.GPUMemoryGiB),
		},
		Minutes:           uint32(pkg.Minutes),
		PriceMicros:       pkg.PriceMicros,
		Available:         available,
		UnavailableReason: reason,
	}
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
	// A failed placement says why in failure_reason alone
	if p.State == store.PlacementTerminating || p.State == store.PlacementTerminated {
		out.TerminationReason = p.TerminationReason
	}
	ended := p.State == store.PlacementTerminated || p.State == store.PlacementFailed
	if ended && p.TerminatedAt != nil {
		out.EndedAt = timestamppb.New(*p.TerminatedAt)
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
