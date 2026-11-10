package api

import (
	"context"
	"errors"
	"strconv"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/jackc/pgx/v5"

	"github.com/bugnet-sh/bugnet-router/internal/bench"
	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

var errBenchNoQueue = errors.New("api: benchmarks need the job queue, which this router lacks")

// StartBench checks a benchmark, writes its row and queues its first wave
func (o *OperatorServer) StartBench(
	ctx context.Context, req *connect.Request[routerv1.StartBenchRequest],
) (*connect.Response[routerv1.StartBenchResponse], error) {
	m, s := req.Msg, o.s
	if m.GetActor() == "" {
		return nil, invalid("actor is required")
	}
	if s.d.River == nil {
		return nil, withInfo(connect.NewError(connect.CodeFailedPrecondition, errBenchNoQueue),
			reasonFeatureUnavailable)
	}
	runs, conc := int(m.GetRuns()), int(m.GetConcurrency())
	switch {
	case runs < 1 || runs > s.d.Bench.MaxRuns:
		return nil, invalid("runs must be 1 to %d", s.d.Bench.MaxRuns)
	case conc < 1 || conc > s.d.Health.ProbeWorkers:
		return nil, invalid("concurrency must be 1 to %d", s.d.Health.ProbeWorkers)
	}
	prov, ok := s.d.Registry.Get(m.GetProvider())
	if !ok {
		return nil, invalid("provider is not configured")
	}
	healths, err := s.d.Store.ProviderHealths(ctx)
	if err != nil {
		return nil, s.status(err)
	}
	for _, h := range healths {
		if h.Provider == prov.Name() && h.Blocked != "" {
			return nil, withInfo(connect.NewError(connect.CodeFailedPrecondition,
				errors.New("the provider's account is blocked")), reasonProviderBlocked)
		}
	}
	size, ok := bench.SizeNamed(prov, m.GetSize())
	if !ok {
		return nil, invalid("the provider sells no size by that name")
	}
	estimate := bench.Estimate(prov, size, runs)
	if estimate > s.d.Bench.ConfirmAboveMicros && m.GetConfirmCostMicros() < estimate {
		return nil, withMeta(connect.NewError(connect.CodeFailedPrecondition,
			errors.New("the estimated cost needs confirming")), reasonCostUnconfirmed,
			map[string]string{"estimated_cost_micros": strconv.FormatInt(estimate, 10)})
	}
	b := store.Bench{
		ID: bench.NewID(time.Now()), Provider: prov.Name(), SizeName: size.Name, Runs: runs,
		Concurrency: conc, EstimatedCostMicros: estimate, Actor: "operator:" + m.GetActor(),
		CreatedAt: time.Now(),
	}
	err = s.d.Store.WithTx(ctx, func(tx pgx.Tx) error {
		if err := s.d.Store.Tx(tx).InsertBench(ctx, b); err != nil {
			return err
		}
		_, err := s.d.River.InsertTx(ctx, tx, jobargs.Bench{
			BenchID: b.ID, AllowPaid: m.GetAllowPaidStarts(),
		}, nil)
		return err
	})
	if err != nil {
		return nil, s.status(err)
	}
	s.d.Log.Info("operator started a benchmark", "actor", b.Actor, "bench", b.ID,
		"provider", b.Provider, "size", b.SizeName, "runs", runs, "concurrency", conc,
		"estimated_cost_micros", estimate, "allow_paid_starts", m.GetAllowPaidStarts())
	return connect.NewResponse(&routerv1.StartBenchResponse{
		BenchId: b.ID, EstimatedCostMicros: estimate,
	}), nil
}

// ReportStarts groups the start samples of a benchmark or a time window by provider, size, kind and
// start path
func (o *OperatorServer) ReportStarts(
	ctx context.Context, req *connect.Request[routerv1.ReportStartsRequest],
) (*connect.Response[routerv1.ReportStartsResponse], error) {
	m, s := req.Msg, o.s
	f := store.SampleFilter{Provider: m.GetProvider(), Paths: m.GetStartPaths()}
	for _, k := range m.GetKinds() {
		kind, ok := sampleKinds[k]
		if !ok {
			return nil, invalid("kinds must name placement, canary or bench")
		}
		f.Kinds = append(f.Kinds, kind)
	}
	out := &routerv1.ReportStartsResponse{}
	switch w := m.GetWindow().(type) {
	case *routerv1.ReportStartsRequest_BenchId:
		b, err := s.d.Store.BenchByID(ctx, w.BenchId)
		if errors.Is(err, store.ErrNotFound) {
			return nil, withInfo(connect.NewError(connect.CodeNotFound,
				errors.New("no benchmark has that id")), reasonBenchUnknown)
		}
		if err != nil {
			return nil, s.status(err)
		}
		f.BenchID, out.Planned = b.ID, uint32(b.Runs)
	case *routerv1.ReportStartsRequest_Since:
		f.Since = w.Since.AsTime()
	default:
		return nil, invalid("a bench_id or a since time is required")
	}
	samples, err := s.d.Store.SampleQuery(ctx, f)
	if err != nil {
		return nil, s.status(err)
	}
	for _, r := range bench.Build(samples) {
		out.Reports = append(out.Reports, startReportMsg(r))
	}
	return connect.NewResponse(out), nil
}

// sampleKinds maps the proto's sample kinds to the store's
var sampleKinds = map[routerv1.SampleKind]store.SampleKind{
	routerv1.SampleKind_SAMPLE_KIND_PLACEMENT: store.SamplePlacement,
	routerv1.SampleKind_SAMPLE_KIND_CANARY:    store.SampleCanary,
	routerv1.SampleKind_SAMPLE_KIND_BENCH:     store.SampleBench,
}

func startReportMsg(r bench.Report) *routerv1.StartReport {
	out := &routerv1.StartReport{
		Provider: r.Provider, Size: r.Size, Ok: uint32(r.OK), Failed: uint32(r.Failed),
		Capacity: uint32(r.Refused), SuccessRate: r.SuccessRate,
		SuccessLowerBound: r.SuccessLower, Create: stepMsg(r.Create), Ready: stepMsg(r.Ready),
		Runner: stepMsg(r.Runner), Gate: stepMsg(r.Gate), FirstCommand: stepMsg(r.FirstCmd),
		WarmCommand: stepMsg(r.WarmCmd), Destroy: stepMsg(r.Destroy), Total: stepMsg(r.Total),
		CostMicros: r.CostMicros, Errors: map[string]uint32{}, StartPath: r.StartPath,
		Bind: stepMsg(r.Bind), PaidToReady: stepMsg(r.PaidToReady),
		FirstFrame: stepMsg(r.FirstFrame), PaidToFirstFrame: stepMsg(r.PaidToFirstFrame),
		Wake: stepMsg(r.Wake),
	}
	for k, v := range sampleKinds {
		if v == r.Kind {
			out.Kind = k
			break
		}
	}
	for code, n := range r.Errors {
		out.Errors[code] = uint32(n)
	}
	return out
}

func stepMsg(s bench.Steps) *routerv1.StepStats {
	return &routerv1.StepStats{
		Samples: uint32(s.Samples), P50Ms: uint32(s.P50), P90Ms: uint32(s.P90),
		P99Ms: uint32(s.P99),
	}
}
