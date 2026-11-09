package api_test

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

func (e *env) startBench(
	t *testing.T, req *routerv1.StartBenchRequest,
) (*routerv1.StartBenchResponse, error) {
	t.Helper()
	res, err := e.operator(t, operatorToken).StartBench(t.Context(), connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return res.Msg, nil
}

func benchRequest() *routerv1.StartBenchRequest {
	return &routerv1.StartBenchRequest{
		Provider: "local", Runs: 5, Concurrency: 2, Actor: "ada",
	}
}

func benchJobs(t *testing.T, e *env) int {
	t.Helper()
	var n int
	err := e.pool.QueryRow(t.Context(), `select count(*) from river_job where kind = 'bench'`).
		Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestStartBenchWritesTheRowAndQueuesTheFirstWave(t *testing.T) {
	e := newEnv(t, options{})
	res, err := e.startBench(t, benchRequest())
	if err != nil {
		t.Fatal(err)
	}
	if res.GetEstimatedCostMicros() <= 0 {
		t.Fatalf("estimate %d", res.GetEstimatedCostMicros())
	}
	b, err := e.store.BenchByID(t.Context(), res.GetBenchId())
	if err != nil {
		t.Fatal(err)
	}
	eq(t, [4]any{b.Provider, b.SizeName, b.Runs, b.Concurrency}, [4]any{"local", "small", 5, 2})
	eq(t, b.Actor, "operator:ada")
	eq(t, b.EstimatedCostMicros, res.GetEstimatedCostMicros())
	eq(t, b.FinishedAt.IsZero(), true)
	eq(t, benchJobs(t, e), 1)
}

func TestStartBenchConfirmsCost(t *testing.T) {
	// 5 runs are estimated at 9 micros each
	e := newEnv(t, options{bench: config.Bench{ConfirmAboveMicros: 40, MaxRuns: 200}})
	_, err := e.startBench(t, benchRequest())
	mustCode(t, err, connect.CodeFailedPrecondition, "COST_UNCONFIRMED")
	var info *errdetails.ErrorInfo
	var ce *connect.Error
	if !errors.As(err, &ce) {
		t.Fatalf("error %v is not a Connect error", err)
	}
	for _, d := range ce.Details() {
		if v, derr := d.Value(); derr == nil {
			info, _ = v.(*errdetails.ErrorInfo)
		}
	}
	est, perr := strconv.ParseInt(info.GetMetadata()["estimated_cost_micros"], 10, 64)
	if perr != nil || est <= 40 {
		t.Fatalf("metadata %v", info.GetMetadata())
	}
	eq(t, benchJobs(t, e), 0)

	// A confirmation below the estimate is not enough, one that covers it is
	low := benchRequest()
	low.ConfirmCostMicros = est - 1
	_, err = e.startBench(t, low)
	mustCode(t, err, connect.CodeFailedPrecondition, "COST_UNCONFIRMED")
	ok := benchRequest()
	ok.ConfirmCostMicros = est
	if _, err := e.startBench(t, ok); err != nil {
		t.Fatal(err)
	}
	eq(t, benchJobs(t, e), 1)
}

func TestStartBenchBlockedProvider(t *testing.T) {
	e := newEnv(t, options{})
	_, err := e.store.RecordAccount(t.Context(), "local", "banned", "x", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.startBench(t, benchRequest())
	mustCode(t, err, connect.CodeFailedPrecondition, "PROVIDER_BLOCKED")
	eq(t, benchJobs(t, e), 0)
}

func TestStartBenchChecksItsArguments(t *testing.T) {
	e := newEnv(t, options{bench: config.Bench{ConfirmAboveMicros: 500000, MaxRuns: 10}})
	for name, change := range map[string]func(*routerv1.StartBenchRequest){
		"no actor":       func(r *routerv1.StartBenchRequest) { r.Actor = "" },
		"no runs":        func(r *routerv1.StartBenchRequest) { r.Runs = 0 },
		"too many runs":  func(r *routerv1.StartBenchRequest) { r.Runs = 11 },
		"no concurrency": func(r *routerv1.StartBenchRequest) { r.Concurrency = 0 },
		"too wide":       func(r *routerv1.StartBenchRequest) { r.Concurrency = 5 },
		"unknown":        func(r *routerv1.StartBenchRequest) { r.Provider = "nowhere" },
		"unknown size":   func(r *routerv1.StartBenchRequest) { r.Size = "9x9" },
	} {
		req := benchRequest()
		change(req)
		_, err := e.startBench(t, req)
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("%s: got %v", name, err)
		}
	}
	eq(t, benchJobs(t, e), 0)
}

func TestReportStartsGroupsByKind(t *testing.T) {
	e := newEnv(t, options{})
	res, err := e.startBench(t, benchRequest())
	if err != nil {
		t.Fatal(err)
	}
	add := func(kind store.SampleKind, bench, outcome string, ready int) {
		err := e.store.InsertStartSample(t.Context(), store.StartSample{
			Kind: kind, BenchID: bench, Provider: "local", SizeName: "small",
			StartedAt: time.Now().Add(-time.Minute), Outcome: outcome, CreateMS: ready / 2,
			ReadyMS: ready, RunnerMS: store.NoStep, GateMS: store.NoStep, FirstCmdMS: 40,
			WarmCmdMS: store.NoStep, DestroyMS: store.NoStep, CostMicros: 7,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 1; i <= 4; i++ {
		add(store.SampleBench, res.GetBenchId(), "ok", i*100)
	}
	add(store.SampleBench, res.GetBenchId(), "failed", 0)
	add(store.SampleCanary, "", "ok", 300)
	add(store.SamplePlacement, "", "ok", 200)

	op := e.operator(t, operatorToken)
	got, err := op.ReportStarts(t.Context(), connect.NewRequest(&routerv1.ReportStartsRequest{
		Window: &routerv1.ReportStartsRequest_BenchId{BenchId: res.GetBenchId()},
	}))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, got.Msg.GetPlanned(), uint32(5))
	eq(t, len(got.Msg.GetReports()), 1)
	r := got.Msg.GetReports()[0]
	eq(t, r.GetKind(), routerv1.SampleKind_SAMPLE_KIND_BENCH)
	eq(t, [2]uint32{r.GetOk(), r.GetFailed()}, [2]uint32{4, 1})
	eq(t, r.GetReady().GetP50Ms(), uint32(200))
	eq(t, r.GetCostMicros(), int64(35))

	all, err := op.ReportStarts(t.Context(), connect.NewRequest(&routerv1.ReportStartsRequest{
		Window: &routerv1.ReportStartsRequest_Since{Since: hourAgo()},
	}))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(all.Msg.GetReports()), 3)
	eq(t, all.Msg.GetPlanned(), uint32(0))
	canary, err := op.ReportStarts(t.Context(), connect.NewRequest(&routerv1.ReportStartsRequest{
		Window: &routerv1.ReportStartsRequest_Since{Since: hourAgo()},
		Kinds:  []routerv1.SampleKind{routerv1.SampleKind_SAMPLE_KIND_CANARY},
	}))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(canary.Msg.GetReports()), 1)

	_, err = op.ReportStarts(t.Context(), connect.NewRequest(&routerv1.ReportStartsRequest{
		Window: &routerv1.ReportStartsRequest_BenchId{BenchId: "b_missing"},
	}))
	mustCode(t, err, connect.CodeNotFound, "BENCH_UNKNOWN")
	_, err = op.ReportStarts(t.Context(), connect.NewRequest(&routerv1.ReportStartsRequest{}))
	mustCode(t, err, connect.CodeInvalidArgument, "")
}

func hourAgo() *timestamppb.Timestamp { return timestamppb.New(time.Now().Add(-time.Hour)) }
