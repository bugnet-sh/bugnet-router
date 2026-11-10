package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

type benchOperator struct {
	routerv1connect.UnimplementedOperatorServiceHandler
	estimate int64
	starts   []*routerv1.StartBenchRequest
	reports  []*routerv1.ReportStartsRequest
}

func (o *benchOperator) StartBench(
	_ context.Context, req *connect.Request[routerv1.StartBenchRequest],
) (*connect.Response[routerv1.StartBenchResponse], error) {
	o.starts = append(o.starts, req.Msg)
	if req.Msg.GetConfirmCostMicros() < o.estimate {
		ce := connect.NewError(connect.CodeFailedPrecondition, errors.New("confirm the cost"))
		d, err := connect.NewErrorDetail(&errdetails.ErrorInfo{
			Reason: "COST_UNCONFIRMED", Domain: "bugnet.router",
			Metadata: map[string]string{"estimated_cost_micros": strconv.FormatInt(o.estimate, 10)},
		})
		if err != nil {
			return nil, err
		}
		ce.AddDetail(d)
		return nil, ce
	}
	return connect.NewResponse(&routerv1.StartBenchResponse{
		BenchId: "b_20261105_1412_k3qz", EstimatedCostMicros: o.estimate,
	}), nil
}

func (o *benchOperator) ReportStarts(
	_ context.Context, req *connect.Request[routerv1.ReportStartsRequest],
) (*connect.Response[routerv1.ReportStartsResponse], error) {
	o.reports = append(o.reports, req.Msg)
	steps := func(p50 uint32) *routerv1.StepStats {
		return &routerv1.StepStats{Samples: 29, P50Ms: p50, P90Ms: p50 * 2, P99Ms: p50 * 3}
	}
	return connect.NewResponse(&routerv1.ReportStartsResponse{
		Planned: 30,
		Reports: []*routerv1.StartReport{{
			Provider: "e2b", Size: "2x2", Kind: routerv1.SampleKind_SAMPLE_KIND_BENCH, Ok: 29,
			Failed: 1, SuccessRate: 29.0 / 30, SuccessLowerBound: 0.833, Create: steps(610),
			Ready: steps(660), FirstCommand: steps(90), Total: steps(2080), CostMicros: 22000,
			Errors: map[string]uint32{"timeout": 1},
		}},
	}), nil
}

func serveBench(t *testing.T, o *benchOperator) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(routerv1connect.NewOperatorServiceHandler(o))
	srv := httptest.NewUnstartedServer(mux)
	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	srv.Config.Protocols = &protocols
	srv.Start()
	t.Cleanup(srv.Close)
	return writeSettings(t, "[api]\nlisten_addr = \""+srv.Listener.Addr().String()+"\"\n")
}

var operatorVars = map[string]string{
	"BUGNET_OPERATOR_TOKEN": strings.Repeat("o", 32), "USER": "ada",
}

func TestBenchStartAsksForConfirmationOfACostlyRun(t *testing.T) {
	o := &benchOperator{estimate: 750000}
	settings := serveBench(t, o)
	_, stderr, code := runCLI(t, env(operatorVars), "--config", settings, "bench", "start",
		"--provider", "e2b", "--runs", "30")
	eq(t, code, 1)
	if !strings.Contains(stderr, "--yes") {
		t.Fatalf("stderr %q", stderr)
	}
	eq(t, len(o.starts), 1)

	out, stderr, code := runCLI(t, env(operatorVars), "--config", settings, "bench", "start",
		"--provider", "e2b", "--runs", "30", "--concurrency", "2", "--size", "2x2", "--yes",
		"--allow-paid-starts")
	if code != 0 {
		t.Fatalf("exited %d: %s", code, stderr)
	}
	eq(t, len(o.starts), 3)
	got := o.starts[2]
	eq(t, got.GetConfirmCostMicros(), int64(750000))
	eq(t, [3]uint32{got.GetRuns(), got.GetConcurrency(), 0}, [3]uint32{30, 2, 0})
	eq(t, [3]any{got.GetProvider(), got.GetSize(), got.GetActor()}, [3]any{"e2b", "2x2", "ada"})
	eq(t, got.GetAllowPaidStarts(), true)
	eq(t, strings.Contains(out, "bench=b_20261105_1412_k3qz"), true)
	eq(t, strings.Contains(out, "estimated_cost=$0.750"), true)
}

func TestBenchStartNeedsNoConfirmationUnderTheLimit(t *testing.T) {
	o := &benchOperator{estimate: 0}
	settings := serveBench(t, o)
	_, stderr, code := runCLI(t, env(operatorVars), "--config", settings, "bench", "start",
		"--provider", "e2b")
	if code != 0 {
		t.Fatalf("exited %d: %s", code, stderr)
	}
	eq(t, len(o.starts), 1)
	eq(t, o.starts[0].GetRuns(), uint32(30))
	eq(t, o.starts[0].GetConcurrency(), uint32(3))
}

func TestBenchReportPrintsTheTable(t *testing.T) {
	o := &benchOperator{}
	settings := serveBench(t, o)
	out, stderr, code := runCLI(t, env(operatorVars), "--config", settings, "bench", "report",
		"--bench-id", "b_1", "--kind", "bench,canary")
	if code != 0 {
		t.Fatalf("exited %d: %s", code, stderr)
	}
	eq(t, o.reports[0].GetBenchId(), "b_1")
	eq(t, len(o.reports[0].GetKinds()), 2)
	for _, want := range []string{"bench b_1  e2b 2x2  probe  30 runs planned",
		"ok 29   failed 1", "0.61 s", "1.83 s", "2.08 s", "$0.022 measured"} {
		if !strings.Contains(out, want) {
			t.Errorf("report lacks %q:\n%s", want, out)
		}
	}
	out, _, code = runCLI(t, env(operatorVars), "--config", settings, "bench", "report",
		"--since", "48h", "--provider", "e2b", "--json")
	eq(t, code, 0)
	var parsed struct {
		Reports []struct{ Provider string } `json:"reports"`
		Planned int                         `json:"planned"`
	}
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("json %q: %v", out, err)
	}
	eq(t, parsed.Reports[0].Provider, "e2b")
	if since := o.reports[1].GetSince().AsTime(); time.Since(since) < 47*time.Hour {
		t.Fatalf("since %v", since)
	}
	eq(t, o.reports[1].GetProvider(), "e2b")
}

func TestBenchReportRefusesAnUnknownKind(t *testing.T) {
	_, _, code := runCLI(t, env(operatorVars), "bench", "report", "--kind", "weird")
	eq(t, code, 2)
}

// timedRouter places every order at once and runs true at once
type timedRouter struct {
	routerv1connect.UnimplementedSandboxRouterServiceHandler
	created, stopped, execed atomic.Int32
}

func (r *timedRouter) CreatePlacement(
	_ context.Context, _ *connect.Request[routerv1.CreatePlacementRequest],
) (*connect.Response[routerv1.CreatePlacementResponse], error) {
	n := r.created.Add(1)
	time.Sleep(5 * time.Millisecond)
	return connect.NewResponse(&routerv1.CreatePlacementResponse{
		Handle: "sbx_" + strconv.Itoa(int(n)),
		Placement: &routerv1.Placement{
			State: routerv1.PlacementState_PLACEMENT_STATE_READY,
		},
	}), nil
}

func (r *timedRouter) Exec(
	_ context.Context, req *connect.Request[routerv1.ExecRequest],
) (*connect.Response[routerv1.ExecResponse], error) {
	r.execed.Add(1)
	code := int32(0)
	if strings.HasSuffix(req.Msg.GetHandle(), "_3") {
		code = 1
	}
	return connect.NewResponse(&routerv1.ExecResponse{
		State: routerv1.JobState_JOB_STATE_EXITED, ExitCode: &code,
	}), nil
}

func (r *timedRouter) StopPlacement(
	_ context.Context, _ *connect.Request[routerv1.StopPlacementRequest],
) (*connect.Response[routerv1.StopPlacementResponse], error) {
	r.stopped.Add(1)
	return connect.NewResponse(&routerv1.StopPlacementResponse{}), nil
}

func TestBenchRouterTimesOrdersAndStopsThem(t *testing.T) {
	r := &timedRouter{}
	settings := serve(t, r)
	packages := filepath.Join(t.TempDir(), "packages.json")
	if err := os.WriteFile(packages, []byte(catalogJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	settings = appendTo(t, settings, "\npackages_file = \""+packages+"\"\n")
	_, priv := newKey(t)
	vars := env(map[string]string{
		"BUGNET_INTERNAL_TOKEN": strings.Repeat("i", 32), "BUGNET_ORDER_PRIVATE_KEY": priv,
	})
	out, stderr, code := runCLI(t, vars, "--config", settings, "bench", "router",
		"--package", "small-10m", "--runs", "6", "--concurrency", "3")
	if code != 0 {
		t.Fatalf("exited %d: %s", code, stderr)
	}
	eq(t, r.created.Load(), int32(6))
	eq(t, r.stopped.Load(), int32(6))
	for _, want := range []string{"start_path plain", "6 runs, concurrency 3", "ok 5   failed 1",
		"total"} {
		if !strings.Contains(out, want) {
			t.Errorf("table lacks %q:\n%s", want, out)
		}
	}
	out, _, code = runCLI(t, vars, "--config", settings, "bench", "router",
		"--package", "small-10m", "--runs", "2", "--json")
	eq(t, code, 0)
	var parsed struct {
		StartPath string           `json:"start_path"`
		Samples   []map[string]any `json:"samples"`
	}
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("json %q: %v", out, err)
	}
	eq(t, parsed.StartPath, "plain")
	eq(t, len(parsed.Samples), 2)
}

// appendTo adds text to a settings file, which must end its api table
func appendTo(t *testing.T, path, text string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// A top-level key goes before the first table
	body := strings.TrimPrefix(text, "\n") + string(b)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestBenchRouterNeedsAnOrderKey(t *testing.T) {
	_, _, code := runCLI(t, env(map[string]string{"BUGNET_INTERNAL_TOKEN": "x"}),
		"bench", "router", "--package", "small-10m")
	eq(t, code, 1)
}

func TestSimulateReadsTheRecordedSamples(t *testing.T) {
	url := testenv.DatabaseURL(t)
	if err := store.Migrate(t.Context(), url); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(t.Context(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	st := store.New(pool)
	add := func(name, outcome string, ready int) {
		err := st.InsertStartSample(t.Context(), store.StartSample{
			Kind: store.SampleCanary, Provider: name, SizeName: "s", StartedAt: time.Now(),
			Outcome: outcome, CreateMS: ready / 2, ReadyMS: ready, RunnerMS: store.NoStep,
			GateMS: store.NoStep, FirstCmdMS: store.NoStep, WarmCmdMS: store.NoStep,
			DestroyMS: store.NoStep,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for range 5 {
		add("down", "failed", 2000)
		add("up", "ok", 1500)
	}
	vars := env(map[string]string{"BUGNET_DATABASE_URL": url})
	out, stderr, code := runCLI(t, vars, "simulate", "--providers", "down,up",
		"--budget-seconds", "10,30", "--orders", "200")
	if code != 0 {
		t.Fatalf("exited %d: %s", code, stderr)
	}
	for _, want := range []string{"10 samples", "budget  10 s  ready 100.0%  failed over 100.0%",
		"budget  30 s", "p50 3.50 s"} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
	out, _, code = runCLI(t, vars, "simulate", "--json", "--orders", "50")
	eq(t, code, 0)
	var rows []map[string]float64
	if err := json.Unmarshal([]byte(out), &rows); err != nil {
		t.Fatalf("json %q: %v", out, err)
	}
	eq(t, len(rows), 1)
	_, _, code = runCLI(t, env(nil), "simulate")
	eq(t, code, 1)
	_, _, code = runCLI(t, vars, "simulate", "--budget-seconds", "soon")
	eq(t, code, 2)
}
