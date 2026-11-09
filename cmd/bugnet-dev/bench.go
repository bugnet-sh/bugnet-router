package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bugnet-sh/bugnet-router/internal/bench"
	"github.com/bugnet-sh/bugnet-router/internal/ordertoken"
	"github.com/bugnet-sh/bugnet-router/internal/simulate"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// startPathCold is the start path router mode measures until Reserve and the pools exist
const startPathCold = "cold"

// benchCmd runs a benchmark subcommand
func (c *cli) benchCmd(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("%w: bench takes start, report or router", errUsage)
	}
	sub := map[string]func([]string) error{
		"start": c.benchStart, "report": c.benchReport, "router": c.benchRouter,
	}[args[0]]
	if sub == nil {
		return fmt.Errorf("%w: bench takes start, report or router", errUsage)
	}
	return sub(args[1:])
}

func (c *cli) benchStart(args []string) error {
	fs := newFlags("bench start")
	prov := fs.String("provider", "", "provider to measure")
	size := fs.String("size", "", "size name, the provider's smallest when empty")
	runs := fs.Int("runs", 30, "probes to run")
	conc := fs.Int("concurrency", 3, "probes at a time")
	yes := fs.Bool("yes", false, "accept the estimated cost")
	paid := fs.Bool("allow-paid-starts", false, "go on when a start costs a fee")
	actor := fs.String("actor", "", "who starts it, $USER when empty")
	if err := parse(fs, args, false); err != nil {
		return err
	}
	if err := required("provider", *prov); err != nil {
		return err
	}
	who, err := c.actorOf(*actor)
	if err != nil {
		return err
	}
	op, err := c.operator()
	if err != nil {
		return err
	}
	req := &routerv1.StartBenchRequest{
		Provider: *prov, Size: *size, Runs: uint32(*runs), Concurrency: uint32(*conc),
		AllowPaidStarts: *paid, Actor: who,
	}
	res, err := op.StartBench(c.ctx, connect.NewRequest(req))
	if est, ok := unconfirmed(err); ok {
		if err := c.print([][2]string{{"estimated_cost", usd(est)}}); err != nil {
			return err
		}
		if !*yes {
			return fmt.Errorf("the estimate is over the confirmation limit, run again with --yes")
		}
		req.ConfirmCostMicros = est
		res, err = op.StartBench(c.ctx, connect.NewRequest(req))
	}
	if err != nil {
		return err
	}
	return c.print([][2]string{
		{"bench", res.Msg.GetBenchId()},
		{"estimated_cost", usd(res.Msg.GetEstimatedCostMicros())},
	})
}

// unconfirmed reads the estimate out of a COST_UNCONFIRMED refusal
func unconfirmed(err error) (int64, bool) {
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return 0, false
	}
	for _, d := range ce.Details() {
		v, derr := d.Value()
		if derr != nil {
			continue
		}
		if info, ok := v.(*errdetails.ErrorInfo); ok && info.GetReason() == "COST_UNCONFIRMED" {
			n, perr := strconv.ParseInt(info.GetMetadata()["estimated_cost_micros"], 10, 64)
			return n, perr == nil
		}
	}
	return 0, false
}

func usd(micros int64) string { return fmt.Sprintf("$%.3f", float64(micros)/1e6) }

func (c *cli) benchReport(args []string) error {
	fs := newFlags("bench report")
	id := fs.String("bench-id", "", "one benchmark")
	since := fs.Duration("since", 24*time.Hour, "every sample since this long ago")
	kinds := fs.String("kind", "", "bench, canary or placement, comma separated, all when empty")
	prov := fs.String("provider", "", "one provider, all when empty")
	asJSON := fs.Bool("json", false, "print the reports as JSON")
	if err := parse(fs, args, false); err != nil {
		return err
	}
	req := &routerv1.ReportStartsRequest{Provider: *prov}
	if *id != "" {
		req.Window = &routerv1.ReportStartsRequest_BenchId{BenchId: *id}
	} else {
		req.Window = &routerv1.ReportStartsRequest_Since{
			Since: timestamppb.New(time.Now().Add(-*since)),
		}
	}
	for _, k := range strings.FieldsFunc(*kinds, func(r rune) bool { return r == ',' }) {
		kind, ok := map[string]routerv1.SampleKind{
			"placement": routerv1.SampleKind_SAMPLE_KIND_PLACEMENT,
			"canary":    routerv1.SampleKind_SAMPLE_KIND_CANARY,
			"bench":     routerv1.SampleKind_SAMPLE_KIND_BENCH,
		}[strings.TrimSpace(k)]
		if !ok {
			return fmt.Errorf("%w: --kind takes bench, canary or placement", errUsage)
		}
		req.Kinds = append(req.Kinds, kind)
	}
	op, err := c.operator()
	if err != nil {
		return err
	}
	res, err := op.ReportStarts(c.ctx, connect.NewRequest(req))
	if err != nil {
		return err
	}
	if *asJSON {
		b, err := protojson.MarshalOptions{Multiline: true}.Marshal(res.Msg)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(c.stdout, string(b))
		return err
	}
	for _, r := range res.Msg.GetReports() {
		head := fmt.Sprintf("%s %s  %s", r.GetProvider(), r.GetSize(), kindName(r.GetKind()))
		if *id != "" {
			head = fmt.Sprintf("bench %s  %s  %d runs planned", *id, head, res.Msg.GetPlanned())
		}
		if err := bench.Format(c.stdout, head, reportOf(r), 0); err != nil {
			return err
		}
	}
	return nil
}

func kindName(k routerv1.SampleKind) string {
	switch k {
	case routerv1.SampleKind_SAMPLE_KIND_PLACEMENT:
		return "placement"
	case routerv1.SampleKind_SAMPLE_KIND_CANARY:
		return "canary"
	case routerv1.SampleKind_SAMPLE_KIND_BENCH:
		return "probe"
	}
	return "unknown"
}

// reportOf is the bench report of a StartReport message
func reportOf(r *routerv1.StartReport) bench.Report {
	steps := func(s *routerv1.StepStats) bench.Steps {
		return bench.Steps{
			Samples: int(s.GetSamples()), P50: int(s.GetP50Ms()), P90: int(s.GetP90Ms()),
			P99: int(s.GetP99Ms()),
		}
	}
	return bench.Report{
		Provider: r.GetProvider(), Size: r.GetSize(), OK: int(r.GetOk()),
		Failed: int(r.GetFailed()), Refused: int(r.GetCapacity()), SuccessRate: r.GetSuccessRate(),
		SuccessLower: r.GetSuccessLowerBound(), Create: steps(r.GetCreate()),
		Ready: steps(r.GetReady()), Runner: steps(r.GetRunner()), Gate: steps(r.GetGate()),
		FirstCmd: steps(r.GetFirstCommand()), WarmCmd: steps(r.GetWarmCommand()),
		Destroy: steps(r.GetDestroy()), Total: steps(r.GetTotal()), CostMicros: r.GetCostMicros(),
	}
}

/* Router mode */

// routerRun is one order timed on the client, from CreatePlacement to the first command's exit
type routerRun struct {
	ReadyMS    int    `json:"ready_ms"`
	FirstCmdMS int    `json:"first_cmd_ms"`
	TotalMS    int    `json:"total_ms"`
	Error      string `json:"error,omitempty"`
}

func (c *cli) benchRouter(args []string) error {
	fs := newFlags("bench router")
	pkg := fs.String("package", "", "package to order")
	runs := fs.Int("runs", 20, "orders to place")
	conc := fs.Int("concurrency", 2, "orders at a time")
	asJSON := fs.Bool("json", false, "print the raw samples as JSON")
	if err := parse(fs, args, false); err != nil {
		return err
	}
	if err := required("package", *pkg); err != nil {
		return err
	}
	if *runs < 1 || *conc < 1 {
		return fmt.Errorf("%w: --runs and --concurrency must be at least 1", errUsage)
	}
	// Every run signs its own order token, so a missing key stops the benchmark first
	if _, err := c.sign(order{kind: ordertoken.KindStart, pkg: *pkg, ttl: defaultTTL}); err != nil {
		return err
	}
	api, err := c.client()
	if err != nil {
		return err
	}
	out := make([]routerRun, *runs)
	slots := make(chan struct{}, *conc)
	var wg sync.WaitGroup
	for i := range *runs {
		slots <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			out[i] = c.timeOrder(api, *pkg)
		}()
	}
	wg.Wait()
	if *asJSON {
		b, err := json.MarshalIndent(map[string]any{
			"start_path": startPathCold, "package": *pkg, "samples": out,
		}, "", "  ")
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(c.stdout, string(b))
		return err
	}
	return bench.Format(c.stdout, fmt.Sprintf(
		"router %s  start_path %s  %d runs, concurrency %d", *pkg, startPathCold, *runs, *conc),
		routerReport(*pkg, out), 0)
}

// routerReport turns the timed orders into a report whose total is payment to first command
func routerReport(pkg string, runs []routerRun) bench.Report {
	r := bench.Report{Provider: "router", Size: pkg, Errors: map[string]int{}}
	var ready, first, total []int
	for _, run := range runs {
		if run.Error != "" {
			r.Failed++
			continue
		}
		r.OK++
		ready = append(ready, run.ReadyMS)
		first = append(first, run.FirstCmdMS)
		total = append(total, run.TotalMS)
	}
	if n := r.OK + r.Failed; n > 0 {
		r.SuccessRate = float64(r.OK) / float64(n)
	}
	stat := func(ms []int) bench.Steps {
		return bench.Steps{
			Samples: len(ms), P50: bench.Percentile(ms, 0.5), P90: bench.Percentile(ms, 0.9),
			P99: bench.Percentile(ms, 0.99),
		}
	}
	r.Ready, r.FirstCmd, r.Total = stat(ready), stat(first), stat(total)
	return r
}

// timeOrder places one order through CreatePlacement, runs `true` through Exec and stops it
func (c *cli) timeOrder(api routerv1connect.SandboxRouterServiceClient, pkg string) routerRun {
	var run routerRun
	fail := func(err error) routerRun {
		run.Error = err.Error()
		return run
	}
	token, err := c.sign(order{kind: ordertoken.KindStart, pkg: pkg, ttl: defaultTTL})
	if err != nil {
		return fail(err)
	}
	start := time.Now()
	res, err := api.CreatePlacement(c.ctx, connect.NewRequest(&routerv1.CreatePlacementRequest{
		OrderToken: token,
	}))
	if err != nil {
		return fail(err)
	}
	h, p := res.Msg.GetHandle(), res.Msg.GetPlacement()
	defer func() {
		_, _ = api.StopPlacement(c.ctx, connect.NewRequest(&routerv1.StopPlacementRequest{
			Handle: h,
		}))
	}()
	for p.GetState() == routerv1.PlacementState_PLACEMENT_STATE_PLACING {
		r, err := api.GetPlacement(c.ctx, connect.NewRequest(&routerv1.GetPlacementRequest{
			Handle: h, WaitMs: pollWaitMs, KnownState: p.GetState(),
		}))
		if err != nil {
			return fail(err)
		}
		p = r.Msg.GetPlacement()
	}
	if p.GetState() != routerv1.PlacementState_PLACEMENT_STATE_READY {
		return fail(fmt.Errorf("placement is %s", stateName(p.GetState())))
	}
	run.ReadyMS = int(time.Since(start) / time.Millisecond)
	first := time.Now()
	e, err := api.Exec(c.ctx, connect.NewRequest(&routerv1.ExecRequest{
		Handle: h, Argv: []string{"true"}, WaitMs: pollWaitMs,
	}))
	if err != nil {
		return fail(err)
	}
	if e.Msg.GetState() != routerv1.JobState_JOB_STATE_EXITED || e.Msg.GetExitCode() != 0 {
		return fail(fmt.Errorf("the command ended %s with code %d", e.Msg.GetState(),
			e.Msg.GetExitCode()))
	}
	run.FirstCmdMS = int(time.Since(first) / time.Millisecond)
	run.TotalMS = int(time.Since(start) / time.Millisecond)
	return run
}

/* Simulator */

func (c *cli) simulate(args []string) error {
	fs := newFlags("simulate")
	since := fs.Duration("since", 7*24*time.Hour, "replay the samples of this long ago")
	budgets := fs.String("budget-seconds", "30", "placing budget, several separated by commas")
	provs := fs.String("providers", "", "providers in the order tried, by name when empty")
	orders := fs.Int("orders", 1000, "orders to replay per setting")
	asJSON := fs.Bool("json", false, "print the results as JSON")
	if err := parse(fs, args, false); err != nil {
		return err
	}
	var settings []simulate.Settings
	for _, raw := range strings.Split(*budgets, ",") {
		secs, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil || secs < 1 {
			return fmt.Errorf("%w: --budget-seconds takes whole seconds", errUsage)
		}
		settings = append(settings, simulate.Settings{
			Budget: time.Duration(secs) * time.Second, Orders: *orders, Seed: 1,
		})
	}
	url := c.getenv("BUGNET_DATABASE_URL")
	if url == "" {
		return errors.New("BUGNET_DATABASE_URL is not set")
	}
	pool, err := pgxpool.New(c.ctx, url)
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	defer pool.Close()
	samples, err := store.New(pool).SampleQuery(c.ctx, store.SampleFilter{
		Since: time.Now().Add(-*since),
	})
	if err != nil {
		return err
	}
	names := splitList(*provs)
	if len(names) == 0 {
		for _, m := range samples {
			if !slices.Contains(names, m.Provider) {
				names = append(names, m.Provider)
			}
		}
		slices.Sort(names)
	}
	type row struct {
		BudgetSeconds   int     `json:"budget_seconds"`
		Ready           float64 `json:"ready"`
		FailedOver      float64 `json:"failed_over"`
		Refunded        float64 `json:"refunded"`
		ReadyP50Seconds float64 `json:"ready_p50_seconds"`
		ReadyP90Seconds float64 `json:"ready_p90_seconds"`
	}
	var rows []row
	for _, s := range settings {
		s.Providers = names
		r := simulate.Run(samples, s)
		rows = append(rows, row{
			BudgetSeconds: int(s.Budget.Seconds()), Ready: r.ReadyShare(),
			FailedOver: r.FailoverShare(), Refunded: r.RefundShare(),
			ReadyP50Seconds: r.ReadyP50.Seconds(), ReadyP90Seconds: r.ReadyP90.Seconds(),
		})
	}
	if *asJSON {
		b, err := json.MarshalIndent(rows, "", "  ")
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(c.stdout, string(b))
		return err
	}
	if _, err := fmt.Fprintf(c.stdout, "%d samples, providers %s\n", len(samples),
		strings.Join(names, ",")); err != nil {
		return err
	}
	for _, r := range rows {
		_, err := fmt.Fprintf(c.stdout,
			"budget %3d s  ready %5.1f%%  failed over %5.1f%%  refunded %5.1f%%  "+
				"p50 %.2f s  p90 %.2f s\n", r.BudgetSeconds, r.Ready*100, r.FailedOver*100,
			r.Refunded*100, r.ReadyP50Seconds, r.ReadyP90Seconds)
		if err != nil {
			return err
		}
	}
	return nil
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
