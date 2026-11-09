package worker_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/bench"
	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/worker"
)

// startBench stores a benchmark of runs on name and queues its first wave
func startBench(
	t *testing.T, e *env, name string, runs, concurrency int, allowPaid bool,
) store.Bench {
	t.Helper()
	prov, ok := e.registry.Get(name)
	if !ok {
		t.Fatalf("no provider %s", name)
	}
	size, _ := bench.SizeNamed(prov, "")
	b := store.Bench{
		ID: bench.NewID(time.Now()), Provider: name, SizeName: size.Name, Runs: runs,
		Concurrency: concurrency, Actor: "operator:test", CreatedAt: time.Now(),
	}
	if err := e.store.InsertBench(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	_, err := e.river.Insert(t.Context(), jobargs.Bench{BenchID: b.ID, AllowPaid: allowPaid}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func waitBenchDone(t *testing.T, e *env, id string) store.Bench {
	t.Helper()
	var b store.Bench
	eventually(t, "the benchmark to finish", func() bool {
		var err error
		b, err = e.store.BenchByID(t.Context(), id)
		return err == nil && !b.FinishedAt.IsZero()
	})
	return b
}

func benchSamples(t *testing.T, e *env, id string) []store.StartSample {
	t.Helper()
	got, err := e.store.SampleQuery(t.Context(), store.SampleFilter{BenchID: id})
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// waves reads the run numbers of a benchmark's probe jobs, wave by wave
func probeRuns(t *testing.T, e *env, id string) []int {
	t.Helper()
	rows, err := e.pool.Query(t.Context(), `
		select coalesce((args->>'run')::int, 0) from river_job
		where kind = 'probe' and args->>'bench_id' = $1 order by 1`, id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []int
	for rows.Next() {
		var n int
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		out = append(out, n)
	}
	return out
}

func TestBenchWaves(t *testing.T) {
	defer worker.SetBenchPoll(50 * time.Millisecond)()
	e := newEnv(t, options{canaries: true})
	e.start(t)
	b := startBench(t, e, "local", 5, 2, false)
	b = waitBenchDone(t, e, b.ID)

	samples := benchSamples(t, e, b.ID)
	eq(t, len(samples), 5)
	for _, m := range samples {
		eq(t, m.Kind, store.SampleBench)
		eq(t, m.Outcome, store.OutcomeOK)
		eq(t, m.SizeName, b.SizeName)
	}
	// Waves of 2, 2 and 1: the run numbers start at the wave times the concurrency
	eq(t, probeRuns(t, e, b.ID), []int{0, 1, 2, 3, 4})
	reports := bench.Build(samples)
	eq(t, len(reports), 1)
	eq(t, reports[0].Kind, store.SampleBench)
	eq(t, reports[0].OK, 5)
	owned, _ := e.local.ListOwned(t.Context(), nil)
	eq(t, len(owned), 0)
}

func TestBenchWaveWaitsForItsProbes(t *testing.T) {
	e := newEnv(t, options{canaries: true})
	prov, _ := e.registry.Get("local")
	size, _ := bench.SizeNamed(prov, "")
	b := store.Bench{
		ID: "b_1", Provider: "local", SizeName: size.Name, Runs: 2, Concurrency: 2,
		Actor: "operator:test", CreatedAt: time.Now(),
	}
	if err := e.store.InsertBench(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	a := jobargs.Bench{BenchID: b.ID}
	// River is not running, so the probes the first look queues stay out
	wait, err := e.worker.BenchWave(t.Context(), a)
	eq(t, [2]any{wait, err}, [2]any{true, error(nil)})
	eq(t, probeRuns(t, e, b.ID), []int{0, 1})
	wait, err = e.worker.BenchWave(t.Context(), a)
	eq(t, [2]any{wait, err}, [2]any{true, error(nil)})
	eq(t, probeRuns(t, e, b.ID), []int{0, 1})
}

func TestBenchRecordsRefusalsAndGoesOn(t *testing.T) {
	defer worker.SetBenchPoll(50 * time.Millisecond)()
	e := newEnv(t, options{canaries: true})
	e.start(t)
	// With every slot taken, each run records capacity and the benchmark still ends
	if err := e.store.SetCapacity(t.Context(), "local", 1); err != nil {
		t.Fatal(err)
	}
	slot := uuid.New()
	if ok, err := e.store.ReserveProbeSlot(t.Context(), "local", slot, time.Now()); err != nil ||
		!ok {
		t.Fatalf("take the slot: %v %v", ok, err)
	}
	b := startBench(t, e, "local", 3, 3, false)
	waitBenchDone(t, e, b.ID)
	samples := benchSamples(t, e, b.ID)
	eq(t, len(samples), 3)
	for _, m := range samples {
		eq(t, m.Outcome, store.OutcomeCapacity)
	}
	reports := bench.Build(samples)
	eq(t, reports[0].Refused, 3)
	eq(t, reports[0].Failed, 0)
}

// feeAfterFirst is a local provider whose first start is included and the rest cost a fee
type feeAfterFirst struct {
	*metered
	starts atomic.Int32
}

func (p *feeAfterFirst) Create(
	ctx context.Context, req provider.CreateRequest,
) (provider.Ref, error) {
	p.starts.Add(1)
	return p.metered.Create(ctx, req)
}

func (p *feeAfterFirst) StartCostMicros(provider.Size) (int64, bool) {
	if p.starts.Load() == 0 {
		return 0, true
	}
	return 3000, true
}

func TestBoatWaveStopsAtPaidStart(t *testing.T) {
	defer worker.SetBenchPoll(50 * time.Millisecond)()
	boat := &feeAfterFirst{metered: newMetered(t, "boat")}
	e := newEnv(t, options{canaries: true, providers: []provider.Provider{boat}})
	e.start(t)
	b := startBench(t, e, "boat", 4, 2, false)
	waitBenchDone(t, e, b.ID)
	// The first start was free, so one probe ran, and the next would cost a fee
	eq(t, len(benchSamples(t, e, b.ID)), 1)
	eq(t, int(boat.starts.Load()), 1)
}

func TestBoatWaveGoesOnWhenPaidStartsAreAllowed(t *testing.T) {
	defer worker.SetBenchPoll(50 * time.Millisecond)()
	boat := &feeAfterFirst{metered: newMetered(t, "boat")}
	e := newEnv(t, options{canaries: true, providers: []provider.Provider{boat}})
	e.start(t)
	b := startBench(t, e, "boat", 4, 2, true)
	waitBenchDone(t, e, b.ID)
	samples := benchSamples(t, e, b.ID)
	eq(t, len(samples), 4)
	var paid int64
	for _, m := range samples {
		paid += m.CostMicros
	}
	if paid < 3*3000 {
		t.Fatalf("measured cost %d should hold the fees of three paid starts", paid)
	}
}

func TestBenchJobsSetTheirOwnTimeouts(t *testing.T) {
	eq(t, worker.BenchTimeouts(), [2]time.Duration{30 * time.Second, 120 * time.Second})
}
