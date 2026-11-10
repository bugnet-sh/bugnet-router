package worker

import (
	"context"
	"errors"
	"time"

	"github.com/riverqueue/river"

	"github.com/bugnet-sh/bugnet-router/internal/bench"
	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/probe"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// benchTimeout is CP 13.4's, under River's 3-minute rescue
const benchTimeout = 30 * time.Second

// benchPoll is how long a wave waits before it looks for its samples again
var benchPoll = 15 * time.Second

type benchWorker struct {
	river.WorkerDefaults[jobargs.Bench]
	w *Worker
}

func (*benchWorker) Timeout(*river.Job[jobargs.Bench]) time.Duration { return benchTimeout }

// Work runs one look at a wave and snoozes while the wave's probes are out
func (bw *benchWorker) Work(ctx context.Context, j *river.Job[jobargs.Bench]) error {
	wait, err := bw.w.benchWave(ctx, j.Args)
	if err != nil {
		return err
	}
	if wait {
		return river.JobSnooze(benchPoll)
	}
	return nil
}

// benchWave looks at wave a.Wave of a benchmark. It queues the wave's probes the first time, waits
// while any is out, and then queues the next wave from the count of samples stored, or finishes
// the benchmark when its runs are done. A wave on a provider that charges per start has one probe,
// and the benchmark ends when the next start would cost a fee, unless a.AllowPaid is set.
func (w *Worker) benchWave(ctx context.Context, a jobargs.Bench) (wait bool, err error) {
	b, err := w.store.BenchByID(ctx, a.BenchID)
	if errors.Is(err, store.ErrNotFound) {
		w.log.Warn("bench job for a benchmark that is gone", "bench", a.BenchID)
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !b.FinishedAt.IsZero() {
		return false, nil
	}
	prov, ok := w.registry.Get(b.Provider)
	if !ok {
		return false, w.finishBench(ctx, b, "the provider is no longer configured")
	}
	size, ok := bench.SizeNamed(prov, b.SizeName)
	if !ok {
		return false, w.finishBench(ctx, b, "the provider no longer sells the size")
	}
	from := a.Wave * b.Concurrency
	total, live, err := w.store.BenchProbeJobs(ctx, b.ID, from, from+b.Concurrency)
	if err != nil {
		return false, err
	}
	stored, err := w.store.BenchSampleCount(ctx, b.ID)
	if err != nil {
		return false, err
	}
	switch {
	case total == 0:
		return w.launchWave(ctx, b, a, prov, size, from, stored)
	case live > 0:
		return true, nil
	case stored >= b.Runs || a.Wave >= 2*b.Runs+10:
		return false, w.finishBench(ctx, b, "")
	}
	next := jobargs.Bench{BenchID: b.ID, Wave: a.Wave + 1, AllowPaid: a.AllowPaid}
	_, err = w.river.Insert(ctx, next, nil)
	return false, err
}

// launchWave queues the probes of a wave that has none, from the first run number from
func (w *Worker) launchWave(
	ctx context.Context, b store.Bench, a jobargs.Bench, prov provider.Provider,
	size provider.Size, from, stored int,
) (bool, error) {
	n := min(b.Concurrency, b.Runs-stored)
	if n <= 0 {
		return false, w.finishBench(ctx, b, "")
	}
	if _, costly := provider.As[provider.StartCoster](prov); costly && !a.AllowPaid {
		if !probe.StartFree(prov, size) {
			return false, w.finishBench(ctx, b, "the next start would cost a fee")
		}
		// Starts that run together could each spend the last included start
		n = 1
	}
	params := make([]river.InsertManyParams, n)
	for i := range params {
		params[i] = river.InsertManyParams{Args: jobargs.Probe{
			Sample: string(store.SampleBench), Provider: b.Provider, Size: size, BenchID: b.ID,
			Run: from + i, AllowPaid: a.AllowPaid,
		}}
	}
	if _, err := w.river.InsertMany(ctx, params); err != nil {
		return false, err
	}
	return true, nil
}

// finishBench marks a benchmark finished and logs why when it ended early
func (w *Worker) finishBench(ctx context.Context, b store.Bench, why string) error {
	if err := w.store.FinishBench(ctx, b.ID, w.now()); err != nil {
		return err
	}
	if why != "" {
		w.log.Warn("benchmark stopped early", "bench", b.ID, "provider", b.Provider, "why", why)
	}
	return nil
}
