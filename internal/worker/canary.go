package worker

import (
	"context"
	"time"

	"github.com/riverqueue/river"

	"github.com/bugnet-sh/bugnet-router/internal/health"
	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/probe"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

const (
	// canaryEvery is how often canary_tick runs
	canaryEvery   = time.Minute
	canaryTimeout = 15 * time.Second
	// probeTimeout is CP 13.4's, under River's 3-minute rescue
	probeTimeout  = 2 * time.Minute
	rollupEvery   = 30 * time.Second
	rollupTimeout = 20 * time.Second
	oldProbeAfter = 15 * time.Minute
)

type canaryTickWorker struct {
	river.WorkerDefaults[jobargs.CanaryTick]
	w *Worker
}

func (*canaryTickWorker) Timeout(*river.Job[jobargs.CanaryTick]) time.Duration {
	return canaryTimeout
}

// Work never fails the job, since the next tick comes a minute later
func (cw *canaryTickWorker) Work(ctx context.Context, _ *river.Job[jobargs.CanaryTick]) error {
	cw.w.canaryTick(ctx)
	return nil
}

// schedule is the canary intervals of the worker's settings
func (w *Worker) schedule() health.Schedule {
	h := w.d.Health
	return health.Schedule{
		Every: h.CanaryEvery, EveryDown: h.CanaryEveryDown, BoatEvery: h.BoatCanaryEvery,
		PassiveWindow: h.PassiveWindow,
	}
}

// CanarySize is the cheapest size of prov that covers the canary's vcpu and memory
func (w *Worker) CanarySize(prov provider.Provider) (provider.Size, bool) {
	return prov.SizeFor(provider.Resources{
		VCPU: w.d.Health.CanaryVCPU, MemoryMiB: w.d.Health.CanaryMemoryMiB,
	})
}

// CanaryStartFree reports whether name gets canaries now, its canary's start costing nothing
func (w *Worker) CanaryStartFree(name string) bool {
	prov, ok := w.registry.Get(name)
	if !ok {
		return false
	}
	size, ok := w.CanarySize(prov)
	return ok && probe.StartFree(prov, size)
}

// canaryTick queues one canary for each provider that is due, one waiting or running at most
func (w *Worker) canaryTick(ctx context.Context) {
	if w.registry == nil {
		return
	}
	sched := w.schedule()
	now := w.now()
	for _, prov := range w.registry.Ordered() {
		size, ok := w.CanarySize(prov)
		if !ok {
			w.log.Warn("no size covers the canary", "provider", prov.Name())
			continue
		}
		facts, err := w.store.CanaryFactsOf(ctx, prov.Name())
		if err != nil {
			w.log.Warn("cannot read the canary facts", "provider", prov.Name(), "err", err)
			continue
		}
		_, costly := prov.(provider.StartCoster)
		in := health.Inputs{
			Name: prov.Name(), Facts: facts, Costly: costly, StartFree: probe.StartFree(prov, size),
		}
		if !sched.Due(in, now) {
			continue
		}
		_, err = w.river.Insert(ctx, jobargs.Probe{
			Sample: string(store.SampleCanary), Provider: prov.Name(), Size: size,
		}, nil)
		if err != nil {
			w.log.Warn("cannot queue a canary", "provider", prov.Name(), "err", err)
		}
	}
}

type probeWorker struct {
	river.WorkerDefaults[jobargs.Probe]
	w *Worker
}

func (*probeWorker) Timeout(*river.Job[jobargs.Probe]) time.Duration { return probeTimeout }

// Work logs a probe that failed to run and never retries it, since a retry would start a second
// sandbox, and a failed start is a sample and not a failed job
func (pw *probeWorker) Work(ctx context.Context, j *river.Job[jobargs.Probe]) error {
	a := j.Args
	m, ran, err := pw.w.probe.Run(ctx, probe.Spec{
		Kind: store.SampleKind(a.Sample), Provider: a.Provider, Size: a.Size, BenchID: a.BenchID,
		Run: a.Run,
	})
	switch {
	case err != nil:
		pw.w.log.Error("probe failed to run", "provider", a.Provider, "err", err)
	case !ran:
		pw.w.log.Debug("probe skipped", "provider", a.Provider)
	case m.Outcome == store.OutcomeFailed:
		pw.w.log.Warn("probe failed", "provider", a.Provider, "code", m.ErrorCode)
	}
	return nil
}

type healthRollupWorker struct {
	river.WorkerDefaults[jobargs.HealthRollup]
	w *Worker
}

func (*healthRollupWorker) Timeout(*river.Job[jobargs.HealthRollup]) time.Duration {
	return rollupTimeout
}

// Work never fails the job, since the next rollup comes 30 s later
func (hw *healthRollupWorker) Work(ctx context.Context, _ *river.Job[jobargs.HealthRollup]) error {
	hw.w.healthRollup(ctx)
	return nil
}

// healthRollup writes each provider's state from the samples of the last health.window
func (w *Worker) healthRollup(ctx context.Context) {
	if w.registry == nil {
		return
	}
	h := w.d.Health
	now := w.now()
	rows, err := w.store.ProviderHealths(ctx)
	if err != nil {
		w.log.Warn("cannot read the provider health rows", "err", err)
		return
	}
	previous := map[string]string{}
	for _, r := range rows {
		previous[r.Provider] = r.State
	}
	counts := map[string]health.Counts{}
	for _, prov := range w.registry.Ordered() {
		c, err := w.store.HealthWindow(ctx, prov.Name(), now.Add(-h.Window))
		if err != nil {
			// A provider unread this run keeps its row, and the others are judged without it
			w.log.Warn("cannot count the start samples", "provider", prov.Name(), "err", err)
			continue
		}
		counts[prov.Name()] = c
	}
	rules := health.Rules{
		MinSamples: h.MinSamples, DownBelow: h.DownBelow, SlowReadyP95: h.SlowReadyP95,
	}
	states, guarded := rules.Rollup(counts, previous)
	if guarded {
		w.log.Error("every provider's canaries failed at once, so the canary is taken to be "+
			"broken and the states stay", "providers", len(states))
	}
	for name, st := range states {
		before, err := w.store.WriteHealth(ctx, name, st, now)
		if err != nil {
			w.log.Warn("cannot write the provider health", "provider", name, "err", err)
			continue
		}
		if before != "" && before != st.State {
			w.log.Warn("provider health changed", "provider", name, "from", before,
				"to", st.State, "samples", st.Samples, "success_lb", st.SuccessLB)
		}
	}
}

// noteProbes remembers the age of the oldest probe sandbox each provider listed
func (w *Worker) noteProbes(name string, owned []provider.Summary) {
	var oldest time.Duration
	now := w.now()
	for _, o := range owned {
		if o.Labels["bugnet_probe"] != "" && !o.CreatedAt.IsZero() {
			oldest = max(oldest, now.Sub(o.CreatedAt))
		}
	}
	w.probesMu.Lock()
	defer w.probesMu.Unlock()
	if w.probeAges == nil {
		w.probeAges = map[string]time.Duration{}
	}
	w.probeAges[name] = oldest
}

// OldProbes lists the providers whose last sweep listing held a probe sandbox over 15 minutes
// old, with its age
func (w *Worker) OldProbes() map[string]time.Duration {
	w.probesMu.Lock()
	defer w.probesMu.Unlock()
	out := map[string]time.Duration{}
	for name, age := range w.probeAges {
		if age > oldProbeAfter {
			out[name] = age
		}
	}
	return out
}
