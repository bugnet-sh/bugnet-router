package worker_test

import (
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/gpu"
	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/worker"
)

func addSample(
	t *testing.T, e *env, name string, kind store.SampleKind, outcome string, ago time.Duration,
) {
	t.Helper()
	err := e.store.InsertStartSample(t.Context(), store.StartSample{
		Kind: kind, Provider: name, SizeName: "small", StartedAt: time.Now().Add(-ago),
		Outcome: outcome, CreateMS: 10, ReadyMS: 100, RunnerMS: store.NoStep, GateMS: store.NoStep,
		FirstCmdMS: store.NoStep, WarmCmdMS: store.NoStep, DestroyMS: store.NoStep,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func probeJobs(t *testing.T, e *env, name string) int {
	t.Helper()
	var n int
	err := e.pool.QueryRow(t.Context(), `
		select count(*) from river_job where kind = 'probe' and args->>'provider' = $1
			and state in ('available', 'pending', 'running', 'scheduled')`, name).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func stateOf(t *testing.T, e *env, name string) store.ProviderHealth {
	t.Helper()
	rows, err := e.store.ProviderHealths(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if r.Provider == name {
			return r
		}
	}
	t.Fatalf("no health row for %s in %+v", name, rows)
	return store.ProviderHealth{}
}

func TestCanaryTickQueuesOneProbeAtATime(t *testing.T) {
	e := newEnv(t, options{canaries: true})
	e.worker.CanaryTick(t.Context())
	e.worker.CanaryTick(t.Context())
	eq(t, probeJobs(t, e, "local"), 1)
	var queue string
	var attempts int
	err := e.pool.QueryRow(t.Context(), `select queue, max_attempts from river_job
		where kind = 'probe'`).Scan(&queue, &attempts)
	eq(t, err, nil)
	eq(t, queue, jobargs.QueueProbes)
	// A retry would start a second sandbox
	eq(t, attempts, 1)
}

func TestCanaryTickLeavesOutWhatIsNotDue(t *testing.T) {
	e := newEnv(t, options{canaries: true})
	ctx := t.Context()
	// A canary a minute ago, a placement ready now and a blocked account each hold one back
	addSample(t, e, "local", store.SampleCanary, "ok", time.Minute)
	e.worker.CanaryTick(ctx)
	eq(t, probeJobs(t, e, "local"), 0)

	e = newEnv(t, options{canaries: true})
	e.placeReady(t, "order-1")
	e.worker.CanaryTick(ctx)
	eq(t, probeJobs(t, e, "local"), 0)

	e = newEnv(t, options{canaries: true})
	if _, err := e.store.RecordAccount(ctx, "local", "banned", "x", time.Now()); err != nil {
		t.Fatal(err)
	}
	e.worker.CanaryTick(ctx)
	eq(t, probeJobs(t, e, "local"), 0)

	// Past the interval, with nothing else against it, a canary is queued
	e = newEnv(t, options{canaries: true})
	addSample(t, e, "local", store.SampleCanary, "ok", 7*time.Minute)
	e.worker.CanaryTick(ctx)
	eq(t, probeJobs(t, e, "local"), 1)
}

// priced is a local provider that charges a fee per start, as Boat does
type priced struct {
	*metered
	fee int64
}

func (p priced) StartCostMicros(provider.Size) (int64, bool) { return p.fee, true }

func TestCanaryTickNeverPaysForAStart(t *testing.T) {
	boat := priced{metered: newMetered(t, "boat"), fee: 3000}
	e := newEnv(t, options{canaries: true, providers: []provider.Provider{boat}})
	e.worker.CanaryTick(t.Context())
	eq(t, probeJobs(t, e, "boat"), 0)
	free := priced{metered: newMetered(t, "boat"), fee: 0}
	e = newEnv(t, options{canaries: true, providers: []provider.Provider{free}})
	e.worker.CanaryTick(t.Context())
	eq(t, probeJobs(t, e, "boat"), 1)
}

func TestCanaryTickSkipsAGPUOnlyProviderQuietly(t *testing.T) {
	l4 := provider.Size{Name: "l4", Resources: provider.Resources{
		VCPU: 8, MemoryMiB: 32768, DiskGiB: 100, GPUCount: 1, GPUMemoryGiB: 24, GPUModel: gpu.L4,
	}}
	tiny := provider.Size{Name: "tiny", Resources: provider.Resources{MemoryMiB: 256}}
	var logs syncBuffer
	e := newEnv(t, options{canaries: true, log: &logs, providers: []provider.Provider{
		newSized(t, "gpu", l4), newSized(t, "cpu", tiny), newSized(t, "mixed", small, l4),
	}})
	for range 3 {
		e.worker.CanaryTick(t.Context())
	}
	// Each GPU size would cover the canary, yet a GPU-only provider gets no canary
	eq(t, probeJobs(t, e, "gpu"), 0)
	eq(t, e.worker.CanaryStartFree("gpu"), false)
	eq(t, strings.Count(logs.String(), `msg="no canaries on a GPU-only provider" provider=gpu`), 1)
	// A CPU provider with no size for the canary is a mistake, so it warns every tick
	eq(t, strings.Count(logs.String(), `msg="no size covers the canary" provider=cpu`), 3)
	eq(t, strings.Count(logs.String(), "provider=mixed"), 0)
	eq(t, probeJobs(t, e, "mixed"), 1)
}

func TestProbeJobWritesACanarySample(t *testing.T) {
	e := newEnv(t, options{canaries: true})
	e.start(t)
	e.run(t, jobargs.CanaryTick{})
	var samples []store.StartSample
	eventually(t, "the canary's sample", func() bool {
		var err error
		samples, err = e.store.StartSamples(t.Context(), "local", time.Now().Add(-time.Hour))
		return err == nil && len(samples) > 0 && probeJobs(t, e, "local") == 0
	})
	eq(t, len(samples), 1)
	eq(t, samples[0].Kind, store.SampleCanary)
	eq(t, samples[0].Outcome, "ok")
	owned, _ := e.local.ListOwned(t.Context(), nil)
	eq(t, len(owned), 0)
}

func TestCanaryJobsSetTheirOwnTimeouts(t *testing.T) {
	eq(t, worker.CanaryTimeouts(), [3]time.Duration{
		15 * time.Second, 2 * time.Minute, 20 * time.Second,
	})
}

func TestRollupMarksTwoFailedCanariesDown(t *testing.T) {
	a, b := newMetered(t, "a"), newMetered(t, "b")
	e := newEnv(t, options{canaries: true, providers: []provider.Provider{a, b}})
	for range 2 {
		addSample(t, e, "a", store.SampleCanary, "failed", time.Minute)
	}
	for range 3 {
		addSample(t, e, "b", store.SamplePlacement, "ok", time.Minute)
	}
	addSample(t, e, "b", store.SampleCanary, "ok", time.Minute)
	e.worker.HealthRollup(t.Context())
	got := stateOf(t, e, "a")
	eq(t, got.State, "down")
	eq(t, got.Samples, 2)
	eq(t, stateOf(t, e, "b").State, "ok")
	eq(t, stateOf(t, e, "b").Samples, 4)
	since := got.StateSince
	// A second rollup finds the same and keeps when the state began
	e.worker.HealthRollup(t.Context())
	eq(t, stateOf(t, e, "a").StateSince, since)
	addSample(t, e, "a", store.SampleCanary, "ok", time.Second)
	addSample(t, e, "a", store.SampleCanary, "ok", time.Second)
	e.worker.HealthRollup(t.Context())
	// Two failures in four samples is under the bound, but the canaries are fine again
	eq(t, stateOf(t, e, "a").State, "ok")
}

func TestRollupKeepsStatesWhenEveryCanaryFailed(t *testing.T) {
	a, b := newMetered(t, "a"), newMetered(t, "b")
	e := newEnv(t, options{canaries: true, providers: []provider.Provider{a, b}})
	for _, name := range []string{"a", "b"} {
		for range 2 {
			addSample(t, e, name, store.SampleCanary, "failed", time.Minute)
		}
	}
	e.worker.HealthRollup(t.Context())
	eq(t, stateOf(t, e, "a").State, "ok")
	eq(t, stateOf(t, e, "b").State, "ok")
}

func TestRollupIgnoresWhatSaysNothingAboutTheProvider(t *testing.T) {
	e := newEnv(t, options{canaries: true})
	for _, outcome := range []string{"capacity", "rate_limited", "quota"} {
		for range 6 {
			addSample(t, e, "local", store.SamplePlacement, outcome, time.Minute)
			addSample(t, e, "local", store.SampleCanary, outcome, time.Minute)
		}
	}
	e.worker.HealthRollup(t.Context())
	got := stateOf(t, e, "local")
	eq(t, got.State, "ok")
	eq(t, got.Samples, 0)
}

func TestSweepRemembersAProbeSandboxOverFifteenMinutesOld(t *testing.T) {
	old := time.Now().Add(20 * time.Minute)
	e := newEnv(t, options{canaries: true, now: func() time.Time { return old }})
	e.start(t)
	l := e.owned()
	l["bugnet_probe"] = "canary"
	l["probe"] = "r1"
	e.sandbox(t, e.local, l)
	eq(t, len(e.worker.OldProbes()), 0)
	e.run(t, jobargs.Sweep{})
	got := e.worker.OldProbes()
	eq(t, len(got), 1)
	if age := got["local"]; age < 15*time.Minute {
		t.Fatalf("probe age %s, want over 15 minutes", age)
	}
}
