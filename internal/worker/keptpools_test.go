package worker_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/pool"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/worker"
)

// keptPool is a provider that keeps its pools itself as Koyeb does, noting each pool it is asked
// to make or size and, when held is set, listing every pool member as one no claim took
type keptPool struct {
	*local.Provider
	mu    *sync.Mutex
	specs *[]provider.PoolSpec
	// failEnsure refuses every EnsurePool
	failEnsure bool
	held       bool
	idle       int64
}

func newKept(t *testing.T) keptPool {
	t.Helper()
	return keptPool{
		Provider: newLocal(t, "local"), mu: &sync.Mutex{}, specs: &[]provider.PoolSpec{},
	}
}

func (k keptPool) EnsurePool(_ context.Context, spec provider.PoolSpec) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	*k.specs = append(*k.specs, spec)
	if k.failEnsure {
		return errors.New("koyeb refused the pool")
	}
	return nil
}

func (keptPool) Claim(context.Context, string, string) (provider.Claim, error) {
	return provider.Claim{}, errors.New("no claims in this test")
}

func (k keptPool) IdleHourlyCostMicros(provider.Size) int64 { return k.idle }

func (k keptPool) ListOwned(
	ctx context.Context, labels map[string]string,
) ([]provider.Summary, error) {
	owned, err := k.Provider.ListOwned(ctx, labels)
	for i, s := range owned {
		if k.held && s.Labels["bugnet_pool"] != "" && s.Labels["pool_member"] == "" {
			owned[i].PoolHeld = true
		}
	}
	return owned, err
}

// asked is the pools the provider was asked to make or size, oldest first
func (k keptPool) asked() []provider.PoolSpec {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]provider.PoolSpec(nil), *k.specs...)
}

func TestPoolEnsureSendsTheTarget(t *testing.T) {
	k := newKept(t)
	e := newEnv(t, options{pools: poolSettings(2, 5), providers: []provider.Provider{k}})
	e.start(t)
	e.tick(t)
	// The pool is named after the deployment and the size, and its labels never name a placement
	want := provider.PoolSpec{
		Name: "bn-main-small", Size: small, Target: 2,
		Labels: map[string]string{
			"bugnet": "1", "bugnet_deployment": "main", "bugnet_pool": "small",
		},
	}
	eq(t, k.asked(), []provider.PoolSpec{want})
	// The router fills nothing of its own on a provider-kept pool
	eq(t, e.fillJobs(t), 0)
	eq(t, len(e.members(t, "")), 0)
	eq(t, e.inUse(t, "local"), 0)

	// A tick that finds the target unchanged leaves the provider alone
	e.tick(t)
	eq(t, len(k.asked()), 1)
	// Misses are demand and raise the target, which the provider is told
	for range 90 {
		eq(t, e.store.RecordMiss(t.Context(), "local", "small", time.Now()), nil)
	}
	e.tick(t)
	want.Target = 5
	eq(t, k.asked()[1], want)

	// pool_ensure sends the stored target again, which a pool the provider lost needs
	e.run(t, jobargs.PoolEnsure{Provider: "local"})
	eq(t, len(k.asked()), 3)
	eq(t, k.asked()[2], want)

	// A drain keeps refill off, which for a provider-kept pool is a size of 0
	n, err := e.worker.Pools().Drain(t.Context(), pool.Key{Provider: "local", Size: "small"},
		time.Hour, "operator")
	eq(t, err, nil)
	eq(t, n, 0)
	e.run(t, jobargs.PoolEnsure{Provider: "local"})
	want.Target = 0
	eq(t, k.asked()[3], want)
	e.tick(t)
	eq(t, len(k.asked()), 4)
}

func TestPoolEnsureFailureIsTriedAgain(t *testing.T) {
	k := newKept(t)
	k.failEnsure = true
	e := newEnv(t, options{pools: poolSettings(1, 3), providers: []provider.Provider{k}})
	// The tick reports the refusal and the next tick asks again, since nothing was set
	for range 2 {
		if err := e.worker.Pools().Tick(t.Context()); err == nil {
			t.Fatal("a tick whose pool the provider refused reported nothing")
		}
	}
	eq(t, len(k.asked()), 2)
}

func TestKoyebPoolIdleCostCounts(t *testing.T) {
	// A day of two unclaimed members at 1000 micros an hour each is 48000 micros
	const day = 2 * 1000 * 24
	t.Run("in ListPools", func(t *testing.T) {
		k := newKept(t)
		k.idle = 1000
		e := newEnv(t, options{pools: poolSettings(2, 5), providers: []provider.Provider{k}})
		e.tick(t)
		st, err := e.worker.Pools().Status(t.Context())
		eq(t, err, nil)
		eq(t, len(st), 1)
		eq(t, st[0].KeptBy, pool.KeptByProvider)
		eq(t, st[0].Target, 2)
		eq(t, st[0].CostMicrosLastDay, int64(day))
	})
	t.Run("in the spend cap", func(t *testing.T) {
		k := newKept(t)
		k.idle = 1000
		s := poolSettings(2, 5)
		s.MaxDailyMicros = day
		e := newEnv(t, options{pools: s, providers: []provider.Provider{k}})
		for range 90 {
			eq(t, e.store.RecordMiss(t.Context(), "local", "small", time.Now()), nil)
		}
		e.tick(t)
		// The first tick saw no cost, then the target 5 costs 120000 a day, over the cap
		e.tick(t)
		capped, cost, limit, err := e.worker.Pools().Spend(t.Context())
		eq(t, err, nil)
		eq(t, capped, true)
		eq(t, limit, int64(day))
		eq(t, cost >= day, true)
		targets, err := e.store.Targets(t.Context())
		eq(t, err, nil)
		eq(t, targets[0].Target, 2)
	})
	t.Run("free while Light Sleep is free", func(t *testing.T) {
		k := newKept(t)
		e := newEnv(t, options{pools: poolSettings(2, 5), providers: []provider.Provider{k}})
		e.tick(t)
		capped, cost, _, err := e.worker.Pools().Spend(t.Context())
		eq(t, err, nil)
		eq(t, capped, false)
		eq(t, cost, int64(0))
	})
}

func TestSweepKeepsPoolHeld(t *testing.T) {
	labels := func(e *env) map[string]string {
		l := e.owned()
		l["bugnet_pool"] = "small"
		return l
	}
	t.Run("a member no claim took stays however old", func(t *testing.T) {
		k := newKept(t)
		k.held = true
		clk := &clock{}
		e := newEnv(t, options{
			pools: poolSettings(1, 1), workerClock: clk, providers: []provider.Provider{k},
		})
		ref := e.sandbox(t, k, labels(e))
		e.start(t)
		clk.Advance(3 * time.Hour)
		e.run(t, jobargs.Sweep{})
		if !exists(t, k, ref) {
			t.Fatal("the sweep destroyed a member a provider-kept pool still holds")
		}
	})
	t.Run("a claimed member with no row goes once it is old", func(t *testing.T) {
		k := newKept(t)
		clk := &clock{}
		e := newEnv(t, options{
			pools: poolSettings(1, 1), workerClock: clk, providers: []provider.Provider{k},
		})
		ref := e.sandbox(t, k, labels(e))
		e.start(t)
		e.run(t, jobargs.Sweep{})
		if !exists(t, k, ref) {
			t.Fatal("a young sandbox without a row was destroyed")
		}
		clk.Advance(11 * time.Minute)
		e.run(t, jobargs.Sweep{})
		if exists(t, k, ref) {
			t.Fatal("a pool sandbox no row and no pool holds survived ten minutes")
		}
	})
}

func TestPoolEnsureRunsOnStartAndEveryFiveMinutes(t *testing.T) {
	kept, plain := newKept(t), newLocal(t, "local")
	on := worker.Deps{Pools: *poolSettings(1, 3)}
	// Only a provider that keeps its own pools and has one listed gets the job
	got := worker.PollEvery(on, kept)
	eq(t, got["pool_ensure/local"], 5*time.Minute)
	eq(t, len(worker.PollEvery(on, plain)), len(worker.PollEvery(worker.Deps{}, plain)))
	if _, ok := worker.PollEvery(on, plain)["pool_ensure/local"]; ok {
		t.Fatal("a router-kept pool got a pool_ensure job")
	}
	off := on
	off.Pools.Enabled = false
	if _, ok := worker.PollEvery(off, kept)["pool_ensure/local"]; ok {
		t.Fatal("a pool_ensure job runs with pools off")
	}
	unlisted := worker.Deps{Pools: config.DefaultPools()}
	if _, ok := worker.PollEvery(unlisted, kept)["pool_ensure/local"]; ok {
		t.Fatal("a pool_ensure job runs for a provider with no pool listed")
	}
}
