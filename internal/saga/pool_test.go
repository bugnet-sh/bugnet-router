package saga_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/ordertoken"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/pool"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// poolEnv is an env whose saga sees one pool of provider a's size small and whose manager fills it
type poolEnv struct {
	*env
	mgr *pool.Manager
}

func newPoolEnv(t *testing.T, adjust func(*saga.Deps), ps ...provider.Provider) *poolEnv {
	t.Helper()
	settings := config.DefaultPools()
	// With no headroom a pool that sees no demand sits at its min of one member
	settings.Headroom = 0
	settings.Pool = []config.Pool{{
		Provider: ps[0].Name(), Size: "small", Min: 1, Max: 3, MaxAge: time.Hour,
	}}
	cfgs := pool.Configs(settings)
	e := newEnvWith(t, func(d *saga.Deps) {
		w := pool.NewWarmth(d.Store, cfgs, nil)
		d.Pools = w
		d.Engine = placement.New(d.Registry, d.Tracker, w)
		if adjust != nil {
			adjust(d)
		}
	}, ps...)
	mgr, err := pool.New(pool.Deps{
		Store: e.store, Registry: mustRegistry(t, ps...), Saga: e.saga, River: e.river,
		Valkey: e.vk, Prefix: e.prefix, Settings: settings,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &poolEnv{env: e, mgr: mgr}
}

func mustRegistry(t *testing.T, ps ...provider.Provider) *provider.Registry {
	t.Helper()
	reg, err := provider.NewRegistry(ps, provider.Features{})
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

// fill ticks the pool and fills every member the tick started, as the worker would
func (e *poolEnv) fill(t *testing.T) []store.Member {
	t.Helper()
	ctx := t.Context()
	if err := e.mgr.Tick(ctx); err != nil {
		t.Fatalf("pool tick: %v", err)
	}
	members, err := e.store.LiveMembers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range members {
		if m.State == store.MemberFilling {
			if err := e.mgr.Fill(ctx, m.ID); err != nil {
				t.Fatalf("fill: %v", err)
			}
		}
	}
	e.saga.Wait()
	members, err = e.store.LiveMembers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return members
}

func (e *poolEnv) member(t *testing.T, id uuid.UUID) store.Member {
	t.Helper()
	m, err := e.store.MemberByID(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func (e *poolEnv) misses(t *testing.T) int {
	t.Helper()
	var n int
	err := e.pool.QueryRow(t.Context(), `select coalesce(sum(misses), 0)::int from pool_misses`).
		Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func (e *poolEnv) recount(t *testing.T, name string) int {
	t.Helper()
	_, now, err := e.store.RecountCapacity(t.Context(), name, time.Hour, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return now
}

func (e *poolEnv) retireJobs(t *testing.T) int {
	t.Helper()
	var n int
	err := e.pool.QueryRow(t.Context(),
		`select count(*) from river_job where kind = 'pool_retire'`).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func (e *poolEnv) startPath(t *testing.T, id uuid.UUID) string {
	t.Helper()
	return e.reload(t, id).StartPath
}

func TestPooledAttemptTakesAWarmMember(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a"})
	e := newPoolEnv(t, nil, a)
	members := e.fill(t)
	eq(t, len(members), 1)
	eq(t, members[0].State, store.MemberWarm)
	eq(t, e.inUse(t, "a"), 1)

	p, h := e.placeReady(t, "order-1")
	eq(t, p.StartPath, "pooled")
	m := e.member(t, members[0].ID)
	eq(t, m.State, store.MemberBound)
	eq(t, *m.PlacementID, p.ID)
	eq(t, m.HoldsSlot, false)
	// The member's slot became the placement's, so nothing was taken or freed
	eq(t, e.inUse(t, "a"), 1)
	eq(t, e.recount(t, "a"), 1)
	eq(t, p.ProviderRef.ID, m.ProviderRef.ID)
	eq(t, p.SandboxCreatedAt != nil, true)
	attempts, err := e.store.Attempts(t.Context(), p.ID)
	eq(t, err, nil)
	eq(t, len(attempts), 1)
	eq(t, [2]string{attempts[0].Path, attempts[0].State}, [2]string{"pooled", "ready"})
	eq(t, attempts[0].MemberID, m.ID)
	// The sandbox serves the gate the placement's key opens, and the sampler runs
	eq(t, p.GateReadyAt != nil, true)
	var path string
	err = e.pool.QueryRow(t.Context(), `select start_path from start_samples
		where placement_id = $1`, p.ID).Scan(&path)
	eq(t, err, nil)
	eq(t, path, "pooled")

	// Ending the placement destroys the sandbox, and the member row stays bound
	if _, err := e.saga.Stop(t.Context(), h); err != nil {
		t.Fatalf("stop: %v", err)
	}
	eq(t, e.inUse(t, "a"), 0)
	eq(t, e.member(t, m.ID).State, store.MemberBound)
}

func TestClaimMissFallsToColdOnTheSameCandidate(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a"})
	e := newPoolEnv(t, nil, a)
	e.fill(t)
	first, _ := e.placeReady(t, "order-1")
	eq(t, first.StartPath, "pooled")
	eq(t, e.misses(t), 0)

	// The view of warm members is two seconds old, so the second order still tries a claim
	second, _ := e.placeReady(t, "order-2")
	eq(t, second.StartPath, "cold")
	eq(t, second.Provider, "a")
	eq(t, e.misses(t), 1)
	attempts, err := e.store.Attempts(t.Context(), second.ID)
	eq(t, err, nil)
	eq(t, len(attempts), 1)
	eq(t, attempts[0].Path, "cold")
	eq(t, e.inUse(t, "a"), 2)
	eq(t, e.recount(t, "a"), 2)
}

func TestOneMissPerOrder(t *testing.T) {
	a := newScripted(newLocal(t, local.Options{Name: "a"}),
		map[string]answer{"setup": {`{"error":"gate_failed"}`, 1}})
	b := newLocal(t, local.Options{Name: "b"})
	e := newPoolEnv(t, nil, a, b)
	// No warm member anywhere, a boot failure on a and a start on b
	p, _ := e.placeReady(t, "order-1")
	eq(t, p.Provider, "b")
	eq(t, e.misses(t), 1)
}

func TestRestrictedPackageNeverClaimsAMember(t *testing.T) {
	a := newScripted(newLocal(t, local.Options{Name: "a"}), map[string]answer{})
	a.caps = func(c provider.Capabilities) provider.Capabilities {
		c.EgressControl = true
		return c
	}
	e := newPoolEnv(t, func(d *saga.Deps) {
		d.Boot.EgressHost = "egress.example"
	}, a)
	members := e.fill(t)
	eq(t, len(members), 1)
	p, _, err := e.saga.Create(t.Context(), e.token(t, ordertoken.Claims{
		OrderID: "order-r", Kind: ordertoken.KindStart, PackageID: "small-10m-restricted",
		AmountMicros: smallPrice,
	}), placement.Requirements{})
	if err != nil {
		t.Fatal(err)
	}
	e.saga.Wait()
	p = e.reload(t, p.ID)
	eq(t, p.State, store.PlacementReady)
	eq(t, p.StartPath, "cold")
	// The warm member stays, and a restricted order is no demand for it
	eq(t, e.member(t, members[0].ID).State, store.MemberWarm)
	eq(t, e.misses(t), 0)
	reqs := *a.reqs
	last := reqs[len(reqs)-1]
	eq(t, last.Egress != nil, true)
}

func TestAbandonedPooledAttemptRetiresItsMember(t *testing.T) {
	a := newScripted(newLocal(t, local.Options{Name: "a"}), map[string]answer{})
	e := newPoolEnv(t, nil, a)
	members := e.fill(t)
	eq(t, len(members), 1)
	// The pool phase ran already, so the full phase of the claim is the setup that now fails
	a.answers["setup"] = answer{`{"error":"gate_failed"}`, 1}
	p, _ := e.place(t, "order-1")
	eq(t, p.State, store.PlacementFailed)
	m := e.member(t, members[0].ID)
	eq(t, m.State, store.MemberRetiring)
	eq(t, e.retireJobs(t), 1)
	// The attempt's slot is back and the member holds none, so nothing leaks
	eq(t, e.inUse(t, "a"), 0)
	eq(t, e.recount(t, "a"), 0)
	attempts, err := e.store.Attempts(t.Context(), p.ID)
	eq(t, err, nil)
	eq(t, attempts[0].State, "failed")
	// Retiring destroys the sandbox and nothing sets it back to warm
	eq(t, e.mgr.Retire(t.Context(), m.ID, store.EndFailed), nil)
	eq(t, e.member(t, m.ID).State, store.MemberGone)
	eq(t, len(owned(t, a)), 0)
}

func TestUnwakeableMemberIsRetiredAndTheOrderStartsCold(t *testing.T) {
	a := &unparkFails{Provider: newLocal(t, local.Options{Name: "a"})}
	e := newPoolEnv(t, nil, a)
	members := e.fill(t)
	eq(t, len(members), 1)
	a.fail.Store(true)
	p, _ := e.placeReady(t, "order-1")
	eq(t, p.StartPath, "cold")
	eq(t, e.member(t, members[0].ID).State, store.MemberRetiring)
	// The failed claim gave its slot back, and the cold create took the one the placement holds
	eq(t, e.inUse(t, "a"), 1)
	eq(t, e.recount(t, "a"), 1)
	eq(t, e.mgr.Retire(t.Context(), members[0].ID, store.EndFailed), nil)
	eq(t, e.inUse(t, "a"), 1)
	eq(t, e.recount(t, "a"), 1)
}

func TestFailStuckRetiresBoundMembers(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a"})
	e := newPoolEnv(t, nil, a)
	members := e.fill(t)
	// A placement that is placing, holding a member as its saga would mid-claim
	p, _, err := e.saga.Create(t.Context(), e.startToken(t, "order-1"), placement.Requirements{})
	if err != nil {
		t.Fatal(err)
	}
	e.saga.Wait()
	p = e.reload(t, p.ID)
	eq(t, p.State, store.PlacementReady)
	_, err = e.pool.Exec(t.Context(), `update placements set state = 'placing', ready_at = null
		where id = $1`, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, e.saga.FailStuck(t.Context(), p.ID, saga.ReasonPlacingStalled), nil)
	e.saga.Wait()
	eq(t, e.member(t, members[0].ID).State, store.MemberRetiring)
	eq(t, e.retireJobs(t), 1)
	eq(t, e.inUse(t, "a"), 0)
}

func TestBootPoolStopsAfterHarden(t *testing.T) {
	a := newScripted(newLocal(t, local.Options{Name: "a"}),
		map[string]answer{"harden": {`{"firewall":"nft"}`, 0}})
	e := newEnvWith(t, func(d *saga.Deps) {
		d.Boot.Harden = map[string]config.HardenRule{"a": {Required: true}}
	}, a)
	ref, rep, err := e.saga.Boot(t.Context(), a, created(t, a), saga.BootPlan{Pool: true})
	eq(t, err, nil)
	eq(t, a.calls(), []string{"setup pool", "harden"})
	eq(t, rep.GateReady, false)
	eq(t, ref.Meta["gate_host"], "")
}

func TestPooledPathRunsPrepareOnce(t *testing.T) {
	a := newScripted(newLocal(t, local.Options{Name: "a"}), map[string]answer{})
	e := newPoolEnv(t, nil, a)
	e.fill(t)
	eq(t, a.calls(), []string{"setup pool"})
	e.placeReady(t, "order-1")
	// The full phase and the sampler's run follow the pool phase, and nothing else repeats
	eq(t, a.calls(), []string{"setup pool", "setup sample=false", "setup sample=true"})
}

func TestEgressCAVersionStoredOnThePooledPath(t *testing.T) {
	ca := testCA(t)
	// Only the four real providers get the CA, and boxd's sandboxes install it from the spec
	a := newScripted(newLocal(t, local.Options{Name: "boxd"}), map[string]answer{})
	e := newPoolEnv(t, func(d *saga.Deps) {
		d.Boot.CAPEM, d.Boot.CAVersion = ca, 3
	}, a)
	e.fill(t)
	p, _ := e.placeReady(t, "order-1")
	eq(t, p.StartPath, "pooled")
	eq(t, p.EgressCAVersion, 3)
}

func TestPoolFillKeepsTheSampleAndTheSealedRef(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a"})
	e := newPoolEnv(t, nil, a)
	members := e.fill(t)
	m := members[0]
	eq(t, m.ProviderRef != nil, true)
	eq(t, m.Origin, "pool")
	eq(t, m.WarmAt != nil && m.SandboxCreatedAt != nil, true)
	var kind, path string
	var memberID uuid.UUID
	var ready int
	err := e.pool.QueryRow(t.Context(), `
		select kind, start_path, member_id, ready_ms from start_samples where kind = 'pool'`).
		Scan(&kind, &path, &memberID, &ready)
	eq(t, err, nil)
	eq(t, [2]string{kind, path}, [2]string{"pool", "pool"})
	eq(t, memberID, m.ID)
	if ready <= 0 {
		t.Fatalf("pool sample ready_ms = %d", ready)
	}
	// The labels carry the pool and the member, and never a placement
	owned := owned(t, a)
	eq(t, len(owned), 1)
	eq(t, owned[0].Labels["pool_member"], m.ID.String())
	eq(t, owned[0].Labels["bugnet_pool"], "small")
	_, has := owned[0].Labels["placement"]
	eq(t, has, false)
}

func TestSetupSpecCarriesAFreshSeed(t *testing.T) {
	var b saga.BootSettings
	ref := provider.Ref{Provider: "e2b", ID: "x"}
	seeds := map[string]bool{}
	for range 5 {
		spec := saga.SetupSpecFor(b, "e2b", ref, "key", false)
		raw, err := base64.StdEncoding.DecodeString(spec.Seed)
		eq(t, err, nil)
		eq(t, len(raw), 32)
		seeds[spec.Seed] = true
	}
	eq(t, len(seeds), 5)
	pooled := saga.PoolSetupSpecFor(b, "e2b", ref)
	eq(t, pooled.Phase, "pool")
	eq(t, pooled.GateKey, "")
	eq(t, pooled.GateListen, "")
	raw, err := json.Marshal(pooled)
	eq(t, err, nil)
	eq(t, strings.Contains(string(raw), `"phase":"pool"`), true)
	eq(t, pooled.Seed != "", true)
}

func TestWithinLifetimeCountsFromSandboxCreatedAt(t *testing.T) {
	c := newCapped(t, t.TempDir(), time.Hour)
	now := time.Now()
	p := store.Placement{CreatedAt: now}
	deadline := now.Add(50 * time.Minute)
	eq(t, saga.WithinLifetime(c, p, deadline), nil)
	// The same placement on a sandbox the provider made 30 minutes earlier has 85 minutes of life
	old := now.Add(-30 * time.Minute)
	p.SandboxCreatedAt = &old
	if err := saga.WithinLifetime(c, p, deadline); err == nil {
		t.Fatal("a sandbox 30 minutes old was allowed 85 minutes")
	}
}

// unparkFails is a local provider whose parked sandboxes cannot be woken once fail is set
type unparkFails struct {
	*local.Provider
	fail atomic.Bool
}

func (u *unparkFails) Unpark(ctx context.Context, ref provider.Ref) error {
	if u.fail.Load() {
		return &provider.Error{Code: provider.CodeInternal, Err: errors.New("agent gone")}
	}
	return u.Provider.Unpark(ctx, ref)
}
