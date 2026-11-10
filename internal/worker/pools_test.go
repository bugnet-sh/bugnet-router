package worker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/pool"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// poolSettings is one pool of provider local's size small, with no headroom so demand sizes it
func poolSettings(lowest, highest int) *config.Pools {
	s := config.DefaultPools()
	s.Headroom = 0
	s.Pool = []config.Pool{{Provider: "local", Size: "small", Min: lowest, Max: highest,
		MaxAge: time.Hour}}
	return &s
}

func (e *env) tick(t *testing.T) {
	t.Helper()
	if err := e.worker.Pools().Tick(t.Context()); err != nil {
		t.Fatalf("pool tick: %v", err)
	}
}

func (e *env) members(t *testing.T, state string) []store.Member {
	t.Helper()
	all, err := e.store.LiveMembers(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var out []store.Member
	for _, m := range all {
		if state == "" || m.State == state {
			out = append(out, m)
		}
	}
	return out
}

func (e *env) fillAll(t *testing.T) {
	t.Helper()
	for _, m := range e.members(t, store.MemberFilling) {
		if err := e.worker.Pools().Fill(t.Context(), m.ID); err != nil {
			t.Fatalf("fill: %v", err)
		}
	}
	e.saga.Wait()
}

func (e *env) fillJobs(t *testing.T) int {
	t.Helper()
	return count(t, e, `select count(*) from river_job where kind = 'pool_fill'`)
}

func TestPoolTickFillsTheDeficit(t *testing.T) {
	e := newEnv(t, options{pools: poolSettings(3, 3)})
	e.tick(t)
	// At most max_fills_per_tick of the deficit, a slot and a job for each
	eq(t, len(e.members(t, store.MemberFilling)), 2)
	eq(t, e.fillJobs(t), 2)
	eq(t, e.inUse(t, "local"), 2)
	e.tick(t)
	eq(t, len(e.members(t, store.MemberFilling)), 3)
	eq(t, e.fillJobs(t), 3)
	e.fillAll(t)
	eq(t, len(e.members(t, store.MemberWarm)), 3)
	e.tick(t)
	eq(t, len(e.members(t, "")), 3)
	eq(t, e.fillJobs(t), 3)
	eq(t, e.inUse(t, "local"), 3)
}

func TestPoolTickStoresTheTarget(t *testing.T) {
	e := newEnv(t, options{pools: poolSettings(1, 5)})
	e.tick(t)
	targets, err := e.store.Targets(t.Context())
	eq(t, err, nil)
	eq(t, len(targets), 1)
	eq(t, targets[0].Target, 1)
	eq(t, time.Since(targets[0].ComputedAt) < time.Minute, true)
	// Misses are demand, 90 of them over 15 minutes at a 60 s refill make ceil(6) members
	for range 90 {
		eq(t, e.store.RecordMiss(t.Context(), "local", "small", time.Now()), nil)
	}
	e.tick(t)
	targets, err = e.store.Targets(t.Context())
	eq(t, err, nil)
	eq(t, targets[0].Target, 5)
	// A pool no longer configured loses its row, so router-api never shows a stale target
	eq(t, e.store.PutTarget(t.Context(), store.PoolTarget{
		Provider: "local", SizeName: "gone", Target: 3, ComputedAt: time.Now()}), nil)
	e.tick(t)
	targets, err = e.store.Targets(t.Context())
	eq(t, err, nil)
	eq(t, len(targets), 1)
}

// failingCreates is a local provider whose creates all fail
type failingCreates struct{ *local.Provider }

func (failingCreates) Create(context.Context, provider.CreateRequest) (provider.Ref, error) {
	return provider.Ref{}, &provider.Error{Code: provider.CodeInternal, CanFailOver: true,
		Err: errors.New("create refused")}
}

func TestThreeFailedFillsPauseThePool(t *testing.T) {
	bad := failingCreates{newLocal(t, "local")}
	e := newEnv(t, options{pools: poolSettings(3, 3), providers: []provider.Provider{bad}})
	e.tick(t)
	e.fillAll(t)
	eq(t, len(e.members(t, store.MemberFilling)), 0)
	e.tick(t)
	e.fillAll(t)
	pauses, err := e.store.Pauses(t.Context())
	eq(t, err, nil)
	eq(t, len(pauses), 1)
	eq(t, pauses[0].Reason, store.PauseFailing)
	if until := time.Until(pauses[0].Until); until < 9*time.Minute || until > 11*time.Minute {
		t.Fatalf("paused for %s, want 10 minutes", until)
	}
	// A paused pool starts no fill
	before := e.fillJobs(t)
	e.tick(t)
	eq(t, e.fillJobs(t), before)
	st, err := e.worker.Pools().Status(t.Context())
	eq(t, err, nil)
	eq(t, st[0].PausedReason, store.PauseFailing)
	// The failed members are retiring, and the tick queues their jobs
	for _, m := range e.members(t, store.MemberRetiring) {
		eq(t, e.worker.Pools().Retire(t.Context(), m.ID, store.EndFailed), nil)
	}
	eq(t, e.inUse(t, "local"), 0)
}

func TestPoolTickRetiresOldMembers(t *testing.T) {
	clk := &clock{}
	e := newEnv(t, options{pools: poolSettings(1, 1), workerClock: clk})
	e.tick(t)
	e.fillAll(t)
	old := e.members(t, store.MemberWarm)
	eq(t, len(old), 1)
	// A filling member whose job died, ten minutes old
	stuck, err := e.store.StartFill(t.Context(), "local", "small", 1, nil)
	eq(t, err, nil)
	clk.Advance(2 * time.Hour)
	e.tick(t)
	eq(t, e.member(t, old[0].ID).State, store.MemberRetiring)
	eq(t, e.member(t, old[0].ID).EndReason, store.EndAged)
	eq(t, e.member(t, stuck.ID).State, store.MemberRetiring)
	eq(t, e.member(t, stuck.ID).EndReason, store.EndFailed)
	// Each retire job is queued once, and a repeat tick adds none
	e.tick(t)
	for _, m := range []uuid.UUID{old[0].ID, stuck.ID} {
		eq(t, count(t, e, `select count(*) from river_job
			where kind = 'pool_retire' and args->>'member' = $1`, m.String()), 1)
	}
	for _, m := range []uuid.UUID{old[0].ID, stuck.ID} {
		eq(t, e.worker.Pools().Retire(t.Context(), m, store.EndAged), nil)
		eq(t, e.member(t, m).State, store.MemberGone)
	}
	eq(t, len(owned(t, e.local)), 0)
}

func (e *env) member(t *testing.T, id uuid.UUID) store.Member {
	t.Helper()
	m, err := e.store.MemberByID(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func owned(t *testing.T, p provider.Provider) []provider.Summary {
	t.Helper()
	o, err := p.ListOwned(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func TestSpendCapDropsTargetsToMin(t *testing.T) {
	s := poolSettings(1, 5)
	// One micro a day is spent by the first fill's running time
	s.MaxDailyMicros = 1
	e := newEnv(t, options{pools: s})
	for range 90 {
		eq(t, e.store.RecordMiss(t.Context(), "local", "small", time.Now()), nil)
	}
	e.tick(t)
	targets, err := e.store.Targets(t.Context())
	eq(t, err, nil)
	eq(t, targets[0].Target, 5)
	e.fillAll(t)
	time.Sleep(50 * time.Millisecond)
	e.tick(t)
	targets, err = e.store.Targets(t.Context())
	eq(t, err, nil)
	eq(t, targets[0].Target, 1)
	capped, cost, limit, err := e.worker.Pools().Spend(t.Context())
	eq(t, err, nil)
	eq(t, capped, true)
	eq(t, limit, int64(1))
	if cost < 1 {
		t.Fatalf("the day's cost is %d micros, want at least 1", cost)
	}
	// Reserve reads this key, set for two ticks while the cap holds
	n, err := e.vk.Do(t.Context(), e.vk.B().Exists().Key(e.prefix+pool.SpendKey).Build()).
		AsInt64()
	eq(t, err, nil)
	eq(t, n, int64(1))
}

func TestDrainRetiresWarmMembersAndKeepsRefillOff(t *testing.T) {
	e := newEnv(t, options{pools: poolSettings(2, 2)})
	e.tick(t)
	e.fillAll(t)
	eq(t, len(e.members(t, store.MemberWarm)), 2)
	n, err := e.worker.Pools().Drain(t.Context(), pool.Key{Provider: "local", Size: "small"},
		time.Hour, "ops")
	eq(t, err, nil)
	eq(t, n, 2)
	eq(t, len(e.members(t, store.MemberRetiring)), 2)
	e.tick(t)
	// Refill is off for the pause, so the tick starts nothing
	eq(t, len(e.members(t, store.MemberFilling)), 0)
	unknown := pool.Key{Provider: "local", Size: "nope"}
	_, err = e.worker.Pools().Drain(t.Context(), unknown, 0, "ops")
	eq(t, errors.Is(err, pool.ErrUnknownPool), true)
}

func TestPoolRetireNeverFreesASlotItDoesNotHold(t *testing.T) {
	e := newEnv(t, options{pools: poolSettings(1, 1)})
	e.tick(t)
	e.fillAll(t)
	p, _ := e.placeReady(t, "order-pooled")
	eq(t, p.StartPath, "pooled")
	m, err := e.store.MemberOfPlacement(t.Context(), p.ID)
	eq(t, err, nil)
	before := e.inUse(t, "local")
	// A bound member whose attempt failed retires, and the placement's slot is not its own
	eq(t, e.worker.Pools().Retire(t.Context(), m.ID, store.EndFailed), nil)
	eq(t, e.inUse(t, "local"), before)
}

func TestSweepPoolRules(t *testing.T) {
	clk := &clock{}
	e := newEnv(t, options{pools: poolSettings(1, 1), workerClock: clk})
	ctx := t.Context()
	local := e.local
	labels := func(m store.Member) map[string]string {
		l := e.owned()
		l["bugnet_pool"] = "small"
		l["pool_member"] = m.ID.String()
		return l
	}
	made := func(state string) (store.Member, provider.Ref) {
		m, err := e.store.StartFill(ctx, "local", "small", 1, nil)
		if err != nil {
			t.Fatal(err)
		}
		ref := e.sandbox(t, local, labels(m))
		if err := e.store.MarkMemberCreated(ctx, m.ID, ref, time.Now()); err != nil {
			t.Fatal(err)
		}
		sqlRun(t, e, `update pool_members set state = $2 where id = $1`, m.ID, state)
		return m, ref
	}
	_, fillingRef := made(store.MemberFilling)
	_, warmRef := made(store.MemberWarm)
	_, retiringRef := made(store.MemberRetiring)
	_, goneRef := made(store.MemberGone)
	heldOpen, heldOpenRef := made(store.MemberHeld)
	heldOrphan, heldOrphanRef := made(store.MemberHeld)
	// A reservation that is open keeps its member, one that ended does not
	reserve := func(m store.Member, state string) {
		sqlRun(t, e, `insert into reservations (id, quote_sha256, package_id, requirements,
				placement_id, key_id, member_id, path, state, expires_at, hold_until)
			values ($1, '\x00', 'p', '{}', $2, 'k', $3, 'pooled', $4, now() + interval '1 hour',
				now() + interval '1 hour')`, uuid.New(), uuid.New(), m.ID, state)
	}
	reserve(heldOpen, "ready")
	reserve(heldOrphan, "lapsed")
	// A bound member of a ready placement stays, its row or no row
	poolLabels := func() map[string]string {
		return map[string]string{
			"bugnet": "1", "bugnet_deployment": "main", "bugnet_pool": "small",
			"pool_member": uuid.NewString(),
		}
	}
	bound := e.placing(t)
	boundRef := e.sandbox(t, local, poolLabels())
	if ok, err := e.store.ReservePlacementCapacity(ctx, bound.ID, "local", 1); err != nil || !ok {
		t.Fatalf("reserve: %v %v", ok, err)
	}
	if err := e.store.MarkPlacementReady(ctx, bound.ID, boundRef, small.Name, time.Now(),
		time.Now().Add(time.Hour), nil); err != nil {
		t.Fatal(err)
	}
	lostRow := e.sandbox(t, local, poolLabels())
	e.start(t)
	e.run(t, jobargs.Sweep{})
	for name, s := range map[string]struct {
		ref  provider.Ref
		kept bool
	}{
		"filling":       {fillingRef, true},
		"warm":          {warmRef, true},
		"retiring":      {retiringRef, false},
		"gone":          {goneRef, false},
		"held open":     {heldOpenRef, true},
		"held orphaned": {heldOrphanRef, false},
		"bound ready":   {boundRef, true},
		"lost row":      {lostRow, true},
	} {
		if exists(t, local, s.ref) != s.kept {
			t.Errorf("%s: the sweep kept = %v, want %v", name, exists(t, local, s.ref), s.kept)
		}
	}

	// Ten minutes on, a filling member's job counts as dead, and a lost row's sandbox goes
	clk.Advance(11 * time.Minute)
	e.run(t, jobargs.Sweep{})
	if exists(t, local, fillingRef) || exists(t, local, lostRow) {
		t.Error("a stale filling member or a sandbox whose row was lost survived ten minutes")
	}
	if !exists(t, local, warmRef) || !exists(t, local, heldOpenRef) {
		t.Error("a warm or held member was destroyed by an ordinary sweep")
	}
	// A warm member past its max age plus an hour is the tick's miss, and the sweep destroys it
	clk.Advance(3 * time.Hour)
	e.run(t, jobargs.Sweep{})
	if exists(t, local, warmRef) {
		t.Error("a warm member two hours past its max age survived the sweep")
	}
}

func TestSweepKeepsAProviderHeldMember(t *testing.T) {
	e := newEnv(t, options{pools: poolSettings(1, 1)})
	ref := e.sandbox(t, e.local, map[string]string{
		"bugnet": "1", "bugnet_deployment": "main", "bugnet_pool": "small",
	})
	e.start(t)
	e.run(t, jobargs.Sweep{})
	// A pool sandbox with no row goes only once it is ten minutes old, and a young one stays
	if !exists(t, e.local, ref) {
		t.Fatal("a young pool sandbox without a row was destroyed")
	}
}

func TestRetentionPoolRows(t *testing.T) {
	e := newEnv(t, options{})
	ctx := t.Context()
	member := func(state string, endedAgo time.Duration, placement *uuid.UUID) uuid.UUID {
		id := uuid.New()
		sqlRun(t, e, `insert into pool_members (id, provider, size_name, origin, state,
				idempotency_key, placement_id, ended_at)
			values ($1, 'local', 'small', 'pool', $2, $3, $4,
				case when $5 > 0 then now() - make_interval(secs => $5) end)`,
			id, state, "pool-"+id.String(), placement, endedAgo.Seconds())
		return id
	}
	day := 24 * time.Hour
	oldGone := member(store.MemberGone, 31*day, nil)
	newGone := member(store.MemberGone, 29*day, nil)
	warm := member(store.MemberWarm, 0, nil)
	// A bound member ages from the end of its placement
	endedPlacement := e.placing(t)
	if err := e.store.MarkPlacementFailed(ctx, endedPlacement.ID, "boom", true); err != nil {
		t.Fatal(err)
	}
	sqlRun(t, e, `update placements set terminated_at = now() - interval '31 days' where id = $1`,
		endedPlacement.ID)
	livePlacement := e.placing(t)
	oldBound := member(store.MemberBound, 0, &endedPlacement.ID)
	liveBound := member(store.MemberBound, 0, &livePlacement.ID)
	// A member a reservation still refers to goes after the reservation does
	kept := member(store.MemberGone, 31*day, nil)
	reserve := func(ended time.Duration) {
		sqlRun(t, e, `insert into reservations (id, quote_sha256, package_id, requirements,
				placement_id, key_id, member_id, path, state, expires_at, hold_until, ended_at)
			values ($1, '\x00', 'p', '{}', $2, 'k', $3, 'pooled', 'released', now(), now(),
				now() - make_interval(secs => $4))`,
			uuid.New(), uuid.New(), kept, ended.Seconds())
	}
	reserve(day)
	sqlRun(t, e, `insert into pool_misses (provider, size_name, minute, misses)
		values ('local', 'small', now() - interval '2 days', 3), ('local', 'small', now(), 1)`)

	eq(t, e.worker.Retain(ctx), false)
	has := func(id uuid.UUID) bool {
		return count(t, e, `select count(*) from pool_members where id = $1`, id) == 1
	}
	eq(t, [6]bool{has(oldGone), has(newGone), has(warm), has(oldBound), has(liveBound),
		has(kept)}, [6]bool{false, true, true, false, true, true})
	eq(t, count(t, e, `select count(*) from reservations`), 1)
	eq(t, count(t, e, `select count(*) from pool_misses`), 1)
	// A reservation lives 7 days past its end, and its member goes in the same pass after it
	sqlRun(t, e, `update reservations set ended_at = now() - interval '8 days'`)
	eq(t, e.worker.Retain(ctx), false)
	eq(t, count(t, e, `select count(*) from reservations`), 0)
	eq(t, has(kept), false)
}

func TestCostDailyKnowsMemberSandboxes(t *testing.T) {
	e := newEnv(t, options{pools: poolSettings(1, 1)})
	e.tick(t)
	e.fillAll(t)
	m := e.members(t, store.MemberWarm)[0]
	ours, err := e.store.OursAmong(t.Context(), "local", []string{m.ProviderRef.ID, "other"})
	eq(t, err, nil)
	eq(t, ours, map[string]bool{m.ProviderRef.ID: true})
	ids, err := e.store.SandboxIDsBetween(t.Context(), "local", time.Now().Add(-time.Hour),
		time.Now().Add(time.Hour))
	eq(t, err, nil)
	eq(t, ids, []string{m.ProviderRef.ID})
	members, err := e.store.MembersCreatedBetween(t.Context(), "local",
		time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	eq(t, err, nil)
	eq(t, len(members), 1)
}
