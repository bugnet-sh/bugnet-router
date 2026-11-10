package pool_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/pool"
	"github.com/bugnet-sh/bugnet-router/internal/spend"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestTarget(t *testing.T) {
	c := pool.Config{Min: 2, Max: 5}
	eq(t, pool.Target(c, 0, time.Minute, 2), 2)
	// ceil(1.2) + 2
	eq(t, pool.Target(c, 0.02, time.Minute, 2), 4)
	// 62, so max binds
	eq(t, pool.Target(c, 1, time.Minute, 2), 5)
	// Headroom alone never takes a pool under its min
	eq(t, pool.Target(pool.Config{Min: 3, Max: 5}, 0, time.Minute, 0), 3)
}

func TestWarmthReadsEverySecondsAndNamesNoUnknownPool(t *testing.T) {
	st := newStore(t)
	now := time.Now()
	clk := &now
	w := pool.NewWarmth(st, []pool.Config{{Key: pool.Key{Provider: "local", Size: "small"},
		Min: 2, Max: 6}}, func() time.Time { return *clk })
	eq(t, w.HasPool("local", "small"), true)
	eq(t, w.HasPool("local", "big"), false)
	eq(t, w.WarmAvailable("local", "small"), false)

	m, err := st.StartFill(t.Context(), "local", "small", 1, nil)
	eq(t, err, nil)
	eq(t, st.MarkMemberWarm(t.Context(), m.ID, refOf("sb-1"), "", now), nil)
	// The view is read again after two seconds, not before
	eq(t, w.WarmAvailable("local", "small"), false)
	*clk = now.Add(3 * time.Second)
	eq(t, w.WarmAvailable("local", "small"), true)
	eq(t, w.WarmAvailable("local", "big"), false)

	// With no stored target a pool reads as its min, and a stored one is bounded by min and max
	eq(t, w.Target("local", "small"), 2)
	eq(t, st.PutTarget(t.Context(), store.PoolTarget{Provider: "local", SizeName: "small",
		Target: 4, ComputedAt: *clk}), nil)
	*clk = clk.Add(3 * time.Second)
	eq(t, w.Target("local", "small"), 4)
	eq(t, st.PutTarget(t.Context(), store.PoolTarget{Provider: "local", SizeName: "small",
		Target: 40, ComputedAt: *clk}), nil)
	*clk = clk.Add(3 * time.Second)
	eq(t, w.Target("local", "small"), 6)
}

func TestStaleTargetReadsAsMin(t *testing.T) {
	st := newStore(t)
	now := time.Now()
	clk := &now
	w := pool.NewWarmth(st, []pool.Config{{Key: pool.Key{Provider: "local", Size: "small"},
		Min: 2, Max: 6}}, func() time.Time { return *clk })
	eq(t, st.PutTarget(t.Context(), store.PoolTarget{Provider: "local", SizeName: "small",
		Target: 5, ComputedAt: now.Add(-2 * time.Minute)}), nil)
	eq(t, w.Target("local", "small"), 2)
	eq(t, st.PutTarget(t.Context(), store.PoolTarget{Provider: "local", SizeName: "small",
		Target: 5, ComputedAt: now.Add(-30 * time.Second)}), nil)
	*clk = clk.Add(3 * time.Second)
	eq(t, w.Target("local", "small"), 5)
	// A nil view knows no pool, as a router with no pools sees
	var none *pool.Warmth
	eq(t, none.HasPool("local", "small"), false)
	eq(t, none.WarmAvailable("local", "small"), false)
	eq(t, none.Target("local", "small"), 0)
}

func TestOverheadMicros(t *testing.T) {
	base := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	at := func(m int) *time.Time { v := base.Add(time.Duration(m) * time.Minute); return &v }
	const hourly, parked = 3_600_000, 360_000
	// Ten minutes filling, fifty parked, then retired unused
	unused := store.Member{
		State: store.MemberGone, CreatedAt: base, WarmAt: at(10), EndedAt: at(60),
	}
	eq(t, pool.OverheadMicros(unused, hourly, parked, base, base.Add(2*time.Hour)),
		int64(600_000+300_000))
	// Only the part inside the window counts
	eq(t, pool.OverheadMicros(unused, hourly, parked, base.Add(30*time.Minute),
		base.Add(2*time.Hour)), int64(180_000))
	// A member a placement bound pays no running time here after its claim
	bound := store.Member{
		State: store.MemberBound, CreatedAt: base, WarmAt: at(10), ClaimedAt: at(20),
		BoundAt: at(20),
	}
	eq(t, pool.OverheadMicros(bound, hourly, parked, base, base.Add(2*time.Hour)),
		int64(600_000+60_000))
	// A held member that ends unbound keeps running until it ends
	held := store.Member{
		State: store.MemberGone, CreatedAt: base, WarmAt: at(10), ClaimedAt: at(20),
		EndedAt: at(30),
	}
	eq(t, pool.OverheadMicros(held, hourly, parked, base, base.Add(2*time.Hour)),
		int64(600_000+60_000+600_000))
	// A member still warm costs up to now
	warm := store.Member{State: store.MemberWarm, CreatedAt: base, WarmAt: at(10)}
	eq(t, pool.OverheadMicros(warm, hourly, parked, base, *at(70)), int64(600_000+360_000))
	// The meter prices the same time, whatever the window
	eq(t, spend.MemberCost(unused, hourly, parked, base.Add(2*time.Hour)), int64(900_000))
}

func newStore(t *testing.T) *store.Store {
	t.Helper()
	url := testenv.DatabaseURL(t)
	if err := store.Migrate(t.Context(), url); err != nil {
		t.Fatal(err)
	}
	pgPool, err := newPool(t, url)
	if err != nil {
		t.Fatal(err)
	}
	return store.New(pgPool)
}
