package health_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/bugnet-sh/bugnet-router/internal/health"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

type clock struct{ now time.Time }

func (c *clock) Now() time.Time              { return c.now }
func (c *clock) add(d time.Duration)         { c.now = c.now.Add(d) }
func newClock() *clock                       { return &clock{now: time.Now()} }
func (c *clock) tracker() *placement.Tracker { return placement.NewTracker(c.Now) }

func open(t *testing.T) (*store.Store, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	url := testenv.DatabaseURL(t)
	if err := store.Migrate(ctx, url); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return store.New(pool), pool
}

func TestBlockedProviderSkippedWithin10s(t *testing.T) {
	st, _ := open(t)
	ctx := context.Background()
	c := newClock()
	shared := health.NewShared(st, c.tracker(), nil, health.WithClock(c.Now))
	if !shared.Healthy("a") {
		t.Fatal("a provider with no row is blocked")
	}
	if _, err := st.RecordAccount(ctx, "a", "banned", "team banned", c.now); err != nil {
		t.Fatal(err)
	}
	// This replica read the rows a moment ago, so the block shows by the next read
	c.add(health.DefaultEvery - time.Second)
	if !shared.Healthy("a") {
		t.Fatal("the rows were read again before they were due")
	}
	c.add(time.Second)
	if shared.Healthy("a") {
		t.Fatal("a blocked provider is still healthy after 10 s")
	}
	if !shared.Healthy("b") {
		t.Fatal("a block on a spread to b")
	}
	// A clear brings it back by the next read
	if _, err := st.RecordAccount(ctx, "a", "", "", c.now); err != nil {
		t.Fatal(err)
	}
	c.add(health.DefaultEvery)
	if !shared.Healthy("a") {
		t.Fatal("a cleared provider stays skipped")
	}
}

func TestLocalBreakerStillCloses(t *testing.T) {
	st, _ := open(t)
	c := newClock()
	tracker := c.tracker()
	shared := health.NewShared(st, tracker, nil, health.WithClock(c.Now))
	for range 3 {
		tracker.RecordFailure("a")
	}
	if shared.Healthy("a") {
		t.Fatal("three local failures left the provider healthy")
	}
	c.add(121 * time.Second)
	if !shared.Healthy("a") {
		t.Fatal("the breaker stayed closed past its 120 s")
	}
}

func TestLastReadingStandsForAMinuteWhenAReadFails(t *testing.T) {
	st, pool := open(t)
	ctx := context.Background()
	c := newClock()
	shared := health.NewShared(st, c.tracker(), nil, health.WithClock(c.Now))
	if _, err := st.RecordAccount(ctx, "a", "unfunded", "no credit", c.now); err != nil {
		t.Fatal(err)
	}
	if shared.Healthy("a") {
		t.Fatal("a blocked provider is healthy")
	}
	pool.Close()
	c.add(30 * time.Second)
	if shared.Healthy("a") {
		t.Fatal("the last reading was dropped after 30 s of failed reads")
	}
	c.add(31 * time.Second)
	if !shared.Healthy("a") {
		t.Fatal("a reading over a minute old still blocks, only the local tracker should count")
	}
}

func TestReadyP95FollowsTheLocalTracker(t *testing.T) {
	st, _ := open(t)
	c := newClock()
	tracker := c.tracker()
	shared := health.NewShared(st, tracker, nil, health.WithClock(c.Now))
	tracker.RecordReady("a", 3*time.Second)
	if got := shared.ReadyP95("a"); got != 3*time.Second {
		t.Fatalf("ready p95 = %v, want the tracker's 3s", got)
	}
}

func writeState(t *testing.T, st *store.Store, name, state string, samples int, p95 time.Duration) {
	t.Helper()
	_, err := st.WriteHealth(context.Background(), name, store.HealthState{
		State: state, Samples: samples, SuccessLB: 0.4, ReadyP95: p95,
	}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
}

func TestDownRowSeenByAnotherReplicaWithin10s(t *testing.T) {
	st, _ := open(t)
	c := newClock()
	replicaA := health.NewShared(st, c.tracker(), nil, health.WithClock(c.Now))
	replicaB := health.NewShared(st, c.tracker(), nil, health.WithClock(c.Now))
	eq(t, replicaA.Healthy("a") && replicaB.Healthy("a"), true)
	// The rollup that runs on replica A writes the row both replicas read
	writeState(t, st, "a", store.HealthDown, 8, 0)
	c.add(health.DefaultEvery)
	eq(t, replicaB.Healthy("a"), false)
	eq(t, replicaA.Healthy("a"), false)
	eq(t, replicaB.Healthy("b"), true)
	// A slow provider is still tried, only a down one is skipped
	writeState(t, st, "a", store.HealthSlow, 8, 12*time.Second)
	c.add(health.DefaultEvery)
	eq(t, replicaB.Healthy("a"), true)
	writeState(t, st, "a", store.HealthOK, 8, time.Second)
	c.add(health.DefaultEvery)
	eq(t, replicaB.Healthy("a"), true)
}

func TestLocalBreakerStillTripsAlone(t *testing.T) {
	st, _ := open(t)
	c := newClock()
	tracker := c.tracker()
	shared := health.NewShared(st, tracker, nil, health.WithClock(c.Now))
	writeState(t, st, "a", store.HealthOK, 8, time.Second)
	c.add(health.DefaultEvery)
	eq(t, shared.Healthy("a"), true)
	for range 3 {
		tracker.RecordFailure("a")
	}
	// No rollup ran, and the row still says ok
	eq(t, shared.Healthy("a"), false)
	eq(t, shared.Healthy("b"), true)
}

func TestReadyP95PrefersTheSharedReading(t *testing.T) {
	st, _ := open(t)
	c := newClock()
	tracker := c.tracker()
	for range 6 {
		tracker.RecordReady("a", 2*time.Second)
	}
	shared := health.NewShared(st, tracker, nil, health.WithClock(c.Now))
	eq(t, shared.ReadyP95("a"), 2*time.Second)
	// Under 5 samples the shared figure does not count
	writeState(t, st, "a", store.HealthSlow, 4, 30*time.Second)
	c.add(health.DefaultEvery)
	eq(t, shared.ReadyP95("a"), 2*time.Second)
	writeState(t, st, "a", store.HealthSlow, 5, 30*time.Second)
	c.add(health.DefaultEvery)
	eq(t, shared.ReadyP95("a"), 30*time.Second)
}

func TestSharedTotalsAreTheRollupsWholeAttemptFigures(t *testing.T) {
	st, _ := open(t)
	ctx := context.Background()
	c := newClock()
	shared := health.NewShared(st, c.tracker(), nil, health.WithClock(c.Now))
	cold, pooled, n := shared.Totals("a")
	if cold != 0 || pooled != 0 || n != 0 {
		t.Fatalf("a provider without a row has totals %v, %v, %d", cold, pooled, n)
	}
	_, err := st.WriteHealth(ctx, "a", store.HealthState{
		State: store.HealthOK, Samples: 7, ColdTotalP95: 4 * time.Second,
		PooledTotalP95: 900 * time.Millisecond,
	}, c.now)
	if err != nil {
		t.Fatal(err)
	}
	c.add(health.DefaultEvery)
	cold, pooled, n = shared.Totals("a")
	if cold != 4*time.Second || pooled != 900*time.Millisecond || n != 7 {
		t.Fatalf("totals %v, %v, %d, want 4s, 900ms, 7", cold, pooled, n)
	}
}
