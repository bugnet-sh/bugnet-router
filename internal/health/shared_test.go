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
