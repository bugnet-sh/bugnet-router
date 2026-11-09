package store_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
	"github.com/bugnet-sh/bugnet-router/migrations"
)

func TestMigrationHealthUpDownUp(t *testing.T) {
	url := testenv.DatabaseURL(t)
	ctx := t.Context()
	cfg, err := pgx.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	db := stdlib.OpenDB(*cfg)
	t.Cleanup(func() { _ = db.Close() })
	gp, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS)
	if err != nil {
		t.Fatal(err)
	}
	v := migrationVersion(t, "health")
	tables := func() int {
		var n int
		err := db.QueryRowContext(ctx, `
			select count(*) from information_schema.tables
			where table_schema = current_schema()
				and table_name in ('start_samples', 'probe_slots')`).Scan(&n)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	if _, err := gp.UpTo(ctx, v-1); err != nil {
		t.Fatal(err)
	}
	eq(t, tables(), 0)
	if _, err := gp.UpTo(ctx, v); err != nil {
		t.Fatal(err)
	}
	eq(t, tables(), 2)
	if _, err := gp.DownTo(ctx, v-1); err != nil {
		t.Fatal(err)
	}
	eq(t, tables(), 0)
	if _, err := gp.UpTo(ctx, v); err != nil {
		t.Fatal(err)
	}
	eq(t, tables(), 2)
}

func sample(provider string, kind store.SampleKind, outcome string, at time.Time, readyMS int,
) store.StartSample {
	return store.StartSample{
		Kind: kind, Provider: provider, SizeName: "small", StartedAt: at, Outcome: outcome,
		CreateMS: 100, ReadyMS: readyMS, RunnerMS: store.NoStep, GateMS: 20,
		FirstCmdMS: store.NoStep, WarmCmdMS: store.NoStep, DestroyMS: store.NoStep,
		Replica: "r1",
	}
}

func TestStartSampleRoundTrip(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	at := time.Now().UTC().Truncate(time.Millisecond)
	in := sample("a", store.SamplePlacement, store.OutcomeFailed, at, 900)
	in.PlacementID = uuid.New()
	in.SandboxID = "sbx-1"
	in.ErrorCode, in.Error, in.CostMicros = "internal", "boom", 42
	if err := s.InsertStartSample(ctx, in); err != nil {
		t.Fatal(err)
	}
	probe := sample("a", store.SampleCanary, store.OutcomeOK, at.Add(time.Second), 700)
	probe.BenchID = ""
	if err := s.InsertStartSample(ctx, probe); err != nil {
		t.Fatal(err)
	}
	got, err := s.StartSamples(ctx, "a", at.Add(-time.Minute))
	if err != nil || len(got) != 2 {
		t.Fatalf("samples = %+v, %v", got, err)
	}
	in.ID, probe.ID = got[0].ID, got[1].ID
	eq(t, got[0], in)
	eq(t, got[1], probe)
	// A step that did not run is null, not zero
	eq(t, got[0].RunnerMS, store.NoStep)
	eq(t, got[0].GateMS, 20)
}

func TestHealthWindowCountsOnlyWhatCounts(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	now := time.Now().UTC()
	add := func(kind store.SampleKind, outcome string, ago time.Duration, ready int) {
		t.Helper()
		m := sample("a", kind, outcome, now.Add(-ago), ready)
		if err := s.InsertStartSample(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	for i := 1; i <= 20; i++ {
		add(store.SamplePlacement, store.OutcomeOK, time.Duration(i)*time.Minute/2, i*100)
	}
	add(store.SamplePlacement, store.OutcomeFailed, time.Minute, 5000)
	// These say nothing about the provider
	add(store.SamplePlacement, store.OutcomeCapacity, time.Minute, 0)
	add(store.SampleCanary, store.OutcomeRateLimited, 30*time.Second, 0)
	add(store.SampleCanary, store.OutcomeQuota, 20*time.Second, 0)
	// Canaries, newest last: ok, failed, then a capacity answer that is skipped over
	add(store.SampleCanary, store.OutcomeOK, 9*time.Minute, 400)
	add(store.SampleCanary, store.OutcomeFailed, 3*time.Minute, 0)
	add(store.SampleCanary, store.OutcomeCapacity, 2*time.Minute, 0)
	// Out of the window
	add(store.SamplePlacement, store.OutcomeFailed, time.Hour, 0)

	c, err := s.HealthWindow(ctx, "a", now.Add(-15*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	// 20 placements and a canary are ok, one placement and one canary failed
	eq(t, c.OK, 21)
	eq(t, c.Failed, 2)
	// The nearest-rank p95 of 21 ok values is the 20th, 1900 ms
	eq(t, c.ReadyP95, 1900*time.Millisecond)
	eq(t, c.LastCanaries, []string{"failed", "ok"})

	none, err := s.HealthWindow(ctx, "b", now.Add(-15*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, none.OK+none.Failed, 0)
	eq(t, none.ReadyP95, time.Duration(0))
	eq(t, len(none.LastCanaries), 0)
}

func TestWriteHealthMovesStateSinceOnlyOnAChange(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	t0 := time.Now().UTC().Truncate(time.Millisecond)
	h := store.HealthState{State: store.HealthOK, Samples: 3, SuccessLB: 0.9}
	before, err := s.WriteHealth(ctx, "a", h, t0)
	eq(t, before, "")
	if err != nil {
		t.Fatal(err)
	}
	h.State, h.ReadyP95 = store.HealthDown, 1500*time.Millisecond
	before, err = s.WriteHealth(ctx, "a", h, t0.Add(time.Minute))
	eq(t, before, "ok")
	if err != nil {
		t.Fatal(err)
	}
	before, err = s.WriteHealth(ctx, "a", h, t0.Add(2*time.Minute))
	eq(t, before, "down")
	if err != nil {
		t.Fatal(err)
	}
	// An account block written beside it stays
	if _, err := s.RecordAccount(ctx, "a", "banned", "x", t0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteHealth(ctx, "a", h, t0.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	rows, err := s.ProviderHealths(ctx)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows = %+v, %v", rows, err)
	}
	r := rows[0]
	eq(t, r.State, "down")
	eq(t, r.StateSince, t0.Add(time.Minute))
	eq(t, r.Samples, 3)
	eq(t, r.ReadyP95, 1500*time.Millisecond)
	eq(t, r.Blocked, "banned")
}

func TestProbeSlotIsHeldAndReleasedOnce(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	holdCapacity(t, s, "e2b", 2, 1, 0)
	id := uuid.New()
	ok, err := s.ReserveProbeSlot(ctx, "e2b", id, time.Now())
	if err != nil || !ok {
		t.Fatalf("probe slot = %v, %v", ok, err)
	}
	n, err := s.ProbeSlotCount(ctx, "e2b")
	eq(t, n, 1)
	if err != nil {
		t.Fatal(err)
	}
	// The provider is full now, and a second probe finds no slot and leaves no row
	ok, err = s.ReserveProbeSlot(ctx, "e2b", uuid.New(), time.Now())
	if err != nil || ok {
		t.Fatalf("second probe slot = %v, %v", ok, err)
	}
	n, _ = s.ProbeSlotCount(ctx, "e2b")
	eq(t, n, 1)
	use := func() int {
		rows, err := s.Capacities(ctx)
		if err != nil || len(rows) != 1 {
			t.Fatalf("capacity = %+v, %v", rows, err)
		}
		return rows[0].InUse
	}
	eq(t, use(), 2)
	if err := s.ReleaseProbeSlot(ctx, "e2b", id); err != nil {
		t.Fatal(err)
	}
	// A second release frees nothing
	if err := s.ReleaseProbeSlot(ctx, "e2b", id); err != nil {
		t.Fatal(err)
	}
	eq(t, use(), 1)
	n, _ = s.ProbeSlotCount(ctx, "e2b")
	eq(t, n, 0)
}

func TestCanaryFactsOfReadsSamplesPlacementsAndHealth(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	f, err := s.CanaryFactsOf(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	eq(t, f, store.CanaryFacts{State: "ok"})
	at := time.Now().UTC().Truncate(time.Millisecond)
	if err := s.InsertStartSample(ctx, sample("a", store.SampleCanary, "ok", at, 1)); err != nil {
		t.Fatal(err)
	}
	// A placement sample is not a canary
	err = s.InsertStartSample(ctx, sample("a", store.SamplePlacement, "ok", at.Add(time.Hour), 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecordAccount(ctx, "a", "auth", "", at); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteHealth(ctx, "a", store.HealthState{State: "down"}, at); err != nil {
		t.Fatal(err)
	}
	f, err = s.CanaryFactsOf(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	eq(t, f, store.CanaryFacts{LastCanary: at, State: "down", Blocked: "auth"})
}
