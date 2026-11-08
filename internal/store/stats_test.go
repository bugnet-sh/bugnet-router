package store_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
	"github.com/bugnet-sh/bugnet-router/migrations"
)

const (
	txSig = "5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnb" +
		"JLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW"
	txExt = "2GxEQDvUNB1J644jDee6TixtWGkyKRydgKnTXmVKohxq" +
		"vUqm7uMfhRnnCWgiWyiqDTX1DAJ2fB2BU8dWS4E7moZ5"
)

func TestMigrationStatsUpDownUp(t *testing.T) {
	url := testenv.DatabaseURL(t)
	ctx := context.Background()
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
	v := migrationVersion(t, "stats")
	present := func() []bool {
		var table, column, index bool
		err := db.QueryRowContext(ctx, `select
			to_regclass('stats_snapshot') is not null,
			exists (select 1 from information_schema.columns
				where table_name = 'orders' and column_name = 'tx'),
			to_regclass('placements_created_idx') is not null`).Scan(&table, &column, &index)
		if err != nil {
			t.Fatal(err)
		}
		return []bool{table, column, index}
	}
	if _, err := gp.UpTo(ctx, v); err != nil {
		t.Fatal(err)
	}
	eq(t, present(), []bool{true, true, true})
	_, err = db.ExecContext(ctx, `insert into stats_snapshot (id, router_at) values (2, now())`)
	if err == nil {
		t.Fatal("a second snapshot row was accepted")
	}
	if _, err := gp.DownTo(ctx, v-1); err != nil {
		t.Fatal(err)
	}
	eq(t, present(), []bool{false, false, false})
	if _, err := gp.UpTo(ctx, v); err != nil {
		t.Fatal(err)
	}
	eq(t, present(), []bool{true, true, true})
}

func TestStatsSnapshotRoundTrip(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	if _, err := s.ReadStatsSnapshot(ctx); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("before the first run: %v", err)
	}
	t0 := time.Date(2026, 11, 7, 14, 2, 31, 0, time.UTC)
	first := store.StatsSnapshot{RouterAt: t0}
	if err := s.WriteStatsSnapshot(ctx, first); err != nil {
		t.Fatal(err)
	}
	got, err := s.ReadStatsSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, got, first)
	second := store.StatsSnapshot{
		Body: []byte(`{"version":1}`), RouterAt: t0.Add(30 * time.Second),
		Payments: []byte(`{"version": 1}`), PaymentsAt: t0,
	}
	if err := s.WriteStatsSnapshot(ctx, second); err != nil {
		t.Fatal(err)
	}
	got, err = s.ReadStatsSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, string(got.Body), string(second.Body))
	eq(t, got.RouterAt, second.RouterAt)
	eq(t, got.PaymentsAt, second.PaymentsAt)
	eq(t, string(got.Payments), `{"version": 1}`)
}

func TestOrdersKeepTheTxClaim(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	p, _, err := s.CreatePlacement(ctx, store.NewPlacement{
		OrderID: "start", HandleSHA256: []byte("h"), PackageID: "pkg", Tx: txSig,
	})
	if err != nil {
		t.Fatal(err)
	}
	ready(t, s, p)
	if _, _, err := s.ExtendWithOrder(ctx, store.Order{
		ID: "more", PlacementID: p.ID, Tx: txExt,
	}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ExtendWithOrder(ctx, store.Order{
		ID: "bare", PlacementID: p.ID,
	}, time.Minute); err != nil {
		t.Fatal(err)
	}
	for id, want := range map[string]string{"start": txSig, "more": txExt, "bare": ""} {
		o, err := s.OrderByID(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if o.Tx != want {
			t.Errorf("order %s tx %q, want %q", id, o.Tx, want)
		}
	}
}

// session is one placement the sessions query reads, with its start order
type session struct {
	created              time.Time
	state                string
	provider             string
	ready, terminating   *time.Time
	terminated           *time.Time
	termination, failure string
	payer, feePayer, tx  string
	amount               int64
	jobs                 int
}

func seedSession(t *testing.T, pool *pgxpool.Pool, s session) uuid.UUID {
	t.Helper()
	ctx := t.Context()
	id := uuid.New()
	order := "o-" + id.String()
	null := func(v string) any {
		if v == "" {
			return nil
		}
		return v
	}
	_, err := pool.Exec(ctx, `
		insert into placements (id, handle_sha256, order_id, package_id, state, provider,
			created_at, ready_at, terminating_at, terminated_at, termination_reason,
			failure_reason)
		values ($1, $2, $3, 'pkg', $4, $5, $6, $7, $8, $9, $10, $11)`,
		id, []byte(order), order, s.state, null(s.provider), s.created, s.ready,
		s.terminating, s.terminated, null(s.termination), null(s.failure))
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `
		insert into orders (order_id, kind, placement_id, payer, fee_payer, amount_micros, tx)
		values ($1, 'start', $2, $3, $4, $5, $6)`,
		order, id, null(s.payer), null(s.feePayer), s.amount, null(s.tx))
	if err != nil {
		t.Fatal(err)
	}
	for i := range s.jobs {
		_, err := pool.Exec(ctx, `insert into jobs (id, placement_id, state)
			values ($1, $2, 'exited')`, fmt.Sprintf("j-%s-%d", id, i), id)
		if err != nil {
			t.Fatal(err)
		}
	}
	return id
}

func at(t time.Time) *time.Time { return &t }

func TestListStatsSessionsPages(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := t.Context()
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	since := now.Add(-30 * 24 * time.Hour)
	seen := map[uuid.UUID]bool{}
	for i := range 120 {
		// Pairs share a created_at, so the id breaks the tie
		created := now.Add(-time.Duration(i/2) * time.Minute)
		seen[seedSession(t, pool, session{created: created, state: "terminated",
			terminated: at(created.Add(time.Minute))})] = false
	}
	placing := seedSession(t, pool, session{created: now, state: "placing"})
	old := seedSession(t, pool, session{created: now.Add(-31 * 24 * time.Hour),
		state: "terminated"})

	var lens []int
	var beforeAt time.Time
	var beforeID uuid.UUID
	for range 4 {
		rows, err := s.ListStatsSessions(ctx, since, beforeAt, beforeID, 51)
		if err != nil {
			t.Fatal(err)
		}
		page := rows[:min(len(rows), 50)]
		lens = append(lens, len(page))
		for i, r := range page {
			if r.ID == placing || r.ID == old {
				t.Fatalf("row %s should be left out", r.ID)
			}
			if done, ok := seen[r.ID]; !ok || done {
				t.Fatalf("row %s unknown or twice", r.ID)
			}
			seen[r.ID] = true
			if i > 0 {
				prev := page[i-1]
				if r.CreatedAt.After(prev.CreatedAt) ||
					r.CreatedAt.Equal(prev.CreatedAt) && r.ID.String() > prev.ID.String() {
					t.Fatalf("rows out of order at %d", i)
				}
			}
		}
		if len(rows) <= 50 {
			break
		}
		beforeAt, beforeID = page[49].CreatedAt, page[49].ID
	}
	eq(t, lens, []int{50, 50, 20})
	for id, done := range seen {
		if !done {
			t.Fatalf("row %s never paged", id)
		}
	}
}

func TestListStatsSessionsFields(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := t.Context()
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	created := now.Add(-time.Hour)
	ready, stopping := created.Add(2*time.Second), created.Add(10*time.Minute)
	id := seedSession(t, pool, session{
		created: created, state: "terminated", provider: "boat", ready: at(ready),
		terminating: at(stopping), terminated: at(stopping.Add(3 * time.Second)),
		termination: "stop", payer: walletA, feePayer: walletB, tx: txSig, amount: 50000,
		jobs: 3,
	})
	_, err := pool.Exec(ctx, `insert into orders (order_id, kind, placement_id, amount_micros)
		values ('ext', 'extend', $1, 45000)`, id)
	if err != nil {
		t.Fatal(err)
	}
	// Another order names walletA as its fee payer, so walletA is a facilitator
	seedSession(t, pool, session{created: created.Add(-time.Minute), state: "failed",
		failure: "boot_failed", feePayer: walletA, terminated: at(created)})

	rows, err := s.ListStatsSessions(ctx, now.Add(-30*24*time.Hour), time.Time{}, uuid.Nil, 51)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(rows), 2)
	eq(t, rows[0], store.StatsSessionRow{
		ID: id, CreatedAt: created, ReadyAt: at(ready), EndedAt: at(stopping),
		State: "terminated", Provider: "boat", PackageID: "pkg", TerminationReason: "stop",
		Commands: 3, PriceMicros: 95000, Payer: walletA, FeePayer: walletB, Tx: txSig,
		PayerWasFeePayer: true,
	})
	eq(t, rows[1].FailureReason, "boot_failed")
	eq(t, rows[1].Provider, "")
	eq(t, rows[1].PayerWasFeePayer, false)
	eq(t, rows[1].EndedAt, at(created))
}

func TestListStatsSessionsTakesEveryCursorTime(t *testing.T) {
	s, pool := newStoreWithPool(t)
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	created := now.Add(-time.Hour)
	seedSession(t, pool, session{created: created, state: "terminated", terminated: at(now)})
	// The first and the last time stats.DecodeCursor lets through
	for before, want := range map[time.Time]int{
		time.Unix(0, 0).UTC(): 0,
		time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC): 1,
	} {
		rows, err := s.ListStatsSessions(t.Context(), now.Add(-30*24*time.Hour), before,
			uuid.New(), 51)
		if err != nil {
			t.Fatalf("before %s: %v", before, err)
		}
		eq(t, len(rows), want)
	}
}

// figuresAt is the time every figures test reads at
var figuresAt = time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)

// run seeds a started placement, ended at end unless end is zero
func run(t *testing.T, pool *pgxpool.Pool, provider string, ready, end time.Time) {
	t.Helper()
	s := session{created: ready.Add(-time.Second), state: "ready", provider: provider,
		ready: at(ready)}
	if !end.IsZero() {
		s.state, s.terminating, s.terminated = "terminated", at(end), at(end.Add(time.Second))
	}
	seedSession(t, pool, s)
}

func day(d int) time.Time { return time.Date(2026, 11, d, 0, 0, 0, 0, time.UTC) }

func clock(d, h, m, s, ms int) time.Time {
	return time.Date(2026, 11, d, h, m, s, ms*int(time.Millisecond), time.UTC)
}

// figures reads the figures at figuresAt and keeps only what a test compares
func figures(t *testing.T, s *store.Store) store.StatsFigures {
	t.Helper()
	f, err := s.StatsFigures(t.Context(), figuresAt)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, f.At, figuresAt)
	eq(t, f.SpentMicros, (*int64)(nil))
	for _, d := range f.Days {
		eq(t, d.SpentMicros, (*int64)(nil))
	}
	return f
}

func TestStatsFigures(t *testing.T) {
	s, pool := newStoreWithPool(t)
	run(t, pool, "boat", clock(6, 23, 0, 0, 0), clock(7, 1, 0, 0, 0))
	run(t, pool, "e2b", clock(7, 10, 0, 0, 700), clock(7, 10, 10, 0, 200))
	run(t, pool, "boat", clock(7, 10, 5, 0, 0), clock(7, 10, 25, 0, 0))
	run(t, pool, "boat", clock(8, 11, 0, 0, 0), time.Time{})
	seedSession(t, pool, session{created: clock(8, 9, 0, 0, 0), state: "failed",
		failure: "boot_failed", terminated: at(clock(8, 9, 1, 0, 0))})
	// Ready after the read time, so it counts nowhere
	run(t, pool, "boat", figuresAt.Add(time.Second), time.Time{})

	f := figures(t, s)
	eq(t, f.Active, map[string]int64{"boat": 1})
	eq(t, f.Sandboxes, int64(4))
	eq(t, f.ComputeSeconds, int64(7200+599+1200+3600))
	eq(t, f.Days, []store.StatsDay{
		{Day: day(6), Sandboxes: 1, ComputeSeconds: 3600, PeakActive: 1},
		{Day: day(7), Sandboxes: 2, ComputeSeconds: 3600 + 599 + 1200, PeakActive: 2,
			MedianSeconds: 1200},
		{Day: day(8), Sandboxes: 1, ComputeSeconds: 3600, PeakActive: 1},
	})
	eq(t, f.Providers, []store.StatsProvider{
		{Provider: "boat", Sandboxes7d: 3, Active: 1},
		{Provider: "e2b", Sandboxes7d: 1},
	})
}

func TestPeakActiveCountsCarriedIn(t *testing.T) {
	s, pool := newStoreWithPool(t)
	run(t, pool, "boat", clock(5, 22, 0, 0, 0), clock(7, 2, 0, 0, 0))
	f := figures(t, s)
	eq(t, f.Days, []store.StatsDay{
		{Day: day(5), Sandboxes: 1, ComputeSeconds: 7200, PeakActive: 1},
		{Day: day(6), ComputeSeconds: 86400, PeakActive: 1},
		{Day: day(7), ComputeSeconds: 7200, PeakActive: 1, MedianSeconds: 28 * 3600},
		{Day: day(8)},
	})
	// An old run outside the 7 days keeps no provider row
	eq(t, f.Providers, []store.StatsProvider{{Provider: "boat", Sandboxes7d: 1}})
}

func TestPeakActiveEndsFirstAtEqualTimes(t *testing.T) {
	s, pool := newStoreWithPool(t)
	run(t, pool, "boat", clock(7, 10, 0, 0, 0), clock(7, 11, 0, 0, 0))
	run(t, pool, "boat", clock(7, 11, 0, 0, 0), clock(7, 12, 0, 0, 0))
	run(t, pool, "e2b", clock(7, 23, 0, 0, 0), day(8))
	f := figures(t, s)
	eq(t, f.Days, []store.StatsDay{
		{Day: day(7), Sandboxes: 3, ComputeSeconds: 3 * 3600, PeakActive: 1,
			MedianSeconds: 3600},
		// The run ending at midnight is not carried into the day, yet ended on it
		{Day: day(8), MedianSeconds: 3600},
	})
}

func TestMedianRoundsDownAndCountsOnlyTheEnded(t *testing.T) {
	s, pool := newStoreWithPool(t)
	run(t, pool, "boat", clock(8, 1, 0, 0, 0), clock(8, 1, 0, 3, 0))
	run(t, pool, "boat", clock(8, 2, 0, 0, 0), clock(8, 2, 0, 6, 0))
	run(t, pool, "boat", clock(8, 3, 0, 0, 0), time.Time{})
	f := figures(t, s)
	eq(t, f.Days[0].MedianSeconds, int64(4))
}

func TestStatsFiguresFirstOrder(t *testing.T) {
	s, pool := newStoreWithPool(t)
	eq(t, figures(t, s).FirstOrderAt, (*time.Time)(nil))
	// An order after the read time is not one yet
	seedSession(t, pool, session{created: figuresAt.Add(time.Second), state: "placing"})
	eq(t, figures(t, s).FirstOrderAt, (*time.Time)(nil))
	// A placement that never started still came with its paid order
	seedSession(t, pool, session{created: clock(3, 9, 0, 0, 0), state: "failed",
		failure: "no_candidate", terminated: at(clock(3, 9, 0, 0, 0))})
	run(t, pool, "boat", clock(5, 10, 0, 0, 0), clock(5, 11, 0, 0, 0))
	eq(t, figures(t, s).FirstOrderAt, at(clock(3, 9, 0, 0, 0)))
}

func TestStatsFiguresWithNoStartedPlacement(t *testing.T) {
	s, pool := newStoreWithPool(t)
	seedSession(t, pool, session{created: clock(8, 9, 0, 0, 0), state: "failed",
		failure: "no_candidate", terminated: at(clock(8, 9, 0, 0, 0))})
	f := figures(t, s)
	eq(t, f.Sandboxes, int64(0))
	eq(t, f.Days, []store.StatsDay{})
	eq(t, f.Providers, []store.StatsProvider{})
	eq(t, f.Active, map[string]int64{})
}
