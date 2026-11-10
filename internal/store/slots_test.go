package store_test

import (
	"context"
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

// takeSlot moves in_use up one, as every holder's transaction does beside its own row
func takeSlot(t *testing.T, s *store.Store, provider string) {
	t.Helper()
	if ok, err := s.ReserveCapacity(context.Background(), provider, 0); err != nil || !ok {
		t.Fatalf("take a slot on %s = %v, %v", provider, ok, err)
	}
}

func holdPlacement(t *testing.T, s *store.Store, provider, order string) store.Placement {
	t.Helper()
	p := newPlacement(t, s, order)
	if ok, err := s.ReservePlacementCapacity(context.Background(), p.ID, provider, 1); err != nil ||
		!ok {
		t.Fatalf("hold a placement slot = %v, %v", ok, err)
	}
	return p
}

// holdAttempt gives an attempt of p its own slot row
func holdAttempt(
	t *testing.T, s *store.Store, pool *pgxpool.Pool, p store.Placement, provider string, n int,
) {
	t.Helper()
	takeSlot(t, s, provider)
	_, err := pool.Exec(context.Background(), `
		insert into placement_attempts (placement_id, attempt, provider, state, slot_provider)
		values ($1, $2, $3, 'creating', $3)`, p.ID, n, provider)
	if err != nil {
		t.Fatal(err)
	}
}

func holdMember(
	t *testing.T, s *store.Store, pool *pgxpool.Pool, provider, state string,
) uuid.UUID {
	t.Helper()
	takeSlot(t, s, provider)
	id := uuid.New()
	_, err := pool.Exec(context.Background(), `
		insert into pool_members
			(id, provider, size_name, origin, state, idempotency_key, holds_slot)
		values ($1, $2, 'small', 'pool', $3, $4, true)`, id, provider, state, "pool-"+id.String())
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func holdProbe(t *testing.T, s *store.Store, pool *pgxpool.Pool, provider string) {
	t.Helper()
	takeSlot(t, s, provider)
	_, err := pool.Exec(context.Background(),
		`insert into probe_slots (id, provider) values ($1, $2)`, uuid.New(), provider)
	if err != nil {
		t.Fatal(err)
	}
}

func recount(t *testing.T, s *store.Store, provider string) int {
	t.Helper()
	_, now, err := s.RecountCapacity(context.Background(), provider, time.Hour, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return now
}

func TestRecountSumsFourHolders(t *testing.T) {
	s, pool := newStoreWithPool(t)
	p := holdPlacement(t, s, "local", "o1")
	holdAttempt(t, s, pool, p, "local", 2)
	holdMember(t, s, pool, "local", "warm")
	holdProbe(t, s, pool, "local")
	eq(t, recount(t, s, "local"), 4)
	eq(t, inUse(t, pool, "local"), 4)
}

func TestReleaseAttemptSlotNeverFreesAnotherAttempt(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	p := holdPlacement(t, s, "local", "o1")
	// The placement row holds attempt 2's slot and attempt 3 holds its own
	_, err := pool.Exec(ctx, `update placements set capacity_attempt = 2 where id = $1`, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	holdAttempt(t, s, pool, p, "local", 3)
	for _, n := range []int{1, 4} {
		ok, err := s.ReleaseAttemptSlot(ctx, p.ID, n)
		eq(t, ok, false)
		eq(t, err, nil)
	}
	eq(t, inUse(t, pool, "local"), 2)
	ok, err := s.ReleaseAttemptSlot(ctx, p.ID, 3)
	eq(t, ok, true)
	eq(t, err, nil)
	eq(t, inUse(t, pool, "local"), 1)
	eq(t, held(t, s, p.ID), "local")
	ok, err = s.ReleaseAttemptSlot(ctx, p.ID, 3)
	eq(t, ok, false)
	eq(t, err, nil)
	ok, err = s.ReleaseAttemptSlot(ctx, p.ID, 2)
	eq(t, ok, true)
	eq(t, err, nil)
	eq(t, inUse(t, pool, "local"), 0)
	eq(t, held(t, s, p.ID), "")
	eq(t, recount(t, s, "local"), 0)
}

func TestNullCapacityAttemptMeansAttemptOne(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	p := holdPlacement(t, s, "local", "o1")
	// A row as migration 00003 left it
	_, err := pool.Exec(ctx, `update placements set capacity_attempt = null where id = $1`, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	ok, err := s.ReleaseAttemptSlot(ctx, p.ID, 2)
	eq(t, ok, false)
	eq(t, err, nil)
	ok, err = s.ReleaseAttemptSlot(ctx, p.ID, 1)
	eq(t, ok, true)
	eq(t, err, nil)
	eq(t, inUse(t, pool, "local"), 0)
}

func TestReservePlacementCapacityNamesItsAttempt(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o1")
	if ok, err := s.ReservePlacementCapacity(ctx, p.ID, "local", 3); err != nil || !ok {
		t.Fatal(ok, err)
	}
	var n *int
	err := pool.QueryRow(ctx, `select capacity_attempt from placements where id = $1`,
		p.ID).Scan(&n)
	if err != nil || n == nil || *n != 3 {
		t.Fatalf("capacity_attempt = %v, %v, want 3", n, err)
	}
	if ok, err := s.ReleasePlacementCapacity(ctx, p.ID); err != nil || !ok {
		t.Fatal(ok, err)
	}
	err = pool.QueryRow(ctx, `select capacity_attempt from placements where id = $1`,
		p.ID).Scan(&n)
	if err != nil || n != nil {
		t.Fatalf("capacity_attempt after the release = %v, %v, want null", n, err)
	}
}

func TestReleaseEndedCapacityFreesMemberSlotsOnGoneRows(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	gone := holdMember(t, s, pool, "local", "gone")
	holdMember(t, s, pool, "local", "held")
	ended := holdPlacement(t, s, "local", "o1")
	holdAttempt(t, s, pool, ended, "local", 2)
	live := holdPlacement(t, s, "local", "o2")
	holdAttempt(t, s, pool, live, "local", 2)
	_, err := pool.Exec(ctx, `
		update placement_attempts set state = 'failed' where placement_id = $1`, live.ID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `update placements set state = $2 where id = $1`,
		ended.ID, store.PlacementFailed)
	if err != nil {
		t.Fatal(err)
	}
	// The gone member, the ended placement and its attempt, and the failed attempt of a live one
	n, err := s.ReleaseEndedCapacity(ctx)
	eq(t, n, 4)
	eq(t, err, nil)
	var holds bool
	err = pool.QueryRow(ctx, `select holds_slot from pool_members where id = $1`, gone).
		Scan(&holds)
	if err != nil || holds {
		t.Fatalf("gone member holds_slot = %v, %v", holds, err)
	}
	// The held member and the live placement's own slot stay
	eq(t, inUse(t, pool, "local"), 2)
	eq(t, recount(t, s, "local"), 2)
	n, err = s.ReleaseEndedCapacity(ctx)
	eq(t, n, 0)
	eq(t, err, nil)
}

func TestReleaseAttemptSlotsFreesEveryAttemptRow(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	p := holdPlacement(t, s, "local", "o1")
	holdAttempt(t, s, pool, p, "local", 2)
	holdAttempt(t, s, pool, p, "local", 3)
	n, err := s.ReleaseAttemptSlots(ctx, p.ID)
	eq(t, n, 2)
	eq(t, err, nil)
	eq(t, inUse(t, pool, "local"), 1)
	eq(t, held(t, s, p.ID), "local")
}

func TestMigration00024UpAndDown(t *testing.T) {
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
	reserveTables := func() (n int) {
		err := db.QueryRowContext(ctx, `
			select count(*) from information_schema.tables
			where table_schema = current_schema() and table_name in
				('pool_members', 'reservations', 'pool_pauses', 'pool_targets', 'pool_misses')`).
			Scan(&n)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	if _, err := gp.UpTo(ctx, 23); err != nil {
		t.Fatalf("up to 23: %v", err)
	}
	eq(t, reserveTables(), 0)
	for _, step := range []struct {
		name string
		run  func() error
		want int
	}{
		{"up", func() error { _, err := gp.UpTo(ctx, 24); return err }, 5},
		{"down", func() error { _, err := gp.DownTo(ctx, 23); return err }, 0},
		{"up again", func() error { _, err := gp.UpTo(ctx, 24); return err }, 5},
	} {
		if err := step.run(); err != nil {
			t.Fatalf("%s: %v", step.name, err)
		}
		eq(t, reserveTables(), step.want)
	}
}
