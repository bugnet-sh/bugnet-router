package store_test

import (
	"context"
	"errors"
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

func TestQuoteKeepsItsHourly(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := t.Context()
	id := uuid.New()
	q := store.Quote{
		PackageID: "gpu-t4-10m", Provider: "lium", SizeName: "lium-t4", CostMicros: 79_334,
		PriceMicros: 87_268, HourlyMicros: 280_000,
	}
	if err := s.PutQuotes(ctx, id, []store.Quote{q}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.QuoteFor(ctx, id, q.PackageID, time.Hour)
	eq(t, []any{got.HourlyMicros, ok, err}, []any{int64(280_000), true, nil})
	// A second serve moves the figure with the price
	q.HourlyMicros, q.PriceMicros = 300_000, 93_500
	if err := s.PutQuotes(ctx, id, []store.Quote{q}); err != nil {
		t.Fatal(err)
	}
	got, ok, err = s.ClaimQuote(ctx, id, q.PackageID, "order-1", 93_500, time.Hour)
	eq(t, []any{got.HourlyMicros, ok, err}, []any{int64(300_000), true, nil})

	// A row written before the column existed reads 0, no cap recorded
	old := uuid.New()
	if _, err := pool.Exec(ctx, `insert into price_quotes (estimate_id, package_id, provider,
		size_name, cost_micros, price_micros) values ($1, 'gpu-t4-10m', 'lium', 'lium-t4',
		79334, 87268)`, old); err != nil {
		t.Fatal(err)
	}
	got, ok, err = s.QuoteFor(ctx, old, "gpu-t4-10m", time.Hour)
	eq(t, []any{got.HourlyMicros, got.PriceMicros, ok, err}, []any{int64(0), int64(87_268), true,
		nil})
}

func TestRentedHourly(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	p := newPlacement(t, s, "order-1")
	eq(t, p.RentedHourlyMicros, int64(0))
	if err := s.SetRentedHourly(ctx, p.ID, 250_000); err != nil {
		t.Fatal(err)
	}
	got, err := s.PlacementByID(ctx, p.ID)
	eq(t, []any{got.RentedHourlyMicros, err}, []any{int64(250_000), nil})
	err = s.SetRentedHourly(ctx, uuid.New(), 250_000)
	eq(t, errors.Is(err, store.ErrNotFound), true)
}

func TestMigrationNodeHourlyUpDownUp(t *testing.T) {
	ctx := context.Background()
	cfg, err := pgx.ParseConfig(testenv.DatabaseURL(t))
	if err != nil {
		t.Fatal(err)
	}
	db := stdlib.OpenDB(*cfg)
	t.Cleanup(func() { _ = db.Close() })
	gp, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS)
	if err != nil {
		t.Fatal(err)
	}
	v := migrationVersion(t, "node_hourly")
	eq(t, v, int64(30))
	columns := func() int {
		var n int
		err := db.QueryRowContext(ctx, `select count(*) from information_schema.columns
			where table_schema = current_schema() and (table_name, column_name) in
				(('price_quotes', 'hourly_micros'), ('placements', 'rented_hourly_micros'))`).
			Scan(&n)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	if _, err := gp.UpTo(ctx, v-1); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := gp.UpTo(ctx, v); err != nil {
			t.Fatal(err)
		}
		eq(t, columns(), 2)
		if _, err := gp.DownTo(ctx, v-1); err != nil {
			t.Fatal(err)
		}
		eq(t, columns(), 0)
	}
}
