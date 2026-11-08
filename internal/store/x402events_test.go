package store_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/bugnet-sh/bugnet-router/internal/testenv"
	"github.com/bugnet-sh/bugnet-router/migrations"
)

func TestMigrationX402EventsIndexUpDownUp(t *testing.T) {
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
	v := migrationVersion(t, "x402_events")
	index := func() int {
		var n int
		err := db.QueryRowContext(ctx, `select count(*) from pg_indexes
			where schemaname = current_schema() and indexname = 'session_events_ref'`).Scan(&n)
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
		eq(t, index(), 1)
		if _, err := gp.DownTo(ctx, v-1); err != nil {
			t.Fatal(err)
		}
		eq(t, index(), 0)
	}
}
