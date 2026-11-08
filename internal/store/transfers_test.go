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

func TestTransferIndexRowOnce(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o-xfer")
	sha := []byte("0123456789abcdef0123456789abcdef")
	found, err := s.TransferIndexed(ctx, sha)
	if err != nil || found {
		t.Fatalf("an unknown token is indexed: %v, %v", found, err)
	}
	for range 2 {
		if err := s.IndexTransfer(ctx, sha, p.ID); err != nil {
			t.Fatal(err)
		}
	}
	var rows int
	if err := pool.QueryRow(ctx, `select count(*) from transfer_index`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if found, err = s.TransferIndexed(ctx, sha); err != nil || !found || rows != 1 {
		t.Fatalf("after two inserts: indexed %v, %v, %d rows, want one", found, err, rows)
	}
}

func TestMigration00007UpDownUp(t *testing.T) {
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
	exists := func() bool {
		var n int
		err := db.QueryRowContext(ctx, `select count(*) from information_schema.tables
			where table_schema = current_schema() and table_name = 'transfer_index'`).Scan(&n)
		if err != nil {
			t.Fatal(err)
		}
		return n == 1
	}
	for _, step := range []struct {
		name string
		run  func() error
		want bool
	}{
		{"up", func() error { _, err := gp.UpTo(ctx, 7); return err }, true},
		{"down", func() error { _, err := gp.DownTo(ctx, 6); return err }, false},
		{"up again", func() error { _, err := gp.UpTo(ctx, 7); return err }, true},
	} {
		if err := step.run(); err != nil {
			t.Fatalf("%s: %v", step.name, err)
		}
		if got := exists(); got != step.want {
			t.Fatalf("%s: transfer_index exists %v, want %v", step.name, got, step.want)
		}
	}
	_, err = db.ExecContext(ctx, `insert into transfer_index (token_sha256, placement_id)
		values ('x', '00000000-0000-0000-0000-000000000009')`)
	if err == nil {
		t.Fatal("an index row for no placement was stored")
	}
}
