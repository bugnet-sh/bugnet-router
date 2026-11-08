package store_test

import (
	"context"
	"net/netip"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
	"github.com/bugnet-sh/bugnet-router/migrations"
)

func TestMigrationEgressSourcesUpAndDown(t *testing.T) {
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
	v := migrationVersion(t, "egress_sources")
	columns := func() int {
		var n int
		err := db.QueryRowContext(ctx, `select count(*) from information_schema.columns
			where table_schema = current_schema() and table_name = 'placements'
				and column_name in ('egress_source', 'egress_source_bind',
					'egress_source_read_at')`).Scan(&n)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	if _, err := gp.UpTo(ctx, v); err != nil {
		t.Fatal(err)
	}
	eq(t, columns(), 3)
	if _, err := gp.DownTo(ctx, v-1); err != nil {
		t.Fatal(err)
	}
	eq(t, columns(), 0)
	if _, err := gp.UpTo(ctx, v); err != nil {
		t.Fatal(err)
	}
	eq(t, columns(), 3)
}

func TestSetEgressSourceMovesTheVersionOnlyOnAChange(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	id := readyPlacement(t, s, "o-src", provider.Ref{Provider: "boat", ID: "sbx1"})
	src := func(a string, bits int) provider.EgressSource {
		ip := netip.MustParseAddr(a)
		return provider.EgressSource{Prefix: netip.PrefixFrom(ip, bits), Bind: ip}
	}
	before, err := s.PlacementByID(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, before.EgressSource.Prefix.IsValid(), false)

	for _, step := range []struct {
		src     provider.EgressSource
		changed bool
	}{
		{src("2001:db8::5", 128), true}, {src("2001:db8::5", 128), false},
		{src("2001:db8::6", 128), true}, {src("2001:db8::6", 64), true},
	} {
		changed, err := s.SetEgressSource(ctx, id, step.src)
		if err != nil {
			t.Fatal(err)
		}
		eq(t, changed, step.changed)
		got, err := s.PlacementByID(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		eq(t, got.EgressSource.Prefix, step.src.Prefix.Masked())
		eq(t, got.EgressSource.Bind, step.src.Bind)
		eq(t, got.EgressSource.ReadAt.IsZero(), false)
	}
	after, _ := s.PlacementByID(ctx, id)
	eq(t, after.SecretsVersion, before.SecretsVersion+3)

	_, err = s.SetEgressSource(ctx, [16]byte{1}, src("2001:db8::5", 128))
	eq(t, err != nil, true)
}
