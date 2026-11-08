package store_test

import (
	"context"
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

func TestMigrationEstimatorUpDownUp(t *testing.T) {
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
	version := migrationVersion(t, "estimator")
	count := func() (tables, columns int) {
		err := db.QueryRowContext(ctx, `
			select count(*) from information_schema.tables
			where table_schema = current_schema() and table_name in
				('source_facts', 'catalogs', 'estimates', 'usage_observations')`).Scan(&tables)
		if err != nil {
			t.Fatal(err)
		}
		err = db.QueryRowContext(ctx, `
			select count(*) from information_schema.columns
			where table_schema = current_schema() and (
				(table_name = 'placements' and column_name in ('estimate_id', 'source_kind',
					'source_canonical', 'source_ref', 'source_revision', 'source_read',
					'bootstrap_state', 'bootstrap_job_id', 'bootstrap_path', 'bootstrap_reason',
					'bootstrap_heartbeat', 'bootstrap_attempt', 'bootstrap_private_attempts'))
				or (table_name = 'jobs' and column_name in
					('oom_killed', 'peak_memory_mib', 'install')))`).Scan(&columns)
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	if _, err := gp.UpTo(ctx, version-1); err != nil {
		t.Fatal(err)
	}
	tables, columns := count()
	eq(t, []int{tables, columns}, []int{0, 0})
	if _, err := gp.UpTo(ctx, version); err != nil {
		t.Fatal(err)
	}
	tables, columns = count()
	eq(t, []int{tables, columns}, []int{4, 16})
	if _, err := gp.DownTo(ctx, version-1); err != nil {
		t.Fatal(err)
	}
	tables, columns = count()
	eq(t, []int{tables, columns}, []int{0, 0})
	if _, err := gp.UpTo(ctx, version); err != nil {
		t.Fatal(err)
	}
}

func TestFactsKeepTheFirstRowAndMoveUsedAtHourly(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	row := store.FactsRow{SourceKey: "github.com/a/b", Revision: "abc",
		ExtractorVersion: "facts-1", Facts: []byte(`{"v":1}`), FactsSHA256: []byte("sum")}
	if _, err := s.GetFacts(ctx, row.SourceKey, row.Revision, row.ExtractorVersion); err == nil {
		t.Fatal("facts before any put")
	}
	if err := s.PutFacts(ctx, row); err != nil {
		t.Fatal(err)
	}
	row.Facts = []byte(`{"v":2}`)
	if err := s.PutFacts(ctx, row); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetFacts(ctx, row.SourceKey, row.Revision, row.ExtractorVersion)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, string(got.Facts), `{"v":1}`)
}

func TestEstimatesReuseAndExpire(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	sha := []byte("catalog")
	if err := s.PutCatalog(ctx, sha, []byte("{}")); err != nil {
		t.Fatal(err)
	}
	if err := s.PutCatalog(ctx, sha, []byte("{}")); err != nil {
		t.Fatal(err)
	}
	e := store.EstimateRow{ID: uuid.New(), EstimatorVersion: "v", InputsSHA256: []byte("in"),
		Inputs: []byte("{}"), Output: []byte("{}"), Availability: []byte("[]"), CatalogSHA256: sha,
		SourceKind: "github", SourceKey: "github.com/a/b", Revision: "abc", Workload: "tests"}
	if err := s.InsertEstimate(ctx, e); err != nil {
		t.Fatal(err)
	}
	id, ok, err := s.ReuseEstimate(ctx, []byte("in"), []byte("[]"), time.Hour)
	eq(t, err, nil)
	eq(t, []any{id, ok}, []any{e.ID, true})
	_, ok, err = s.ReuseEstimate(ctx, []byte("in"), []byte(`["x"]`), time.Hour)
	eq(t, err, nil)
	eq(t, ok, false)
	live, err := s.LiveEstimate(ctx, e.ID, time.Hour)
	eq(t, err, nil)
	eq(t, live.SourceKey, "github.com/a/b")
	time.Sleep(20 * time.Millisecond)
	if _, err := s.LiveEstimate(ctx, e.ID, 10*time.Millisecond); err == nil {
		t.Fatal("an estimate served longer ago than max age was live")
	}
	if _, ok, _ := s.ReuseEstimate(ctx, []byte("in"), []byte("[]"), 10*time.Millisecond); ok {
		t.Fatal("an expired row was reused")
	}
	if _, err := s.EstimateByID(ctx, uuid.New()); err == nil {
		t.Fatal("an unknown estimate was found")
	}
}

func TestPlacementKeepsItsSource(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	sha := []byte("catalog")
	if err := s.PutCatalog(ctx, sha, []byte("{}")); err != nil {
		t.Fatal(err)
	}
	e := store.EstimateRow{ID: uuid.New(), EstimatorVersion: "v", InputsSHA256: []byte("in"),
		Inputs: []byte("{}"), Output: []byte("{}"), Availability: []byte("[]"), CatalogSHA256: sha}
	if err := s.InsertEstimate(ctx, e); err != nil {
		t.Fatal(err)
	}
	p, _, err := s.CreatePlacement(ctx, store.NewPlacement{
		HandleSHA256: []byte("h1"), OrderID: "o1", PackageID: "p",
		Source: &store.PlacementSource{EstimateID: e.ID, Kind: "github",
			Canonical: "github.com/a/b", Ref: "main", Revision: "abcdef0123", Read: true,
			Label: "github.com/a/b@abcdef0"},
	})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, *p.EstimateID, e.ID)
	eq(t, []any{p.SourceKind, p.SourceCanonical, p.SourceRef, p.SourceRevision, p.SourceRead,
		p.SourceLabel}, []any{"github", "github.com/a/b", "main", "abcdef0123", true,
		"github.com/a/b@abcdef0"})
	bare, _, err := s.CreatePlacement(ctx, store.NewPlacement{
		HandleSHA256: []byte("h2"), OrderID: "o2", PackageID: "p"})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, bare.EstimateID == nil, true)
	eq(t, []any{bare.SourceKind, bare.SourceRead, bare.SourceLabel}, []any{"", false, ""})
}
