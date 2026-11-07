package store_test

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/oynozan/bugnet-router/internal/store"
	"github.com/oynozan/bugnet-router/internal/testenv"
	"github.com/oynozan/bugnet-router/internal/x402"
	"github.com/oynozan/bugnet-router/migrations"
)

// x402Version is the goose version of the migration that adds the x402 tables
const x402Version = 8

const (
	payToA = "11111111111111111111111111111111"
	payToB = "So11111111111111111111111111111111111111112"
)

// x402Tables and x402Columns are the tables and placements columns the x402 migration adds
var (
	x402Tables  = []string{"x402_services", "x402_refusals"}
	x402Columns = []string{"applied_hosts", "needs_secrets"}
)

// indexedService is an indexed GET service of the placement at a price of one cent
func indexedService(placement uuid.UUID, host, prefix string) store.X402Service {
	return store.X402Service{
		PlacementID: placement,
		ServiceID:   x402.ServiceID("GET", host, prefix),
		Spec: x402.Spec{
			Host: host, Prefix: prefix, Method: "GET", MaxPriceMicros: 10_000, PayTo: payToA,
			Indexed: true,
		},
	}
}

func x402Schema(t *testing.T, db *sql.DB) (tables, columns int) {
	t.Helper()
	ctx := context.Background()
	err := db.QueryRowContext(ctx, `
		select count(*) from information_schema.tables
		where table_schema = current_schema() and table_name = any($1)`, x402Tables).
		Scan(&tables)
	if err != nil {
		t.Fatal(err)
	}
	err = db.QueryRowContext(ctx, `
		select count(*) from information_schema.columns
		where table_schema = current_schema() and table_name = 'placements'
			and column_name = any($1)`, x402Columns).Scan(&columns)
	if err != nil {
		t.Fatal(err)
	}
	return tables, columns
}

func TestMigrationX402UpDownUp(t *testing.T) {
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
	if _, err := gp.UpTo(ctx, x402Version-1); err != nil {
		t.Fatalf("up to %d: %v", x402Version-1, err)
	}
	_, err = db.ExecContext(ctx, `
		insert into placements (id, handle_sha256, order_id, package_id, state)
		values ('00000000-0000-0000-0000-000000000001', 'h1', 'o1', 'p', 'ready')`)
	if err != nil {
		t.Fatal(err)
	}
	up := func() error { _, err := gp.UpTo(ctx, x402Version); return err }
	for _, step := range []struct {
		name            string
		run             func() error
		tables, columns int
	}{
		{"up", up, len(x402Tables), len(x402Columns)},
		{"down", func() error { _, err := gp.DownTo(ctx, x402Version-1); return err }, 0, 0},
		{"up again", up, len(x402Tables), len(x402Columns)},
	} {
		if err := step.run(); err != nil {
			t.Fatalf("%s: %v", step.name, err)
		}
		tables, columns := x402Schema(t, db)
		if tables != step.tables || columns != step.columns {
			t.Fatalf("%s: %d tables and %d columns, want %d and %d", step.name, tables, columns,
				step.tables, step.columns)
		}
	}
	var needs bool
	var applied int
	err = db.QueryRowContext(ctx, `
		select needs_secrets, cardinality(applied_hosts) from placements`).Scan(&needs, &applied)
	if err != nil || needs || applied != 0 {
		t.Fatalf("an older placement reads needs_secrets %v and %d applied hosts (%v), "+
			"want false and none", needs, applied, err)
	}
	_, err = db.ExecContext(ctx, `
		insert into x402_refusals (placement_id, reason, count, last_at)
		values ('00000000-0000-0000-0000-000000000009', 'not_enabled', 1, now())`)
	if err == nil {
		t.Fatal("a refusal row for no placement was stored")
	}
}

// checkViolation says whether err is Postgres refusing a row on a check constraint
func checkViolation(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "23514"
}

func TestX402ServiceChecks(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o-x402-checks")
	good := indexedService(p.ID, "api.example.com", "/v1/")
	if err := s.PutX402Service(ctx, good); err != nil {
		t.Fatalf("a good indexed row: %v", err)
	}
	unindexed := indexedService(p.ID, "api.example.com", "/v2/")
	unindexed.Spec.Indexed, unindexed.Spec.PaidPath = false, "/v2/search"
	if err := s.PutX402Service(ctx, unindexed); err != nil {
		t.Fatalf("a good unindexed row: %v", err)
	}
	for name, change := range map[string]func(*store.X402Service){
		"service id xs_ABC":        func(r *store.X402Service) { r.ServiceID = "xs_ABC" },
		"prefix /":                 func(r *store.X402Service) { r.Spec.Prefix = "/" },
		"method PUT":               func(r *store.X402Service) { r.Spec.Method = "PUT" },
		"price 0":                  func(r *store.X402Service) { r.Spec.MaxPriceMicros = 0 },
		"indexed with a paid path": func(r *store.X402Service) { r.Spec.PaidPath = "/v3/run" },
		"unindexed without one":    func(r *store.X402Service) { r.Spec.Indexed = false },
	} {
		t.Run(name, func(t *testing.T) {
			row := indexedService(p.ID, "api.example.com", "/v3/")
			change(&row)
			if err := s.PutX402Service(ctx, row); !checkViolation(err) {
				t.Fatalf("stored with %v, want a check violation", err)
			}
		})
	}
	t.Run("state gone", func(t *testing.T) {
		_, err := pool.Exec(ctx, `update x402_services set state = 'gone'
			where placement_id = $1 and service_id = $2`, p.ID, good.ServiceID)
		if !checkViolation(err) {
			t.Fatalf("stored with %v, want a check violation", err)
		}
	})
}

func TestPutX402ServiceUpdatesTogether(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o-x402-put")
	row := indexedService(p.ID, "api.example.com", "/v1/")
	row.State = store.X402Active
	if err := s.PutX402Service(ctx, row); err != nil {
		t.Fatal(err)
	}
	first := oneService(t, s, p.ID)
	if first.State != store.X402Applying || first.Spec != row.Spec ||
		first.ServiceID != row.ServiceID || first.PlacementID != p.ID {
		t.Fatalf("first put stored %+v, want the spec in applying", first)
	}
	past := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	_, err := pool.Exec(ctx, `
		update x402_services set state = 'failed', failure_reason = 'PORT_443_TAKEN',
			requests = 7, paid = 3, uncertain = 1, updated_at = $2
		where placement_id = $1`, p.ID, past)
	if err != nil {
		t.Fatal(err)
	}
	row.Spec.MaxPriceMicros, row.Spec.PayTo = 20_000, payToB
	row.Spec.Indexed, row.Spec.PaidPath = false, "/v1/search"
	if err := s.PutX402Service(ctx, row); err != nil {
		t.Fatal(err)
	}
	got := oneService(t, s, p.ID)
	if got.Spec != row.Spec {
		t.Fatalf("second put stored spec %+v, want %+v", got.Spec, row.Spec)
	}
	if got.State != store.X402Applying || got.FailureReason != "" {
		t.Fatalf("second put left state %q reason %q, want applying and no reason", got.State,
			got.FailureReason)
	}
	if got.Requests != 7 || got.Paid != 3 || got.Uncertain != 1 {
		t.Fatalf("counters %d, %d, %d, want 7, 3, 1 kept", got.Requests, got.Paid,
			got.Uncertain)
	}
	if !got.UpdatedAt.After(past) || got.UpdatedAt.Location() != time.UTC {
		t.Fatalf("updated_at %v, want after %v in UTC", got.UpdatedAt, past)
	}
}

func oneService(t *testing.T, s *store.Store, placement uuid.UUID) store.X402Service {
	t.Helper()
	rows, err := s.X402Services(context.Background(), placement)
	if err != nil || len(rows) != 1 {
		t.Fatalf("services %+v (%v), want one", rows, err)
	}
	return rows[0]
}

func TestDisableKeepsCounters(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o-x402-disable")
	a := indexedService(p.ID, "api.example.com", "/v1/")
	b := indexedService(p.ID, "api.example.com", "/v2/")
	for _, row := range []store.X402Service{a, b} {
		if err := s.PutX402Service(ctx, row); err != nil {
			t.Fatal(err)
		}
	}
	past := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	_, err := pool.Exec(ctx, `
		update x402_services set state = 'failed', failure_reason = 'PORT_443_TAKEN',
			requests = 9, paid = 4, uncertain = 2, updated_at = $3
		where placement_id = $1 and service_id = $2`, p.ID, a.ServiceID, past)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DisableX402Service(ctx, p.ID, a.ServiceID); err != nil {
		t.Fatal(err)
	}
	rows, err := s.X402Services(ctx, p.ID)
	if err != nil || len(rows) != 2 {
		t.Fatalf("services %+v (%v), want both, the disabled one included", rows, err)
	}
	if rows[0].ServiceID > rows[1].ServiceID {
		t.Fatalf("services in order %s, %s, want by service id", rows[0].ServiceID,
			rows[1].ServiceID)
	}
	for _, r := range rows {
		if r.ServiceID != a.ServiceID {
			if r.State != store.X402Applying {
				t.Fatalf("the other service is %q, want applying", r.State)
			}
			continue
		}
		if r.State != store.X402Disabled || r.FailureReason != "" {
			t.Fatalf("disabled service is %q reason %q, want disabled and no reason", r.State,
				r.FailureReason)
		}
		if r.Requests != 9 || r.Paid != 4 || r.Uncertain != 2 {
			t.Fatalf("counters %d, %d, %d, want 9, 4, 2 kept", r.Requests, r.Paid, r.Uncertain)
		}
		if !r.UpdatedAt.After(past) {
			t.Fatalf("updated_at %v, want after %v", r.UpdatedAt, past)
		}
	}
	other := newPlacement(t, s, "o-x402-disable-other")
	for name, c := range map[string]struct {
		placement uuid.UUID
		id        string
	}{
		"unknown id":      {p.ID, x402.ServiceID("POST", "api.example.com", "/v1/")},
		"other placement": {other.ID, b.ServiceID},
	} {
		if err := s.DisableX402Service(ctx, c.placement, c.id); !errors.Is(err,
			store.ErrNotFound) {
			t.Errorf("%s: %v, want ErrNotFound", name, err)
		}
	}
}

func TestPlacementDeleteCascadesX402Rows(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o-x402-cascade")
	if err := s.PutX402Service(ctx, indexedService(p.ID, "api.example.com", "/v1/")); err != nil {
		t.Fatal(err)
	}
	insertRefusal(t, pool, p.ID, "not_enabled", 2, time.Now())
	if _, err := pool.Exec(ctx, `delete from placements where id = $1`, p.ID); err != nil {
		t.Fatalf("delete placement: %v", err)
	}
	var services, refusals int
	err := pool.QueryRow(ctx, `select (select count(*) from x402_services),
		(select count(*) from x402_refusals)`).Scan(&services, &refusals)
	if err != nil || services != 0 || refusals != 0 {
		t.Fatalf("%d services and %d refusals left (%v), want none", services, refusals, err)
	}
}

func insertRefusal(
	t *testing.T, pool *pgxpool.Pool, placement uuid.UUID, reason string, n int, at time.Time,
) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		insert into x402_refusals (placement_id, reason, count, last_at) values ($1, $2, $3, $4)`,
		placement, reason, n, at)
	if err != nil {
		t.Fatal(err)
	}
}

func TestX402Refusals(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o-x402-refusals")
	other := newPlacement(t, s, "o-x402-refusals-other")
	if got, err := s.X402Refusals(ctx, p.ID); err != nil || len(got) != 0 {
		t.Fatalf("refusals %+v (%v), want none", got, err)
	}
	at := time.Date(2026, 11, 7, 9, 30, 0, 0, time.UTC)
	insertRefusal(t, pool, p.ID, "unpaid_cap", 3, at)
	insertRefusal(t, pool, p.ID, "not_enabled", 1, at.Add(time.Minute))
	insertRefusal(t, pool, other.ID, "refused_host", 5, at)
	got, err := s.X402Refusals(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := []store.X402Refusal{
		{Reason: "not_enabled", Count: 1, LastAt: at.Add(time.Minute)},
		{Reason: "unpaid_cap", Count: 3, LastAt: at},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("refusals %+v, want %+v", got, want)
	}
}

func TestSecretHosts(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o-secret-hosts")
	other := newPlacement(t, s, "o-secret-hosts-other")
	if got, err := s.SecretHosts(ctx, p.ID); err != nil || len(got) != 0 {
		t.Fatalf("hosts %v (%v), want none", got, err)
	}
	for i, c := range []struct {
		placement uuid.UUID
		hosts     []string
		fetchOnly bool
		state     string
	}{
		{p.ID, []string{"api.openai.com", "api.github.com"}, false, "active"},
		{p.ID, []string{"api.github.com", "files.example.com"}, true, "failed"},
		{other.ID, []string{"api.other.com"}, false, "active"},
	} {
		_, err := pool.Exec(ctx, `
			insert into secrets (id, placement_id, name, hosts, fetch_only, placeholder_sha256,
				sealed, key_id, source, state)
			values ($1, $2, $3, $4, $5, $6, 'sealed', 'k1', 'gateway', $7)`,
			uuid.New(), c.placement, "S"+string(rune('A'+i)), c.hosts, c.fetchOnly,
			[]byte{byte(i)}, c.state)
		if err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.SecretHosts(ctx, p.ID)
	want := []string{"api.github.com", "api.openai.com", "files.example.com"}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("hosts %v (%v), want %v", got, err, want)
	}
}
