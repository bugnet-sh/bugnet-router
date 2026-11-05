package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/valkey-io/valkey-go"

	"github.com/oynozan/bugnet-router/internal/store"
	"github.com/oynozan/bugnet-router/internal/testenv"
	"github.com/oynozan/bugnet-router/internal/vkinfo"
)

var healthy = fixed(vkinfo.Memory{Used: 79, Max: 100, Policy: "noeviction"})

// migrated returns a pool on a fresh migrated database
func migrated(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := testenv.DatabaseURL(t)
	if err := store.Migrate(t.Context(), url); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := pgxpool.New(t.Context(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// probe asks /monitor once and returns its status and body
func probe(st *store.Store, vk valkey.Client, read vkinfo.Reader) (int, string) {
	rec := httptest.NewRecorder()
	monitor(discard, st, vk, read)(rec, httptest.NewRequest(http.MethodGet, "/monitor", nil))
	return rec.Code, rec.Body.String()
}

func TestMonitorOKOnACleanDatabase(t *testing.T) {
	st := store.New(migrated(t))
	vk, _ := testenv.Valkey(t)
	unreadable := func(context.Context, valkey.Client) (vkinfo.Memory, error) {
		return vkinfo.Parse("used_memory:10\r\nmaxmemory:0\r\n")
	}
	for name, read := range map[string]vkinfo.Reader{
		"under the alert": healthy, "local valkey": vkinfo.Read, "info without a field": unreadable,
	} {
		if code, body := probe(st, vk, read); code != http.StatusOK || body != "ok\n" {
			t.Errorf("monitor with %s answered %d %q, want 200 ok", name, code, body)
		}
	}
}

func TestMonitorAnswersWithinItsTimeout(t *testing.T) {
	pool := migrated(t)
	seedPlacement(t, pool, store.PlacementReady, time.Now().Add(-time.Hour))
	vk, _ := testenv.Valkey(t)
	hung := func(ctx context.Context, _ valkey.Client) (vkinfo.Memory, error) {
		<-ctx.Done()
		return vkinfo.Memory{}, ctx.Err()
	}
	answered := make(chan string, 1)
	go func() {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/monitor", nil)
		monitor(discard, store.New(pool), vk, hung)(rec, req)
		answered <- strconv.Itoa(rec.Code) + " " + rec.Body.String()
	}()
	select {
	case got := <-answered:
		want := "503 1 placement is over 3 minutes past its deadline\nvalkey memory is unknown\n"
		if got != want {
			t.Fatalf("monitor with a hung valkey answered %q, want %q", got, want)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("monitor with a hung valkey gave no answer within 3 s")
	}
}

func seedPlacement(t *testing.T, pool *pgxpool.Pool, state string, deadline time.Time) {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(t.Context(), `insert into placements
		(id, handle_sha256, order_id, package_id, state, deadline)
		values ($1, $2, $3, 'pkg', $4, $5)`, id, id[:], id.String(), state, deadline)
	if err != nil {
		t.Fatal(err)
	}
}

// seedStopped inserts a stopped placement terminating from since, or from before the column if zero
func seedStopped(t *testing.T, pool *pgxpool.Pool, since, deadline time.Time) {
	t.Helper()
	id := uuid.New()
	var at *time.Time
	if !since.IsZero() {
		at = &since
	}
	_, err := pool.Exec(t.Context(), `insert into placements
		(id, handle_sha256, order_id, package_id, state, deadline, termination_reason,
			terminating_at)
		values ($1, $2, $3, 'pkg', $4, $5, 'stop', $6)`,
		id, id[:], id.String(), store.PlacementTerminating, deadline, at)
	if err != nil {
		t.Fatal(err)
	}
}

func outboxEvent(t *testing.T, pool *pgxpool.Pool, attempts int, delivered bool) {
	t.Helper()
	_, err := pool.Exec(t.Context(), `insert into outbox (id, event_type, payload, attempts,
		delivered_at) values ($1, 'placement.failed', '{}', $2, case when $3 then now() end)`,
		uuid.New(), attempts, delivered)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMonitorReportsEachProblem(t *testing.T) {
	vk, _ := testenv.Valkey(t)
	opt := valkey.ClientOption{InitAddress: []string{testenv.ValkeyAddr()}}
	closed, err := valkey.NewClient(opt)
	if err != nil {
		t.Fatal(err)
	}
	closed.Close()
	unreachable, err := pgxpool.New(t.Context(), "postgres://localhost:1/unused")
	if err != nil {
		t.Fatal(err)
	}
	defer unreachable.Close()
	now := time.Now()
	for _, c := range []struct {
		name     string
		postgres bool
		seed     func(*testing.T, *pgxpool.Pool)
		vk       valkey.Client
		read     vkinfo.Reader
		want     string
	}{
		{name: "postgres unreachable", vk: vk, read: healthy,
			want: "postgres is unreachable\n"},
		{name: "valkey unreachable", postgres: true, vk: closed,
			read: fixed(vkinfo.Memory{Policy: "allkeys-lru"}), want: "valkey is unreachable\n"},
		{name: "overdue placements", postgres: true, vk: vk, read: healthy,
			seed: func(t *testing.T, pool *pgxpool.Pool) {
				seedPlacement(t, pool, store.PlacementReady, now.Add(-4*time.Minute))
				seedPlacement(t, pool, store.PlacementTerminating, now.Add(-time.Hour))
				seedPlacement(t, pool, store.PlacementReady, now.Add(-2*time.Minute))
				seedPlacement(t, pool, store.PlacementTerminated, now.Add(-time.Hour))
				seedPlacement(t, pool, store.PlacementFailed, now.Add(-time.Hour))
			}, want: "2 placements are over 3 minutes past their deadline\n"},
		{name: "one overdue placement", postgres: true, vk: vk, read: healthy,
			seed: func(t *testing.T, pool *pgxpool.Pool) {
				seedPlacement(t, pool, store.PlacementTerminating, now.Add(-10*time.Minute))
			}, want: "1 placement is over 3 minutes past its deadline\n"},
		{name: "stops left terminating", postgres: true, vk: vk, read: healthy,
			seed: func(t *testing.T, pool *pgxpool.Pool) {
				seedStopped(t, pool, now.Add(-4*time.Minute), now.Add(50*time.Minute))
				seedStopped(t, pool, time.Time{}, now.Add(50*time.Minute))
				seedStopped(t, pool, now.Add(-time.Minute), now.Add(50*time.Minute))
				seedStopped(t, pool, now.Add(-10*time.Minute), now.Add(-5*time.Minute))
			}, want: "1 placement is over 3 minutes past its deadline\n" +
				"2 placements have been terminating for over 3 minutes\n"},
		{name: "one stop left terminating", postgres: true, vk: vk, read: healthy,
			seed: func(t *testing.T, pool *pgxpool.Pool) {
				seedStopped(t, pool, now.Add(-4*time.Minute), now.Add(50*time.Minute))
			}, want: "1 placement has been terminating for over 3 minutes\n"},
		{name: "stuck outbox events", postgres: true, vk: vk, read: healthy,
			seed: func(t *testing.T, pool *pgxpool.Pool) {
				outboxEvent(t, pool, store.MaxOutboxAttempts, false)
				outboxEvent(t, pool, store.MaxOutboxAttempts+3, false)
				outboxEvent(t, pool, store.MaxOutboxAttempts-1, false)
				outboxEvent(t, pool, store.MaxOutboxAttempts, true)
			}, want: "2 outbox events are stuck\n"},
		{name: "one stuck outbox event", postgres: true, vk: vk, read: healthy,
			seed: func(t *testing.T, pool *pgxpool.Pool) {
				outboxEvent(t, pool, store.MaxOutboxAttempts, false)
			}, want: "1 outbox event is stuck\n"},
		{name: "valkey memory unknown", postgres: true, vk: vk,
			read: func(context.Context, valkey.Client) (vkinfo.Memory, error) {
				return vkinfo.Memory{}, errors.New("valkey info memory: i/o timeout")
			}, want: "valkey memory is unknown\n"},
		{name: "valkey memory", postgres: true, vk: vk,
			read: fixed(vkinfo.Memory{Used: 80, Max: 100, Policy: "noeviction"}),
			want: "valkey memory is at 80% of maxmemory\n"},
		{name: "valkey policy", postgres: true, vk: vk,
			read: fixed(vkinfo.Memory{Used: 1 << 40, Policy: "allkeys-lru"}),
			want: "valkey maxmemory-policy is allkeys-lru, the router needs noeviction\n"},
		{name: "valkey policy beside a missing number", postgres: true, vk: vk,
			read: func(context.Context, valkey.Client) (vkinfo.Memory, error) {
				return vkinfo.Parse("maxmemory:100\r\nmaxmemory_policy:volatile-ttl\r\n")
			}, want: "valkey maxmemory-policy is volatile-ttl, the router needs noeviction\n"},
		{name: "several", vk: vk,
			read: fixed(vkinfo.Memory{Used: 2047, Max: 2048, Policy: "volatile-lru"}),
			want: "postgres is unreachable\nvalkey memory is at 99% of maxmemory\n" +
				"valkey maxmemory-policy is volatile-lru, the router needs noeviction\n"},
	} {
		t.Run(c.name, func(t *testing.T) {
			pool := unreachable
			if c.postgres {
				pool = migrated(t)
			}
			if c.seed != nil {
				c.seed(t, pool)
			}
			code, body := probe(store.New(pool), c.vk, c.read)
			if code != http.StatusServiceUnavailable || body != c.want {
				t.Fatalf("monitor answered %d %q, want 503 %q", code, body, c.want)
			}
		})
	}
}
