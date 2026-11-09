package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
	"github.com/bugnet-sh/bugnet-router/internal/vkinfo"
	"github.com/bugnet-sh/bugnet-router/internal/wallet"
)

var healthy = fixed(vkinfo.Memory{Used: 79, Max: 100, Policy: "noeviction"})

// testPools is a mining pool list of one entry, so /monitor finds nothing wrong with it
const testPools = "testdata/mining-pools.txt"

// testPrefix holds no keys, so /monitor finds no payer guard mark under it
const testPrefix = "bn:monitor-test-empty:"

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
	monitor(discard, st, vk, testPrefix, read, testPools, feedCheck{}, nil, limitsCheck{}, nil)(rec,
		httptest.NewRequest(http.MethodGet, "/monitor", nil))
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
		monitor(discard, store.New(pool), vk, testPrefix, hung, testPools, feedCheck{}, nil,
			limitsCheck{}, nil)(rec, req)
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
		{name: "valkey refuses info", postgres: true, vk: vk,
			read: func(ctx context.Context, c valkey.Client) (vkinfo.Memory, error) {
				_, err := c.Do(ctx, c.B().Arbitrary("BUGNET_NO_SUCH_COMMAND").Build()).ToString()
				return vkinfo.Memory{}, fmt.Errorf("valkey info memory: %w", err)
			}, want: "valkey refuses INFO, so its memory and policy go unchecked\n"},
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

func TestMonitorShowsEmptyPoolList(t *testing.T) {
	st := store.New(migrated(t))
	vk, _ := testenv.Valkey(t)
	dir := t.TempDir()
	empty := filepath.Join(dir, "empty.txt")
	bad := filepath.Join(dir, "bad.txt")
	if err := os.WriteFile(empty, []byte("# none yet\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bad, []byte("pool.example.com\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]string{
		filepath.Join(dir, "missing.txt"): "the mining pool list is empty or missing\n",
		empty:                             "the mining pool list is empty or missing\n",
		bad:                               "the mining pool list cannot be read\n",
	} {
		rec := httptest.NewRecorder()
		monitor(discard, st, vk, testPrefix, healthy, path, feedCheck{}, nil, limitsCheck{}, nil)(rec,
			httptest.NewRequest(http.MethodGet, "/monitor", nil))
		if rec.Code != http.StatusServiceUnavailable || rec.Body.String() != want ||
			strings.Contains(rec.Body.String(), "pool.example.com") {
			t.Errorf("monitor with %s answered %d %q, want 503 %q", path, rec.Code,
				rec.Body.String(), want)
		}
	}
}

func TestMonitorShowsPayerTreatedAsAbsent(t *testing.T) {
	st := store.New(migrated(t))
	vk, prefix := testenv.Valkey(t)
	ask := func() (int, string) {
		rec := httptest.NewRecorder()
		monitor(discard, st, vk, prefix, healthy, testPools, feedCheck{}, nil, limitsCheck{}, nil)(rec,
			httptest.NewRequest(http.MethodGet, "/monitor", nil))
		return rec.Code, rec.Body.String()
	}
	if code, body := ask(); code != http.StatusOK {
		t.Fatalf("before the mark: %d %q", code, body)
	}
	err := vk.Do(t.Context(), vk.B().Set().Key(prefix+wallet.UnguardedKey).Value("x").
		Ex(time.Hour).Build()).Error()
	if err != nil {
		t.Fatal(err)
	}
	code, body := ask()
	want := "the payer claim is treated as absent, since no fee_payer came and " +
		"wallets.facilitator_addresses is empty"
	if code != http.StatusServiceUnavailable || strings.TrimSpace(body) != want {
		t.Fatalf("with the mark: %d %q, want 503 %q", code, body, want)
	}
}

func TestMonitorShowsStaleStats(t *testing.T) {
	st := store.New(migrated(t))
	vk, _ := testenv.Valkey(t)
	ask := func(feed feedCheck) (int, string) {
		rec := httptest.NewRecorder()
		monitor(discard, st, vk, testPrefix, healthy, testPools, feed, nil, limitsCheck{}, nil)(rec,
			httptest.NewRequest(http.MethodGet, "/monitor", nil))
		return rec.Code, rec.Body.String()
	}
	on := feedCheck{on: true}
	if code, body := ask(on); code != http.StatusOK {
		t.Fatalf("before the first snapshot: %d %q", code, body)
	}
	write := func(snap store.StatsSnapshot) {
		t.Helper()
		if err := st.WriteStatsSnapshot(t.Context(), snap); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now()
	write(store.StatsSnapshot{RouterAt: now, Body: []byte("{}"), Payments: []byte("{}"),
		PaymentsAt: now})
	if code, body := ask(on); code != http.StatusOK {
		t.Fatalf("a fresh snapshot: %d %q", code, body)
	}
	write(store.StatsSnapshot{RouterAt: now.Add(-3 * time.Minute), Body: []byte("{}"),
		Payments: []byte("{}"), PaymentsAt: now.Add(-7 * time.Minute)})
	code, body := ask(on)
	lines := strings.Split(body, "\n")
	if code != http.StatusServiceUnavailable || len(lines) != 3 ||
		!strings.HasPrefix(lines[0], "stats snapshot 18") ||
		!strings.HasSuffix(lines[0], " s old") || lines[1] != "stats: Payments part 7 min old" {
		t.Fatalf("a stale snapshot: %d %q", code, body)
	}
	if code, _ := ask(feedCheck{}); code != http.StatusOK {
		t.Fatalf("the feed off still reads the row: %d", code)
	}
	write(store.StatsSnapshot{RouterAt: now})
	_, body = ask(feedCheck{on: true, noToken: true})
	want := "stats: no Payments part yet, so /api/stats answers 503, " +
		"BUGNET_PAYMENTS_ROUTER_TOKEN is empty in router-worker\n"
	if body != want {
		t.Fatalf("no Payments part: %q, want %q", body, want)
	}
}

func TestMonitorShowsTheSessionDiskBudget(t *testing.T) {
	st := store.New(migrated(t))
	vk, _ := testenv.Valkey(t)
	for percent, want := range map[int64]string{
		79: "", 80: "session records at 80% of their disk budget",
		130: "session records at 130% of their disk budget",
	} {
		rec := httptest.NewRecorder()
		disk := func(context.Context) (int64, error) { return percent, nil }
		monitor(discard, st, vk, testPrefix, healthy, testPools, feedCheck{}, disk,
			limitsCheck{}, nil)(rec, httptest.NewRequest(http.MethodGet, "/monitor", nil))
		if want == "" {
			if rec.Code != http.StatusOK {
				t.Fatalf("at %d%% /monitor gave %d: %s", percent, rec.Code, rec.Body)
			}
			continue
		}
		if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("at %d%% /monitor gave %d: %s", percent, rec.Code, rec.Body)
		}
	}
	rec := httptest.NewRecorder()
	failing := func(context.Context) (int64, error) { return 0, errors.New("no size") }
	monitor(discard, st, vk, testPrefix, healthy, testPools, feedCheck{}, failing,
		limitsCheck{}, nil)(rec, httptest.NewRequest(http.MethodGet, "/monitor", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("an unread size gave %d: %s", rec.Code, rec.Body)
	}
}

func TestMonitorEgressSourcesLine(t *testing.T) {
	pool := migrated(t)
	st := store.New(pool)
	vk, _ := testenv.Valkey(t)
	id := uuid.New()
	_, err := pool.Exec(t.Context(), `insert into placements
		(id, handle_sha256, order_id, package_id, state, deadline, provider)
		values ($1, $2, $3, 'pkg', $4, now() + interval '1 hour', 'boat')`,
		id, id[:], id.String(), store.PlacementReady)
	if err != nil {
		t.Fatal(err)
	}
	ask := func(providers ...string) (int, string) {
		rec := httptest.NewRecorder()
		monitor(discard, st, vk, testPrefix, healthy, testPools, feedCheck{}, nil, limitsCheck{}, providers)(rec,
			httptest.NewRequest(http.MethodGet, "/monitor", nil))
		return rec.Code, rec.Body.String()
	}
	// Without secrets there is nothing a missing address would hurt
	if code, body := ask("boat"); code != http.StatusOK {
		t.Fatalf("no secrets: %d %s", code, body)
	}
	_, _, err = st.PutSecret(t.Context(), store.Secret{
		ID: uuid.New(), PlacementID: id, Name: "HF_TOKEN", Hosts: []string{"huggingface.co"},
		PlaceholderSHA256: []byte("ph"), Sealed: []byte("box"), KeyID: "k1",
		Source: store.SourceGateway, State: store.SecretActive,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := "egress sources: 1 ready boat placements with secrets and no usable address\n"
	if code, body := ask("boat"); code != http.StatusServiceUnavailable || body != want {
		t.Fatalf("secrets and no address: %d %q, want 503 %q", code, body, want)
	}
	// A provider with no source check is never counted
	if code, body := ask(); code != http.StatusOK {
		t.Fatalf("check off: %d %s", code, body)
	}
	src := netip.MustParseAddr("2001:db8::5")
	_, err = st.SetEgressSource(t.Context(), id, provider.EgressSource{
		Prefix: netip.PrefixFrom(src, 128), Bind: src,
	})
	if err != nil {
		t.Fatal(err)
	}
	if code, body := ask("boat"); code != http.StatusOK {
		t.Fatalf("address stored: %d %s", code, body)
	}
}
