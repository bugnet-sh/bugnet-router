package worker_test

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/stats"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/worker"
)

// fakeTotals answers like Payments' totals call, or fails with err
type fakeTotals struct {
	mu    sync.Mutex
	part  stats.PaymentsPart
	err   error
	calls int
}

func (f *fakeTotals) Totals(context.Context) (stats.PaymentsPart, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.part, f.err
}

func (f *fakeTotals) fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

// fixedClock is a settable time
type fixedClock struct {
	mu sync.Mutex
	at time.Time
}

func (c *fixedClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *fixedClock) set(at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = at
}

func paidPart(earned int64) stats.PaymentsPart {
	return stats.PaymentsPart{
		Version: 1, Totals: stats.Money{EarnedMicros: earned},
		Limits: map[string]int64{"order.max_micros": 100000000},
	}
}

func statsEnv(t *testing.T, o options) (*env, *fakeTotals, *fixedClock) {
	t.Helper()
	totals := &fakeTotals{part: paidPart(61700000)}
	clk := &fixedClock{at: time.Now().UTC().Truncate(time.Second)}
	o.stats = &worker.Stats{Payments: totals,
		Limits: map[string]int64{"sandbox.max_running_jobs": 16}}
	o.now = clk.now
	e := newEnv(t, o)
	o.stats.Catalog = e.catalog
	return e, totals, clk
}

func (e *env) snapshot(t *testing.T) store.StatsSnapshot {
	t.Helper()
	if err := e.worker.Snapshot(t.Context()); err != nil {
		t.Fatal(err)
	}
	snap, err := e.store.ReadStatsSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return snap
}

// served decodes the parts of a stored body these tests compare
type served struct {
	GeneratedAt string                       `json:"generatedAt"`
	Totals      struct{ EarnedMicros int64 } `json:"totals"`
	Packages    []stats.Package              `json:"packages"`
	Limits      map[string]int64             `json:"limits"`
}

func decodeBody(t *testing.T, body []byte) served {
	t.Helper()
	var s served
	if err := json.Unmarshal(body, &s); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSnapshotKeepsLastPaymentsPart(t *testing.T) {
	e, totals, clk := statsEnv(t, options{})
	t0 := clk.now()
	first := e.snapshot(t)
	eq(t, first.RouterAt, t0)
	eq(t, first.PaymentsAt, t0)
	body := decodeBody(t, first.Body)
	eq(t, body.Totals.EarnedMicros, int64(61700000))
	eq(t, body.GeneratedAt, t0.Format(time.RFC3339))
	eq(t, body.Limits, map[string]int64{"sandbox.max_running_jobs": 16,
		"order.max_micros": 100000000})
	eq(t, []string{body.Packages[0].ID}, []string{smallPkg})

	totals.fail(errors.New("payments answered a status other than 200"))
	clk.set(t0.Add(30 * time.Second))
	snap := e.snapshot(t)
	eq(t, snap.PaymentsAt, t0)
	eq(t, snap.RouterAt, t0.Add(30*time.Second))
	again := decodeBody(t, snap.Body)
	eq(t, again.Totals.EarnedMicros, body.Totals.EarnedMicros)
	eq(t, again.Limits, body.Limits)
	// The page shows how old the money is
	eq(t, again.GeneratedAt, t0.Format(time.RFC3339))

	totals.fail(nil)
	clk.set(t0.Add(time.Minute))
	eq(t, e.snapshot(t).PaymentsAt, t0.Add(time.Minute))
}

func TestNoPaymentsPartStoresNoBody(t *testing.T) {
	e, totals, clk := statsEnv(t, options{})
	totals.fail(errors.New("connection refused"))
	snap := e.snapshot(t)
	eq(t, snap.Body, []byte(nil))
	eq(t, snap.Payments, []byte(nil))
	eq(t, snap.RouterAt, clk.now())
	h := stats.NewHandler(e.store, e.catalog, &config.Config{Stats: config.Stats{
		RowCache: time.Second, SessionsCache: 4, MaxQueries: 1, PerSourcePerMinute: 60,
		PerSourceBurst: 20, MaxSources: 4,
	}}, nil, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/stats", nil))
	eq(t, w.Code, http.StatusServiceUnavailable)
}

func TestSnapshotLogsTokenProblemsAtErrorLevel(t *testing.T) {
	var logs syncBuffer
	e, totals, clk := statsEnv(t, options{log: &logs})
	errorLines := func() int { return strings.Count(logs.String(), "level=ERROR") }
	totals.fail(stats.ErrNoToken)
	e.snapshot(t)
	clk.set(clk.now().Add(30 * time.Second))
	e.snapshot(t)
	eq(t, errorLines(), 1)
	clk.set(clk.now().Add(30 * time.Second))
	e.snapshot(t)
	eq(t, errorLines(), 2)

	totals.fail(fmt.Errorf("%w, status 401", stats.ErrToken))
	e.snapshot(t)
	e.snapshot(t)
	eq(t, errorLines(), 3)
	totals.fail(nil)
	e.snapshot(t)
	totals.fail(fmt.Errorf("%w, status 404", stats.ErrToken))
	e.snapshot(t)
	eq(t, errorLines(), 4)
	totals.fail(errors.New("payments answered a version other than 1"))
	e.snapshot(t)
	eq(t, errorLines(), 4)
	eq(t, strings.Count(logs.String(), "keeping the last good part"), 1)
}

func TestSnapshotDropsPaymentsDaysBeforeTheFloor(t *testing.T) {
	var logs syncBuffer
	e, totals, clk := statsEnv(t, options{log: &logs})
	today := clk.now().UTC().Truncate(24 * time.Hour)
	back := func(n int) string { return today.AddDate(0, 0, -n).Format("2006-01-02") }
	totals.mu.Lock()
	totals.part.Daily = []stats.PaymentsDay{
		{Day: "0001-01-01"}, {Day: "1970-01-01"}, {Day: back(91)}, {Day: back(90)},
		{Day: back(12)}, {Day: back(11)}, {Day: back(0)},
	}
	totals.mu.Unlock()
	daily := func() []string {
		var body struct {
			Daily []struct{ Day string } `json:"daily"`
		}
		if err := json.Unmarshal(e.snapshot(t).Body, &body); err != nil {
			t.Fatal(err)
		}
		var days []string
		for _, d := range body.Daily {
			days = append(days, d.Day)
		}
		return days
	}
	dropped := func() int { return strings.Count(logs.String(), "before the floor") }

	// Without an order the floor is 90 days back
	days := daily()
	eq(t, []any{len(days), days[0], dropped()}, []any{91, back(90), 1})
	eq(t, strings.Contains(logs.String(), "level=WARN"), true)
	eq(t, strings.Contains(logs.String(), "count=3"), true)

	// The first order moves it to the day before that order's
	_, err := e.pool.Exec(t.Context(), `
		insert into placements (id, handle_sha256, order_id, package_id, state, created_at,
			failure_reason, terminated_at)
		values ($1, $2, 'first-order', $3, 'failed', $4, 'no_candidate', $4)`,
		uuid.New(), []byte("first-order"), smallPkg, today.AddDate(0, 0, -10).Add(3*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	days = daily()
	eq(t, []any{len(days), days[0], dropped()}, []any{12, back(11), 2})
	eq(t, strings.Contains(logs.String(), "count=5"), true)
	for _, value := range []string{"0001-01-01", "1970-01-01", back(91)} {
		if strings.Contains(logs.String(), value) {
			t.Errorf("the log names the dropped day %s", value)
		}
	}
}

func TestSnapshotListsTheServersStartsLandOn(t *testing.T) {
	// dear costs 33,334 of small-10m's 50,000 for 10 minutes, under the saga's 0.5 floor
	dear := local.New(local.Options{
		Name: "dear", Root: t.TempDir(), Sizes: []provider.Size{small},
		HourlyCostMicros: 200000, Increment: time.Second,
	})
	loc := newLocal(t, "local")
	e, _, _ := statsEnv(t, options{providers: []provider.Provider{dear, loc}})
	run := int64((24*60 - 5) / 10 * 10)
	want := []stats.Server{{
		Provider: "local", Size: "small", VCPU: 2, MemoryMiB: 2048, DiskGiB: 10,
		LongestRunMinutes: &run, User: cmp.Or(loc.Capabilities().ExecUser, "root"),
		Available: true,
		Packages:  []stats.ServerPackage{{ID: smallPkg, Minutes: 10, PriceMicros: smallPrice}},
	}}
	eq(t, e.servers(t), want)

	// Three quick failures take local out of placements, which leaves it listed but unavailable
	for range 3 {
		e.tracker.RecordFailure("local")
	}
	want[0].Available = false
	eq(t, e.servers(t), want)
}

// servers is a fresh snapshot body's servers
func (e *env) servers(t *testing.T) []stats.Server {
	t.Helper()
	var body struct {
		Servers []stats.Server `json:"servers"`
	}
	if err := json.Unmarshal(e.snapshot(t).Body, &body); err != nil {
		t.Fatal(err)
	}
	return body.Servers
}

func TestSnapshotLeavesOutAProviderImagesAcceptRefuses(t *testing.T) {
	// old runs an image the saga's images.accept leaves out
	old, loc := newLocal(t, "old"), newLocal(t, "local")
	e, _, _ := statsEnv(t, options{
		providers: []provider.Provider{old, loc},
		features: provider.Features{Images: map[string]string{
			"old": "2026.10.1", "local": "2026.11.1",
		}},
		imageAccept: []string{"2026.11.1"},
	})
	listed := []string{}
	for _, s := range e.servers(t) {
		listed = append(listed, s.Provider)
	}
	eq(t, listed, []string{"local"})
}

func TestSnapshotJobLimits(t *testing.T) {
	eq(t, worker.StatsEvery, 30*time.Second)
	eq(t, worker.StatsTimeout, 25*time.Second)
	eq(t, worker.StatsJobTimeout(), 25*time.Second)
	opts := jobargs.StatsSnapshot{}.InsertOpts()
	eq(t, opts.MaxAttempts, 1)
	eq(t, opts.Queue, river.QueueDefault)
	eq(t, jobargs.StatsSnapshot{}.Kind(), "stats_snapshot")

	// The periodic job runs on start, which writes the row before 30 s pass
	e, _, _ := statsEnv(t, options{periodic: true})
	e.start(t)
	eventually(t, "the first snapshot", func() bool {
		_, err := e.store.ReadStatsSnapshot(t.Context())
		return err == nil
	})
}

func TestNoStatsJobWithoutTheFeed(t *testing.T) {
	e := newEnv(t, options{periodic: true})
	e.start(t)
	e.finished(t, "watch")
	_, err := e.store.ReadStatsSnapshot(t.Context())
	eq(t, errors.Is(err, store.ErrNotFound), true)
	// A stray job does nothing without the feed's settings
	e.run(t, jobargs.StatsSnapshot{})
	_, err = e.store.ReadStatsSnapshot(t.Context())
	eq(t, errors.Is(err, store.ErrNotFound), true)
}

func TestFeedLeaksNothing(t *testing.T) {
	e, _, clk := statsEnv(t, options{})
	ctx := t.Context()
	const (
		orderID     = "ord-7f3a9c41d2"
		sandboxID   = "sbx-q8w7e6r5t4"
		sourceLabel = "github.com/acme/secret-model"
		errText     = "upstream refused: quota for org acme-corp"
		host        = "db.internal.acme-corp.example"
		handleHash  = "handle-hash-5e4d3c2b1a"
	)
	ready := clk.now().Add(-time.Hour)
	ended := ready.Add(10 * time.Minute)
	placementID := uuid.New()
	_, err := e.pool.Exec(ctx, `
		insert into placements (id, handle_sha256, order_id, package_id, state, provider,
			provider_ref, created_at, ready_at, terminating_at, terminated_at,
			termination_reason, source_label, ssh_alias)
		values ($1, $2, $3, $4, 'terminated', 'local', $5, $6, $7, $8, $8, $9, $10, $11)`,
		placementID, []byte(handleHash), orderID, smallPkg,
		fmt.Sprintf(`{"provider": "local", "id": %q, "host": %q}`, sandboxID, host),
		ready.Add(-time.Second), ready, ended, errText, sourceLabel, host)
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.pool.Exec(ctx, `
		insert into orders (order_id, kind, placement_id, amount_micros)
		values ($1, 'start', $2, 50000)`, orderID, placementID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.pool.Exec(ctx, `
		insert into placement_attempts (placement_id, attempt, provider, idempotency_key, state,
			error_code, error)
		values ($1, 1, 'local', $2, 'failed', 'quota', $3)`, placementID, orderID, errText)
	if err != nil {
		t.Fatal(err)
	}
	e.snapshot(t)
	h := stats.NewHandler(e.store, e.catalog, &config.Config{Stats: config.Stats{
		RowCache: time.Second, SessionsCache: 4, MaxQueries: 1, PerSourcePerMinute: 60,
		PerSourceBurst: 20, MaxSources: 4,
	}}, clk.now, nil)
	srv := httptest.NewServer(h)
	defer srv.Close()
	var bodies []string
	for _, path := range []string{"/api/stats", "/api/stats/sessions"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		eq(t, resp.StatusCode, http.StatusOK)
		bodies = append(bodies, string(raw))
	}
	both := strings.Join(bodies, "\n")
	if !strings.Contains(bodies[1], `"endReason":"other"`) {
		t.Fatalf("the raw reason did not go out as other: %s", bodies[1])
	}
	for _, s := range []string{orderID, sandboxID, sourceLabel, errText, host, handleHash,
		"acme", placementID.String()} {
		if strings.Contains(both, s) {
			t.Errorf("the feed carries %q", s)
		}
	}
}
