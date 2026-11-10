package worker_test

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// spender is a local provider that lists and bills what the test sets
type spender struct {
	*local.Provider
	mu      sync.Mutex
	pages   map[string]provider.Usage
	reads   []string
	foreign map[string]provider.ForeignPage
	listed  []string
}

func (s *spender) ReadUsage(
	_ context.Context, _, _ time.Time, refs []provider.Ref, cursor string,
) (provider.Usage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads = append(s.reads, cursor)
	u := s.pages[cursor]
	if cursor == "" {
		for _, r := range refs {
			u.Sandboxes = append(u.Sandboxes, provider.SandboxUsage{ID: r.ID, CostMicros: 7})
		}
	}
	return u, nil
}

func (s *spender) ListLabelled(
	_ context.Context, _ map[string]string, cursor string, _ int,
) (provider.ForeignPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listed = append(s.listed, cursor)
	return s.foreign[cursor], nil
}

func (s *spender) cursors() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.reads)
}

func newSpender(t *testing.T, name string) *spender {
	t.Helper()
	return &spender{Provider: newLocal(t, name)}
}

func today() time.Time { return time.Now().UTC().Truncate(24 * time.Hour) }

func TestRecountFixesDrift(t *testing.T) {
	e := newEnv(t, options{})
	e.start(t)
	e.placeReady(t, "order-1")
	_, err := e.store.ReserveProbeSlot(t.Context(), "local", uuid.New(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	// A probe whose worker died and a counter that drifted
	_, err = e.pool.Exec(t.Context(), `insert into probe_slots (id, provider, reserved_at)
		values ($1, 'local', now() - interval '11 minutes')`, uuid.New())
	if err == nil {
		_, err = e.pool.Exec(t.Context(),
			`update provider_capacity set in_use = 9 where provider = 'local'`)
	}
	if err != nil {
		t.Fatal(err)
	}
	e.run(t, jobargs.CapacityRecount{})
	// The placement and the live probe slot are counted, the old slot is deleted
	eq(t, e.inUse(t, "local"), 2)
	n, err := e.store.ProbeSlotCount(t.Context(), "local")
	if err != nil {
		t.Fatal(err)
	}
	eq(t, n, 1)
}

func TestReserveRacingRecountStaysExact(t *testing.T) {
	e := newEnv(t, options{})
	ctx := t.Context()
	if err := e.store.SetCapacity(ctx, "local", 10); err != nil {
		t.Fatal(err)
	}
	p := e.placing(t)
	// A reserve holds the capacity row, then claims the placement and adds its slot
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		`select in_use from provider_capacity where provider = 'local' for update`); err != nil {
		t.Fatal(err)
	}
	type result struct {
		after int
		err   error
	}
	done := make(chan result, 1)
	go func() {
		_, after, err := e.store.RecountCapacity(ctx, "local", 10*time.Minute, time.Now())
		done <- result{after, err}
	}()
	select {
	case r := <-done:
		t.Fatalf("the recount did not wait for the reserve: %+v", r)
	case <-time.After(300 * time.Millisecond):
	}
	_, err = tx.Exec(ctx, `update placements set capacity_provider = 'local' where id = $1`, p.ID)
	if err == nil {
		_, err = tx.Exec(ctx,
			`update provider_capacity set in_use = in_use + 1 where provider = 'local'`)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	r := <-done
	if r.err != nil {
		t.Fatal(r.err)
	}
	// A recount that kept its old snapshot of placements would write 0
	eq(t, r.after, 1)
	eq(t, e.inUse(t, "local"), 1)
}

func TestForeignCountSkipsKnownDeployments(t *testing.T) {
	old := time.Now().Add(-3 * time.Hour)
	summary := func(id, deployment string, made time.Time) provider.Summary {
		labels := map[string]string{"bugnet": "1"}
		if deployment != "" {
			labels["bugnet_deployment"] = deployment
		}
		return provider.Summary{
			Ref: provider.Ref{Provider: "m", ID: id}, Labels: labels, CreatedAt: made,
		}
	}
	m := newSpender(t, "m")
	m.foreign = map[string]provider.ForeignPage{"": {Sandboxes: []provider.Summary{
		summary("known-main", "main", old),
		summary("known-dev", "dev", old),
		summary("own", "stage", old),
		summary("stranger", "other", old),
		summary("unlabelled", "", old),
		summary("young", "other", time.Now().Add(-time.Hour)),
		summary("unaged", "other", time.Time{}),
	}}}
	e := newEnv(t, options{providers: []provider.Provider{m}, deployment: "stage"})
	e.start(t)
	e.run(t, jobargs.ForeignCount{Provider: "m"})
	got, err := e.store.ForeignSandboxes(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	eq(t, got["m"], 2)
}

func TestForeignCountKeepsItsPlaceBetweenRuns(t *testing.T) {
	old := time.Now().Add(-3 * time.Hour)
	stranger := func(id string) provider.Summary {
		return provider.Summary{
			Ref: provider.Ref{Provider: "m", ID: id}, CreatedAt: old,
			Labels: map[string]string{"bugnet": "1", "bugnet_deployment": "other"},
		}
	}
	m := newSpender(t, "m")
	m.foreign = map[string]provider.ForeignPage{
		"":   {Sandboxes: []provider.Summary{stranger("a")}, Next: "c2"},
		"c2": {Sandboxes: []provider.Summary{stranger("b")}},
	}
	e := newEnv(t, options{providers: []provider.Provider{m}})
	e.start(t)
	e.run(t, jobargs.ForeignCount{Provider: "m"})
	got, _ := e.store.ForeignSandboxes(t.Context())
	eq(t, got["m"], 0)
	e.run(t, jobargs.ForeignCount{Provider: "m"})
	got, _ = e.store.ForeignSandboxes(t.Context())
	eq(t, got["m"], 2)
	eq(t, m.listed, []string{"", "c2"})
}

// costDay waits for the day's row to be read to its last page
func (e *env) costDay(t *testing.T, name string, day time.Time) store.CostDay {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		row, found, err := e.store.CostDayFor(t.Context(), name, day, false)
		if err != nil {
			t.Fatal(err)
		}
		if found && row.Cursor == "" {
			return row
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("the cost day of %s was not read to its last page", name)
	return store.CostDay{}
}

func TestCostDailyResumesFromCursor(t *testing.T) {
	page := func(next string, micros ...int64) provider.Usage {
		u := provider.Usage{TotalMicros: -1, Complete: true, Attributed: true, Next: next}
		for _, c := range micros {
			u.Sandboxes = append(u.Sandboxes, provider.SandboxUsage{
				ID: uuid.NewString(), CostMicros: c, Deployment: "dev",
			})
		}
		return u
	}
	pages := func() map[string]provider.Usage {
		return map[string]provider.Usage{
			"": page("p2", 10, 20), "p2": page("p3", 30), "p3": page("", 40, 50),
		}
	}
	t.Run("three pages, one job each", func(t *testing.T) {
		m := newSpender(t, "m")
		m.pages = pages()
		e := newEnv(t, options{providers: []provider.Provider{m}})
		e.start(t)
		e.run(t, jobargs.CostDaily{Provider: "m", Day: today().Format("2006-01-02")})
		row := e.costDay(t, "m", today())
		eq(t, m.cursors(), []string{"", "p2", "p3"})
		eq(t, row.TheirsMicros, int64(150))
		eq(t, row.TheirsComplete, true)
	})
	t.Run("a stored cursor is where the next job reads", func(t *testing.T) {
		m := newSpender(t, "m")
		m.pages = pages()
		e := newEnv(t, options{providers: []provider.Provider{m}})
		e.start(t)
		err := e.store.SaveCostDay(t.Context(), store.CostDay{
			Provider: "m", Day: today(), TheirsMicros: 100, TheirsComplete: true, Cursor: "p2",
			CheckedAt: time.Now(), FirstCheckedAt: time.Now(),
		})
		if err != nil {
			t.Fatal(err)
		}
		e.run(t, jobargs.CostDaily{Provider: "m", Day: today().Format("2006-01-02")})
		row := e.costDay(t, "m", today())
		eq(t, m.cursors(), []string{"p2", "p3"})
		eq(t, row.TheirsMicros, int64(220))
	})
}

func TestAttemptSandboxIDIsNotUnknown(t *testing.T) {
	m := newSpender(t, "m")
	e := newEnv(t, options{providers: []provider.Provider{m}})
	e.start(t)
	p, _ := e.placeReady(t, "order-1")
	spans, err := e.store.PlacementSpans(t.Context(), p.ID)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(spans), 1)
	eq(t, spans[0].SandboxID, p.ProviderRef.ID)
	named := func(id, deployment string) provider.SandboxUsage {
		return provider.SandboxUsage{ID: id, CostMicros: 1, Deployment: deployment}
	}
	m.pages = map[string]provider.Usage{"": {
		TotalMicros: -1, Complete: true, Attributed: true,
		Sandboxes: []provider.SandboxUsage{
			named("another-deployment", "dev"), named("leak", "main"), named("nobody", ""),
		},
	}}
	e.run(t, jobargs.CostDaily{Provider: "m", Day: today().Format("2006-01-02")})
	row := e.costDay(t, "m", today())
	// The attempt's own id, the sandbox of another known deployment and the repeat are not unknown
	eq(t, row.UnknownIDs, 2)
	eq(t, slices.Sorted(slices.Values(row.SeenUnknown)), []string{"leak", "nobody"})
	if row.OursMicros <= 0 {
		t.Fatalf("our meter read %d for a placement that ran", row.OursMicros)
	}
}

func TestTerminationWritesCostAndBytes(t *testing.T) {
	var logs syncBuffer
	e := newEnv(t, options{log: &logs})
	e.start(t)
	p, h := e.placeReady(t, "order-1")
	ctx := t.Context()
	for _, c := range []struct {
		kind string
		n    int
	}{{"edge", 1234}, {"egress", 56}} {
		if err := e.vk.Do(ctx, e.vk.B().Set().Key(e.prefix+c.kind+":used:"+p.ID.String()).
			Value(strconv.Itoa(c.n)).Build()).Error(); err != nil {
			t.Fatal(err)
		}
	}
	// An order that paid next to nothing, so the placement's cost is over the margin floor
	if _, err := e.pool.Exec(ctx, `update orders set amount_micros = 1 where placement_id = $1`,
		p.ID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	if _, err := e.saga.Stop(ctx, h); err != nil {
		t.Fatal(err)
	}
	got := e.reload(t, p.ID)
	if got.CostMicros <= 0 || got.EdgeBytes != 1234 || got.EgressBytes != 56 {
		t.Fatalf("placement cost %d, edge %d, egress %d", got.CostMicros, got.EdgeBytes,
			got.EgressBytes)
	}
	events := e.pendingEvents(t, "placement.terminated")
	eq(t, len(events), 1)
	ev := events[0]
	if ev.CostMicros == nil || *ev.CostMicros != got.CostMicros || ev.EdgeBytes == nil ||
		*ev.EdgeBytes != 1234 || ev.EgressBytes == nil || *ev.EgressBytes != 56 {
		t.Fatalf("event %+v does not carry the cost and the bytes", ev)
	}
	attempts, err := e.store.Attempts(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(attempts), 1)
	a := attempts[0]
	if a.SandboxID == "" || a.SizeName == "" || a.HourlyMicros != 1000 || a.DestroyedAt == nil {
		t.Fatalf("attempt %+v lacks its sandbox, size, price or destroy time", a)
	}
	// TestMarginLog: the error line names the placement
	if !strings.Contains(logs.String(), "placement cost is over the margin floor") ||
		!strings.Contains(logs.String(), p.ID.String()) {
		t.Fatalf("no margin error in the log:\n%s", logs.String())
	}
}

func TestMarginLogStaysQuietUnderTheFloor(t *testing.T) {
	var logs syncBuffer
	e := newEnv(t, options{log: &logs})
	e.start(t)
	_, h := e.placeReady(t, "order-1")
	if _, err := e.saga.Stop(t.Context(), h); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), "over the margin floor") {
		t.Fatalf("a placement that paid 50000 and cost almost nothing was logged:\n%s",
			logs.String())
	}
}

func (e *env) costJobs(t *testing.T) int {
	t.Helper()
	var n int
	err := e.pool.QueryRow(t.Context(),
		`select count(*) from river_job where kind = 'cost_daily'`).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestCostDailyTickQueuesTheDayBeforeOnce(t *testing.T) {
	m := newSpender(t, "m")
	m.pages = map[string]provider.Usage{"": {TotalMicros: -1, Complete: true, Attributed: true}}
	// local has no usage to read, so only the spender gets a job
	e := newEnv(t, options{providers: []provider.Provider{m, newLocal(t, "plain")}})
	e.start(t)
	e.run(t, jobargs.CostDailyTick{})
	yesterday := today().AddDate(0, 0, -1)
	row := e.costDay(t, "m", yesterday)
	eq(t, row.TheirsComplete, true)
	eq(t, e.costJobs(t), 1)
	// A day read to its end and complete is not read again
	e.run(t, jobargs.CostDailyTick{})
	eq(t, e.costJobs(t), 1)
	// A day read to its end that the provider had not finished is
	m.pages = map[string]provider.Usage{"": {TotalMicros: 5, Complete: false, Attributed: true}}
	if _, err := e.pool.Exec(t.Context(),
		`update provider_cost_daily set theirs_complete = false where provider = 'm'`); err != nil {
		t.Fatal(err)
	}
	e.run(t, jobargs.CostDailyTick{})
	deadline := time.Now().Add(10 * time.Second)
	for e.costJobs(t) < 2 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	eq(t, e.costJobs(t), 2)
}
