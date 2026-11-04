package worker_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	"github.com/oynozan/bugnet-router/internal/catalog"
	"github.com/oynozan/bugnet-router/internal/handle"
	"github.com/oynozan/bugnet-router/internal/jobargs"
	"github.com/oynozan/bugnet-router/internal/ordertoken"
	"github.com/oynozan/bugnet-router/internal/placement"
	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/provider/local"
	"github.com/oynozan/bugnet-router/internal/saga"
	"github.com/oynozan/bugnet-router/internal/store"
	"github.com/oynozan/bugnet-router/internal/testenv"
	"github.com/oynozan/bugnet-router/internal/worker"
)

const (
	smallPkg   = "small-10m"
	smallPrice = 50000
	morePkg    = "small-10m-more"
	morePrice  = 45000
)

const catalogJSON = `{"packages": [
	{"id": "small-10m", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10, "price_micros": 50000},
	{"id": "small-10m-more", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10, "price_micros": 45000, "extension_of": "small-10m"}
]}`

var small = provider.Size{
	Name: "small", Resources: provider.Resources{VCPU: 2, MemoryMiB: 2048, DiskGiB: 10},
}

func newLocal(t *testing.T, name string) *local.Provider {
	t.Helper()
	return local.New(local.Options{
		Name:             name,
		Root:             t.TempDir(),
		Sizes:            []provider.Size{small},
		HourlyCostMicros: 1000,
		Increment:        time.Second,
	})
}

// flaky fails ListOwned when listFails is set and Destroy for the sandbox named failID
type flaky struct {
	*local.Provider
	listFails bool
	failID    string
}

func (f *flaky) ListOwned(ctx context.Context, l map[string]string) ([]provider.Summary, error) {
	if f.listFails {
		return nil, &provider.Error{Code: provider.CodeInternal, Err: errors.New("list refused")}
	}
	return f.Provider.ListOwned(ctx, l)
}

func (f *flaky) Destroy(ctx context.Context, ref provider.Ref) error {
	if ref.ID == f.failID {
		return &provider.Error{Code: provider.CodeInternal, Err: errors.New("destroy refused")}
	}
	return f.Provider.Destroy(ctx, ref)
}

// clock is wall time shifted by an offset the test can move
type clock struct {
	mu     sync.Mutex
	offset time.Duration
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Now().Add(c.offset)
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.offset += d
}

// syncBuffer collects log output written from several goroutines
type syncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

/* Environment */

type options struct {
	periodic    bool // keeps the sweep and outbox schedules instead of running jobs by hand
	eventsURL   string
	eventKey    ed25519.PrivateKey
	sagaClock   *clock
	workerClock *clock
	log         io.Writer
	providers   []provider.Provider
}

type env struct {
	saga   *saga.Saga
	store  *store.Store
	pool   *pgxpool.Pool
	river  *river.Client[pgx.Tx]
	local  provider.Provider
	priv   ed25519.PrivateKey
	events <-chan *river.Event
}

func newEnv(t *testing.T, o options) *env {
	t.Helper()
	ctx := context.Background()
	url := testenv.DatabaseURL(t)
	if err := store.Migrate(ctx, url); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	vk, prefix := testenv.Valkey(t)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if o.eventKey == nil {
		if _, o.eventKey, err = ed25519.GenerateKey(rand.Reader); err != nil {
			t.Fatal(err)
		}
	}
	hk := make([]byte, 32)
	if _, err := rand.Read(hk); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "packages.json")
	if err := os.WriteFile(path, []byte(catalogJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	cat, err := catalog.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	ps := o.providers
	if len(ps) == 0 {
		ps = []provider.Provider{newLocal(t, "local")}
	}
	reg := provider.NewRegistry(ps)
	tracker := placement.NewTracker(time.Now)
	out := o.log
	if out == nil {
		out = io.Discard
	}
	st := store.New(pool)
	sd := saga.Deps{
		Store:       st,
		Engine:      placement.New(reg, tracker),
		Registry:    reg,
		Catalog:     cat,
		Valkey:      vk,
		Prefix:      prefix,
		OrderKey:    pub,
		HandleKey:   hk,
		MarginFloor: 0.5,
		Tracker:     tracker,
		ReplicaID:   "worker-test",
		Log:         slog.New(slog.NewTextHandler(out, nil)),
	}
	if o.sagaClock != nil {
		sd.Now = o.sagaClock.Now
	}
	wd := worker.Deps{EventsURL: o.eventsURL, SigningKey: o.eventKey}
	if o.workerClock != nil {
		wd.Now = o.workerClock.Now
	}
	w, err := worker.New(pool, sd, wd)
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	if !o.periodic {
		w.River().PeriodicJobs().Clear()
	}
	t.Cleanup(w.Saga().Wait)
	return &env{
		saga: w.Saga(), store: st, pool: pool, river: w.River(), local: ps[0], priv: priv,
	}
}

// start runs the River client until the test ends
func (e *env) start(t *testing.T) {
	t.Helper()
	events, cancel := e.river.Subscribe(river.EventKindJobCompleted, river.EventKindJobFailed,
		river.EventKindJobCancelled, river.EventKindJobSnoozed)
	t.Cleanup(cancel)
	e.events = events
	if err := e.river.Start(context.Background()); err != nil {
		t.Fatalf("start river: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := e.river.StopAndCancel(ctx); err != nil {
			t.Errorf("stop river: %v", err)
		}
	})
}

// finished waits for the jobs of kind with ids, or for any one job of kind without ids, to complete
func (e *env) finished(t *testing.T, kind string, ids ...int64) {
	t.Helper()
	waiting := map[int64]bool{}
	for _, id := range ids {
		waiting[id] = true
	}
	timeout := time.After(20 * time.Second)
	for {
		select {
		case ev := <-e.events:
			if ev.Job.Kind != kind || (len(ids) > 0 && !waiting[ev.Job.ID]) {
				continue
			}
			if ev.Kind != river.EventKindJobCompleted {
				t.Fatalf("%s job %d ended %s: %+v", kind, ev.Job.ID, ev.Kind, ev.Job.Errors)
			}
			delete(waiting, ev.Job.ID)
			if len(waiting) == 0 {
				return
			}
		case <-timeout:
			t.Fatalf("%s jobs %v did not complete", kind, ids)
		}
	}
}

// run inserts a job and waits for it to complete
func (e *env) run(t *testing.T, args river.JobArgs) {
	t.Helper()
	res, err := e.river.Insert(t.Context(), args, nil)
	if err != nil {
		t.Fatalf("insert %s: %v", args.Kind(), err)
	}
	e.finished(t, args.Kind(), res.Job.ID)
}

/* Placements and sandboxes */

func (e *env) token(t *testing.T, c ordertoken.Claims) string {
	t.Helper()
	c.ExpiresAt = time.Now().Add(time.Hour)
	tok, err := ordertoken.Sign(e.priv, c)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// placeReady places a start order through the saga and waits until placing ends
func (e *env) placeReady(t *testing.T, order string) (store.Placement, string) {
	t.Helper()
	tok := e.token(t, ordertoken.Claims{
		OrderID: order, Kind: ordertoken.KindStart, PackageID: smallPkg, AmountMicros: smallPrice,
	})
	p, h, err := e.saga.Create(t.Context(), tok, placement.Requirements{})
	if err != nil {
		t.Fatalf("create %s: %v", order, err)
	}
	e.saga.Wait()
	p = e.reload(t, p.ID)
	if p.State != store.PlacementReady {
		t.Fatalf("placement %s is %s (%s), want ready", order, p.State, p.FailureReason)
	}
	return p, h
}

func (e *env) extendToken(t *testing.T, order, h string) string {
	t.Helper()
	sha, err := handle.Hash(h)
	if err != nil {
		t.Fatal(err)
	}
	return e.token(t, ordertoken.Claims{
		OrderID: order, Kind: ordertoken.KindExtend, PackageID: morePkg, AmountMicros: morePrice,
		HandleSHA256: hex.EncodeToString(sha),
	})
}

// placing records a placement in placing, as Create does before placing starts
func (e *env) placing(t *testing.T) store.Placement {
	t.Helper()
	sha := make([]byte, 32)
	if _, err := rand.Read(sha); err != nil {
		t.Fatal(err)
	}
	p, _, err := e.store.CreatePlacement(t.Context(), store.NewPlacement{
		HandleSHA256: sha, OrderID: uuid.NewString(), PackageID: smallPkg,
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// readyPlacement records a ready placement on its own sandbox without any deadline job
func (e *env) readyPlacement(
	t *testing.T, prov provider.Provider, readyAt, deadline time.Time,
) (store.Placement, provider.Ref) {
	t.Helper()
	ctx := t.Context()
	p := e.placing(t)
	ref := e.sandbox(t, prov, labelsFor(p.ID))
	if ok, err := e.store.ReserveCapacity(ctx, prov.Name()); err != nil || !ok {
		t.Fatalf("reserve: %v %v", ok, err)
	}
	if err := e.store.MarkPlacementReady(ctx, p.ID, ref, small.Name, readyAt,
		deadline); err != nil {
		t.Fatal(err)
	}
	return e.reload(t, p.ID), ref
}

func labelsFor(id uuid.UUID) map[string]string {
	return map[string]string{"bugnet": "1", "placement": id.String()}
}

func (e *env) sandbox(t *testing.T, prov provider.Provider, l map[string]string) provider.Ref {
	t.Helper()
	ref, err := prov.Create(t.Context(), provider.CreateRequest{
		IdempotencyKey: uuid.NewString(), Size: small, Labels: l, Lifetime: time.Hour,
	})
	if err != nil {
		t.Fatalf("create sandbox: %v", err)
	}
	return ref
}

func exists(t *testing.T, prov provider.Provider, ref provider.Ref) bool {
	t.Helper()
	all, err := prov.ListOwned(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return slices.ContainsFunc(all, func(s provider.Summary) bool { return s.Ref.ID == ref.ID })
}

func (e *env) reload(t *testing.T, id uuid.UUID) store.Placement {
	t.Helper()
	p, err := e.store.PlacementByID(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func (e *env) pendingEvents(t *testing.T, eventType string) []saga.Event {
	t.Helper()
	pending, err := e.store.PendingOutbox(t.Context(), time.Now(), 100)
	if err != nil {
		t.Fatal(err)
	}
	var out []saga.Event
	for _, ev := range pending {
		if ev.EventType != eventType {
			continue
		}
		var x saga.Event
		if err := json.Unmarshal(ev.Payload, &x); err != nil {
			t.Fatal(err)
		}
		out = append(out, x)
	}
	return out
}

func (e *env) inUse(t *testing.T, name string) int {
	t.Helper()
	var n int
	err := e.pool.QueryRow(t.Context(), `select coalesce(
		(select in_use from provider_capacity where provider = $1), 0)`, name).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

type deadlineJob struct {
	state       rivertype.JobState
	scheduledAt time.Time
}

func (e *env) deadlineJobs(t *testing.T, id uuid.UUID) []deadlineJob {
	t.Helper()
	res, err := e.river.JobList(t.Context(), river.NewJobListParams().Kinds("deadline"))
	if err != nil {
		t.Fatal(err)
	}
	var out []deadlineJob
	for _, j := range res.Jobs {
		var args jobargs.Deadline
		if err := json.Unmarshal(j.EncodedArgs, &args); err != nil {
			t.Fatal(err)
		}
		if args.PlacementID == id {
			out = append(out, deadlineJob{state: j.State, scheduledAt: j.ScheduledAt})
		}
	}
	return out
}

/* Deadline */

// pastPaidTime puts the saga's clock far enough back that a new placement's deadline has passed
func pastPaidTime() *clock {
	return &clock{offset: -(10*time.Minute + 5*time.Second)}
}

func TestDeadlineTerminates(t *testing.T) {
	e := newEnv(t, options{sagaClock: pastPaidTime()})
	p, _ := e.placeReady(t, "order-1")
	e.start(t)
	e.finished(t, "deadline")

	got := e.reload(t, p.ID)
	if got.State != store.PlacementTerminated {
		t.Fatalf("placement is %s, want terminated", got.State)
	}
	evs := e.pendingEvents(t, saga.EventTerminated)
	if len(evs) != 1 || evs[0].Reason != "deadline" {
		t.Fatalf("terminated events %+v, want one with reason deadline", evs)
	}
	if exists(t, e.local, *p.ProviderRef) {
		t.Fatal("sandbox still exists after the deadline")
	}
	if n := e.inUse(t, "local"); n != 0 {
		t.Fatalf("in use %d after the deadline, want 0", n)
	}

	e.run(t, jobargs.Deadline{PlacementID: p.ID})
	e.run(t, jobargs.Deadline{PlacementID: uuid.New()})
	if evs := e.pendingEvents(t, saga.EventTerminated); len(evs) != 1 {
		t.Fatalf("%d terminated events after repeated deadlines, want 1", len(evs))
	}
}

func TestDeadlineReschedulesAfterExtend(t *testing.T) {
	e := newEnv(t, options{sagaClock: pastPaidTime()})
	p, h := e.placeReady(t, "order-1")
	ext, err := e.saga.Extend(t.Context(), h, e.extendToken(t, "order-2", h))
	if err != nil {
		t.Fatalf("extend: %v", err)
	}
	if !ext.Deadline.After(time.Now().Add(9 * time.Minute)) {
		t.Fatalf("extended deadline %v is not about ten minutes out", ext.Deadline)
	}
	e.start(t)
	e.finished(t, "deadline")

	got := e.reload(t, p.ID)
	if got.State != store.PlacementReady || !got.Deadline.Equal(*ext.Deadline) {
		t.Fatalf("placement %s with deadline %v, want ready until %v", got.State, got.Deadline,
			ext.Deadline)
	}
	var completed, atNew int
	for _, j := range e.deadlineJobs(t, p.ID) {
		switch {
		case j.state == rivertype.JobStateCompleted:
			completed++
		case j.state == rivertype.JobStateScheduled && j.scheduledAt.Equal(*ext.Deadline):
			atNew++
		default:
			t.Fatalf("unexpected deadline job %+v", j)
		}
	}
	// One job comes from the extend and one from the worker re-reading the deadline
	if completed != 1 || atNew != 2 {
		t.Fatalf("%d completed and %d scheduled at the new deadline, want 1 and 2", completed,
			atNew)
	}
}

/* Sweep */

func TestSweepDestroysExpiredAfterDowntime(t *testing.T) {
	e := newEnv(t, options{periodic: true})
	now := time.Now()
	p, ref := e.readyPlacement(t, e.local, now.Add(-20*time.Minute), now.Add(-10*time.Minute))
	// Too recently overdue for its sandbox to count as expired, so only its placement ends it
	late, lateRef := e.readyPlacement(t, e.local, now.Add(-10*time.Minute),
		now.Add(-10*time.Second))
	e.start(t)
	e.finished(t, "sweep")

	for _, id := range []uuid.UUID{p.ID, late.ID} {
		if got := e.reload(t, id); got.State != store.PlacementTerminated {
			t.Fatalf("placement is %s after the first sweep, want terminated", got.State)
		}
	}
	evs := e.pendingEvents(t, saga.EventTerminated)
	if len(evs) != 2 || evs[0].Reason != "deadline" || evs[1].Reason != "deadline" {
		t.Fatalf("terminated events %+v, want two with reason deadline", evs)
	}
	if exists(t, e.local, ref) || exists(t, e.local, lateRef) {
		t.Fatal("a sandbox still exists after the first sweep")
	}
	if n := e.inUse(t, "local"); n != 0 {
		t.Fatalf("in use %d after the sweep, want 0", n)
	}
}

func TestSweepDestroysOrphans(t *testing.T) {
	clk := &clock{}
	broken := &flaky{Provider: newLocal(t, "broken"), listFails: true}
	a := newLocal(t, "a")
	b := &flaky{Provider: newLocal(t, "b")}
	e := newEnv(t, options{workerClock: clk, providers: []provider.Provider{broken, a, b}})
	ctx := t.Context()
	now := time.Now()

	orphanA := e.sandbox(t, a, labelsFor(uuid.New()))
	orphanB := e.sandbox(t, b, labelsFor(uuid.New()))
	stuck := e.sandbox(t, b, labelsFor(uuid.New()))
	b.failID = stuck.ID
	ready, home := e.readyPlacement(t, a, now, now.Add(time.Hour))
	strayA := e.sandbox(t, a, labelsFor(ready.ID))
	strayB := e.sandbox(t, b, labelsFor(ready.ID))
	failed := e.placing(t)
	if err := e.store.MarkPlacementFailed(ctx, failed.ID, "boom", true); err != nil {
		t.Fatal(err)
	}
	failedBox := e.sandbox(t, a, labelsFor(failed.ID))
	ended, endedBox := e.readyPlacement(t, b, now, now.Add(time.Hour))
	if _, err := e.store.MarkPlacementTerminating(ctx, ended.ID); err != nil {
		t.Fatal(err)
	}
	if err := e.store.MarkPlacementTerminated(ctx, ended.ID, now); err != nil {
		t.Fatal(err)
	}
	unlabelled := e.sandbox(t, a, map[string]string{"bugnet": "1"})
	badLabel := e.sandbox(t, a, map[string]string{"bugnet": "1", "placement": "not-a-uuid"})
	foreign := e.sandbox(t, a, map[string]string{"team": "other"})
	e.start(t)

	e.run(t, jobargs.Sweep{})
	gone := map[string]struct {
		p   provider.Provider
		ref provider.Ref
	}{
		"orphan on a": {a, orphanA}, "orphan on b": {b, orphanB}, "stray on a": {a, strayA},
		"stray on b": {b, strayB}, "failed": {a, failedBox}, "terminated": {b, endedBox},
	}
	for name, s := range gone {
		if exists(t, s.p, s.ref) {
			t.Errorf("%s sandbox survived the sweep", name)
		}
	}
	kept := map[string]struct {
		p   provider.Provider
		ref provider.Ref
	}{
		"home": {a, home}, "young unlabelled": {a, unlabelled}, "young bad label": {a, badLabel},
		"foreign": {a, foreign}, "failed destroy": {b, stuck},
	}
	for name, s := range kept {
		if !exists(t, s.p, s.ref) {
			t.Errorf("%s sandbox was destroyed", name)
		}
	}

	clk.Advance(11 * time.Minute)
	e.run(t, jobargs.Sweep{})
	if exists(t, a, unlabelled) || exists(t, a, badLabel) {
		t.Error("sandboxes without a placement label survived past ten minutes")
	}
	if !exists(t, a, home) || !exists(t, a, foreign) {
		t.Error("the ready placement's or the foreign sandbox was destroyed")
	}
	if got := e.reload(t, ready.ID); got.State != store.PlacementReady {
		t.Fatalf("ready placement is %s after sweeping, want ready", got.State)
	}
}

func TestSweepFailsStuckPlacing(t *testing.T) {
	e := newEnv(t, options{})
	ctx := t.Context()
	now := time.Now()
	stale := e.placing(t)
	err := e.store.HeartbeatPlacement(ctx, stale.ID, "gone", now.Add(-90*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	silent := e.placing(t)
	_, err = e.pool.Exec(ctx, `update placements set created_at = $2 where id = $1`, silent.ID,
		now.Add(-3*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	fresh := e.placing(t)
	err = e.store.HeartbeatPlacement(ctx, fresh.ID, "alive", now.Add(-10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	young := e.placing(t)

	// The stale placement's saga went away holding a slot and a sandbox for its attempt
	if ok, err := e.store.ReserveCapacity(ctx, "local"); err != nil || !ok {
		t.Fatalf("reserve: %v %v", ok, err)
	}
	if err := e.store.RecordAttempt(ctx, store.Attempt{
		PlacementID: stale.ID, Attempt: 1, Provider: "local", IdempotencyKey: "k1",
		State: "creating",
	}); err != nil {
		t.Fatal(err)
	}
	staleBox := e.sandbox(t, e.local, labelsFor(stale.ID))
	freshBox := e.sandbox(t, e.local, labelsFor(fresh.ID))
	e.start(t)
	e.run(t, jobargs.Sweep{})

	for _, id := range []uuid.UUID{stale.ID, silent.ID} {
		got := e.reload(t, id)
		if got.State != store.PlacementFailed || !got.Refundable ||
			got.FailureReason != saga.ReasonPlacingStalled {
			t.Errorf("stalled placement %+v, want failed refundable placing_stalled", got)
		}
	}
	for _, id := range []uuid.UUID{fresh.ID, young.ID} {
		if got := e.reload(t, id); got.State != store.PlacementPlacing {
			t.Errorf("live placement is %s, want placing", got.State)
		}
	}
	evs := e.pendingEvents(t, saga.EventFailed)
	if len(evs) != 2 {
		t.Fatalf("%d failed events, want 2", len(evs))
	}
	for _, ev := range evs {
		if ev.Reason != saga.ReasonPlacingStalled || !ev.Refundable {
			t.Errorf("failed event %+v, want refundable placing_stalled", ev)
		}
	}
	if exists(t, e.local, staleBox) || !exists(t, e.local, freshBox) {
		t.Fatal("want the stalled placement's sandbox destroyed and the live one kept")
	}
	if n := e.inUse(t, "local"); n != 0 {
		t.Fatalf("in use %d after failing the stalled placement, want 0", n)
	}
}

/* Outbox */

// delivery is one request the receiver accepted
type delivery struct {
	raw       []byte
	id        string
	eventType string
	createdAt time.Time
	payload   json.RawMessage
}

// receiver is an events endpoint that verifies every request's signature
type receiver struct {
	srv      *httptest.Server
	pub      ed25519.PublicKey
	mu       sync.Mutex
	statuses []int // answered in turn before fail applies
	fail     int   // the status for every later request, 200 when zero
	delay    time.Duration
	inFlight int
	most     int // the most requests in flight at once
	got      []delivery
	problems []string
}

func newReceiver(t *testing.T, pub ed25519.PublicKey, statuses ...int) *receiver {
	t.Helper()
	r := &receiver{pub: pub, statuses: statuses}
	r.srv = httptest.NewServer(r)
	t.Cleanup(r.srv.Close)
	return r
}

func (r *receiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	body, err := io.ReadAll(req.Body)
	problem := ""
	var d struct {
		ID        string          `json:"id"`
		Type      string          `json:"type"`
		CreatedAt time.Time       `json:"created_at"`
		Payload   json.RawMessage `json:"payload"`
	}
	sig, serr := base64.StdEncoding.DecodeString(req.Header.Get("X-Bugnet-Signature"))
	switch {
	case err != nil:
		problem = "read body: " + err.Error()
	case req.Method != http.MethodPost:
		problem = "method " + req.Method
	case req.Header.Get("Content-Type") != "application/json":
		problem = "content type " + req.Header.Get("Content-Type")
	case serr != nil || !ed25519.Verify(r.pub, body, sig):
		problem = "bad signature"
	case json.Unmarshal(body, &d) != nil:
		problem = "bad body " + string(body)
	case req.Header.Get("X-Bugnet-Event-Id") != d.ID:
		problem = "event id header " + req.Header.Get("X-Bugnet-Event-Id")
	}
	r.mu.Lock()
	status := http.StatusOK
	switch {
	case problem != "":
		r.problems = append(r.problems, problem)
		status = http.StatusBadRequest
	case len(r.statuses) > 0:
		status, r.statuses = r.statuses[0], r.statuses[1:]
	case r.fail != 0:
		status = r.fail
	}
	if problem == "" {
		r.got = append(r.got, delivery{
			raw: body, id: d.ID, eventType: d.Type, createdAt: d.CreatedAt, payload: d.Payload,
		})
	}
	delay := r.delay
	r.inFlight++
	r.most = max(r.most, r.inFlight)
	r.mu.Unlock()
	time.Sleep(delay)
	r.mu.Lock()
	r.inFlight--
	r.mu.Unlock()
	w.WriteHeader(status)
}

func (r *receiver) deliveries() []delivery {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.got)
}

func (r *receiver) set(fn func(*receiver)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	fn(r)
}

func (r *receiver) checkClean(t *testing.T) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.problems) > 0 {
		t.Fatalf("receiver rejected requests: %v", r.problems)
	}
}

// insertEvent adds an outbox event whose payload carries n and returns its id
func (e *env) insertEvent(t *testing.T, n string) uuid.UUID {
	t.Helper()
	ctx := t.Context()
	err := e.store.WithTx(ctx, func(tx pgx.Tx) error {
		return e.store.InsertOutbox(ctx, tx, saga.EventReady, map[string]string{"n": n})
	})
	if err != nil {
		t.Fatal(err)
	}
	var id uuid.UUID
	err = e.pool.QueryRow(ctx, `select id from outbox where payload->>'n' = $1`, n).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

type outboxRow struct {
	createdAt   time.Time
	deliveredAt *time.Time
	attempts    int
	lastError   string
	nextAttempt *time.Time
}

func (e *env) outboxRow(t *testing.T, id uuid.UUID) outboxRow {
	t.Helper()
	var r outboxRow
	err := e.pool.QueryRow(t.Context(), `
		select created_at, delivered_at, attempts, coalesce(last_error, ''), next_attempt_at
		from outbox where id = $1`, id).Scan(
		&r.createdAt, &r.deliveredAt, &r.attempts, &r.lastError, &r.nextAttempt)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestOutboxDeliversSigned(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	recv := newReceiver(t, pub)
	e := newEnv(t, options{periodic: true, eventsURL: recv.srv.URL, eventKey: priv})
	id := e.insertEvent(t, "one")
	e.start(t)

	eventually(t, "the event to be delivered", func() bool {
		return e.outboxRow(t, id).deliveredAt != nil
	})
	recv.checkClean(t)
	got := recv.deliveries()
	if len(got) != 1 {
		t.Fatalf("%d deliveries, want 1", len(got))
	}
	d := got[0]
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(d.raw, &keys); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 4 {
		t.Fatalf("body %s, want exactly id, type, created_at and payload", d.raw)
	}
	row := e.outboxRow(t, id)
	if d.id != id.String() || d.eventType != saga.EventReady || !d.createdAt.Equal(row.createdAt) {
		t.Fatalf("delivered %+v, want event %s created at %v", d, id, row.createdAt)
	}
	var payload map[string]string
	if err := json.Unmarshal(d.payload, &payload); err != nil || payload["n"] != "one" {
		t.Fatalf("payload %s: %v", d.payload, err)
	}
	if row.attempts != 0 {
		t.Fatalf("%d failed attempts recorded for a delivered event", row.attempts)
	}
}

func TestOutboxRetriesOnFailure(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	clk := &clock{}
	var logs syncBuffer
	recv := newReceiver(t, pub, http.StatusServiceUnavailable)
	e := newEnv(t, options{
		eventsURL: recv.srv.URL, eventKey: priv, workerClock: clk, log: &logs,
	})
	id := e.insertEvent(t, "one")
	e.start(t)

	e.run(t, jobargs.Outbox{})
	row := e.outboxRow(t, id)
	if row.deliveredAt != nil || row.attempts != 1 || !strings.Contains(row.lastError, "503") {
		t.Fatalf("after a 503 the row is %+v, want one failed attempt naming 503", row)
	}
	if d := row.nextAttempt.Sub(clk.Now()); d < 500*time.Millisecond || d > time.Second {
		t.Fatalf("first retry in %v, want one second", d)
	}
	e.run(t, jobargs.Outbox{})
	if n := len(recv.deliveries()); n != 1 {
		t.Fatalf("%d requests before the retry was due, want 1", n)
	}
	clk.Advance(time.Second)
	e.run(t, jobargs.Outbox{})
	if row := e.outboxRow(t, id); row.deliveredAt == nil || len(recv.deliveries()) != 2 {
		t.Fatalf("after the retry the row is %+v with %d requests, want delivered on the second",
			row, len(recv.deliveries()))
	}

	recv.set(func(r *receiver) { r.fail = http.StatusInternalServerError })
	dead := e.insertEvent(t, "dead")
	for n := range 10 {
		e.run(t, jobargs.Outbox{})
		row := e.outboxRow(t, dead)
		want := min(time.Second<<n, 5*time.Minute)
		if d := row.nextAttempt.Sub(clk.Now()); row.attempts != n+1 || d > want ||
			d < want-500*time.Millisecond {
			t.Fatalf("after %d failures: %d attempts, retry in %v, want %v", n+1, row.attempts,
				d, want)
		}
		clk.Advance(5 * time.Minute)
	}
	e.run(t, jobargs.Outbox{})
	if n := len(recv.deliveries()); n != 12 {
		t.Fatalf("%d requests, want no more after ten failures", n)
	}
	if row := e.outboxRow(t, dead); row.deliveredAt != nil || row.attempts != 10 {
		t.Fatalf("dead event row %+v, want undelivered after 10 attempts", row)
	}
	if n := strings.Count(logs.String(), "outbox event is dead"); n != 1 {
		t.Fatalf("dead event logged %d times, want once\n%s", n, logs.String())
	}
	recv.checkClean(t)
}

func TestOutboxWithoutURLKeepsEventsAndWarnsOnce(t *testing.T) {
	var logs syncBuffer
	e := newEnv(t, options{log: &logs})
	id := e.insertEvent(t, "one")
	e.start(t)
	e.run(t, jobargs.Outbox{})
	e.run(t, jobargs.Outbox{})
	if row := e.outboxRow(t, id); row.deliveredAt != nil || row.attempts != 0 {
		t.Fatalf("row %+v, want pending and untried", row)
	}
	if n := strings.Count(logs.String(), "BUGNET_EVENTS_URL is not set"); n != 1 {
		t.Fatalf("missing url warned %d times, want once\n%s", n, logs.String())
	}
}

func TestOutboxConcurrentRunsSendEachOnce(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	recv := newReceiver(t, pub)
	recv.delay = 150 * time.Millisecond
	e := newEnv(t, options{eventsURL: recv.srv.URL, eventKey: priv})
	for _, n := range []string{"a", "b", "c", "d"} {
		e.insertEvent(t, n)
	}
	e.start(t)
	var params []river.InsertManyParams
	for range 3 {
		params = append(params, river.InsertManyParams{Args: jobargs.Outbox{}})
	}
	res, err := e.river.InsertMany(t.Context(), params)
	if err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for _, r := range res {
		ids = append(ids, r.Job.ID)
	}
	e.finished(t, "outbox", ids...)
	recv.checkClean(t)
	seen := map[string]int{}
	for _, d := range recv.deliveries() {
		seen[d.id]++
	}
	if len(seen) != 4 {
		t.Fatalf("delivered %v, want four events", seen)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("event %s sent %d times", id, n)
		}
	}
}

func TestOutboxScheduleRunsOneJobAtATime(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	recv := newReceiver(t, pub)
	recv.delay = 1200 * time.Millisecond
	e := newEnv(t, options{periodic: true, eventsURL: recv.srv.URL, eventKey: priv})
	for _, n := range []string{"a", "b", "c"} {
		e.insertEvent(t, n)
	}
	e.start(t)
	eventually(t, "three deliveries", func() bool { return len(recv.deliveries()) == 3 })
	recv.checkClean(t)
	recv.set(func(r *receiver) {
		if r.most != 1 {
			t.Errorf("%d deliveries ran at once, want the schedule to run one outbox job", r.most)
		}
	})
}
