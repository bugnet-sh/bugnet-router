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
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/dpkeys"
	"github.com/bugnet-sh/bugnet-router/internal/edgeevents"
	"github.com/bugnet-sh/bugnet-router/internal/handle"
	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/ordertoken"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/secrets"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
	"github.com/bugnet-sh/bugnet-router/internal/worker"
	"github.com/bugnet-sh/bugnet-router/internal/x402"
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

// newLocal boots with the built bugnet-run and destroys its sandboxes when the test ends
func newLocal(t *testing.T, name string) *local.Provider {
	t.Helper()
	p := local.New(local.Options{
		Name:             name,
		Root:             t.TempDir(),
		Sizes:            []provider.Size{small},
		HourlyCostMicros: 1000,
		Increment:        time.Second,
		RunnerPath:       runnerBin,
	})
	// Each boot starts a gate and a sampler on the host, which only a destroy stops
	t.Cleanup(func() {
		owned, _ := p.ListOwned(context.Background(), nil)
		for _, o := range owned {
			_ = p.Destroy(context.Background(), o.Ref)
		}
	})
	return p
}

// flaky is a local provider whose listing and destroys fail on demand
type flaky struct {
	*local.Provider
	listFails    bool
	listHangs    bool         // a sweep's listing of the whole deployment waits for its context
	zeroAge      bool         // lists every sandbox with a zero creation time
	failID       string       // the sandbox whose every destroy fails
	failDestroys atomic.Int32 // how many of the next destroys fail
}

func (f *flaky) ListOwned(ctx context.Context, l map[string]string) ([]provider.Summary, error) {
	if f.listFails {
		return nil, &provider.Error{Code: provider.CodeInternal, Err: errors.New("list refused")}
	}
	if _, one := l["placement"]; f.listHangs && !one {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	owned, err := f.Provider.ListOwned(ctx, l)
	for i := range owned {
		if f.zeroAge {
			owned[i].CreatedAt = time.Time{}
		}
	}
	return owned, err
}

func (f *flaky) Destroy(ctx context.Context, ref provider.Ref) error {
	if ref.ID == f.failID || f.failDestroys.Add(-1) >= 0 {
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
	deployment  string
	// dataplaneWorkers sets Deps.DataplaneWorkers, the default when zero
	dataplaneWorkers int
	// features lists the providers of each data-plane feature
	features provider.Features
	// egress is where apply_secrets points forwarders
	egress worker.Egress
	// wallets are the saga's and the wallet jobs' wallet settings
	wallets config.Wallets
	// defaultWorkers sets Deps.DefaultWorkers, the default when zero
	defaultWorkers int
	// stats turns on the stats_snapshot job
	stats *worker.Stats
	// now is the worker's clock when set, beside workerClock
	now func() time.Time
	// sessions sets Deps.Sessions, each zero size at its default
	sessions config.Sessions
	// limitsPoll and maxConcurrent set the limit polls and account checks
	limitsPoll    config.LimitsPoll
	maxConcurrent map[string]int
	// sources turns on the source check with these settings
	sources *secrets.SourceSettings
	// sourcesClock is the source check's clock, the real one when nil
	sourcesClock *clock
	// canaries lets the canary tick find a size, which no test but the canary ones wants
	canaries bool
}

type env struct {
	saga       *saga.Saga
	store      *store.Store
	pool       *pgxpool.Pool
	river      *river.Client[pgx.Tx]
	local      provider.Provider
	priv       ed25519.PrivateKey
	deployment string
	events     <-chan *river.Event
	vk         valkey.Client
	prefix     string
	keys       dpkeys.Keys
	worker     *worker.Worker
	catalog    *catalog.Catalog
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
	reg, err := provider.NewRegistry(ps, o.features)
	if err != nil {
		t.Fatal(err)
	}
	tracker := placement.NewTracker(time.Now)
	out := o.log
	if out == nil {
		out = io.Discard
	}
	_, keys := testenv.DataPlaneKeys(t)
	st := store.New(pool, store.WithRefSealer(keys),
		store.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	sd := saga.Deps{
		Store:        st,
		Engine:       placement.New(reg, tracker),
		Registry:     reg,
		Catalog:      cat,
		Valkey:       vk,
		Prefix:       prefix,
		OrderKey:     pub,
		HandleKey:    hk,
		MarginFloor:  0.5,
		Tracker:      tracker,
		ReplicaID:    "worker-test",
		DeploymentID: o.deployment,
		Log:          slog.New(slog.NewTextHandler(out, nil)),
		Events:       edgeevents.New(vk, prefix, slog.New(slog.NewTextHandler(out, nil))),
		Keys:         keys,
		Wallets:      o.wallets,
	}
	if o.sagaClock != nil {
		sd.Now = o.sagaClock.Now
	}
	wd := worker.Deps{
		EventsURL: o.eventsURL, SigningKey: o.eventKey, DataplaneWorkers: o.dataplaneWorkers,
		Egress: o.egress, DefaultWorkers: o.defaultWorkers, Stats: o.stats,
		Sessions: o.sessions, LimitsPoll: o.limitsPoll, MaxConcurrent: o.maxConcurrent,
	}
	if o.sources != nil {
		sdeps := secrets.SourcesDeps{
			Settings: *o.sources, Registry: reg, Store: st, Valkey: vk, Prefix: prefix,
			Events: sd.Events, Log: sd.Log,
		}
		if o.sourcesClock != nil {
			sdeps.Now = o.sourcesClock.Now
		}
		if wd.Sources, err = secrets.NewSources(sdeps); err != nil {
			t.Fatal(err)
		}
	}
	if !o.canaries {
		// No size covers this, so a periodic canary_tick starts no probe beside the test's own
		wd.Health = config.DefaultHealth()
		wd.Health.CanaryVCPU = 1 << 10
	}
	if o.workerClock != nil {
		wd.Now = o.workerClock.Now
	}
	if o.now != nil {
		wd.Now = o.now
	}
	w, err := worker.New(pool, sd, wd)
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	if !o.periodic {
		w.River().PeriodicJobs().Clear()
	}
	t.Cleanup(w.Saga().Wait)
	if o.deployment == "" {
		o.deployment = "main"
	}
	return &env{
		saga: w.Saga(), store: st, pool: pool, river: w.River(), local: ps[0], priv: priv,
		deployment: o.deployment, vk: vk, prefix: prefix, keys: keys, worker: w, catalog: cat,
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

// next returns the next event of a job of kind
func (e *env) next(t *testing.T, kind string) *river.Event {
	t.Helper()
	timeout := time.After(20 * time.Second)
	for {
		select {
		case ev := <-e.events:
			if ev.Job.Kind == kind {
				return ev
			}
		case <-timeout:
			t.Fatalf("no %s job ended", kind)
			return nil
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
	ref := e.sandbox(t, prov, e.labelsFor(p.ID))
	if ok, err := e.store.ReservePlacementCapacity(ctx, p.ID, prov.Name()); err != nil || !ok {
		t.Fatalf("reserve: %v %v", ok, err)
	}
	if err := e.store.MarkPlacementReady(ctx, p.ID, ref, small.Name, readyAt,
		deadline); err != nil {
		t.Fatal(err)
	}
	return e.reload(t, p.ID), ref
}

// owned labels a sandbox as this deployment's without naming a placement
func (e *env) owned() map[string]string {
	return map[string]string{"bugnet": "1", "bugnet_deployment": e.deployment}
}

func (e *env) labelsFor(id uuid.UUID) map[string]string {
	l := e.owned()
	l["placement"] = id.String()
	return l
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

func TestDeadlineTerminatesWithUnopenableCredential(t *testing.T) {
	e := newEnv(t, options{sagaClock: pastPaidTime()})
	p, _ := e.placeReady(t, "order-1")
	k9, err := dpkeys.Parse("k9:" + base64.StdEncoding.EncodeToString(make([]byte, 32)))
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.pool.Exec(t.Context(), `
		update placements set provider_ref = jsonb_set(provider_ref, '{meta}',
			coalesce(provider_ref->'meta', '{}') || jsonb_build_object('envd_token', $2::text))
		where id = $1`, p.ID, k9.SealRef(p.ID, "envd-plain"))
	if err != nil {
		t.Fatal(err)
	}
	e.start(t)
	e.finished(t, "deadline")

	got := e.reload(t, p.ID)
	if got.State != store.PlacementTerminated || got.CapacityProvider != "" {
		t.Fatalf("placement %+v, want terminated holding no slot", got)
	}
	if exists(t, e.local, *p.ProviderRef) || e.inUse(t, "local") != 0 {
		t.Fatalf("sandbox exists %v with %d in use, want gone and 0",
			exists(t, e.local, *p.ProviderRef), e.inUse(t, "local"))
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

func TestDeadlineRetryFinishesTerminating(t *testing.T) {
	prov := &flaky{Provider: newLocal(t, "local")}
	e := newEnv(t, options{sagaClock: pastPaidTime(), providers: []provider.Provider{prov}})
	p, _ := e.placeReady(t, "order-1")
	prov.failDestroys.Store(1)
	e.start(t)

	if ev := e.next(t, "deadline"); ev.Kind != river.EventKindJobFailed {
		t.Fatalf("first deadline job ended %s, want failed on the refused destroy", ev.Kind)
	}
	if got := e.reload(t, p.ID); got.State != store.PlacementTerminating {
		t.Fatalf("placement is %s after the refused destroy, want terminating", got.State)
	}
	if ev := e.next(t, "deadline"); ev.Kind != river.EventKindJobCompleted {
		t.Fatalf("deadline retry ended %s: %+v", ev.Kind, ev.Job.Errors)
	}
	got := e.reload(t, p.ID)
	if got.State != store.PlacementTerminated || got.CapacityProvider != "" {
		t.Fatalf("placement %+v after the retry, want terminated holding no slot", got)
	}
	evs := e.pendingEvents(t, saga.EventTerminated)
	if len(evs) != 1 || evs[0].Reason != "deadline" {
		t.Fatalf("terminated events %+v, want one with reason deadline", evs)
	}
	if exists(t, prov, *p.ProviderRef) || e.inUse(t, "local") != 0 {
		t.Fatalf("sandbox exists %v with %d in use, want gone and 0",
			exists(t, prov, *p.ProviderRef), e.inUse(t, "local"))
	}
}

/* Sweep */

func TestStuckSweepJobIsRescued(t *testing.T) {
	e := newEnv(t, options{periodic: true})
	ctx := t.Context()
	orphan := e.sandbox(t, e.local, e.labelsFor(uuid.New()))
	res, err := e.river.Insert(ctx, jobargs.Sweep{}, worker.SingleRunOpts())
	if err != nil {
		t.Fatal(err)
	}
	// A worker that crashed ten minutes into this sweep left it running, which blocks the schedule
	_, err = e.pool.Exec(ctx, `
		update river_job set state = 'running', attempt = 1, attempted_by = array['gone'],
			attempted_at = now() - interval '10 minutes'
		where id = $1`, res.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	e.start(t)
	e.finished(t, "sweep", res.Job.ID)
	if exists(t, e.local, orphan) {
		t.Fatal("orphan survived the rescued sweep")
	}
}

func TestSweepDestroysExpiredAfterDowntime(t *testing.T) {
	e := newEnv(t, options{periodic: true})
	now := time.Now()
	p, ref := e.readyPlacement(t, e.local, now.Add(-20*time.Minute), now.Add(-10*time.Minute))
	// Too recently overdue for its sandbox to count as expired, so only its placement ends it
	late, lateRef := e.readyPlacement(t, e.local, now.Add(-10*time.Minute),
		now.Add(-10*time.Second))
	e.start(t)
	e.finished(t, "sweep")
	e.finished(t, "watch")

	for _, id := range []uuid.UUID{p.ID, late.ID} {
		if got := e.reload(t, id); got.State != store.PlacementTerminated {
			t.Fatalf("placement is %s after the first sweep and watch, want terminated", got.State)
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

	orphanA := e.sandbox(t, a, e.labelsFor(uuid.New()))
	orphanB := e.sandbox(t, b, e.labelsFor(uuid.New()))
	stuck := e.sandbox(t, b, e.labelsFor(uuid.New()))
	b.failID = stuck.ID
	ready, home := e.readyPlacement(t, a, now, now.Add(time.Hour))
	strayA := e.sandbox(t, a, e.labelsFor(ready.ID))
	strayB := e.sandbox(t, b, e.labelsFor(ready.ID))
	failed := e.placing(t)
	if err := e.store.MarkPlacementFailed(ctx, failed.ID, "boom", true); err != nil {
		t.Fatal(err)
	}
	failedBox := e.sandbox(t, a, e.labelsFor(failed.ID))
	ended, endedBox := e.readyPlacement(t, b, now, now.Add(time.Hour))
	if _, err := e.store.MarkPlacementTerminating(ctx, ended.ID, "stop", now); err != nil {
		t.Fatal(err)
	}
	if err := e.store.MarkPlacementTerminated(ctx, ended.ID, now); err != nil {
		t.Fatal(err)
	}
	unlabelled := e.sandbox(t, a, e.owned())
	bad := e.owned()
	bad["placement"] = "not-a-uuid"
	badLabel := e.sandbox(t, a, bad)
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

func TestSweepFinishesTerminating(t *testing.T) {
	prov := &flaky{Provider: newLocal(t, "local")}
	e := newEnv(t, options{providers: []provider.Provider{prov}})
	ctx := t.Context()
	now := time.Now()
	expired, expiredBox := e.readyPlacement(t, prov, now.Add(-20*time.Minute),
		now.Add(-10*time.Minute))
	// Its stop broke off after destroying the sandbox, so only the overdue pass can finish it
	stopped, stoppedBox := e.readyPlacement(t, prov, now.Add(-10*time.Minute),
		now.Add(-10*time.Second))
	if _, err := e.store.MarkPlacementTerminating(ctx, stopped.ID, "stop", now); err != nil {
		t.Fatal(err)
	}
	if err := prov.Provider.Destroy(ctx, stoppedBox); err != nil {
		t.Fatal(err)
	}
	prov.failDestroys.Store(1)
	e.start(t)

	e.run(t, jobargs.Sweep{})
	e.run(t, jobargs.Watch{})
	for _, id := range []uuid.UUID{expired.ID, stopped.ID} {
		if got := e.reload(t, id); got.State != store.PlacementTerminated ||
			got.CapacityProvider != "" {
			t.Fatalf("placement %+v after the sweep, want terminated holding no slot", got)
		}
	}
	if exists(t, prov, expiredBox) || e.inUse(t, "local") != 0 {
		t.Fatalf("expired sandbox exists %v with %d in use, want gone and 0",
			exists(t, prov, expiredBox), e.inUse(t, "local"))
	}
	e.run(t, jobargs.Sweep{})
	e.run(t, jobargs.Watch{})
	e.run(t, jobargs.Deadline{PlacementID: expired.ID})
	reasons := map[uuid.UUID]string{}
	for _, ev := range e.pendingEvents(t, saga.EventTerminated) {
		if _, dup := reasons[ev.PlacementID]; dup {
			t.Fatalf("placement %s ended twice", ev.PlacementID)
		}
		reasons[ev.PlacementID] = ev.Reason
	}
	if len(reasons) != 2 || reasons[expired.ID] != saga.ReasonDeadline ||
		reasons[stopped.ID] != saga.ReasonStop {
		t.Fatalf("terminated reasons %v, want deadline for the expired and stop for the stopped",
			reasons)
	}
}

func TestWatchFinishesStopLeftTerminating(t *testing.T) {
	e := newEnv(t, options{})
	ctx := t.Context()
	now := time.Now()
	stuck, stuckBox := e.readyPlacement(t, e.local, now.Add(-5*time.Minute), now.Add(time.Hour))
	busy, busyBox := e.readyPlacement(t, e.local, now.Add(-5*time.Minute), now.Add(time.Hour))
	// One stop broke off three minutes ago, the other is still destroying its sandbox
	if _, err := e.store.MarkPlacementTerminating(ctx, stuck.ID, "stop",
		now.Add(-3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.MarkPlacementTerminating(ctx, busy.ID, "stop",
		now.Add(-30*time.Second)); err != nil {
		t.Fatal(err)
	}
	e.start(t)
	e.run(t, jobargs.Watch{})
	e.run(t, jobargs.Sweep{})

	if got := e.reload(t, stuck.ID); got.State != store.PlacementTerminated {
		t.Fatalf("stuck stop is %s, want terminated", got.State)
	}
	if exists(t, e.local, stuckBox) {
		t.Fatal("the stuck stop's sandbox survived")
	}
	evs := e.pendingEvents(t, saga.EventTerminated)
	if len(evs) != 1 || evs[0].PlacementID != stuck.ID || evs[0].Reason != saga.ReasonStop {
		t.Fatalf("terminated events %+v, want one for the stuck stop with reason stop", evs)
	}
	if got := e.reload(t, busy.ID); got.State != store.PlacementTerminating ||
		!exists(t, e.local, busyBox) {
		t.Fatalf("recent stop is %s, want it left to its owner", got.State)
	}
}

func TestWatchReleasesSlotsOfEndedPlacements(t *testing.T) {
	e := newEnv(t, options{})
	ctx := t.Context()
	p := e.placing(t)
	if ok, err := e.store.ReservePlacementCapacity(ctx, p.ID, "local"); err != nil || !ok {
		t.Fatalf("reserve: %v %v", ok, err)
	}
	// The release after the failure never ran, so the slot outlived the placement
	if err := e.store.MarkPlacementFailed(ctx, p.ID, "boom", true); err != nil {
		t.Fatal(err)
	}
	e.start(t)
	e.run(t, jobargs.Watch{})
	if n := e.inUse(t, "local"); n != 0 {
		t.Fatalf("in use %d after the watch, want the failed placement's slot freed", n)
	}
}

func TestWatchRunsWhileSweepListingHangs(t *testing.T) {
	prov := &flaky{Provider: newLocal(t, "local"), listHangs: true}
	e := newEnv(t, options{periodic: true, providers: []provider.Provider{prov}})
	ctx := t.Context()
	stale := e.placing(t)
	if err := e.store.HeartbeatPlacement(ctx, stale.ID, "gone",
		time.Now().Add(-90*time.Second)); err != nil {
		t.Fatal(err)
	}
	e.start(t)
	e.finished(t, "watch")
	if got := e.reload(t, stale.ID); got.State != store.PlacementFailed {
		t.Fatalf("stalled placement is %s while the sweep hangs, want failed", got.State)
	}
}

func TestSweepKeepsOtherDeployments(t *testing.T) {
	clk := &clock{}
	e := newEnv(t, options{deployment: "blue", workerClock: clk})
	own := e.sandbox(t, e.local, e.labelsFor(uuid.New()))
	kept := map[string]provider.Ref{
		"green orphan": e.sandbox(t, e.local, map[string]string{
			"bugnet": "1", "bugnet_deployment": "green", "placement": uuid.NewString(),
		}),
		"green without a placement": e.sandbox(t, e.local, map[string]string{
			"bugnet": "1", "bugnet_deployment": "green",
		}),
		"no deployment": e.sandbox(t, e.local, map[string]string{
			"bugnet": "1", "placement": uuid.NewString(),
		}),
	}
	e.start(t)
	clk.Advance(11 * time.Minute)
	e.run(t, jobargs.Sweep{})
	if exists(t, e.local, own) {
		t.Error("this deployment's orphan survived the sweep")
	}
	for name, ref := range kept {
		if !exists(t, e.local, ref) {
			t.Errorf("%s sandbox was destroyed", name)
		}
	}
}

func TestSweepKeepsUnlabelledOfUnknownAge(t *testing.T) {
	clk := &clock{}
	var logs syncBuffer
	prov := &flaky{Provider: newLocal(t, "local"), zeroAge: true}
	e := newEnv(t, options{workerClock: clk, log: &logs, providers: []provider.Provider{prov}})
	unlabelled := e.sandbox(t, prov, e.owned())
	orphan := e.sandbox(t, prov, e.labelsFor(uuid.New()))
	e.start(t)
	clk.Advance(time.Hour)
	e.run(t, jobargs.Sweep{})
	if !exists(t, prov, unlabelled) {
		t.Fatal("sandbox of unknown age without a placement label was destroyed")
	}
	if exists(t, prov, orphan) {
		t.Fatal("orphan of unknown age survived, want it judged by its placement")
	}
	if n := strings.Count(logs.String(), "unknown age"); n != 1 {
		t.Fatalf("unknown age warned %d times, want once\n%s", n, logs.String())
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
	if ok, err := e.store.ReservePlacementCapacity(ctx, stale.ID, "local"); err != nil || !ok {
		t.Fatalf("reserve: %v %v", ok, err)
	}
	if err := e.store.RecordAttempt(ctx, store.Attempt{
		PlacementID: stale.ID, Attempt: 1, Provider: "local", IdempotencyKey: "k1",
		State: "creating",
	}); err != nil {
		t.Fatal(err)
	}
	staleBox := e.sandbox(t, e.local, e.labelsFor(stale.ID))
	freshBox := e.sandbox(t, e.local, e.labelsFor(fresh.ID))
	e.start(t)
	e.run(t, jobargs.Watch{})
	e.saga.Wait()

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
	stuck := e.insertEvent(t, "stuck")
	for n := range 14 {
		e.run(t, jobargs.Outbox{})
		row := e.outboxRow(t, stuck)
		want := min(time.Second<<n, 5*time.Minute)
		if d := row.nextAttempt.Sub(clk.Now()); row.attempts != n+1 || d > want ||
			d < want-500*time.Millisecond {
			t.Fatalf("after %d failures: %d attempts, retry in %v, want %v", n+1, row.attempts,
				d, want)
		}
		clk.Advance(5 * time.Minute)
	}
	if n := strings.Count(logs.String(), "outbox event keeps failing"); n != 1 ||
		!strings.Contains(logs.String(), "level=ERROR msg=\"outbox event keeps failing\"") {
		t.Fatalf("failing event alerted %d times, want once at error level\n%s", n,
			logs.String())
	}
	recv.set(func(r *receiver) { r.fail = 0 })
	e.run(t, jobargs.Outbox{})
	if row := e.outboxRow(t, stuck); row.deliveredAt == nil || row.attempts != 14 {
		t.Fatalf("event row %+v, want delivered on the try after 14 failures", row)
	}
	if n := len(recv.deliveries()); n != 17 {
		t.Fatalf("%d requests, want every failed try and the delivery", n)
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
	if n := strings.Count(logs.String(), "events_url is not set"); n != 1 {
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

func TestOutboxRedactsTransportError(t *testing.T) {
	closed := httptest.NewServer(http.NotFoundHandler())
	addr := closed.Listener.Addr().String()
	closed.Close()
	var logs syncBuffer
	url := "http://hook-user:hook-pass@" + addr + "/events?token=query-secret"
	e := newEnv(t, options{eventsURL: url, log: &logs})
	id := e.insertEvent(t, "one")
	e.start(t)
	e.run(t, jobargs.Outbox{})

	row := e.outboxRow(t, id)
	if row.attempts != 1 || !strings.HasPrefix(row.lastError, "Post: dial tcp") {
		t.Fatalf("row %+v, want one failed attempt naming the method and the dial error", row)
	}
	for _, secret := range []string{"query-secret", "hook-user", "hook-pass", "/events"} {
		if strings.Contains(row.lastError, secret) || strings.Contains(logs.String(), secret) {
			t.Fatalf("%q leaked into last_error %q or the logs\n%s", secret, row.lastError,
				logs.String())
		}
	}
	if !strings.Contains(logs.String(), "outbox delivery failed") {
		t.Fatalf("failed delivery not logged\n%s", logs.String())
	}
}

func TestWatchReportsStuckEvents(t *testing.T) {
	var logs syncBuffer
	clk := &clock{}
	e := newEnv(t, options{log: &logs, workerClock: clk})
	id := e.insertEvent(t, "1")
	e.insertEvent(t, "2")
	e.start(t)
	e.run(t, jobargs.Watch{})
	if strings.Contains(logs.String(), "outbox holds stuck events") {
		t.Fatalf("watch alerted with no event at the threshold\n%s", logs.String())
	}
	_, err := e.pool.Exec(t.Context(), `update outbox set attempts = $2 where id = $1`, id,
		store.MaxOutboxAttempts)
	if err != nil {
		t.Fatal(err)
	}
	e.run(t, jobargs.Watch{})
	e.run(t, jobargs.Watch{})
	clk.Advance(9 * time.Minute)
	e.run(t, jobargs.Watch{})
	if n := strings.Count(logs.String(), "outbox holds stuck events"); n != 1 ||
		!strings.Contains(logs.String(), "events=1") ||
		!strings.Contains(logs.String(), "level=ERROR") {
		t.Fatalf("watch alerted %d times, want once at error level with the count\n%s", n,
			logs.String())
	}
	clk.Advance(time.Minute)
	e.run(t, jobargs.Watch{})
	if n := strings.Count(logs.String(), "outbox holds stuck events"); n != 2 {
		t.Fatalf("watch alerted %d times, want again after 10 minutes\n%s", n, logs.String())
	}
	if _, err := e.pool.Exec(t.Context(),
		`update outbox set delivered_at = now() where id = $1`, id); err != nil {
		t.Fatal(err)
	}
	clk.Advance(10 * time.Minute)
	e.run(t, jobargs.Watch{})
	if n := strings.Count(logs.String(), "outbox holds stuck events"); n != 2 {
		t.Fatalf("watch alerted %d times, want no alert once the event was delivered\n%s", n,
			logs.String())
	}
}

func TestOutboxAlertsOnceTheTenthFailureIsRecorded(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	var logs syncBuffer
	recv := newReceiver(t, pub)
	recv.fail = http.StatusInternalServerError
	e := newEnv(t, options{eventsURL: recv.srv.URL, eventKey: priv, log: &logs})
	ctx := t.Context()
	id := e.insertEvent(t, "stuck")
	// One failure short of the alert, while the database refuses to record the tenth failure
	_, err = e.pool.Exec(ctx, `update outbox set attempts = 9 where id = $1`, id)
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.pool.Exec(ctx,
		`alter table outbox add constraint refuse_tenth check (attempts < 10)`)
	if err != nil {
		t.Fatal(err)
	}
	e.start(t)
	res, err := e.river.Insert(ctx, jobargs.Outbox{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ev := e.next(t, "outbox"); ev.Kind != river.EventKindJobFailed {
		t.Fatalf("outbox job ended %s, want failed on the refused update", ev.Kind)
	}
	if _, err := e.river.JobCancel(ctx, res.Job.ID); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), "outbox event keeps failing") {
		t.Fatalf("failing event alerted although its failure was rolled back\n%s",
			logs.String())
	}
	if row := e.outboxRow(t, id); row.attempts != 9 {
		t.Fatalf("row %+v, want the ninth failure still the last recorded", row)
	}

	if _, err := e.pool.Exec(ctx, `alter table outbox drop constraint refuse_tenth`); err != nil {
		t.Fatal(err)
	}
	e.run(t, jobargs.Outbox{})
	if row := e.outboxRow(t, id); row.attempts != 10 || row.deliveredAt != nil {
		t.Fatalf("row %+v, want undelivered after 10 attempts", row)
	}
	if n := strings.Count(logs.String(), "outbox event keeps failing"); n != 1 {
		t.Fatalf("failing event alerted %d times, want once\n%s", n, logs.String())
	}
}

func TestDataplaneQueueHasEightWorkers(t *testing.T) {
	q := worker.Queues(worker.Deps{})
	if q[jobargs.QueueDataplane].MaxWorkers != 8 || len(q) != 3 {
		t.Fatalf("queues %+v, want dataplane at 8 workers beside default and probes", q)
	}
	three := worker.Queues(worker.Deps{DataplaneWorkers: 3})
	if three[jobargs.QueueDataplane].MaxWorkers != 3 {
		t.Fatalf("queues %+v, want dataplane at the 3 workers asked for", three)
	}
	if jobargs.QueueDataplane != "dataplane" {
		t.Fatalf("queue %q, want dataplane", jobargs.QueueDataplane)
	}
}

func TestDataplaneQueueRunsJobsOnTheWorkersAskedFor(t *testing.T) {
	prov := &flaky{Provider: newLocal(t, "local"), listHangs: true}
	e := newEnv(t, options{providers: []provider.Provider{prov}, dataplaneWorkers: 2})
	e.start(t)
	ctx := t.Context()
	dataplane := &river.InsertOpts{Queue: jobargs.QueueDataplane}
	res, err := e.river.Insert(ctx, jobargs.Outbox{}, dataplane)
	if err != nil {
		t.Fatal(err)
	}
	e.finished(t, "outbox", res.Job.ID)

	// Each sweep hangs on its listing, so only as many run as the queue has workers
	for range 3 {
		if _, err := e.river.Insert(ctx, jobargs.Sweep{}, dataplane); err != nil {
			t.Fatal(err)
		}
	}
	running := func() int {
		l, err := e.river.JobList(ctx, river.NewJobListParams().Kinds("sweep").
			Queues(jobargs.QueueDataplane).States(rivertype.JobStateRunning))
		if err != nil {
			t.Fatal(err)
		}
		return len(l.Jobs)
	}
	eventually(t, "two running sweeps", func() bool { return running() >= 2 })
	// River polls each queue every second, so a third worker would have taken a job by now
	time.Sleep(1500 * time.Millisecond)
	if n := running(); n != 2 {
		t.Fatalf("%d sweeps running on the dataplane queue, want its 2 workers' worth", n)
	}
}

// held gives a placement a secret, a link and a tunnel token, a preview, an SSH key and an SSH host
func (e *env) held(t *testing.T, id uuid.UUID) {
	t.Helper()
	ctx := t.Context()
	sum := []byte(id.String())
	_, _, err := e.store.PutSecret(ctx, store.Secret{
		ID: uuid.New(), PlacementID: id, Name: "HF_TOKEN", Hosts: []string{"huggingface.co"},
		PlaceholderSHA256: sum, Sealed: []byte("box"), KeyID: "k1",
		Source: store.SourceBridge, State: store.SecretActive,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.store.InsertSecretLink(ctx, store.SecretLink{
		ID: uuid.New(), PlacementID: id, TokenSHA256: sum, PairingCode: "abc",
	}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := e.store.MintEgressToken(ctx, id, sum); err != nil || !ok {
		t.Fatalf("mint token: %v %v", ok, err)
	}
	if _, err := e.store.UpsertPreview(ctx, id, 3000, 5, id.String()[:16]); err != nil {
		t.Fatal(err)
	}
	if err := e.store.AddSSHKey(ctx, id, "SHA256:k", []byte("wire"), 5); err != nil {
		t.Fatal(err)
	}
	err = e.store.PutSSHHost(ctx, store.SSHHost{PlacementID: id, Transport: "gate", SSHDPort: 22,
		SSHUser: "root", HostKey: "ssh-ed25519 AAAA"})
	if err != nil {
		t.Fatal(err)
	}
}

// rowsOf counts a placement's secrets, links, previews, ssh_keys and ssh_hosts rows
func (e *env) rowsOf(t *testing.T, id uuid.UUID) (secrets, links, traffic int, token bool) {
	t.Helper()
	count := func(table string) int {
		var n int
		err := e.pool.QueryRow(t.Context(),
			`select count(*) from `+table+` where placement_id = $1`, id).Scan(&n)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	has, err := e.store.HasEgressToken(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	return count("secrets"), count("secret_links"),
		count("previews") + count("ssh_keys") + count("ssh_hosts"), has
}

// overdue records a ready placement without a sandbox whose deadline passed ago
func (e *env) overdue(t *testing.T, ago time.Duration) uuid.UUID {
	t.Helper()
	p := e.placing(t)
	now := time.Now()
	ref := provider.Ref{Provider: "local", ID: "gone-" + p.ID.String()}
	if err := e.store.MarkPlacementReady(t.Context(), p.ID, ref, small.Name,
		now.Add(-ago-time.Hour), now.Add(-ago)); err != nil {
		t.Fatal(err)
	}
	e.held(t, p.ID)
	return p.ID
}

// endedAgo records a terminated placement that ended ago, its rows left behind
func (e *env) endedAgo(t *testing.T, ago time.Duration) uuid.UUID {
	t.Helper()
	ctx := t.Context()
	p := e.placing(t)
	at := time.Now().Add(-ago)
	ref := provider.Ref{Provider: "local", ID: "gone-" + p.ID.String()}
	if err := e.store.MarkPlacementReady(ctx, p.ID, ref, small.Name, at.Add(-time.Hour),
		at.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	e.held(t, p.ID)
	if _, err := e.store.MarkPlacementTerminating(ctx, p.ID, "stop", at); err != nil {
		t.Fatal(err)
	}
	if err := e.store.MarkPlacementTerminated(ctx, p.ID, at); err != nil {
		t.Fatal(err)
	}
	return p.ID
}

func TestSweepDeletesLeftovers(t *testing.T) {
	e := newEnv(t, options{})
	e.start(t)
	pastHour := e.overdue(t, 2*time.Hour)
	justPast := e.overdue(t, 30*time.Minute)
	eightDays := e.endedAgo(t, 8*24*time.Hour)
	sixDays := e.endedAgo(t, 6*24*time.Hour)
	e.run(t, jobargs.Sweep{})

	type rows struct {
		secrets, links, traffic int
		token                   bool
	}
	got := func(id uuid.UUID) rows {
		s, l, tr, tok := e.rowsOf(t, id)
		return rows{s, l, tr, tok}
	}
	eq(t, got(pastHour), rows{0, 0, 3, false})
	eq(t, got(justPast), rows{1, 1, 3, true})
	// An ended placement's secrets go at once, its previews and SSH records after 7 days
	eq(t, got(eightDays), rows{0, 0, 0, false})
	eq(t, got(sixDays), rows{0, 0, 3, false})
}

func TestSweepDeletesX402Rows(t *testing.T) {
	ctx := t.Context()
	e := newEnv(t, options{})
	spec := x402.Spec{
		Host: "api.example.com", Prefix: "/v1/", Method: "GET", MaxPriceMicros: 1000,
		PayTo: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", PaidPath: "/v1",
	}
	seed := func(p store.Placement) {
		t.Helper()
		id := x402.ServiceID("GET", spec.Host, spec.Prefix)
		row := store.X402Service{PlacementID: p.ID, ServiceID: id, Spec: spec}
		if err := e.store.PutX402Service(ctx, row); err != nil {
			t.Fatal(err)
		}
		if _, err := e.pool.Exec(ctx, `insert into x402_refusals
			(placement_id, reason, count, last_at)
			values ($1, 'PRICE', 1, now())`, p.ID); err != nil {
			t.Fatal(err)
		}
	}
	rows := func(p store.Placement) (n int) {
		t.Helper()
		err := e.pool.QueryRow(ctx, `select (select count(*) from x402_services
			where placement_id = $1) + (select count(*) from x402_refusals
			where placement_id = $1)`, p.ID).Scan(&n)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	set := func(p store.Placement, deadline, ended string) {
		t.Helper()
		_, err := e.pool.Exec(ctx, `update placements set deadline = `+deadline+`,
			terminated_at = `+ended+` where id = $1`, p.ID)
		if err != nil {
			t.Fatal(err)
		}
	}
	past, recent, live, failed := e.placing(t), e.placing(t), e.placing(t), e.placing(t)
	for _, p := range []store.Placement{past, recent, live, failed} {
		seed(p)
	}
	set(past, "now() - interval '2 hours'", "null")
	set(recent, "now() - interval '30 minutes'", "null")
	set(live, "now() + interval '1 hour'", "null")
	if err := e.store.MarkPlacementFailed(ctx, failed.ID, "boom", true); err != nil {
		t.Fatal(err)
	}
	set(failed, "null", "now() - interval '2 hours'")
	e.start(t)

	e.run(t, jobargs.Sweep{})
	eq := func(name string, p store.Placement, want int) {
		t.Helper()
		if got := rows(p); got != want {
			t.Errorf("%s placement has %d x402 rows, want %d", name, got, want)
		}
	}
	eq("past deadline", past, 0)
	eq("failed before ready", failed, 0)
	eq("within the hour", recent, 2)
	eq("live", live, 2)
}
