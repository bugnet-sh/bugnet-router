package saga_test

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
	"maps"
	"os"
	"path/filepath"
	"reflect"
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
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/dpkeys"
	"github.com/bugnet-sh/bugnet-router/internal/edgeevents"
	"github.com/bugnet-sh/bugnet-router/internal/handle"
	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/ordertoken"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

const (
	smallPkg   = "small-10m"
	smallPrice = 50000
	morePkg    = "small-10m-more"
	morePrice  = 45000
	shortPkg   = "small-10m-5"
	shortPrice = 25000
	longPkg    = "small-30m"
	// restrictedPkg reaches huggingface.co alone
	restrictedPkg = "small-10m-restricted"
)

const catalogJSON = `{"packages": [
	{"id": "small-10m", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10, "price_micros": 50000},
	{"id": "small-10m-more", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10, "price_micros": 45000, "extension_of": "small-10m"},
	{"id": "small-10m-5", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 5, "price_micros": 25000, "extension_of": "small-10m"},
	{"id": "small-30m", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 30, "price_micros": 120000},
	{"id": "small-10m-restricted", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10, "price_micros": 50000, "egress": {"allow": ["huggingface.co"]}}
]}`

var small = provider.Size{
	Name: "small", Resources: provider.Resources{VCPU: 2, MemoryMiB: 2048, DiskGiB: 10},
}

// newLocal runs the built bugnet-run unless opts names another, and destroys its sandboxes last
func newLocal(t *testing.T, opts local.Options) *local.Provider {
	t.Helper()
	if opts.Root == "" {
		opts.Root = t.TempDir()
	}
	if opts.RunnerPath == "" {
		opts.RunnerPath = runnerBin
	}
	opts.Sizes = []provider.Size{small}
	opts.HourlyCostMicros = 1000
	opts.Increment = time.Second
	p := local.New(opts)
	// Each boot starts a gate and a sampler on the host, which only a destroy stops
	t.Cleanup(func() {
		owned, _ := p.ListOwned(context.Background(), nil)
		for _, o := range owned {
			_ = p.Destroy(context.Background(), o.Ref)
		}
	})
	return p
}

func failWith(code provider.Code, failOver bool) func(int, provider.CreateRequest) error {
	return func(int, provider.CreateRequest) error {
		return &provider.Error{Code: code, CanFailOver: failOver, Err: errors.New("hook")}
	}
}

// notReady boots sandboxes that never become ready
type notReady struct{ *local.Provider }

func (notReady) WaitReady(context.Context, provider.Ref) error {
	return &provider.Error{Code: provider.CodeInternal, Err: errors.New("boot failed")}
}

// uploader needs the runner uploaded and reports its sandboxes' architecture in Meta
type uploader struct {
	*local.Provider
	arch string
}

func (u uploader) Capabilities() provider.Capabilities {
	c := u.Provider.Capabilities()
	c.NeedsRunnerUpload, c.Arch = true, u.arch
	return c
}

func (u uploader) Create(ctx context.Context, req provider.CreateRequest) (provider.Ref, error) {
	ref, err := u.Provider.Create(ctx, req)
	if err == nil && u.arch != "" {
		ref.Meta = map[string]string{"arch": u.arch}
	}
	return ref, err
}

// slowDestroy holds every destroy long enough for concurrent stops to overlap
type slowDestroy struct{ *local.Provider }

func (s slowDestroy) Destroy(ctx context.Context, ref provider.Ref) error {
	time.Sleep(200 * time.Millisecond)
	return s.Provider.Destroy(ctx, ref)
}

// leaky never becomes ready and fails its first destroy, leaving a sandbox behind
type leaky struct {
	*local.Provider
	destroys *atomic.Int64
}

func (leaky) WaitReady(context.Context, provider.Ref) error {
	return &provider.Error{Code: provider.CodeInternal, Err: errors.New("boot failed")}
}

func (l leaky) Destroy(ctx context.Context, ref provider.Ref) error {
	if l.destroys.Add(1) == 1 {
		return &provider.Error{Code: provider.CodeInternal, Err: errors.New("destroy failed")}
	}
	return l.Provider.Destroy(ctx, ref)
}

// stuckDestroy never becomes ready and its destroy hangs until the caller gives up
type stuckDestroy struct {
	*local.Provider
	unblock chan struct{}
}

func (stuckDestroy) WaitReady(context.Context, provider.Ref) error {
	return &provider.Error{Code: provider.CodeInternal, Err: errors.New("boot failed")}
}

func (s stuckDestroy) Destroy(ctx context.Context, ref provider.Ref) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.unblock:
		return s.Provider.Destroy(ctx, ref)
	}
}

// gatedReady holds WaitReady until the test opens the gate
type gatedReady struct {
	*local.Provider
	waiting chan struct{}
	gate    chan struct{}
}

func (g gatedReady) WaitReady(ctx context.Context, ref provider.Ref) error {
	close(g.waiting)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.gate:
	}
	return g.Provider.WaitReady(ctx, ref)
}

// lifetimeProbe counts SetLifetime calls and fails them while fail is set
type lifetimeProbe struct {
	*local.Provider
	fail  *atomic.Bool
	calls *atomic.Int64
}

func (l lifetimeProbe) SetLifetime(ctx context.Context, ref provider.Ref, until time.Time) error {
	l.calls.Add(1)
	if l.fail.Load() {
		return &provider.Error{Code: provider.CodeInternal, Err: errors.New("lifetime refused")}
	}
	return l.Provider.SetLifetime(ctx, ref, until)
}

// capped caps its sandboxes' lifetime at limit, unlimited when zero, and can hold one SetLifetime
type capped struct {
	*local.Provider
	limit   time.Duration
	calls   *atomic.Int64
	hold    *atomic.Bool  // the next SetLifetime waits for release while set
	held    chan struct{} // closed once a SetLifetime is held
	release chan struct{}
}

func newCapped(t *testing.T, root string, limit time.Duration) capped {
	t.Helper()
	return capped{
		Provider: newLocal(t, local.Options{Name: "a", Root: root}), limit: limit,
		calls: &atomic.Int64{}, hold: &atomic.Bool{}, held: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (c capped) Capabilities() provider.Capabilities {
	caps := c.Provider.Capabilities()
	caps.MaxLifetime = c.limit
	return caps
}

func (c capped) SetLifetime(ctx context.Context, ref provider.Ref, until time.Time) error {
	c.calls.Add(1)
	if c.hold.CompareAndSwap(true, false) {
		close(c.held)
		<-c.release
	}
	return c.Provider.SetLifetime(ctx, ref, until)
}

// failingDestroy refuses every destroy while fail is set
type failingDestroy struct {
	*local.Provider
	fail atomic.Bool
}

func (f *failingDestroy) Destroy(ctx context.Context, ref provider.Ref) error {
	if f.fail.Load() {
		return &provider.Error{Code: provider.CodeInternal, Err: errors.New("destroy refused")}
	}
	return f.Provider.Destroy(ctx, ref)
}

// overlapping holds each destroy 300 ms and records the most that ran at once, failing when fail
type overlapping struct {
	*local.Provider
	fail bool
	mu   *sync.Mutex
	runs *[2]int // running now and most at once
}

func newOverlapping(t *testing.T, fail bool) overlapping {
	t.Helper()
	return overlapping{
		Provider: newLocal(t, local.Options{Name: "a"}), fail: fail, mu: &sync.Mutex{},
		runs: &[2]int{},
	}
}

func (o overlapping) Destroy(ctx context.Context, ref provider.Ref) error {
	o.mu.Lock()
	o.runs[0]++
	o.runs[1] = max(o.runs[1], o.runs[0])
	o.mu.Unlock()
	defer func() {
		o.mu.Lock()
		o.runs[0]--
		o.mu.Unlock()
	}()
	time.Sleep(300 * time.Millisecond)
	if o.fail {
		return &provider.Error{Code: provider.CodeRateLimited, Err: errors.New("slow down")}
	}
	return o.Provider.Destroy(ctx, ref)
}

func (o overlapping) most() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.runs[1]
}

// clock is wall time shifted by an offset the test can move forward
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

type env struct {
	saga      *saga.Saga
	store     *store.Store
	pool      *pgxpool.Pool
	river     *river.Client[pgx.Tx]
	vk        valkey.Client
	prefix    string
	priv      ed25519.PrivateKey
	handleKey []byte
	keys      dpkeys.Keys
}

func newEnv(t *testing.T, ps ...provider.Provider) *env {
	t.Helper()
	return newEnvWith(t, nil, ps...)
}

// newEnvWith lets a test adjust the saga's dependencies before the saga is built
func newEnvWith(t *testing.T, adjust func(*saga.Deps), ps ...provider.Provider) *env {
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
	rc, err := river.NewClient(riverpgxv5.New(pool), &river.Config{})
	if err != nil {
		t.Fatalf("river client: %v", err)
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
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
	reg, err := provider.NewRegistry(ps, provider.Features{})
	if err != nil {
		t.Fatal(err)
	}
	tracker := placement.NewTracker(time.Now)
	_, keys := testenv.DataPlaneKeys(t)
	st := store.New(pool, store.WithRefSealer(keys),
		store.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	deps := saga.Deps{
		Store:       st,
		Engine:      placement.New(reg, tracker, nil),
		Registry:    reg,
		Catalog:     cat,
		River:       rc,
		Valkey:      vk,
		Prefix:      prefix,
		OrderKey:    pub,
		HandleKey:   hk,
		MarginFloor: 0.5,
		Tracker:     tracker,
		ReplicaID:   "replica-test",
		Log:         discard,
		Events:      edgeevents.New(vk, prefix, discard),
		Budget:      budgetDefaults,
		Keys:        keys,
	}
	if adjust != nil {
		adjust(&deps)
	}
	s := saga.New(deps)
	// Close runs first, so no reservation keeper outlives the test
	t.Cleanup(s.Wait)
	t.Cleanup(s.Close)
	return &env{
		saga: s, store: st, pool: pool, river: rc, vk: vk, prefix: prefix, priv: priv,
		handleKey: hk, keys: deps.Keys,
	}
}

func (e *env) token(t *testing.T, c ordertoken.Claims) string {
	t.Helper()
	if c.ExpiresAt.IsZero() {
		c.ExpiresAt = time.Now().Add(time.Hour)
	}
	tok, err := ordertoken.Sign(e.priv, c)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func (e *env) startToken(t *testing.T, order string) string {
	t.Helper()
	return e.token(t, ordertoken.Claims{
		OrderID: order, Kind: ordertoken.KindStart, PackageID: smallPkg, AmountMicros: smallPrice,
	})
}

func (e *env) extendToken(t *testing.T, order, pkg string, amount int64, h string) string {
	t.Helper()
	sha, err := handle.Hash(h)
	if err != nil {
		t.Fatal(err)
	}
	return e.token(t, ordertoken.Claims{
		OrderID: order, Kind: ordertoken.KindExtend, PackageID: pkg, AmountMicros: amount,
		HandleSHA256: hex.EncodeToString(sha),
	})
}

// place creates a placement and waits for its placing to finish
func (e *env) place(t *testing.T, order string) (store.Placement, string) {
	t.Helper()
	p, h, err := e.saga.Create(t.Context(), saga.CreateInput{
		Token: e.startToken(t, order), Requirements: placement.Requirements{},
	})
	if err != nil {
		t.Fatalf("create %s: %v", order, err)
	}
	e.saga.Wait()
	return e.reload(t, p.ID), h
}

func (e *env) placeReady(t *testing.T, order string) (store.Placement, string) {
	t.Helper()
	p, h := e.place(t, order)
	if p.State != store.PlacementReady {
		t.Fatalf("placement %s is %s (%s), want ready", order, p.State, p.FailureReason)
	}
	return p, h
}

func (e *env) reload(t *testing.T, id uuid.UUID) store.Placement {
	t.Helper()
	p, err := e.store.PlacementByID(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func (e *env) events(t *testing.T, eventType string) []saga.Event {
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

func (e *env) oneEvent(t *testing.T, eventType string) saga.Event {
	t.Helper()
	evs := e.events(t, eventType)
	if len(evs) != 1 {
		t.Fatalf("got %d %s events, want 1", len(evs), eventType)
	}
	return evs[0]
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

type attemptRow struct {
	n                     int
	provider, state, code string
}

func (e *env) attempts(t *testing.T, id uuid.UUID) []attemptRow {
	t.Helper()
	rows, err := e.pool.Query(t.Context(), `
		select attempt, coalesce(provider, ''), coalesce(state, ''), coalesce(error_code, '')
		from placement_attempts where placement_id = $1 order by attempt`, id)
	if err != nil {
		t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (attemptRow, error) {
		var a attemptRow
		return a, r.Scan(&a.n, &a.provider, &a.state, &a.code)
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func (e *env) placementCount(t *testing.T) int {
	t.Helper()
	var n int
	if err := e.pool.QueryRow(t.Context(), `select count(*) from placements`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func (e *env) deadlineJobs(t *testing.T) []jobargs.Deadline {
	t.Helper()
	res, err := e.river.JobList(t.Context(), river.NewJobListParams().Kinds("deadline"))
	if err != nil {
		t.Fatal(err)
	}
	var out []jobargs.Deadline
	for _, j := range res.Jobs {
		var args jobargs.Deadline
		if err := json.Unmarshal(j.EncodedArgs, &args); err != nil {
			t.Fatal(err)
		}
		out = append(out, args)
	}
	return out
}

// subscribe receives every placement change published under the test's prefix
func (e *env) subscribe(t *testing.T) <-chan valkey.PubSubMessage {
	t.Helper()
	c, cancel := e.vk.Dedicate()
	t.Cleanup(cancel)
	msgs := make(chan valkey.PubSubMessage, 16)
	subscribed := make(chan struct{})
	var once sync.Once
	_ = c.SetPubSubHooks(valkey.PubSubHooks{
		OnMessage: func(m valkey.PubSubMessage) { msgs <- m },
		OnSubscription: func(s valkey.PubSubSubscription) {
			if s.Kind == "psubscribe" {
				once.Do(func() { close(subscribed) })
			}
		},
	})
	pattern := e.prefix + "placement:*"
	if err := c.Do(t.Context(), c.B().Psubscribe().Pattern(pattern).Build()).Error(); err != nil {
		t.Fatalf("psubscribe: %v", err)
	}
	select {
	case <-subscribed:
	case <-time.After(5 * time.Second):
		t.Fatal("psubscribe was not confirmed")
	}
	return msgs
}

func receive(t *testing.T, msgs <-chan valkey.PubSubMessage) (string, saga.Event) {
	t.Helper()
	select {
	case m := <-msgs:
		var ev saga.Event
		if err := json.Unmarshal([]byte(m.Message), &ev); err != nil {
			t.Fatalf("decode %q: %v", m.Message, err)
		}
		return m.Channel, ev
	case <-time.After(5 * time.Second):
		t.Fatal("no placement message published")
		return "", saga.Event{}
	}
}

func owned(t *testing.T, p provider.Provider) []provider.Summary {
	t.Helper()
	s, err := p.ListOwned(context.Background(), map[string]string{"bugnet": "1"})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func lifetime(t *testing.T, root string, ref *provider.Ref) time.Time {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, ref.ID, "meta.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m struct {
		Deadline time.Time `json:"deadline"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	return m.Deadline
}

func near(a, b time.Time) bool {
	d := a.Sub(b)
	return d > -time.Second && d < time.Second
}

/* Create */

func TestCreatePlacementReady(t *testing.T) {
	root := t.TempDir()
	a := newLocal(t, local.Options{Name: "a", Root: root})
	e := newEnv(t, a)
	msgs := e.subscribe(t)

	p, h, err := e.saga.Create(t.Context(), saga.CreateInput{
		Token: e.startToken(t, "order-1"), Requirements: placement.Requirements{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if want, _ := handle.Derive(e.handleKey, "order-1"); h != want {
		t.Fatalf("handle %q, want %q", h, want)
	}
	if p.State != store.PlacementPlacing || p.PackageID != smallPkg || p.OrderID != "order-1" {
		t.Fatalf("created %+v", p)
	}
	e.saga.Wait()

	got := e.reload(t, p.ID)
	if got.State != store.PlacementReady || got.Provider != "a" || got.ProviderRef == nil ||
		got.CapacityProvider != "a" || e.inUse(t, "a") != 1 {
		t.Fatalf("placement %+v with %d in use on a, want ready holding one slot on a", got,
			e.inUse(t, "a"))
	}
	if got.ReadyAt == nil || got.Deadline == nil ||
		!near(*got.Deadline, got.ReadyAt.Add(10*time.Minute)) {
		t.Fatalf("ready at %v, deadline %v", got.ReadyAt, got.Deadline)
	}
	if lt := lifetime(t, root, got.ProviderRef); !near(lt, got.Deadline.Add(5*time.Minute)) {
		t.Fatalf("provider lifetime %v, want deadline + 5m", lt)
	}

	pending, err := e.store.PendingOutbox(t.Context(), time.Now(), 10)
	if err != nil || len(pending) != 1 || pending[0].EventType != saga.EventReady {
		t.Fatalf("outbox %+v %v", pending, err)
	}
	var fields map[string]any
	if err := json.Unmarshal(pending[0].Payload, &fields); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"placement_id", "order_id", "state", "provider", "deadline",
		"billed_seconds", "refundable"} {
		if _, ok := fields[k]; !ok {
			t.Errorf("event payload %v lacks %q", fields, k)
		}
	}
	ev := e.oneEvent(t, saga.EventReady)
	if ev.PlacementID != p.ID || ev.OrderID != "order-1" || ev.State != store.PlacementReady ||
		ev.Provider != "a" || ev.Deadline == nil || !ev.Deadline.Equal(*got.Deadline) ||
		ev.BilledSeconds != 0 || ev.Refundable {
		t.Fatalf("ready event %+v", ev)
	}

	jobs := e.deadlineJobs(t)
	if len(jobs) != 1 || jobs[0].PlacementID != p.ID {
		t.Fatalf("deadline jobs %+v", jobs)
	}
	res, err := e.river.JobList(t.Context(), river.NewJobListParams().Kinds("deadline"))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Jobs) != 1 || !near(res.Jobs[0].ScheduledAt, *got.Deadline) {
		t.Fatalf("deadline jobs %+v, want one scheduled at %v", res.Jobs, got.Deadline)
	}

	channel, pub := receive(t, msgs)
	if channel != saga.Channel(e.prefix, p.ID) || pub.State != store.PlacementReady {
		t.Fatalf("published %+v on %s", pub, channel)
	}
	if rows := e.attempts(t, p.ID); len(rows) != 1 || rows[0].state != "ready" {
		t.Fatalf("attempts %+v", rows)
	}
}

func TestSandboxesCarryDeploymentLabel(t *testing.T) {
	for _, tc := range []struct{ set, want string }{{"", "main"}, {"blue", "blue"}} {
		a := newLocal(t, local.Options{Name: "a"})
		e := newEnvWith(t, func(d *saga.Deps) { d.DeploymentID = tc.set }, a)
		p, _ := e.placeReady(t, "order-"+tc.want)
		want := map[string]string{"bugnet": "1", "bugnet_deployment": tc.want}
		if got := e.saga.OwnedLabels(); !maps.Equal(got, want) {
			t.Fatalf("owned labels %v, want %v", got, want)
		}
		want["placement"] = p.ID.String()
		got, err := a.ListOwned(t.Context(), want)
		if err != nil || len(got) != 1 || got[0].Ref.ID != p.ProviderRef.ID {
			t.Fatalf("sandboxes labelled %v: %+v %v, want the placement's", want, got, err)
		}
	}
}

func TestCreatePlacementSameOrderReturnsSamePlacement(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a"})
	e := newEnv(t, a)
	tok := e.startToken(t, "order-1")

	type result struct {
		p   store.Placement
		h   string
		err error
	}
	results := make(chan result, 2)
	for range 2 {
		go func() {
			p, h, err := e.saga.Create(t.Context(), saga.CreateInput{
				Token: tok, Requirements: placement.Requirements{},
			})
			results <- result{p, h, err}
		}()
	}
	first, second := <-results, <-results
	if first.err != nil || second.err != nil {
		t.Fatalf("errors %v, %v", first.err, second.err)
	}
	if first.p.ID != second.p.ID || first.h != second.h {
		t.Fatalf("got %s/%s and %s/%s", first.p.ID, first.h, second.p.ID, second.h)
	}
	e.saga.Wait()
	again, h, err := e.saga.Create(t.Context(), saga.CreateInput{
		Token: tok, Requirements: placement.Requirements{},
	})
	if err != nil || again.ID != first.p.ID || h != first.h {
		t.Fatalf("third create %s %q %v", again.ID, h, err)
	}
	e.saga.Wait()
	if got := owned(t, a); len(got) != 1 {
		t.Fatalf("%d sandboxes, want 1", len(got))
	}
	if evs := e.events(t, saga.EventReady); len(evs) != 1 {
		t.Fatalf("%d ready events", len(evs))
	}
	if n := e.inUse(t, "a"); n != 1 {
		t.Fatalf("in use %d, want 1", n)
	}
}

func TestCreateRejectsExpiredAndForgedTokens(t *testing.T) {
	e := newEnv(t, newLocal(t, local.Options{Name: "a"}))
	_, other, _ := ed25519.GenerateKey(rand.Reader)
	forged, err := ordertoken.Sign(other, ordertoken.Claims{
		OrderID: "o", Kind: ordertoken.KindStart, PackageID: smallPkg, AmountMicros: smallPrice,
		ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name  string
		token string
		want  []error
	}{
		{"expired", e.token(t, ordertoken.Claims{
			OrderID: "o", Kind: ordertoken.KindStart, PackageID: smallPkg,
			AmountMicros: smallPrice, ExpiresAt: time.Now().Add(-time.Minute),
		}), []error{saga.ErrBadToken, ordertoken.ErrExpired}},
		{"forged", forged, []error{saga.ErrBadToken, ordertoken.ErrBadSignature}},
		{"garbage", "v1.nope", []error{saga.ErrBadToken}},
		{"extend kind", e.token(t, ordertoken.Claims{
			OrderID: "o", Kind: ordertoken.KindExtend, PackageID: smallPkg,
			AmountMicros: smallPrice, HandleSHA256: "00",
		}), []error{saga.ErrBadToken}},
		{"unknown package", e.token(t, ordertoken.Claims{
			OrderID: "o", Kind: ordertoken.KindStart, PackageID: "nope", AmountMicros: smallPrice,
		}), []error{saga.ErrUnknownPackage}},
	}
	for _, c := range cases {
		_, h, err := e.saga.Create(t.Context(), saga.CreateInput{
			Token: c.token, Requirements: placement.Requirements{},
		})
		for _, want := range c.want {
			if !errors.Is(err, want) {
				t.Errorf("%s: err %v, want %v", c.name, err, want)
			}
		}
		if h != "" {
			t.Errorf("%s: handle %q returned with an error", c.name, h)
		}
	}
	if n := e.placementCount(t); n != 0 {
		t.Fatalf("%d placements recorded", n)
	}
}

func TestCreateRejectsAmountBelowPrice(t *testing.T) {
	e := newEnv(t, newLocal(t, local.Options{Name: "a"}))
	tok := e.token(t, ordertoken.Claims{
		OrderID: "o", Kind: ordertoken.KindStart, PackageID: smallPkg,
		AmountMicros: smallPrice - 1,
	})
	if _, _, err := e.saga.Create(t.Context(), saga.CreateInput{
		Token: tok, Requirements: placement.Requirements{},
	}); !errors.Is(
		err, saga.ErrUnderpaid) {
		t.Fatalf("err %v, want ErrUnderpaid", err)
	}
	if n := e.placementCount(t); n != 0 {
		t.Fatalf("%d placements recorded", n)
	}
}

func TestCreateWithoutCandidateFailsRefundable(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a"})
	e := newEnv(t, a)
	msgs := e.subscribe(t)
	tok := e.startToken(t, "order-1")
	wantH, _ := handle.Derive(e.handleKey, "order-1")

	p, h, err := e.saga.Create(t.Context(), saga.CreateInput{
		Token: tok, Requirements: placement.Requirements{GPU: true},
	})
	if !errors.Is(err, placement.ErrNoCandidate) {
		t.Fatalf("err %v, want ErrNoCandidate", err)
	}
	if h != wantH || p.State != store.PlacementFailed || !p.Refundable ||
		p.FailureReason != saga.ReasonNoCandidate {
		t.Fatalf("placement %+v, handle %q", p, h)
	}
	ev := e.oneEvent(t, saga.EventFailed)
	if ev.PlacementID != p.ID || ev.OrderID != "order-1" || ev.State != store.PlacementFailed ||
		!ev.Refundable || ev.Reason != saga.ReasonNoCandidate || ev.BilledSeconds != 0 {
		t.Fatalf("failed event %+v", ev)
	}
	if channel, pub := receive(t, msgs); channel != saga.Channel(e.prefix, p.ID) ||
		pub.State != store.PlacementFailed {
		t.Fatalf("published %+v on %s", pub, channel)
	}

	// A repeat finds the failed placement even once a candidate exists
	again, h2, err := e.saga.Create(t.Context(), saga.CreateInput{
		Token: tok, Requirements: placement.Requirements{},
	})
	if !errors.Is(err, placement.ErrNoCandidate) || again.ID != p.ID || h2 != wantH ||
		again.State != store.PlacementFailed {
		t.Fatalf("repeat %+v %q %v", again, h2, err)
	}
	e.saga.Wait()
	if n := e.placementCount(t); n != 1 {
		t.Fatalf("%d placements", n)
	}
	if evs := e.events(t, saga.EventFailed); len(evs) != 1 {
		t.Fatalf("%d failed events", len(evs))
	}
	if rows := e.attempts(t, p.ID); len(rows) != 0 {
		t.Fatalf("attempts %+v", rows)
	}
	if n := len(owned(t, a)); n != 0 {
		t.Fatalf("%d sandboxes", n)
	}
}

func TestCreateStoresNeedsSecrets(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a"})
	e := newEnvWith(t, func(d *saga.Deps) {
		reg, err := provider.NewRegistry([]provider.Provider{a},
			provider.Features{Secrets: []string{"a"}})
		if err != nil {
			t.Fatal(err)
		}
		d.Registry, d.Engine = reg, placement.New(reg, d.Tracker, nil)
	}, a)
	for _, c := range []struct {
		order string
		req   placement.Requirements
		want  bool
		state string
	}{
		{"order-secrets", placement.Requirements{Secrets: true}, true, store.PlacementReady},
		{"order-none", placement.Requirements{}, false, store.PlacementReady},
		{"order-secrets-gpu", placement.Requirements{Secrets: true, GPU: true}, true,
			store.PlacementFailed},
	} {
		p, _, err := e.saga.Create(t.Context(), saga.CreateInput{
			Token: e.startToken(t, c.order), Requirements: c.req,
		})
		if err != nil && !errors.Is(err, placement.ErrNoCandidate) {
			t.Fatalf("%s: %v", c.order, err)
		}
		e.saga.Wait()
		got := e.reload(t, p.ID)
		if p.NeedsSecrets != c.want || got.NeedsSecrets != c.want || got.State != c.state {
			t.Errorf("%s: needs secrets %v, stored %v in %s, want %v in %s", c.order,
				p.NeedsSecrets, got.NeedsSecrets, got.State, c.want, c.state)
		}
	}
}

func TestPlacingHeartbeats(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a", ReadyDelay: time.Second})
	e := newEnvWith(t, func(d *saga.Deps) { d.HeartbeatInterval = 50 * time.Millisecond }, a)
	p, _, err := e.saga.Create(t.Context(), saga.CreateInput{
		Token: e.startToken(t, "order-1"), Requirements: placement.Requirements{},
	})
	if err != nil {
		t.Fatal(err)
	}
	beats := map[time.Time]bool{}
	for len(beats) < 3 {
		got := e.reload(t, p.ID)
		if got.State != store.PlacementPlacing {
			t.Fatalf("placement %s after %d heartbeats, want 3 while placing", got.State,
				len(beats))
		}
		if got.SagaHeartbeat != nil {
			if got.SagaOwner != "replica-test" {
				t.Fatalf("saga owner %q", got.SagaOwner)
			}
			beats[*got.SagaHeartbeat] = true
		}
		time.Sleep(20 * time.Millisecond)
	}
	e.saga.Wait()
	if got := e.reload(t, p.ID); got.State != store.PlacementReady {
		t.Fatalf("state %s (%s)", got.State, got.FailureReason)
	}
}

func TestPublishFailureDoesNotFailSaga(t *testing.T) {
	addr := os.Getenv("BUGNET_TEST_VALKEY_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	broken, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}})
	if err != nil {
		t.Fatal(err)
	}
	broken.Close()
	logs := &syncBuffer{}
	a := newLocal(t, local.Options{Name: "a"})
	e := newEnvWith(t, func(d *saga.Deps) {
		d.Valkey = broken
		d.Log = slog.New(slog.NewTextHandler(logs, nil))
	}, a)

	p, h := e.placeReady(t, "order-1")
	got, err := e.saga.Stop(t.Context(), h)
	if err != nil || got.State != store.PlacementTerminated {
		t.Fatalf("stop %+v, %v", got, err)
	}
	e.oneEvent(t, saga.EventReady)
	e.oneEvent(t, saga.EventTerminated)
	if n := strings.Count(logs.String(), "publish placement event"); n != 2 {
		t.Fatalf("%d publish failures logged for %s, want 2\n%s", n, p.ID, logs)
	}
}

/* Placing */

func TestSagaFailsOverOnCapacityError(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a", CreateHook: failWith(provider.CodeCapacity, true)})
	b := newLocal(t, local.Options{Name: "b"})
	e := newEnv(t, a, b)
	p, _ := e.placeReady(t, "order-1")
	if p.Provider != "b" {
		t.Fatalf("placed on %q, want b", p.Provider)
	}
	if na, nb := len(owned(t, a)), len(owned(t, b)); na != 0 || nb != 1 {
		t.Fatalf("sandboxes a=%d b=%d", na, nb)
	}
	if ia, ib := e.inUse(t, "a"), e.inUse(t, "b"); ia != 0 || ib != 1 {
		t.Fatalf("in use a=%d b=%d", ia, ib)
	}
	want := []attemptRow{{1, "a", "failed", "capacity"}, {2, "b", "ready", ""}}
	if got := e.attempts(t, p.ID); len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("attempts %+v, want %+v", got, want)
	}
}

func TestSagaUnknownOutcomeRetriesSameProvider(t *testing.T) {
	var a *local.Provider
	var calls atomic.Int64
	var inner atomic.Bool
	// The first create reaches the provider but its answer is lost
	a = newLocal(t, local.Options{Name: "a", CreateHook: func(
		_ int, req provider.CreateRequest,
	) error {
		if inner.Load() || calls.Add(1) > 1 {
			return nil
		}
		inner.Store(true)
		defer inner.Store(false)
		if _, err := a.Create(context.Background(), req); err != nil {
			return err
		}
		return &provider.Error{Code: provider.CodeOutcomeUnknown, Err: errors.New("timeout")}
	}})
	b := newLocal(t, local.Options{Name: "b"})
	e := newEnv(t, a, b)
	p, _ := e.placeReady(t, "order-1")
	if p.Provider != "a" {
		t.Fatalf("placed on %q, want a", p.Provider)
	}
	if na, nb := len(owned(t, a)), len(owned(t, b)); na != 1 || nb != 0 {
		t.Fatalf("sandboxes a=%d b=%d", na, nb)
	}
	if calls.Load() != 2 {
		t.Fatalf("%d creates on a, want 2", calls.Load())
	}
	if got := e.attempts(t, p.ID); len(got) != 1 || got[0] != (attemptRow{1, "a", "ready", ""}) {
		t.Fatalf("attempts %+v", got)
	}
}

func TestSagaUnknownOutcomeNeverFailsOver(t *testing.T) {
	var a *local.Provider
	var calls atomic.Int64
	var inner atomic.Bool
	a = newLocal(t, local.Options{Name: "a", CreateHook: func(
		_ int, req provider.CreateRequest,
	) error {
		if inner.Load() {
			return nil
		}
		calls.Add(1)
		inner.Store(true)
		defer inner.Store(false)
		if _, err := a.Create(context.Background(), req); err != nil {
			return err
		}
		return &provider.Error{Code: provider.CodeOutcomeUnknown, CanFailOver: true}
	}})
	b := newLocal(t, local.Options{Name: "b"})
	e := newEnv(t, a, b)
	p, _ := e.place(t, "order-1")
	if p.State != store.PlacementFailed || !p.Refundable {
		t.Fatalf("placement %+v", p)
	}
	if calls.Load() != 4 {
		t.Fatalf("%d creates on a, want 4", calls.Load())
	}
	if na, nb := len(owned(t, a)), len(owned(t, b)); na != 0 || nb != 0 {
		t.Fatalf("sandboxes a=%d b=%d, want none", na, nb)
	}
	if ia := e.inUse(t, "a"); ia != 0 {
		t.Fatalf("in use a=%d", ia)
	}
}

func TestSagaAllFailRefundable(t *testing.T) {
	a := notReady{newLocal(t, local.Options{Name: "a"})}
	b := newLocal(t, local.Options{Name: "b", CreateHook: failWith(provider.CodeCapacity, true)})
	e := newEnv(t, a, b)
	msgs := e.subscribe(t)
	p, _ := e.place(t, "order-1")
	// The reason is a stable code that names no provider, the attempts keep the details
	if p.State != store.PlacementFailed || !p.Refundable ||
		p.FailureReason != "provider_capacity" {
		t.Fatalf("placement %+v, want failed refundable for provider_capacity", p)
	}
	ev := e.oneEvent(t, saga.EventFailed)
	if ev.PlacementID != p.ID || ev.State != store.PlacementFailed || !ev.Refundable ||
		ev.BilledSeconds != 0 || ev.Reason != p.FailureReason {
		t.Fatalf("failed event %+v", ev)
	}
	if _, pub := receive(t, msgs); pub.State != store.PlacementFailed {
		t.Fatalf("published %+v", pub)
	}
	if na, nb := len(owned(t, a)), len(owned(t, b)); na != 0 || nb != 0 {
		t.Fatalf("sandboxes a=%d b=%d, want none", na, nb)
	}
	if ia, ib := e.inUse(t, "a"), e.inUse(t, "b"); ia != 0 || ib != 0 {
		t.Fatalf("in use a=%d b=%d", ia, ib)
	}
	got := e.attempts(t, p.ID)
	if len(got) != 2 || got[0].provider != "a" || got[0].state != "failed" ||
		got[1] != (attemptRow{2, "b", "failed", "capacity"}) {
		t.Fatalf("attempts %+v", got)
	}
	if jobs := e.deadlineJobs(t); len(jobs) != 0 {
		t.Fatalf("deadline jobs %+v", jobs)
	}
}

func TestSagaStopsAfterThreeAttempts(t *testing.T) {
	var calls [4]atomic.Int64
	var ps []provider.Provider
	for i := range calls {
		ps = append(ps, newLocal(t, local.Options{
			Name: string(rune('a' + i)),
			CreateHook: func(int, provider.CreateRequest) error {
				calls[i].Add(1)
				return &provider.Error{Code: provider.CodeCapacity, CanFailOver: true}
			},
		}))
	}
	e := newEnv(t, ps...)
	p, _ := e.place(t, "order-1")
	if p.State != store.PlacementFailed {
		t.Fatalf("state %s", p.State)
	}
	for i, want := range []int64{1, 1, 1, 0} {
		if got := calls[i].Load(); got != want {
			t.Errorf("provider %d created %d times, want %d", i, got, want)
		}
	}
}

func TestSagaSkipsFullProvider(t *testing.T) {
	var aCalls atomic.Int64
	a := newLocal(t, local.Options{Name: "a", CreateHook: func(int, provider.CreateRequest) error {
		aCalls.Add(1)
		return nil
	}})
	b := newLocal(t, local.Options{Name: "b"})
	e := newEnv(t, a, b)
	if err := e.store.SetCapacity(t.Context(), "a", 0); err != nil {
		t.Fatal(err)
	}
	p, _ := e.placeReady(t, "order-1")
	if p.Provider != "b" || aCalls.Load() != 0 {
		t.Fatalf("placed on %q after %d creates on a", p.Provider, aCalls.Load())
	}
	if got := e.attempts(t, p.ID); len(got) != 1 || got[0] != (attemptRow{1, "b", "ready", ""}) {
		t.Fatalf("attempts %+v", got)
	}
}

func TestSagaAllFullFailsWithCapacityReason(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a"})
	b := newLocal(t, local.Options{Name: "b"})
	e := newEnv(t, a, b)
	for _, name := range []string{"a", "b"} {
		if err := e.store.SetCapacity(t.Context(), name, 0); err != nil {
			t.Fatal(err)
		}
	}
	p, _ := e.place(t, "order-1")
	if p.State != store.PlacementFailed || !p.Refundable ||
		!strings.Contains(p.FailureReason, "capacity") {
		t.Fatalf("placement %+v, want a capacity failure", p)
	}
	if ev := e.oneEvent(t, saga.EventFailed); ev.Reason != p.FailureReason {
		t.Fatalf("failed event %+v", ev)
	}
	if rows := e.attempts(t, p.ID); len(rows) != 0 {
		t.Fatalf("attempts %+v", rows)
	}
}

// keyed counts ListOwned calls and says whether its creates are keyed to one sandbox
type keyed struct {
	*local.Provider
	once  bool
	lists *atomic.Int64
}

func (k keyed) Capabilities() provider.Capabilities {
	c := k.Provider.Capabilities()
	c.OneSandboxPerKey = k.once
	return c
}

func (k keyed) ListOwned(ctx context.Context, l map[string]string) ([]provider.Summary, error) {
	k.lists.Add(1)
	return k.Provider.ListOwned(ctx, l)
}

func TestReadyOnKeyedProviderSkipsTheSweep(t *testing.T) {
	for _, once := range []bool{true, false} {
		var lists atomic.Int64
		e := newEnv(t, keyed{newLocal(t, local.Options{Name: "a"}), once, &lists})
		e.placeReady(t, "order-1")
		if want := map[bool]int64{true: 0, false: 1}[once]; lists.Load() != want {
			t.Errorf("one sandbox per key %v listed %d times after ready, want %d", once,
				lists.Load(), want)
		}
	}
}

func TestSagaDestroysLeftoverAfterFailOver(t *testing.T) {
	var destroys atomic.Int64
	a := leaky{newLocal(t, local.Options{Name: "a"}), &destroys}
	b := newLocal(t, local.Options{Name: "b"})
	e := newEnv(t, a, b)
	p, _ := e.placeReady(t, "order-1")
	if p.Provider != "b" {
		t.Fatalf("placed on %q, want b", p.Provider)
	}
	if na, nb := len(owned(t, a)), len(owned(t, b)); na != 0 || nb != 1 {
		t.Fatalf("sandboxes a=%d b=%d after %d destroys on a", na, nb, destroys.Load())
	}
	if ia, ib := e.inUse(t, "a"), e.inUse(t, "b"); ia != 0 || ib != 1 {
		t.Fatalf("in use a=%d b=%d", ia, ib)
	}
}

func TestFailStuckFailsOnlyPlacing(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a"})
	b := newLocal(t, local.Options{Name: "b"})
	e := newEnv(t, a, b)
	ctx := t.Context()
	ready, _ := e.placeReady(t, "order-1")
	stuck, _, err := e.store.CreatePlacement(ctx, store.NewPlacement{
		HandleSHA256: []byte("stuck-handle"), OrderID: "order-2", PackageID: smallPkg,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Its saga failed one attempt on a and went away while creating on b
	if ok, err := e.store.ReservePlacementCapacity(ctx, stuck.ID, "b", 1); err != nil || !ok {
		t.Fatalf("reserve: %v %v", ok, err)
	}
	for _, rec := range []store.Attempt{
		{PlacementID: stuck.ID, Attempt: 1, Provider: "a", IdempotencyKey: "k1", State: "failed",
			ErrorCode: "capacity", Error: "full"},
		{PlacementID: stuck.ID, Attempt: 2, Provider: "b", IdempotencyKey: "k2", State: "creating"},
	} {
		if err := e.store.RecordAttempt(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range []provider.Provider{a, b} {
		_, err := p.Create(ctx, provider.CreateRequest{
			IdempotencyKey: "left-" + p.Name(), Size: small, Lifetime: time.Hour,
			Labels: map[string]string{
				"bugnet": "1", "bugnet_deployment": "main", "placement": stuck.ID.String(),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	if err := e.saga.FailStuck(ctx, stuck.ID, saga.ReasonPlacingStalled); err != nil {
		t.Fatalf("fail stuck: %v", err)
	}
	// The failure and its slot commit at once, the sandboxes go in the background
	if n := e.inUse(t, "b"); n != 0 {
		t.Fatalf("in use on b %d right after FailStuck, want the slot released", n)
	}
	e.saga.Wait()
	got := e.reload(t, stuck.ID)
	if got.State != store.PlacementFailed || !got.Refundable ||
		got.FailureReason != saga.ReasonPlacingStalled {
		t.Fatalf("stuck placement %+v, want failed refundable placing_stalled", got)
	}
	ev := e.oneEvent(t, saga.EventFailed)
	if ev.PlacementID != stuck.ID || ev.Reason != saga.ReasonPlacingStalled || !ev.Refundable {
		t.Fatalf("failed event %+v", ev)
	}
	if na, nb := len(owned(t, a)), len(owned(t, b)); na+nb != 1 {
		t.Fatalf("sandboxes a=%d b=%d, want only the ready placement's", na, nb)
	}
	if n := e.inUse(t, "b"); n != 0 {
		t.Fatalf("in use on b %d, want the stuck attempt's slot released", n)
	}
	want := []attemptRow{{1, "a", "failed", "capacity"}, {2, "b", "failed", ""}}
	if got := e.attempts(t, stuck.ID); !slices.Equal(got, want) {
		t.Fatalf("attempts %+v, want %+v", got, want)
	}

	if err := e.saga.FailStuck(ctx, stuck.ID, saga.ReasonPlacingStalled); err != nil {
		t.Fatalf("fail stuck again: %v", err)
	}
	if err := e.saga.FailStuck(ctx, ready.ID, saga.ReasonPlacingStalled); err != nil {
		t.Fatalf("fail stuck on a ready placement: %v", err)
	}
	if got := e.reload(t, ready.ID); got.State != store.PlacementReady {
		t.Fatalf("ready placement is %s after FailStuck, want ready", got.State)
	}
	if n := len(e.events(t, saga.EventFailed)); n != 1 || e.inUse(t, "a")+e.inUse(t, "b") != 1 {
		t.Fatalf("%d failed events and %d slots in use after repeats, want 1 and 1", n,
			e.inUse(t, "a")+e.inUse(t, "b"))
	}
}

func TestFailStuckRacingPlacingReleasesOneSlot(t *testing.T) {
	a := gatedReady{newLocal(t, local.Options{Name: "a"}), make(chan struct{}),
		make(chan struct{})}
	e := newEnv(t, a)
	ctx := t.Context()
	// Another placement holds a slot on a, so releasing one slot twice would show
	other, _, err := e.store.CreatePlacement(ctx, store.NewPlacement{
		HandleSHA256: []byte("other-handle"), OrderID: "order-other", PackageID: smallPkg,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := e.store.ReservePlacementCapacity(ctx, other.ID, "a", 1); err != nil || !ok {
		t.Fatalf("reserve: %v %v", ok, err)
	}
	p, _, err := e.saga.Create(ctx, saga.CreateInput{
		Token: e.startToken(t, "order-1"), Requirements: placement.Requirements{},
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-a.waiting:
	case <-time.After(10 * time.Second):
		t.Fatal("placing never waited for ready")
	}
	if n := e.inUse(t, "a"); n != 2 {
		t.Fatalf("in use %d while placing, want 2", n)
	}
	if err := e.saga.FailStuck(ctx, p.ID, saga.ReasonPlacingStalled); err != nil {
		t.Fatalf("fail stuck: %v", err)
	}
	close(a.gate)
	e.saga.Wait()

	got := e.reload(t, p.ID)
	if got.State != store.PlacementFailed || got.FailureReason != saga.ReasonPlacingStalled ||
		got.CapacityProvider != "" {
		t.Fatalf("placement %+v, want failed placing_stalled holding no slot", got)
	}
	if n := e.inUse(t, "a"); n != 1 {
		t.Fatalf("in use %d after FailStuck and the saga's own cleanup, want 1", n)
	}
	if n := len(e.events(t, saga.EventFailed)); n != 1 {
		t.Fatalf("%d failed events, want 1", n)
	}
	if n := len(owned(t, a)); n != 0 {
		t.Fatalf("%d sandboxes left", n)
	}
}

func TestStuckDestroyDoesNotHangPlacing(t *testing.T) {
	saga.SetCleanupTimeout(t, 200*time.Millisecond)
	a := stuckDestroy{newLocal(t, local.Options{Name: "a"}), make(chan struct{})}
	e := newEnv(t, a)
	t.Cleanup(func() { close(a.unblock) })
	p, _, err := e.saga.Create(t.Context(), saga.CreateInput{
		Token: e.startToken(t, "order-1"), Requirements: placement.Requirements{},
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		e.saga.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("placing hangs on a destroy that never returns")
	}
	if got := e.reload(t, p.ID); got.State != store.PlacementFailed || !got.Refundable {
		t.Fatalf("placement %+v", got)
	}
	if n := e.inUse(t, "a"); n != 0 {
		t.Fatalf("in use %d", n)
	}
}

func TestReadyAfterPlacementLeftPlacing(t *testing.T) {
	a := gatedReady{newLocal(t, local.Options{Name: "a"}), make(chan struct{}),
		make(chan struct{})}
	e := newEnv(t, a)
	msgs := e.subscribe(t)
	p, _, err := e.saga.Create(t.Context(), saga.CreateInput{
		Token: e.startToken(t, "order-1"), Requirements: placement.Requirements{},
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-a.waiting:
	case <-time.After(10 * time.Second):
		t.Fatal("placing never waited for ready")
	}
	if err := e.store.MarkPlacementFailed(t.Context(), p.ID, "stuck", true); err != nil {
		t.Fatal(err)
	}
	close(a.gate)
	e.saga.Wait()

	if got := e.reload(t, p.ID); got.State != store.PlacementFailed ||
		got.FailureReason != "stuck" {
		t.Fatalf("placement %+v", got)
	}
	if n := len(owned(t, a)); n != 0 {
		t.Fatalf("%d sandboxes left", n)
	}
	if n := e.inUse(t, "a"); n != 0 {
		t.Fatalf("in use %d", n)
	}
	if evs := e.events(t, saga.EventReady); len(evs) != 0 {
		t.Fatalf("ready events %+v", evs)
	}
	if jobs := e.deadlineJobs(t); len(jobs) != 0 {
		t.Fatalf("deadline jobs %+v", jobs)
	}
	if got := e.attempts(t, p.ID); len(got) != 1 || got[0].state != "failed" {
		t.Fatalf("attempts %+v", got)
	}
	select {
	case m := <-msgs:
		t.Fatalf("published %s on %s", m.Message, m.Channel)
	case <-time.After(300 * time.Millisecond):
	}
}

/* Runner upload */

func writeFile(t *testing.T, dir, name, content string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestCreateUploadsRunnerForArch(t *testing.T) {
	dir := t.TempDir()
	log := filepath.Join(dir, "runner.log")
	fake := writeFile(t, dir, "fake-runner", "#!/bin/sh\necho \"$@\" >> "+log+"\n"+
		"[ \"$1\" = setup ] && echo '{\"gate\":\"listening\",\"ca\":\"skipped\"}'\nexit 0\n",
		0o755)
	runners := map[string][]byte{"amd64": []byte("amd64 build"), "arm64": []byte("arm64 build")}
	root := t.TempDir()
	u := uploader{newLocal(t, local.Options{Name: "u", Root: root, RunnerPath: fake}), "arm64"}
	e := newEnvWith(t, func(d *saga.Deps) { d.Runners = runners }, u)
	p, _ := e.placeReady(t, "order-1")

	bin := filepath.Join(root, p.ProviderRef.ID, "fs", "tmp", "bugnet", "bin", "bugnet-run")
	data, err := os.ReadFile(bin)
	if err != nil || string(data) != "arm64 build" {
		t.Fatalf("uploaded runner %q, %v", data, err)
	}
	if info, err := os.Stat(bin); err != nil || info.Mode().Perm() != 0o755 {
		t.Fatalf("runner mode %v, %v", info.Mode(), err)
	}
	ran, err := os.ReadFile(log)
	if err != nil || strings.Split(string(ran), "\n")[0] != "ping" {
		t.Fatalf("runner ran with %q, %v, want ping first", ran, err)
	}
}

func TestCreateSkipsProviderWithoutRunnerBuild(t *testing.T) {
	var creates atomic.Int64
	runners := map[string][]byte{"arm64": []byte("arm64 build")}
	u := uploader{Provider: newLocal(t, local.Options{Name: "u",
		CreateHook: func(int, provider.CreateRequest) error {
			creates.Add(1)
			return nil
		}})}
	b := newLocal(t, local.Options{Name: "b"})
	e := newEnvWith(t, func(d *saga.Deps) { d.Runners = runners }, u, b)
	p, _ := e.placeReady(t, "order-1")
	if p.Provider != "b" || creates.Load() != 0 {
		t.Fatalf("placed on %q after %d creates on u, want b and none on u", p.Provider,
			creates.Load())
	}

	only := newEnvWith(t, func(d *saga.Deps) { d.Runners = runners },
		uploader{Provider: newLocal(t, local.Options{Name: "u"})})
	failed, _ := only.place(t, "order-2")
	if failed.State != store.PlacementFailed || !failed.Refundable ||
		failed.FailureReason != "runner_unavailable" {
		t.Fatalf("placement %+v, want failed refundable for runner_unavailable", failed)
	}
}

func TestBootWithoutBuildForTheSandboxArchFailsWithoutFailOver(t *testing.T) {
	runners := map[string][]byte{"amd64": []byte("amd64 build")}
	// The provider promises amd64 but its sandbox turns out arm64, which has no build
	u := uploader{Provider: newLocal(t, local.Options{Name: "u"})}
	b := newLocal(t, local.Options{Name: "b"})
	e := newEnvWith(t, func(d *saga.Deps) { d.Runners = runners }, armSandboxes{u}, b)
	p, _ := e.place(t, "order-1")
	if p.State != store.PlacementFailed || !p.Refundable ||
		p.FailureReason != "runner_unavailable" {
		t.Fatalf("placement %+v, want failed refundable for runner_unavailable", p)
	}
	if nu, nb := len(owned(t, u)), len(owned(t, b)); nu != 0 || nb != 0 {
		t.Fatalf("sandboxes u=%d b=%d, want none", nu, nb)
	}
	if iu := e.inUse(t, "u"); iu != 0 {
		t.Fatalf("in use u=%d", iu)
	}
}

// armSandboxes reports arm64 sandboxes whatever its capabilities say
type armSandboxes struct{ uploader }

func (a armSandboxes) Create(
	ctx context.Context, req provider.CreateRequest,
) (provider.Ref, error) {
	ref, err := a.uploader.Create(ctx, req)
	if err == nil {
		ref.Meta = map[string]string{"arch": "arm64"}
	}
	return ref, err
}

/* Extend */

func TestExtendOncePerOrder(t *testing.T) {
	root := t.TempDir()
	a := newLocal(t, local.Options{Name: "a", Root: root})
	e := newEnv(t, a)
	p, h := e.placeReady(t, "order-1")
	msgs := e.subscribe(t)
	tok := e.extendToken(t, "ext-1", morePkg, morePrice, h)
	want := p.Deadline.Add(10 * time.Minute)

	for i := range 2 {
		got, err := e.saga.Extend(t.Context(), h, tok)
		if err != nil {
			t.Fatalf("extend %d: %v", i, err)
		}
		if got.Deadline == nil || !got.Deadline.Equal(want) {
			t.Fatalf("extend %d deadline %v, want %v", i, got.Deadline, want)
		}
	}
	if got := e.reload(t, p.ID); !got.Deadline.Equal(want) {
		t.Fatalf("stored deadline %v, want %v", got.Deadline, want)
	}
	ev := e.oneEvent(t, saga.EventExtended)
	if ev.State != store.PlacementReady || ev.Deadline == nil || !ev.Deadline.Equal(want) ||
		ev.OrderID != "order-1" || ev.ExtendOrderID != "ext-1" {
		t.Fatalf("extended event %+v", ev)
	}
	if _, pub := receive(t, msgs); pub.Deadline == nil || !pub.Deadline.Equal(want) {
		t.Fatalf("published %+v", pub)
	}
	if lt := lifetime(t, root, p.ProviderRef); !near(lt, want.Add(5*time.Minute)) {
		t.Fatalf("provider lifetime %v, want %v", lt, want.Add(5*time.Minute))
	}
	if jobs := e.deadlineJobs(t); len(jobs) != 2 {
		t.Fatalf("deadline jobs %+v, want 2", jobs)
	}

	same := e.extendToken(t, "ext-2", smallPkg, smallPrice, h)
	got, err := e.saga.Extend(t.Context(), h, same)
	if err != nil || !got.Deadline.Equal(want.Add(10*time.Minute)) {
		t.Fatalf("extend with the base package %v, %v", got.Deadline, err)
	}
}

func TestExtendRejectsOtherHandle(t *testing.T) {
	e := newEnv(t, newLocal(t, local.Options{Name: "a"}))
	p1, h1 := e.placeReady(t, "order-1")
	p2, h2 := e.placeReady(t, "order-2")

	tok := e.extendToken(t, "ext-1", morePkg, morePrice, h1)
	if _, err := e.saga.Extend(t.Context(), h2, tok); !errors.Is(err, saga.ErrHandleMismatch) {
		t.Fatalf("err %v, want ErrHandleMismatch", err)
	}
	if _, err := e.saga.Extend(t.Context(), h1, e.startToken(t, "ext-2")); !errors.Is(
		err, saga.ErrBadToken) {
		t.Fatalf("start token err %v, want ErrBadToken", err)
	}
	for _, p := range []store.Placement{p1, p2} {
		if got := e.reload(t, p.ID); !got.Deadline.Equal(*p.Deadline) {
			t.Fatalf("deadline moved from %v to %v", p.Deadline, got.Deadline)
		}
	}
	if evs := e.events(t, saga.EventExtended); len(evs) != 0 {
		t.Fatalf("extended events %+v", evs)
	}
}

func TestExtendRejectsOtherPackageUnderpaymentAndUnready(t *testing.T) {
	e := newEnv(t, newLocal(t, local.Options{Name: "a"}))
	_, h := e.placeReady(t, "order-1")

	tok := e.extendToken(t, "ext-1", longPkg, 120000, h)
	if _, err := e.saga.Extend(t.Context(), h, tok); !errors.Is(err, saga.ErrNotExtension) {
		t.Fatalf("err %v, want ErrNotExtension", err)
	}
	tok = e.extendToken(t, "ext-2", morePkg, morePrice-1, h)
	if _, err := e.saga.Extend(t.Context(), h, tok); !errors.Is(err, saga.ErrUnderpaid) {
		t.Fatalf("err %v, want ErrUnderpaid", err)
	}

	placingH, sha := handle.Derive(e.handleKey, "order-2")
	_, _, err := e.store.CreatePlacement(t.Context(), store.NewPlacement{
		HandleSHA256: sha, OrderID: "order-2", PackageID: smallPkg,
	})
	if err != nil {
		t.Fatal(err)
	}
	tok = e.extendToken(t, "ext-3", morePkg, morePrice, placingH)
	if _, err := e.saga.Extend(t.Context(), placingH, tok); !errors.Is(err, saga.ErrNotReady) {
		t.Fatalf("err %v, want ErrNotReady", err)
	}
	if _, err := e.saga.Stop(t.Context(), placingH); !errors.Is(err, saga.ErrNotReady) {
		t.Fatalf("stop err %v, want ErrNotReady", err)
	}
}

func TestExtendSetLifetimeFailureRollsBack(t *testing.T) {
	var fail atomic.Bool
	var calls atomic.Int64
	root := t.TempDir()
	a := lifetimeProbe{newLocal(t, local.Options{Name: "a", Root: root}), &fail, &calls}
	e := newEnv(t, a)
	p, h := e.placeReady(t, "order-1")
	tok := e.extendToken(t, "ext-1", morePkg, morePrice, h)

	fail.Store(true)
	_, err := e.saga.Extend(t.Context(), h, tok)
	if _, ok := provider.AsError(err); !ok {
		t.Fatalf("err %v, want the provider error", err)
	}
	if got := e.reload(t, p.ID); !got.Deadline.Equal(*p.Deadline) {
		t.Fatalf("deadline moved from %v to %v", p.Deadline, got.Deadline)
	}
	if evs := e.events(t, saga.EventExtended); len(evs) != 0 {
		t.Fatalf("extended events %+v", evs)
	}
	if jobs := e.deadlineJobs(t); len(jobs) != 1 {
		t.Fatalf("deadline jobs %+v, want only the first", jobs)
	}

	fail.Store(false)
	want := p.Deadline.Add(10 * time.Minute)
	got, err := e.saga.Extend(t.Context(), h, tok)
	if err != nil || !got.Deadline.Equal(want) {
		t.Fatalf("retry %v, %v, want deadline %v", got.Deadline, err, want)
	}
	if lt := lifetime(t, root, p.ProviderRef); !near(lt, want.Add(5*time.Minute)) {
		t.Fatalf("provider lifetime %v, want %v", lt, want.Add(5*time.Minute))
	}
	e.oneEvent(t, saga.EventExtended)
}

func TestExtendRefusesPastMaxLifetime(t *testing.T) {
	root := t.TempDir()
	a := newCapped(t, root, 30*time.Minute)
	e := newEnv(t, a)
	p, h := e.placeReady(t, "order-1")
	want := p.Deadline.Add(10 * time.Minute)
	if _, err := e.saga.Extend(t.Context(), h, e.extendToken(t, "ext-1", morePkg, morePrice,
		h)); err != nil {
		t.Fatalf("extend within the provider's max lifetime: %v", err)
	}

	before := a.calls.Load()
	for range 2 {
		_, err := e.saga.Extend(t.Context(), h, e.extendToken(t, "ext-2", morePkg, morePrice, h))
		if !errors.Is(err, saga.ErrExceedsLifetime) {
			t.Fatalf("extend past the max lifetime gave %v, want ErrExceedsLifetime", err)
		}
	}
	if got := e.reload(t, p.ID); !got.Deadline.Equal(want) {
		t.Fatalf("deadline %v, want %v from the first extension only", got.Deadline, want)
	}
	if n := a.calls.Load() - before; n != 0 {
		t.Fatalf("%d lifetime calls for a refused extension", n)
	}
	if lt := lifetime(t, root, p.ProviderRef); !near(lt, want.Add(5*time.Minute)) {
		t.Fatalf("provider lifetime %v, want %v", lt, want.Add(5*time.Minute))
	}
	recorded, err := e.store.HasExtendOrder(t.Context(), "ext-2", p.ID)
	if err != nil || recorded {
		t.Fatalf("refused order recorded %v, %v", recorded, err)
	}
	e.oneEvent(t, saga.EventExtended)
	if jobs := e.deadlineJobs(t); len(jobs) != 2 {
		t.Fatalf("deadline jobs %+v, want the first and the extension's", jobs)
	}

	unlimited := newEnv(t, newCapped(t, t.TempDir(), 0))
	_, uh := unlimited.placeReady(t, "order-2")
	for _, order := range []string{"ext-3", "ext-4"} {
		tok := unlimited.extendToken(t, order, morePkg, morePrice, uh)
		if _, err := unlimited.saga.Extend(t.Context(), uh, tok); err != nil {
			t.Fatalf("extend %s without a max lifetime: %v", order, err)
		}
	}
}

func TestExtendMaxLifetimeHoldsForConcurrentOrders(t *testing.T) {
	a := newCapped(t, t.TempDir(), 30*time.Minute)
	e := newEnv(t, a)
	p, h := e.placeReady(t, "order-1")
	tok1 := e.extendToken(t, "ext-1", morePkg, morePrice, h)
	tok2 := e.extendToken(t, "ext-2", morePkg, morePrice, h)
	free := sync.OnceFunc(func() { close(a.release) })
	t.Cleanup(free)
	a.hold.Store(true)

	first := make(chan error, 1)
	go func() {
		_, err := e.saga.Extend(t.Context(), h, tok1)
		first <- err
	}()
	select {
	case <-a.held:
	case err := <-first:
		t.Fatalf("first extension ended before setting the lifetime: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("first extension never set the lifetime")
	}
	second := make(chan error, 1)
	go func() {
		_, err := e.saga.Extend(t.Context(), h, tok2)
		second <- err
	}()
	e.waitForLockWait(t)
	free()

	if err := <-first; err != nil {
		t.Fatalf("first extension: %v", err)
	}
	if err := <-second; !errors.Is(err, saga.ErrExceedsLifetime) {
		t.Fatalf("second extension gave %v, want ErrExceedsLifetime", err)
	}
	if got := e.reload(t, p.ID); !got.Deadline.Equal(p.Deadline.Add(10 * time.Minute)) {
		t.Fatalf("deadline %v, want %v", got.Deadline, p.Deadline.Add(10*time.Minute))
	}
	if recorded, err := e.store.HasExtendOrder(t.Context(), "ext-2", p.ID); err != nil ||
		recorded {
		t.Fatalf("refused order recorded %v, %v", recorded, err)
	}
	e.oneEvent(t, saga.EventExtended)
}

// waitForLockWait returns once a session of the test's database waits on a lock
func (e *env) waitForLockWait(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var n int
		err := e.pool.QueryRow(t.Context(), `select count(*) from pg_stat_activity
			where datname = current_database() and wait_event_type = 'Lock'`).Scan(&n)
		if err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("no session waited on a lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestExtendReplayAfterStop(t *testing.T) {
	var fail atomic.Bool
	var calls atomic.Int64
	a := lifetimeProbe{newLocal(t, local.Options{Name: "a"}), &fail, &calls}
	e := newEnv(t, a)
	_, h := e.placeReady(t, "order-1")
	tok := e.extendToken(t, "ext-1", morePkg, morePrice, h)
	extended, err := e.saga.Extend(t.Context(), h, tok)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.saga.Stop(t.Context(), h); err != nil {
		t.Fatal(err)
	}

	before := calls.Load()
	got, err := e.saga.Extend(t.Context(), h, tok)
	if err != nil || got.State != store.PlacementTerminated ||
		!got.Deadline.Equal(*extended.Deadline) {
		t.Fatalf("replay %+v, %v", got, err)
	}
	if n := calls.Load() - before; n != 0 {
		t.Fatalf("%d lifetime calls on a terminated placement", n)
	}
	e.oneEvent(t, saga.EventExtended)
	fresh := e.extendToken(t, "ext-2", morePkg, morePrice, h)
	if _, err := e.saga.Extend(t.Context(), h, fresh); !errors.Is(err, saga.ErrNotReady) {
		t.Fatalf("new order err %v, want ErrNotReady", err)
	}
}

func (e *env) extendOrders(t *testing.T) int {
	t.Helper()
	var n int
	err := e.pool.QueryRow(t.Context(),
		`select count(*) from orders where kind = 'extend'`).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func (e *env) pendingOutbox(t *testing.T) int {
	t.Helper()
	pending, err := e.store.PendingOutbox(t.Context(), time.Now(), 1000)
	if err != nil {
		t.Fatal(err)
	}
	return len(pending)
}

func TestCheckExtendChangesNothingAndExtendAgrees(t *testing.T) {
	var fail atomic.Bool
	var calls atomic.Int64
	root := t.TempDir()
	a := lifetimeProbe{newLocal(t, local.Options{Name: "a", Root: root}), &fail, &calls}
	e := newEnv(t, a)
	p, h := e.placeReady(t, "order-1")
	want := p.Deadline.Add(10 * time.Minute)
	before := calls.Load()
	outbox := e.pendingOutbox(t)

	for _, pkg := range []string{morePkg, smallPkg} {
		got, deadline, err := e.saga.CheckExtend(t.Context(), h, pkg)
		if err != nil {
			t.Fatalf("check %s: %v", pkg, err)
		}
		if got.ID != p.ID || got.State != store.PlacementReady ||
			!got.Deadline.Equal(*p.Deadline) || !deadline.Equal(want) {
			t.Fatalf("check %s gave %+v and %v, want the placement as it is and %v", pkg, got,
				deadline, want)
		}
	}
	if got := e.reload(t, p.ID); !got.Deadline.Equal(*p.Deadline) {
		t.Fatalf("deadline moved from %v to %v", p.Deadline, got.Deadline)
	}
	if n := calls.Load() - before; n != 0 {
		t.Fatalf("%d lifetime calls for a check", n)
	}
	if lt := lifetime(t, root, p.ProviderRef); !near(lt, p.Deadline.Add(5*time.Minute)) {
		t.Fatalf("provider lifetime %v, want %v", lt, p.Deadline.Add(5*time.Minute))
	}
	if evs := e.events(t, saga.EventExtended); len(evs) != 0 {
		t.Fatalf("extended events %+v", evs)
	}
	if n := e.pendingOutbox(t); n != outbox {
		t.Fatalf("%d pending outbox events after the checks, want %d as before", n, outbox)
	}
	if jobs := e.deadlineJobs(t); len(jobs) != 1 {
		t.Fatalf("deadline jobs %+v, want only the first", jobs)
	}
	if n := e.extendOrders(t); n != 0 {
		t.Fatalf("%d extend orders recorded by a check", n)
	}

	got, err := e.saga.Extend(t.Context(), h, e.extendToken(t, "ext-1", morePkg, morePrice, h))
	if err != nil || !got.Deadline.Equal(want) {
		t.Fatalf("extend after the check %v, %v, want deadline %v", got.Deadline, err, want)
	}
	if n := e.pendingOutbox(t); n != outbox+1 {
		t.Fatalf("%d pending outbox events after the extension, want %d", n, outbox+1)
	}
}

func TestCheckExtendAddsTheCheckedPackagesMinutes(t *testing.T) {
	e := newEnv(t, newLocal(t, local.Options{Name: "a"}))
	p, h := e.placeReady(t, "order-1")
	want := p.Deadline.Add(5 * time.Minute)
	if _, deadline, err := e.saga.CheckExtend(t.Context(), h, shortPkg); err != nil ||
		!deadline.Equal(want) {
		t.Fatalf("check of a 5-minute extension gave %v, %v, want %v", deadline, err, want)
	}
	got, err := e.saga.Extend(t.Context(), h, e.extendToken(t, "ext-1", shortPkg, shortPrice, h))
	if err != nil || !got.Deadline.Equal(want) {
		t.Fatalf("5-minute extension gave %v, %v, want %v", got.Deadline, err, want)
	}
}

func TestCheckExtendRefusesAsExtendDoes(t *testing.T) {
	e := newEnv(t, newLocal(t, local.Options{Name: "a"}))
	_, h := e.placeReady(t, "order-1")
	_, stopped := e.placeReady(t, "order-2")
	if _, err := e.saga.Stop(t.Context(), stopped); err != nil {
		t.Fatal(err)
	}
	placing, sha := handle.Derive(e.handleKey, "order-3")
	_, _, err := e.store.CreatePlacement(t.Context(), store.NewPlacement{
		HandleSHA256: sha, OrderID: "order-3", PackageID: smallPkg,
	})
	if err != nil {
		t.Fatal(err)
	}
	unknown, _ := handle.New()

	for name, c := range map[string]struct {
		handle, pkg string
		price       int64
		want        error
	}{
		"unknown handle":        {unknown, morePkg, morePrice, store.ErrNotFound},
		"unknown package":       {h, "no-such-package", morePrice, saga.ErrUnknownPackage},
		"package of another":    {h, longPkg, 120000, saga.ErrNotExtension},
		"placing placement":     {placing, morePkg, morePrice, saga.ErrNotReady},
		"terminated placement":  {stopped, morePkg, morePrice, saga.ErrNotReady},
		"unknown package first": {unknown, "no-such-package", morePrice, saga.ErrUnknownPackage},
	} {
		if _, _, err := e.saga.CheckExtend(t.Context(), c.handle, c.pkg); !errors.Is(err, c.want) {
			t.Errorf("%s: check gave %v, want %v", name, err, c.want)
		}
		tok := e.extendToken(t, "ext-"+name, c.pkg, c.price, c.handle)
		if _, err := e.saga.Extend(t.Context(), c.handle, tok); !errors.Is(err, c.want) {
			t.Errorf("%s: extend gave %v, want %v", name, err, c.want)
		}
	}
	for _, bad := range []string{"", "nope", "sbx_not-a-handle"} {
		_, _, err := e.saga.CheckExtend(t.Context(), bad, morePkg)
		if !errors.Is(err, store.ErrNotFound) {
			t.Errorf("malformed handle %q gave %v, want ErrNotFound", bad, err)
		}
	}
	if evs := e.events(t, saga.EventExtended); len(evs) != 0 {
		t.Fatalf("extended events %+v", evs)
	}
}

func TestCheckExtendRefusesPastMaxLifetime(t *testing.T) {
	a := newCapped(t, t.TempDir(), 30*time.Minute)
	e := newEnv(t, a)
	p, h := e.placeReady(t, "order-1")
	_, deadline, err := e.saga.CheckExtend(t.Context(), h, morePkg)
	if err != nil || !deadline.Equal(p.Deadline.Add(10*time.Minute)) {
		t.Fatalf("check within the provider's max lifetime %v, %v", deadline, err)
	}
	if _, err := e.saga.Extend(t.Context(), h, e.extendToken(t, "ext-1", morePkg, morePrice,
		h)); err != nil {
		t.Fatalf("extend within the provider's max lifetime: %v", err)
	}

	before := a.calls.Load()
	if _, _, err := e.saga.CheckExtend(t.Context(), h, morePkg); !errors.Is(
		err, saga.ErrExceedsLifetime) {
		t.Fatalf("check past the max lifetime gave %v, want ErrExceedsLifetime", err)
	}
	_, err = e.saga.Extend(t.Context(), h, e.extendToken(t, "ext-2", morePkg, morePrice, h))
	if !errors.Is(err, saga.ErrExceedsLifetime) {
		t.Fatalf("extend past the max lifetime gave %v, want ErrExceedsLifetime", err)
	}
	if n := a.calls.Load() - before; n != 0 {
		t.Fatalf("%d lifetime calls for a refused check and extension", n)
	}
	if n := e.extendOrders(t); n != 1 {
		t.Fatalf("%d extend orders, want the first only", n)
	}
}

// listCatalogJSON prices extensions so that catalog order, by price, differs from listing order
const listCatalogJSON = `{"packages": [
	{"id": "small-10m", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10, "price_micros": 50000},
	{"id": "small-30m-cheap", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 30, "price_micros": 20000, "extension_of": "small-10m"},
	{"id": "small-5m-b", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 5, "price_micros": 25000, "extension_of": "small-10m"},
	{"id": "small-5m-a", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 5, "price_micros": 25000, "extension_of": "small-10m"},
	{"id": "small-10m-more", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10, "price_micros": 45000, "extension_of": "small-10m"},
	{"id": "small-30m", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 30, "price_micros": 120000},
	{"id": "small-30m-more", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 30, "price_micros": 100000, "extension_of": "small-30m"}
]}`

// listOrder is how ListExtensions orders the extensions of a small-10m placement
var listOrder = []string{
	"small-5m-a", "small-5m-b", "small-10m-more", "small-10m", "small-30m-cheap",
}

// newListEnv is newEnv over listCatalogJSON, also returning the saga's deps and catalog
func newListEnv(t *testing.T, ps ...provider.Provider) (*env, saga.Deps, *catalog.Catalog) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "list-packages.json")
	if err := os.WriteFile(path, []byte(listCatalogJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	cat, err := catalog.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	var deps saga.Deps
	e := newEnvWith(t, func(d *saga.Deps) {
		d.Catalog = cat
		deps = *d
	}, ps...)
	return e, deps, cat
}

// refusal names the refusal err wraps, ok for none
func refusal(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, saga.ErrNotReady):
		return "not ready"
	case errors.Is(err, saga.ErrExceedsLifetime):
		return "exceeds lifetime"
	}
	return err.Error()
}

// listed lists the placement's extensions as "<id> <refusal>", failing where CheckExtend differs
func (e *env) listed(t *testing.T, h string) []string {
	t.Helper()
	exts, err := e.saga.ListExtensions(t.Context(), h)
	if err != nil {
		t.Fatalf("list extensions: %v", err)
	}
	var out []string
	for _, x := range exts {
		_, _, checkErr := e.saga.CheckExtend(t.Context(), h, x.Package.ID)
		if refusal(checkErr) != refusal(x.Refusal) {
			t.Errorf("%s listed with %v, CheckExtend gave %v", x.Package.ID, x.Refusal, checkErr)
		}
		out = append(out, x.Package.ID+" "+refusal(x.Refusal))
	}
	return out
}

func TestListExtensionsOrdersAndChecksEachAsCheckExtend(t *testing.T) {
	// 30 minutes from creation fits a 10-minute extension but not a 30-minute one
	a := newCapped(t, t.TempDir(), 30*time.Minute)
	e, _, cat := newListEnv(t, a)
	p, h := e.placeReady(t, "order-1")
	before := a.calls.Load()
	outbox := e.pendingOutbox(t)

	want := []string{
		"small-5m-a ok", "small-5m-b ok", "small-10m-more ok", "small-10m ok",
		"small-30m-cheap exceeds lifetime",
	}
	if got := e.listed(t, h); !slices.Equal(got, want) {
		t.Fatalf("listed %q, want %q", got, want)
	}
	exts, err := e.saga.ListExtensions(t.Context(), h)
	if err != nil {
		t.Fatal(err)
	}
	for _, x := range exts {
		if pkg, _ := cat.Get(x.Package.ID); !reflect.DeepEqual(x.Package, pkg) {
			t.Errorf("listed %+v, the catalog has %+v", x.Package, pkg)
		}
	}

	if got := e.reload(t, p.ID); !got.Deadline.Equal(*p.Deadline) {
		t.Fatalf("deadline moved from %v to %v", p.Deadline, got.Deadline)
	}
	if n := a.calls.Load() - before; n != 0 {
		t.Fatalf("%d lifetime calls for a listing", n)
	}
	if n := e.pendingOutbox(t); n != outbox {
		t.Fatalf("%d pending outbox events after the listing, want %d as before", n, outbox)
	}
	if jobs := e.deadlineJobs(t); len(jobs) != 1 {
		t.Fatalf("deadline jobs %+v, want only the first", jobs)
	}
	if n := e.extendOrders(t); n != 0 {
		t.Fatalf("%d extend orders recorded by a listing", n)
	}
}

func TestListExtensionsOrdersByMinutesThenPriceThenID(t *testing.T) {
	ext := func(id string, minutes int, price int64) saga.Extension {
		return saga.Extension{
			Package: catalog.Package{ID: id, Minutes: minutes, PriceMicros: price},
		}
	}
	for _, c := range []struct {
		name          string
		first, second saga.Extension
	}{
		{"fewer minutes, though dearer with a later id", ext("z-5m", 5, 9), ext("a-30m", 30, 1)},
		{"same minutes, cheaper with a later id", ext("z-cheap", 10, 4), ext("a-dear", 10, 6)},
		{"same minutes and price, lower id", ext("a-tie", 10, 4), ext("b-tie", 10, 4)},
	} {
		if saga.ByListing(c.first, c.second) >= 0 || saga.ByListing(c.second, c.first) <= 0 {
			t.Errorf("%s: %s is not listed before %s", c.name, c.first.Package.ID,
				c.second.Package.ID)
		}
	}
}

func TestListExtensionsOfAPlacementNotReady(t *testing.T) {
	e, _, _ := newListEnv(t, newLocal(t, local.Options{Name: "a"}))
	_, stopped := e.placeReady(t, "order-1")
	if _, err := e.saga.Stop(t.Context(), stopped); err != nil {
		t.Fatal(err)
	}
	placing, sha := handle.Derive(e.handleKey, "order-2")
	_, _, err := e.store.CreatePlacement(t.Context(), store.NewPlacement{
		HandleSHA256: sha, OrderID: "order-2", PackageID: smallPkg,
	})
	if err != nil {
		t.Fatal(err)
	}
	var want []string
	for _, id := range listOrder {
		want = append(want, id+" not ready")
	}
	for name, h := range map[string]string{"placing": placing, "terminated": stopped} {
		if got := e.listed(t, h); !slices.Equal(got, want) {
			t.Errorf("%s placement listed %q, want %q", name, got, want)
		}
	}
}

func TestListExtensionsOfAnUnknownHandle(t *testing.T) {
	e, _, _ := newListEnv(t, newLocal(t, local.Options{Name: "a"}))
	unknown, _ := handle.New()
	for _, h := range []string{unknown, "", "nope", "sbx_not-a-handle"} {
		if exts, err := e.saga.ListExtensions(t.Context(), h); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("handle %q listed %+v, %v, want ErrNotFound", h, exts, err)
		}
	}
}

func TestListExtensionsFailsAsCheckExtendWhenTheProviderIsGone(t *testing.T) {
	e, deps, _ := newListEnv(t, newLocal(t, local.Options{Name: "a"}))
	_, h := e.placeReady(t, "order-1")
	empty, err := provider.NewRegistry(nil, provider.Features{})
	if err != nil {
		t.Fatal(err)
	}
	deps.Registry = empty
	other := saga.New(deps)
	exts, err := other.ListExtensions(t.Context(), h)
	if err == nil || errors.Is(err, saga.ErrNotReady) || errors.Is(err, saga.ErrExceedsLifetime) {
		t.Errorf("listing on a saga without the provider gave %+v, %v, want its error", exts, err)
	}
	if _, _, err := other.CheckExtend(t.Context(), h, morePkg); err == nil {
		t.Error("check on a saga without the provider passed")
	}
}

/* Stop and Terminate */

func TestStopDestroysAndReleases(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a"})
	e := newEnv(t, a)
	p, h := e.placeReady(t, "order-1")
	msgs := e.subscribe(t)

	got, err := e.saga.Stop(t.Context(), h)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != store.PlacementTerminated || got.TerminatedAt == nil {
		t.Fatalf("stopped %+v", got)
	}
	if n := len(owned(t, a)); n != 0 {
		t.Fatalf("%d sandboxes left", n)
	}
	if n := e.inUse(t, "a"); n != 0 {
		t.Fatalf("in use %d", n)
	}
	ev := e.oneEvent(t, saga.EventTerminated)
	billed := int64(got.TerminatedAt.Sub(*got.ReadyAt) / time.Second)
	if ev.PlacementID != p.ID || ev.State != store.PlacementTerminated ||
		ev.BilledSeconds != billed || ev.Reason != saga.ReasonStop {
		t.Fatalf("terminated event %+v, want %d billed seconds", ev, billed)
	}
	if _, pub := receive(t, msgs); pub.State != store.PlacementTerminated {
		t.Fatalf("published %+v", pub)
	}

	again, err := e.saga.Stop(t.Context(), h)
	if err != nil || again.State != store.PlacementTerminated ||
		!again.TerminatedAt.Equal(*got.TerminatedAt) {
		t.Fatalf("second stop %+v, %v", again, err)
	}
	if evs := e.events(t, saga.EventTerminated); len(evs) != 1 {
		t.Fatalf("%d terminated events", len(evs))
	}

	unknown, _ := handle.Derive(e.handleKey, "never-ordered")
	for _, bad := range []string{unknown, "not-a-handle"} {
		if _, err := e.saga.Stop(t.Context(), bad); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("stop %q err %v, want ErrNotFound", bad, err)
		}
	}
}

func TestTerminateRecordsReason(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a"})
	e := newEnv(t, a)
	p, _ := e.placeReady(t, "order-1")
	for range 2 {
		if err := e.saga.Terminate(t.Context(), p.ID, *p.Deadline); err != nil {
			t.Fatal(err)
		}
	}
	if got := e.reload(t, p.ID); got.State != store.PlacementTerminated {
		t.Fatalf("state %s", got.State)
	}
	if ev := e.oneEvent(t, saga.EventTerminated); ev.Reason != "deadline" {
		t.Fatalf("terminated event %+v", ev)
	}
	if n := len(owned(t, a)); n != 0 {
		t.Fatalf("%d sandboxes left", n)
	}
}

func TestTerminateLeavesPlacementBeforeItsDeadline(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a"})
	e := newEnv(t, a)
	p, _ := e.placeReady(t, "order-1")
	err := e.saga.Terminate(t.Context(), p.ID, p.Deadline.Add(-time.Minute))
	if !errors.Is(err, saga.ErrNotDue) {
		t.Fatalf("terminate before the deadline gave %v, want ErrNotDue", err)
	}
	if got := e.reload(t, p.ID); got.State != store.PlacementReady {
		t.Fatalf("placement is %s, want ready", got.State)
	}
	if n := len(owned(t, a)); n != 1 {
		t.Fatalf("%d sandboxes, want the placement's one kept", n)
	}
}

func TestExtendInFlightAtDeadlineWins(t *testing.T) {
	a := newCapped(t, t.TempDir(), 0)
	clk := &clock{offset: -(10*time.Minute + 5*time.Second)}
	e := newEnvWith(t, func(d *saga.Deps) { d.Now = clk.Now }, a)
	p, h := e.placeReady(t, "order-1")
	tok := e.extendToken(t, "ext-1", morePkg, morePrice, h)
	free := sync.OnceFunc(func() { close(a.release) })
	t.Cleanup(free)
	a.hold.Store(true)

	extended := make(chan error, 1)
	go func() {
		_, err := e.saga.Extend(t.Context(), h, tok)
		extended <- err
	}()
	select {
	case <-a.held:
	case err := <-extended:
		t.Fatalf("extension ended before setting the lifetime: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("extension never set the lifetime")
	}
	terminated := make(chan error, 1)
	go func() { terminated <- e.saga.Terminate(t.Context(), p.ID, time.Now()) }()
	e.waitForLockWait(t)
	free()

	if err := <-extended; err != nil {
		t.Fatalf("extension: %v", err)
	}
	if err := <-terminated; !errors.Is(err, saga.ErrNotDue) {
		t.Fatalf("terminate gave %v, want ErrNotDue", err)
	}
	want := p.Deadline.Add(10 * time.Minute)
	if got := e.reload(t, p.ID); got.State != store.PlacementReady || !got.Deadline.Equal(want) {
		t.Fatalf("placement %s until %v, want ready until %v", got.State, got.Deadline, want)
	}
	e.oneEvent(t, saga.EventExtended)
	if evs := e.events(t, saga.EventTerminated); len(evs) != 0 {
		t.Fatalf("terminated events %+v after the extension won", evs)
	}
	if n := len(owned(t, a)); n != 1 {
		t.Fatalf("%d sandboxes, want the extended placement's one kept", n)
	}
}

func TestStopFinishedLaterKeepsStopReason(t *testing.T) {
	prov := &failingDestroy{Provider: newLocal(t, local.Options{Name: "a"})}
	e := newEnv(t, prov)
	p, h := e.placeReady(t, "order-1")
	prov.fail.Store(true)
	if _, err := e.saga.Stop(t.Context(), h); err == nil {
		t.Fatal("stop succeeded with a failing destroy")
	}
	if got := e.reload(t, p.ID); got.State != store.PlacementTerminating ||
		got.TerminationReason != saga.ReasonStop {
		t.Fatalf("placement %s for %q, want terminating for stop", got.State,
			got.TerminationReason)
	}
	prov.fail.Store(false)
	if err := e.saga.Terminate(t.Context(), p.ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	if ev := e.oneEvent(t, saga.EventTerminated); ev.Reason != saga.ReasonStop {
		t.Fatalf("terminated event %+v, want reason stop", ev)
	}
}

func TestStopConcurrentReleasesOnce(t *testing.T) {
	e := newEnv(t, slowDestroy{newLocal(t, local.Options{Name: "a"})})
	_, h := e.placeReady(t, "order-1")
	e.placeReady(t, "order-2")

	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for range cap(errs) {
		wg.Go(func() {
			_, err := e.saga.Stop(t.Context(), h)
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if n := e.inUse(t, "a"); n != 1 {
		t.Fatalf("in use %d, want 1", n)
	}
	if evs := e.events(t, saga.EventTerminated); len(evs) != 1 {
		t.Fatalf("%d terminated events", len(evs))
	}
}

// stopTogether stops the handle n times at once and returns each stop's error
func stopTogether(t *testing.T, e *env, h string, n int) []error {
	t.Helper()
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Go(func() { _, errs[i] = e.saga.Stop(t.Context(), h) })
	}
	wg.Wait()
	return errs
}

// sealUnderUnlistedRoot gives a placement's ref a credential that no root the store lists opens
func (e *env) sealUnderUnlistedRoot(t *testing.T, id uuid.UUID) {
	t.Helper()
	k9, err := dpkeys.Parse("k9:" + base64.StdEncoding.EncodeToString(make([]byte, 32)))
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.pool.Exec(t.Context(), `
		update placements set provider_ref = jsonb_set(provider_ref, '{meta}',
			coalesce(provider_ref->'meta', '{}') || jsonb_build_object('envd_token', $2::text))
		where id = $1`, id, k9.SealRef(id, "envd-plain"))
	if err != nil {
		t.Fatal(err)
	}
}

func TestUnopenableCredentialStillStopsAndTerminates(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a"})
	e := newEnv(t, a)
	stopped, h := e.placeReady(t, "order-1")
	due, _ := e.placeReady(t, "order-2")
	for _, p := range []store.Placement{stopped, due} {
		e.sealUnderUnlistedRoot(t, p.ID)
	}
	if n := e.inUse(t, "a"); n != 2 {
		t.Fatalf("in use %d, want 2", n)
	}

	got, err := e.saga.Stop(t.Context(), h)
	if err != nil || got.State != store.PlacementTerminated {
		t.Fatalf("stop gave %s, %v, want terminated", got.State, err)
	}
	if err := e.saga.Terminate(t.Context(), due.ID, *due.Deadline); err != nil {
		t.Fatalf("terminate: %v", err)
	}
	if got := e.reload(t, due.ID); got.State != store.PlacementTerminated {
		t.Fatalf("placement is %s after terminate, want terminated", got.State)
	}
	if n := len(owned(t, a)); n != 0 {
		t.Fatalf("%d sandboxes left", n)
	}
	if n := e.inUse(t, "a"); n != 0 {
		t.Fatalf("in use %d, want both slots released", n)
	}
}

func TestConcurrentStopsShareOneDestroy(t *testing.T) {
	prov := newOverlapping(t, false)
	e := newEnv(t, prov)
	p, h := e.placeReady(t, "order-1")
	for _, err := range stopTogether(t, e, h, 10) {
		if err != nil {
			t.Fatal(err)
		}
	}
	if n := prov.most(); n != 1 {
		t.Fatalf("%d destroys ran at once for 10 stops, want 1", n)
	}
	if got := e.reload(t, p.ID); got.State != store.PlacementTerminated {
		t.Fatalf("placement is %s, want terminated", got.State)
	}
}

func TestStopsOfAFailingDestroyShareIt(t *testing.T) {
	prov := newOverlapping(t, true)
	e := newEnv(t, prov)
	p, h := e.placeReady(t, "order-1")
	// A stop whose destroy failed left the placement terminating
	if _, err := e.store.MarkPlacementTerminating(t.Context(), p.ID, saga.ReasonStop,
		time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, err := range stopTogether(t, e, h, 10) {
		if pe, ok := provider.AsError(err); !ok || pe.Code != provider.CodeRateLimited {
			t.Fatalf("stop with a failing destroy gave %v, want its rate_limited error", err)
		}
	}
	if n := prov.most(); n != 1 {
		t.Fatalf("%d destroys ran at once for 10 stops, want 1", n)
	}
	if got := e.reload(t, p.ID); got.State != store.PlacementTerminating {
		t.Fatalf("placement is %s, want still terminating", got.State)
	}
}

func TestBilledSecondsUseSagaClock(t *testing.T) {
	clk := &clock{}
	a := newLocal(t, local.Options{Name: "a"})
	e := newEnvWith(t, func(d *saga.Deps) { d.Now = clk.Now }, a)
	p, h := e.placeReady(t, "order-1")
	clk.Advance(42 * time.Second)
	got, err := e.saga.Stop(t.Context(), h)
	if err != nil {
		t.Fatal(err)
	}
	if d := got.TerminatedAt.Sub(*p.ReadyAt); d < 42*time.Second || d >= 43*time.Second {
		t.Fatalf("ready at %v, terminated at %v", p.ReadyAt, got.TerminatedAt)
	}
	if ev := e.oneEvent(t, saga.EventTerminated); ev.BilledSeconds != 42 {
		t.Fatalf("terminated event %+v, want 42 billed seconds", ev)
	}
}

func TestChannelPatternMatchesOnlyItsPrefix(t *testing.T) {
	vk, prefix := testenv.Valkey(t)
	glob := prefix + `a*b?[c]\:`
	c, cancel := vk.Dedicate()
	t.Cleanup(cancel)
	msgs := make(chan string, 4)
	subscribed := make(chan struct{})
	var once sync.Once
	_ = c.SetPubSubHooks(valkey.PubSubHooks{
		OnMessage: func(m valkey.PubSubMessage) { msgs <- m.Channel },
		OnSubscription: func(valkey.PubSubSubscription) {
			once.Do(func() { close(subscribed) })
		},
	})
	cmd := c.B().Psubscribe().Pattern(saga.ChannelPattern(glob)).Build()
	if err := c.Do(t.Context(), cmd).Error(); err != nil {
		t.Fatalf("psubscribe: %v", err)
	}
	select {
	case <-subscribed:
	case <-time.After(5 * time.Second):
		t.Fatal("psubscribe was not confirmed")
	}
	id := uuid.New()
	// The pattern with its glob characters unescaped would match this channel first
	unescaped := saga.Channel(prefix+"aXXbYc:", id)
	for _, ch := range []string{unescaped, saga.Channel(glob, id)} {
		pub := vk.B().Publish().Channel(ch).Message("m").Build()
		if err := vk.Do(t.Context(), pub).Error(); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case ch := <-msgs:
		if ch != saga.Channel(glob, id) {
			t.Fatalf("pattern matched %q", ch)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the placement channel did not match")
	}
}

// holdSecrets gives a placement a secret, a link, a gateway key and a tunnel token
func (e *env) holdSecrets(t *testing.T, id uuid.UUID) {
	t.Helper()
	ctx := t.Context()
	_, _, err := e.store.PutSecret(ctx, store.Secret{
		ID: uuid.New(), PlacementID: id, Name: "HF_TOKEN", Hosts: []string{"huggingface.co"},
		PlaceholderSHA256: []byte(id.String()), Sealed: []byte("box"), KeyID: "k1",
		Source: store.SourceBridge, State: store.SecretActive,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.store.InsertSecretLink(ctx, store.SecretLink{
		ID: uuid.New(), PlacementID: id, TokenSHA256: []byte(id.String() + "link"),
		PairingCode: "abc",
	}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.store.AddGatewayKey(ctx, id, "SHA256:gw"); err != nil {
		t.Fatal(err)
	}
	if ok, err := e.store.MintEgressToken(ctx, id, []byte(id.String()+"tok")); err != nil || !ok {
		t.Fatalf("mint token: %v %v", ok, err)
	}
}

// secretsHeld counts the secrets and links rows of the placement and says whether a token stands
func (e *env) secretsHeld(t *testing.T, id uuid.UUID) (secrets, links int, token bool) {
	t.Helper()
	ctx := t.Context()
	all, err := e.store.Secrets(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.pool.QueryRow(ctx, `select count(*) from secret_links where placement_id = $1`,
		id).Scan(&links); err != nil {
		t.Fatal(err)
	}
	token, err = e.store.HasEgressToken(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return len(all), links, token
}

func TestEndDeletesSecretsInTheSameTransaction(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a"})
	e := newEnv(t, a)
	p, h := e.placeReady(t, "order-1")
	e.holdSecrets(t, p.ID)
	s, l, tok := e.secretsHeld(t, p.ID)
	eq(t, [3]any{s, l, tok}, [3]any{1, 1, true})

	if _, err := e.saga.Stop(t.Context(), h); err != nil {
		t.Fatal(err)
	}
	s, l, tok = e.secretsHeld(t, p.ID)
	eq(t, [3]any{s, l, tok}, [3]any{0, 0, false})
	gw, err := e.store.HasGatewayKey(t.Context(), p.ID, "SHA256:gw")
	eq(t, err, nil)
	eq(t, gw, false)
}

func TestFailedPlacementKeepsNoSecrets(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a"})
	e := newEnv(t, a)
	p, _, err := e.store.CreatePlacement(t.Context(), store.NewPlacement{
		HandleSHA256: []byte("stuck-handle"), OrderID: "order-1", PackageID: smallPkg,
	})
	if err != nil {
		t.Fatal(err)
	}
	e.holdSecrets(t, p.ID)
	if err := e.saga.FailStuck(t.Context(), p.ID, "stuck"); err != nil {
		t.Fatal(err)
	}
	e.saga.Wait()
	s, l, tok := e.secretsHeld(t, p.ID)
	eq(t, [3]any{s, l, tok}, [3]any{0, 0, false})
}

func TestListExtensionsHidesAStartPackageItsExtensionRepeats(t *testing.T) {
	path := filepath.Join(t.TempDir(), "same-rate.json")
	same := `{"packages": [
	{"id": "small-10m", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10, "price_micros": 50000},
	{"id": "small-10m-more", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10, "price_micros": 50000, "extension_of": "small-10m"}
]}`
	if err := os.WriteFile(path, []byte(same), 0o644); err != nil {
		t.Fatal(err)
	}
	cat, err := catalog.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	e := newEnvWith(t, func(d *saga.Deps) { d.Catalog = cat }, newCapped(t, t.TempDir(), time.Hour))
	_, h := e.placeReady(t, "order-1")
	if got, want := e.listed(t, h), []string{"small-10m-more ok"}; !slices.Equal(got, want) {
		t.Fatalf("listed %q, want %q", got, want)
	}
}

// agree fails unless in_use on name equals what the four holders count
func (e *env) agree(t *testing.T, name string, want int) {
	t.Helper()
	before, after, err := e.store.RecountCapacity(t.Context(), name, time.Hour, time.Now())
	if err != nil || before != want || after != want {
		t.Fatalf("%s: in_use %d and recount %d, %v, want both %d", name, before, after, err, want)
	}
}

func TestAbandonReleasesByAttempt(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a", CreateHook: failWith(provider.CodeCapacity, true)})
	b := newLocal(t, local.Options{Name: "b"})
	e := newEnv(t, a, b)
	p, _ := e.placeReady(t, "order-1")
	eq(t, p.Provider, "b")
	e.agree(t, "a", 0)
	e.agree(t, "b", 1)
	var attempt *int
	err := e.pool.QueryRow(t.Context(),
		`select capacity_attempt from placements where id = $1`, p.ID).Scan(&attempt)
	if err != nil || attempt == nil || *attempt != 2 {
		t.Fatalf("the placement's slot names attempt %v, %v, want 2", attempt, err)
	}
}

func TestFailStuckFreesAttemptSlots(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a"})
	b := newLocal(t, local.Options{Name: "b"})
	e := newEnv(t, a, b)
	ctx := t.Context()
	stuck, _, err := e.store.CreatePlacement(ctx, store.NewPlacement{
		HandleSHA256: []byte("stuck-handle"), OrderID: "order-2", PackageID: smallPkg,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := e.store.ReservePlacementCapacity(ctx, stuck.ID, "b", 1); err != nil || !ok {
		t.Fatalf("reserve: %v %v", ok, err)
	}
	// A second attempt of the placement holds its own slot on a
	if ok, err := e.store.ReserveCapacity(ctx, "a", 0); err != nil || !ok {
		t.Fatalf("reserve on a: %v %v", ok, err)
	}
	_, err = e.pool.Exec(ctx, `
		insert into placement_attempts (placement_id, attempt, provider, state, slot_provider)
		values ($1, 2, 'a', 'creating', 'a')`, stuck.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.saga.FailStuck(ctx, stuck.ID, saga.ReasonPlacingStalled); err != nil {
		t.Fatalf("fail stuck: %v", err)
	}
	e.saga.Wait()
	e.agree(t, "a", 0)
	e.agree(t, "b", 0)
}
