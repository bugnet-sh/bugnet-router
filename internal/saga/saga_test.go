package saga_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
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
)

const (
	smallPkg   = "small-10m"
	smallPrice = 50000
	morePkg    = "small-10m-more"
	morePrice  = 45000
	longPkg    = "small-30m"
)

const catalogJSON = `{"packages": [
	{"id": "small-10m", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10, "price_micros": 50000},
	{"id": "small-10m-more", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10, "price_micros": 45000, "extension_of": "small-10m"},
	{"id": "small-30m", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 30, "price_micros": 120000}
]}`

var small = provider.Size{
	Name: "small", Resources: provider.Resources{VCPU: 2, MemoryMiB: 2048, DiskGiB: 10},
}

func newLocal(t *testing.T, opts local.Options) *local.Provider {
	t.Helper()
	if opts.Root == "" {
		opts.Root = t.TempDir()
	}
	opts.Sizes = []provider.Size{small}
	opts.HourlyCostMicros = 1000
	opts.Increment = time.Second
	return local.New(opts)
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
	c.NeedsRunnerUpload = true
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
	reg := provider.NewRegistry(ps)
	tracker := placement.NewTracker(time.Now)
	st := store.New(pool)
	deps := saga.Deps{
		Store:       st,
		Engine:      placement.New(reg, tracker),
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
		Log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if adjust != nil {
		adjust(&deps)
	}
	s := saga.New(deps)
	t.Cleanup(s.Wait)
	return &env{
		saga: s, store: st, pool: pool, river: rc, vk: vk, prefix: prefix, priv: priv,
		handleKey: hk,
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
	p, h, err := e.saga.Create(t.Context(), e.startToken(t, order), placement.Requirements{})
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
	pending, err := e.store.PendingOutbox(t.Context(), 100)
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

	p, h, err := e.saga.Create(t.Context(), e.startToken(t, "order-1"), placement.Requirements{})
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
	if got.State != store.PlacementReady || got.Provider != "a" || got.ProviderRef == nil {
		t.Fatalf("placement %+v", got)
	}
	if got.ReadyAt == nil || got.Deadline == nil ||
		!near(*got.Deadline, got.ReadyAt.Add(10*time.Minute)) {
		t.Fatalf("ready at %v, deadline %v", got.ReadyAt, got.Deadline)
	}
	if lt := lifetime(t, root, got.ProviderRef); !near(lt, got.Deadline.Add(5*time.Minute)) {
		t.Fatalf("provider lifetime %v, want deadline + 5m", lt)
	}

	pending, err := e.store.PendingOutbox(t.Context(), 10)
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
			p, h, err := e.saga.Create(t.Context(), tok, placement.Requirements{})
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
	again, h, err := e.saga.Create(t.Context(), tok, placement.Requirements{})
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
		_, h, err := e.saga.Create(t.Context(), c.token, placement.Requirements{})
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
	if _, _, err := e.saga.Create(t.Context(), tok, placement.Requirements{}); !errors.Is(
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

	p, h, err := e.saga.Create(t.Context(), tok, placement.Requirements{GPU: true})
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
	again, h2, err := e.saga.Create(t.Context(), tok, placement.Requirements{})
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

func TestPlacingHeartbeats(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a", ReadyDelay: time.Second})
	e := newEnvWith(t, func(d *saga.Deps) { d.HeartbeatInterval = 50 * time.Millisecond }, a)
	p, _, err := e.saga.Create(t.Context(), e.startToken(t, "order-1"), placement.Requirements{})
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
	if p.State != store.PlacementFailed || !p.Refundable || p.FailureReason == "" {
		t.Fatalf("placement %+v", p)
	}
	ev := e.oneEvent(t, saga.EventFailed)
	if ev.PlacementID != p.ID || ev.State != store.PlacementFailed || !ev.Refundable ||
		ev.BilledSeconds != 0 || ev.Reason == "" {
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

func TestStuckDestroyDoesNotHangPlacing(t *testing.T) {
	saga.SetCleanupTimeout(t, 200*time.Millisecond)
	a := stuckDestroy{newLocal(t, local.Options{Name: "a"}), make(chan struct{})}
	e := newEnv(t, a)
	t.Cleanup(func() { close(a.unblock) })
	p, _, err := e.saga.Create(t.Context(), e.startToken(t, "order-1"), placement.Requirements{})
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
	p, _, err := e.saga.Create(t.Context(), e.startToken(t, "order-1"), placement.Requirements{})
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
	fake := writeFile(t, dir, "fake-runner", "#!/bin/sh\necho \"$@\" >> "+log+"\n", 0o755)
	runners := map[string]string{
		"amd64": writeFile(t, dir, "amd64", "amd64 build", 0o644),
		"arm64": writeFile(t, dir, "arm64", "arm64 build", 0o644),
	}
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
	if err != nil || strings.TrimSpace(string(ran)) != "ping" {
		t.Fatalf("runner ran with %q, %v", ran, err)
	}
}

func TestCreateWithoutRunnerBuildFailsWithoutFailOver(t *testing.T) {
	dir := t.TempDir()
	runners := map[string]string{"arm64": writeFile(t, dir, "arm64", "arm64 build", 0o644)}
	u := uploader{Provider: newLocal(t, local.Options{Name: "u"})}
	b := newLocal(t, local.Options{Name: "b"})
	e := newEnvWith(t, func(d *saga.Deps) { d.Runners = runners }, u, b)
	p, _ := e.place(t, "order-1")
	if p.State != store.PlacementFailed || !p.Refundable {
		t.Fatalf("placement %+v", p)
	}
	if nu, nb := len(owned(t, u)), len(owned(t, b)); nu != 0 || nb != 0 {
		t.Fatalf("sandboxes u=%d b=%d, want none", nu, nb)
	}
	if iu := e.inUse(t, "u"); iu != 0 {
		t.Fatalf("in use u=%d", iu)
	}
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
	if ev.State != store.PlacementReady || ev.Deadline == nil || !ev.Deadline.Equal(want) {
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
		if err := e.saga.Terminate(t.Context(), p.ID, "deadline"); err != nil {
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
