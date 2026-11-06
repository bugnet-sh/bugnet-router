package api_test

import (
	"bytes"
	"cmp"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	routerv1 "github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"github.com/oynozan/bugnet-run/frame"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/valkey-io/valkey-go"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/proto"

	"github.com/oynozan/bugnet-router/internal/api"
	"github.com/oynozan/bugnet-router/internal/catalog"
	"github.com/oynozan/bugnet-router/internal/handle"
	"github.com/oynozan/bugnet-router/internal/jobs"
	"github.com/oynozan/bugnet-router/internal/ordertoken"
	"github.com/oynozan/bugnet-router/internal/placement"
	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/provider/local"
	"github.com/oynozan/bugnet-router/internal/saga"
	"github.com/oynozan/bugnet-router/internal/store"
	"github.com/oynozan/bugnet-router/internal/streams"
	"github.com/oynozan/bugnet-router/internal/testenv"
)

var runnerPath string

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	dir, err := os.MkdirTemp("", "bugnet-run-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer func() { _ = os.RemoveAll(dir) }()
	runnerPath = filepath.Join(dir, "bugnet-run")
	build := exec.Command("go", "build", "-o", runnerPath,
		"github.com/oynozan/bugnet-run/cmd/bugnet-run")
	build.Stdout, build.Stderr = os.Stderr, os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "build bugnet-run:", err)
		return 1
	}
	return m.Run()
}

const internalToken = "api-test-internal-token-0123456789abcdef"

const (
	smallPkg   = "small-10m"
	smallPrice = 50000
	morePkg    = "small-10m-more"
	morePrice  = 45000
	longPkg    = "small-30m"
	longPrice  = 120000
	gpuPkg     = "gpu-10m"
	gpuPrice   = 900000
	maxFile    = 256 << 10
)

const catalogJSON = `{"packages": [
	{"id": "gpu-10m", "resources": {"vcpu": 4, "memory_mib": 8192, "disk_gib": 20,
		"gpu_count": 1, "gpu_memory_gib": 24}, "minutes": 10, "price_micros": 900000},
	{"id": "small-30m", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 30, "price_micros": 120000},
	{"id": "small-10m-more", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10, "price_micros": 45000, "extension_of": "small-10m"},
	{"id": "small-10m", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10, "price_micros": 50000}
]}`

var small = provider.Size{
	Name: "small", Resources: provider.Resources{VCPU: 2, MemoryMiB: 2048, DiskGiB: 10},
}

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

// faulty fails reads of /fault/<code> with that provider error code and can hold readiness, and
// reads of /fault-connect/<code> with one wrapping a backend's Connect error as E2B's do
type faulty struct {
	*local.Provider
	hold        <-chan struct{} // WaitReady waits for it to close, when set
	maxLifetime time.Duration   // replaces the local provider's cap, when set
}

func (f faulty) Capabilities() provider.Capabilities {
	c := f.Provider.Capabilities()
	if f.maxLifetime > 0 {
		c.MaxLifetime = f.maxLifetime
	}
	return c
}

func (f faulty) WaitReady(ctx context.Context, ref provider.Ref) error {
	if f.hold != nil {
		select {
		case <-f.hold:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return f.Provider.WaitReady(ctx, ref)
}

func (f faulty) ReadFile(
	ctx context.Context, ref provider.Ref, path string, offset int64, length int,
) ([]byte, bool, error) {
	if code, ok := strings.CutPrefix(path, "/fault/"); ok {
		return nil, false, &provider.Error{
			Code: provider.Code(code), Err: errors.New("injected by provider local for sbx-1"),
		}
	}
	if code, ok := strings.CutPrefix(path, "/fault-connect/"); ok {
		return nil, false, &provider.Error{
			Code: provider.Code(code), Err: connect.NewError(connect.CodeResourceExhausted,
				errors.New("injected by provider local for sbx-1")),
		}
	}
	return f.Provider.ReadFile(ctx, ref, path, offset, length)
}

type options struct {
	readyDelay  time.Duration
	holdReady   <-chan struct{}
	maxLifetime time.Duration
	secrets     bool  // the registry lists the provider for secrets
	maxRunning  int   // jobs a placement may run at once, the jobs default when zero
	jobBudget   int64 // what one job's output may cost its stream, the jobs default when zero
	// opsPerSecond and opsBurst are each placement's operation limit, the defaults when zero
	opsPerSecond int
	opsBurst     int
	log          *slog.Logger // the API server's log, discarded when nil
	apiValkey    string       // what the API server's Valkey client dials, valkeyAddr() when empty
	apiCatalog   string       // the API server's catalog JSON, the saga's catalogJSON when empty
	apiEngine    api.Engine   // the API server's engine, the saga's engine when nil
}

type env struct {
	api       *api.Server
	saga      *saga.Saga
	client    routerv1connect.SandboxRouterServiceClient
	url       string
	dbURL     string
	pool      *pgxpool.Pool
	store     *store.Store
	jobs      *jobs.Service
	vk        valkey.Client
	tracker   *placement.Tracker
	apiVK     valkey.Client // the API server's own Valkey client
	apiVKName string        // client name of the API server's own Valkey connections
	prefix    string
	root      string
	priv      ed25519.PrivateKey
	handleKey []byte
	proto     atomic.Value
	orders    atomic.Int64
}

func newEnv(t *testing.T, opts options) *env {
	t.Helper()
	ctx := t.Context()
	dbURL := testenv.DatabaseURL(t)
	if err := store.Migrate(ctx, dbURL); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := pgxpool.New(ctx, dbURL)
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
	handleKey := make([]byte, 32)
	if _, err := rand.Read(handleKey); err != nil {
		t.Fatal(err)
	}
	cat := loadCatalog(t, catalogJSON)

	root := t.TempDir()
	prov := local.New(local.Options{
		Name: "local", Root: root, Sizes: []provider.Size{small}, HourlyCostMicros: 1000,
		Increment: time.Second, RunnerPath: runnerPath, ReadyDelay: opts.readyDelay,
	})
	t.Cleanup(func() { destroyAll(t, prov) })
	var features provider.Features
	if opts.secrets {
		features.Secrets = []string{"local"}
	}
	reg, err := provider.NewRegistry([]provider.Provider{faulty{
		Provider: prov, hold: opts.holdReady, maxLifetime: opts.maxLifetime,
	}}, features)
	if err != nil {
		t.Fatal(err)
	}
	tracker := placement.NewTracker(time.Now)
	engine := placement.New(reg, tracker)
	st := store.New(pool)
	sg := saga.New(saga.Deps{
		Store: st, Engine: engine, Registry: reg, Catalog: cat, River: rc, Valkey: vk,
		Prefix: prefix, OrderKey: pub, HandleKey: handleKey, MarginFloor: 0.5,
		Tracker: tracker, ReplicaID: "api-test", Log: discard,
	})
	t.Cleanup(sg.Wait)
	js := jobs.New(jobs.Deps{
		Store: st, Bus: streams.NewBus(vk, prefix), Registry: reg, ReplicaID: "api-test",
		Log: discard, MaxRunning: opts.maxRunning, JobBudget: opts.jobBudget,
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		js.Shutdown(ctx)
	})
	apiName := "bugnet-api-test-" + rand.Text()
	apiVK, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{cmp.Or(opts.apiValkey, valkeyAddr())}, ClientName: apiName,
	})
	if err != nil {
		t.Fatalf("connect to valkey: %v", err)
	}
	t.Cleanup(apiVK.Close)
	log := discard
	if opts.log != nil {
		log = opts.log
	}
	apiCat := cat
	if opts.apiCatalog != "" {
		apiCat = loadCatalog(t, opts.apiCatalog)
	}
	var apiEngine api.Engine = engine
	if opts.apiEngine != nil {
		apiEngine = opts.apiEngine
	}
	srv := api.New(api.Deps{
		Store: st, Saga: sg, Jobs: js, Engine: apiEngine, Catalog: apiCat, Registry: reg,
		Valkey: apiVK, Prefix: prefix, MarginFloor: 0.5, Log: log,
		SandboxOpsPerSecond: opts.opsPerSecond, SandboxOpsBurst: opts.opsBurst,
	})
	t.Cleanup(srv.Close)

	e := &env{
		api: srv, saga: sg, dbURL: dbURL, pool: pool, store: st, jobs: js, vk: vk,
		tracker: tracker, apiVK: apiVK, apiVKName: apiName, prefix: prefix, root: root,
		priv: priv, handleKey: handleKey,
	}
	mux := http.NewServeMux()
	api.Mount(mux, srv, internalToken)
	record := func(w http.ResponseWriter, r *http.Request) {
		e.proto.Store(r.Proto)
		mux.ServeHTTP(w, r)
	}
	hs := httptest.NewUnstartedServer(http.HandlerFunc(record))
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	hs.Config.Protocols = &protocols
	hs.Start()
	t.Cleanup(hs.Close)
	e.url = hs.URL
	e.client = e.clientWith(t, "Bearer "+internalToken, true)
	return e
}

func loadCatalog(t *testing.T, packages string) *catalog.Catalog {
	t.Helper()
	path := filepath.Join(t.TempDir(), "packages.json")
	if err := os.WriteFile(path, []byte(packages), 0o644); err != nil {
		t.Fatal(err)
	}
	cat, err := catalog.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	return cat
}

func valkeyAddr() string {
	if addr := os.Getenv("BUGNET_TEST_VALKEY_ADDR"); addr != "" {
		return addr
	}
	return "localhost:6379"
}

func destroyAll(t *testing.T, p *local.Provider) {
	t.Helper()
	ctx := context.Background()
	owned, err := p.ListOwned(ctx, map[string]string{"bugnet": "1"})
	if err != nil {
		t.Errorf("list sandboxes: %v", err)
		return
	}
	for _, o := range owned {
		if err := p.Destroy(ctx, o.Ref); err != nil {
			t.Errorf("destroy %s: %v", o.Ref.ID, err)
		}
	}
}

// setHeader sends every request with one Authorization header value
type setHeader struct {
	value string
	next  http.RoundTripper
}

func (s setHeader) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", s.value)
	return s.next.RoundTrip(r)
}

// clientWith returns a client sending authorization, over h2c or else HTTP/1.1
func (e *env) clientWith(
	t *testing.T, authorization string, h2c bool,
) routerv1connect.SandboxRouterServiceClient {
	t.Helper()
	var protocols http.Protocols
	if h2c {
		protocols.SetUnencryptedHTTP2(true)
	} else {
		protocols.SetHTTP1(true)
	}
	tr := &http.Transport{Protocols: &protocols}
	t.Cleanup(tr.CloseIdleConnections)
	var rt http.RoundTripper = tr
	if authorization != "" {
		rt = setHeader{value: authorization, next: tr}
	}
	return routerv1connect.NewSandboxRouterServiceClient(&http.Client{Transport: rt}, e.url)
}

/* Tokens and placements */

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

func (e *env) startToken(t *testing.T, pkg string, amount int64) string {
	t.Helper()
	_, tok := e.startOrder(t, pkg, amount)
	return tok
}

// startOrder returns a new start order's ID and its signed token
func (e *env) startOrder(t *testing.T, pkg string, amount int64) (string, string) {
	t.Helper()
	order := fmt.Sprintf("order-%d", e.orders.Add(1))
	return order, e.token(t, ordertoken.Claims{
		OrderID: order, Kind: ordertoken.KindStart, PackageID: pkg, AmountMicros: amount,
	})
}

func (e *env) extendToken(t *testing.T, h, pkg string, amount int64) string {
	t.Helper()
	sha, err := handle.Hash(h)
	if err != nil {
		t.Fatal(err)
	}
	order := fmt.Sprintf("extend-%d", e.orders.Add(1))
	return e.token(t, ordertoken.Claims{
		OrderID: order, Kind: ordertoken.KindExtend, PackageID: pkg, AmountMicros: amount,
		HandleSHA256: hex.EncodeToString(sha),
	})
}

func (e *env) createWith(
	t *testing.T, tok string,
) (*connect.Response[routerv1.CreatePlacementResponse], error) {
	t.Helper()
	return e.createFor(t, tok, nil)
}

func (e *env) createFor(
	t *testing.T, tok string, r *routerv1.Requirements,
) (*connect.Response[routerv1.CreatePlacementResponse], error) {
	t.Helper()
	return e.client.CreatePlacement(t.Context(), connect.NewRequest(
		&routerv1.CreatePlacementRequest{OrderToken: tok, Requirements: r}))
}

func (e *env) create(t *testing.T) (string, *routerv1.Placement) {
	t.Helper()
	resp, err := e.createWith(t, e.startToken(t, smallPkg, smallPrice))
	if err != nil {
		t.Fatalf("create placement: %v", err)
	}
	return resp.Msg.GetHandle(), resp.Msg.GetPlacement()
}

func (e *env) get(
	t *testing.T, h string, known routerv1.PlacementState, waitMs uint32,
) *routerv1.Placement {
	t.Helper()
	resp, err := e.client.GetPlacement(t.Context(), connect.NewRequest(
		&routerv1.GetPlacementRequest{Handle: h, KnownState: known, WaitMs: waitMs}))
	if err != nil {
		t.Fatalf("get placement: %v", err)
	}
	return resp.Msg.GetPlacement()
}

func (e *env) waitReady(t *testing.T, h string) *routerv1.Placement {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		p := e.get(t, h, routerv1.PlacementState_PLACEMENT_STATE_PLACING, 5000)
		switch p.GetState() {
		case routerv1.PlacementState_PLACEMENT_STATE_READY:
			return p
		case routerv1.PlacementState_PLACEMENT_STATE_PLACING:
		default:
			t.Fatalf("placement is %v (%s), want ready", p.GetState(), p.GetFailureReason())
		}
	}
	t.Fatal("placement never became ready")
	return nil
}

func (e *env) readyPlacement(t *testing.T) (string, *routerv1.Placement) {
	t.Helper()
	h, _ := e.create(t)
	return h, e.waitReady(t, h)
}

func (e *env) stop(t *testing.T, h string) *routerv1.Placement {
	t.Helper()
	resp, err := e.client.StopPlacement(t.Context(), connect.NewRequest(
		&routerv1.StopPlacementRequest{Handle: h}))
	if err != nil {
		t.Fatalf("stop placement: %v", err)
	}
	return resp.Msg.GetPlacement()
}

func (e *env) exec(t *testing.T, req *routerv1.ExecRequest) *routerv1.ExecResponse {
	t.Helper()
	resp, err := e.client.Exec(t.Context(), connect.NewRequest(req))
	if err != nil {
		t.Fatalf("exec %q: %v", req.GetArgv(), err)
	}
	return resp.Msg
}

func sh(h, script string) *routerv1.ExecRequest {
	return &routerv1.ExecRequest{Handle: h, Argv: []string{"sh", "-c", script}}
}

/* Assertions */

func wantCode(t *testing.T, err error, want connect.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("no error, want %v", want)
	}
	if got := connect.CodeOf(err); got != want {
		t.Fatalf("error %v has code %v, want %v", err, got, want)
	}
}

// wantInfo checks that err carries a google.rpc.ErrorInfo with this reason
func wantInfo(t *testing.T, err error, reason string) {
	t.Helper()
	var ce *connect.Error
	if !errors.As(err, &ce) {
		t.Fatalf("error %v is not a Connect error", err)
	}
	for _, d := range ce.Details() {
		v, derr := d.Value()
		if derr != nil {
			t.Fatalf("decode error detail: %v", derr)
		}
		if info, ok := v.(*errdetails.ErrorInfo); ok {
			if info.GetReason() != reason || info.GetDomain() != "bugnet.router" {
				t.Fatalf("error info %v, want reason %s in bugnet.router", info, reason)
			}
			return
		}
	}
	t.Fatalf("error %v has no ErrorInfo detail", err)
}

// wantNoInfo checks that err carries no google.rpc.ErrorInfo
func wantNoInfo(t *testing.T, err error) {
	t.Helper()
	var ce *connect.Error
	if !errors.As(err, &ce) {
		t.Fatalf("error %v is not a Connect error", err)
	}
	for _, d := range ce.Details() {
		v, derr := d.Value()
		if derr != nil {
			t.Fatalf("decode error detail: %v", derr)
		}
		if info, ok := v.(*errdetails.ErrorInfo); ok {
			t.Fatalf("error %v carries error info %v, want none", err, info)
		}
	}
}

// wantRetry checks that err carries a google.rpc.RetryInfo and returns its delay
func wantRetry(t *testing.T, err error) time.Duration {
	t.Helper()
	var ce *connect.Error
	if !errors.As(err, &ce) {
		t.Fatalf("error %v is not a Connect error", err)
	}
	for _, d := range ce.Details() {
		v, derr := d.Value()
		if derr != nil {
			t.Fatalf("decode error detail: %v", derr)
		}
		if info, ok := v.(*errdetails.RetryInfo); ok {
			return info.GetRetryDelay().AsDuration()
		}
	}
	t.Fatalf("error %v has no RetryInfo detail", err)
	return 0
}

func httpGet(t *testing.T, url string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(body)
}

/* Tests */

func TestUnauthenticatedRejected(t *testing.T) {
	e := newEnv(t, options{})
	estimate := connect.NewRequest(&routerv1.EstimateRequest{})
	for name, value := range map[string]string{
		"no header":      "",
		"wrong token":    "Bearer wrong-token-0123456789abcdef0123456789",
		"token prefix":   "Bearer " + internalToken[:len(internalToken)-1],
		"no scheme":      internalToken,
		"another scheme": "Basic " + internalToken,
	} {
		c := e.clientWith(t, value, true)
		_, err := c.Estimate(t.Context(), estimate)
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("%s: estimate gave %v, want unauthenticated", name, err)
		}
		_, err = c.CheckExtendPlacement(t.Context(), connect.NewRequest(
			&routerv1.CheckExtendPlacementRequest{
				Handle: "sbx_aaaaaaaaaaaaaaaaaaaaaaaaaa", PackageId: morePkg,
			}))
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("%s: check extend gave %v, want unauthenticated", name, err)
		}
		_, err = c.ListExtensions(t.Context(), connect.NewRequest(
			&routerv1.ListExtensionsRequest{Handle: "sbx_aaaaaaaaaaaaaaaaaaaaaaaaaa"}))
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("%s: list extensions gave %v, want unauthenticated", name, err)
		}
		stream, err := c.WatchPlacement(t.Context(), connect.NewRequest(
			&routerv1.WatchPlacementRequest{Handle: "sbx_aaaaaaaaaaaaaaaaaaaaaaaaaa"}))
		if err == nil {
			for stream.Receive() {
				t.Errorf("%s: watch sent %v", name, stream.Msg())
			}
			err = stream.Err()
			_ = stream.Close()
		}
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("%s: watch gave %v, want unauthenticated", name, err)
		}
	}
	lower := e.clientWith(t, "bearer "+internalToken, true)
	if _, err := lower.Estimate(t.Context(), estimate); err != nil {
		t.Errorf("lowercase scheme: %v", err)
	}
	if _, err := e.client.Estimate(t.Context(), estimate); err != nil {
		t.Fatalf("estimate with the token: %v", err)
	}
	for _, path := range []string{"/healthz", "/readyz"} {
		if code, body := httpGet(t, e.url+path); code != http.StatusOK {
			t.Errorf("%s without a token: %d %s", path, code, body)
		}
	}
}

func TestServesHTTP1AndH2C(t *testing.T) {
	e := newEnv(t, options{})
	for _, h2c := range []bool{true, false} {
		c := e.clientWith(t, "Bearer "+internalToken, h2c)
		_, err := c.Estimate(t.Context(), connect.NewRequest(&routerv1.EstimateRequest{}))
		if err != nil {
			t.Fatalf("estimate (h2c %v): %v", h2c, err)
		}
		want := "HTTP/1.1"
		if h2c {
			want = "HTTP/2.0"
		}
		if got := e.proto.Load(); got != want {
			t.Fatalf("request (h2c %v) arrived over %v, want %s", h2c, got, want)
		}
	}
}

func TestCreateGetReadyExecStop(t *testing.T) {
	e := newEnv(t, options{})
	h, created := e.create(t)
	if !strings.HasPrefix(h, "sbx_") || created.GetPlacementId() == "" ||
		created.GetState() != routerv1.PlacementState_PLACEMENT_STATE_PLACING ||
		created.GetPackageId() != smallPkg || created.GetCreatedAt() == nil {
		t.Fatalf("created %q %v", h, created)
	}

	ready := e.waitReady(t, h)
	if ready.GetPlacementId() != created.GetPlacementId() || ready.GetReadyAt() == nil {
		t.Fatalf("ready placement %v", ready)
	}
	if got := ready.GetDeadline().AsTime().Sub(ready.GetReadyAt().AsTime()); got != 10*time.Minute {
		t.Fatalf("deadline is %v after ready, want 10m", got)
	}

	req := sh(h, "echo out; echo err >&2; exit 3")
	req.IdempotencyKey = "first"
	res := e.exec(t, req)
	if res.GetState() != routerv1.JobState_JOB_STATE_EXITED || res.ExitCode == nil ||
		res.GetExitCode() != 3 || string(res.GetStdoutTail()) != "out\n" ||
		string(res.GetStderrTail()) != "err\n" || res.GetJobId() == "" ||
		res.GetCursor() == "" || res.GetTruncated() {
		t.Fatalf("exec result %v", res)
	}
	again := e.exec(t, req)
	if again.GetJobId() != res.GetJobId() || string(again.GetStdoutTail()) != "out\n" {
		t.Fatalf("re-sent exec %v, want job %s again", again, res.GetJobId())
	}

	ext, err := e.client.ExtendPlacement(t.Context(), connect.NewRequest(
		&routerv1.ExtendPlacementRequest{
			Handle: h, OrderToken: e.extendToken(t, h, morePkg, morePrice),
		}))
	if err != nil {
		t.Fatalf("extend: %v", err)
	}
	moved := ext.Msg.GetPlacement().GetDeadline().AsTime().Sub(ready.GetDeadline().AsTime())
	if moved != 10*time.Minute {
		t.Fatalf("extend moved the deadline by %v, want 10m", moved)
	}

	stopped := e.stop(t, h)
	if stopped.GetState() != routerv1.PlacementState_PLACEMENT_STATE_TERMINATED {
		t.Fatalf("stopped placement %v", stopped)
	}
	if p := e.get(t, h, 0, 0); p.GetState() != routerv1.PlacementState_PLACEMENT_STATE_TERMINATED {
		t.Fatalf("placement after stop %v", p)
	}
	if again := e.stop(t, h); again.GetState() != stopped.GetState() {
		t.Fatalf("second stop %v", again)
	}
	_, err = e.client.Exec(t.Context(), connect.NewRequest(sh(h, "echo late")))
	wantCode(t, err, connect.CodeFailedPrecondition)
}

func (e *env) checkExtend(
	t *testing.T, h, pkg string,
) (*connect.Response[routerv1.CheckExtendPlacementResponse], error) {
	t.Helper()
	return e.client.CheckExtendPlacement(t.Context(), connect.NewRequest(
		&routerv1.CheckExtendPlacementRequest{Handle: h, PackageId: pkg}))
}

func TestCheckExtendThenExtend(t *testing.T) {
	e := newEnv(t, options{})
	h, ready := e.readyPlacement(t)
	check, err := e.checkExtend(t, h, morePkg)
	if err != nil {
		t.Fatalf("check extend: %v", err)
	}
	if !proto.Equal(check.Msg.GetPlacement(), ready) {
		t.Fatalf("check gave placement %v, want it as it is %v", check.Msg.GetPlacement(), ready)
	}
	would := check.Msg.GetDeadline().AsTime()
	if moved := would.Sub(ready.GetDeadline().AsTime()); moved != 10*time.Minute {
		t.Fatalf("check would move the deadline by %v, want 10m", moved)
	}
	more := &routerv1.PackageOption{
		PackageId: morePkg, Resources: smallResources, Minutes: 10, PriceMicros: morePrice,
		Available: true,
	}
	if !proto.Equal(check.Msg.GetPackage(), more) {
		t.Fatalf("check gave package %v, want %v", check.Msg.GetPackage(), more)
	}
	if p := e.get(t, h, 0, 0); !proto.Equal(p, ready) {
		t.Fatalf("placement after the check %v, want %v", p, ready)
	}

	ext, err := e.client.ExtendPlacement(t.Context(), connect.NewRequest(
		&routerv1.ExtendPlacementRequest{
			Handle: h, OrderToken: e.extendToken(t, h, morePkg, morePrice),
		}))
	if err != nil {
		t.Fatalf("extend after the check: %v", err)
	}
	if got := ext.Msg.GetPlacement().GetDeadline().AsTime(); !got.Equal(would) {
		t.Fatalf("extend gave deadline %v, the check said %v", got, would)
	}
}

func TestCheckExtendOfAPackageTheServerLacks(t *testing.T) {
	without := strings.Replace(catalogJSON, `"small-10m-more"`, `"small-10m-other"`, 1)
	e := newEnv(t, options{apiCatalog: without})
	h, _ := e.readyPlacement(t)
	res, err := e.checkExtend(t, h, morePkg)
	if connect.CodeOf(err) != connect.CodeInternal || strings.Contains(err.Error(), morePkg) {
		t.Fatalf("check of a package only the saga has gave %v, %v, want an internal error "+
			"without the package id", res, err)
	}
}

// smallResources are the resources of every small package in catalogJSON
var smallResources = &routerv1.Resources{Vcpu: 2, MemoryMib: 2048, DiskGib: 10}

func (e *env) listExtensions(t *testing.T, h string) ([]*routerv1.PackageOption, error) {
	t.Helper()
	res, err := e.client.ListExtensions(t.Context(), connect.NewRequest(
		&routerv1.ListExtensionsRequest{Handle: h}))
	if err != nil {
		return nil, err
	}
	return res.Msg.GetOptions(), nil
}

// smallExtensions are the options ListExtensions gives a small-10m placement, with reason
func smallExtensions(reason string) []*routerv1.PackageOption {
	return []*routerv1.PackageOption{
		{
			PackageId: morePkg, Resources: smallResources, Minutes: 10, PriceMicros: morePrice,
			Available: reason == "", UnavailableReason: reason,
		},
		{
			PackageId: smallPkg, Resources: smallResources, Minutes: 10, PriceMicros: smallPrice,
			Available: reason == "", UnavailableReason: reason,
		},
	}
}

func equalOptions(a, b []*routerv1.PackageOption) bool {
	return slices.EqualFunc(a, b, func(x, y *routerv1.PackageOption) bool {
		return proto.Equal(x, y)
	})
}

func TestListExtensions(t *testing.T) {
	e := newEnv(t, options{})
	h, ready := e.readyPlacement(t)
	opts, err := e.listExtensions(t, h)
	if err != nil {
		t.Fatalf("list extensions: %v", err)
	}
	if want := smallExtensions(""); !equalOptions(opts, want) {
		t.Fatalf("listed %v, want %v", opts, want)
	}
	for _, o := range opts {
		check, err := e.checkExtend(t, h, o.GetPackageId())
		if err != nil || !proto.Equal(check.Msg.GetPackage(), o) {
			t.Errorf("listed %v, the check gave %v, %v", o, check.Msg.GetPackage(), err)
		}
	}
	if p := e.get(t, h, 0, 0); !proto.Equal(p, ready) {
		t.Fatalf("placement after the listing %v, want %v", p, ready)
	}

	ended, _ := e.readyPlacement(t)
	e.stop(t, ended)
	if opts, err := e.listExtensions(t, ended); err != nil ||
		!equalOptions(opts, smallExtensions("not_ready")) {
		t.Errorf("terminated placement listed %v, %v, want every option not_ready", opts, err)
	}
	capped := newEnv(t, options{maxLifetime: 17 * time.Minute})
	ch, _ := capped.readyPlacement(t)
	if opts, err := capped.listExtensions(t, ch); err != nil ||
		!equalOptions(opts, smallExtensions("exceeds_lifetime")) {
		t.Errorf("capped placement listed %v, %v, want every option exceeds_lifetime", opts, err)
	}
	unknown, _ := handle.New()
	for _, bad := range []string{unknown, "nope", "sbx_not-a-handle"} {
		_, err := e.listExtensions(t, bad)
		wantCode(t, err, connect.CodeNotFound)
	}
}

func TestGetPlacementLongPollWakes(t *testing.T) {
	e := newEnv(t, options{readyDelay: time.Second})
	h, _ := e.create(t)
	start := time.Now()
	p := e.get(t, h, routerv1.PlacementState_PLACEMENT_STATE_PLACING, 10000)
	returned := time.Now()
	if p.GetState() != routerv1.PlacementState_PLACEMENT_STATE_READY {
		t.Fatalf("long poll returned %v, want ready", p)
	}
	if waited := returned.Sub(start); waited < 500*time.Millisecond {
		t.Fatalf("long poll returned after %v, before the sandbox could be ready", waited)
	}
	if lag := returned.Sub(p.GetReadyAt().AsTime()); lag > 200*time.Millisecond {
		t.Fatalf("long poll returned %v after the placement became ready", lag)
	}
}

func TestGetPlacementWithoutChange(t *testing.T) {
	e := newEnv(t, options{})
	h, _ := e.readyPlacement(t)
	for name, known := range map[string]routerv1.PlacementState{
		"unspecified": routerv1.PlacementState_PLACEMENT_STATE_UNSPECIFIED,
		"stale":       routerv1.PlacementState_PLACEMENT_STATE_PLACING,
	} {
		start := time.Now()
		p := e.get(t, h, known, 10000)
		if took := time.Since(start); took > time.Second ||
			p.GetState() != routerv1.PlacementState_PLACEMENT_STATE_READY {
			t.Fatalf("%s known state: %v after %v, want ready at once", name, p, took)
		}
	}
	start := time.Now()
	p := e.get(t, h, routerv1.PlacementState_PLACEMENT_STATE_READY, 300)
	took := time.Since(start)
	if p.GetState() != routerv1.PlacementState_PLACEMENT_STATE_READY ||
		took < 300*time.Millisecond || took > 2*time.Second {
		t.Fatalf("unchanged placement: %v after %v, want ready after 300ms", p, took)
	}
}

func TestGetPlacementEndedReturnsAtOnce(t *testing.T) {
	e := newEnv(t, options{})
	stopped, _ := e.readyPlacement(t)
	e.stop(t, stopped)
	order, tok := e.startOrder(t, gpuPkg, gpuPrice)
	_, err := e.createWith(t, tok)
	wantCode(t, err, connect.CodeResourceExhausted)
	failed, _ := handle.Derive(e.handleKey, order)
	for name, c := range map[string]struct {
		handle string
		state  routerv1.PlacementState
	}{
		"terminated": {stopped, routerv1.PlacementState_PLACEMENT_STATE_TERMINATED},
		"failed":     {failed, routerv1.PlacementState_PLACEMENT_STATE_FAILED},
	} {
		t.Run(name, func(t *testing.T) {
			start := time.Now()
			p := e.get(t, c.handle, c.state, 5000)
			if took := time.Since(start); took > time.Second || p.GetState() != c.state {
				t.Fatalf("known state %v: %v after %v, want it at once", c.state, p, took)
			}
		})
	}
}

// subscription waits for the API server's pattern subscription on a connection other than not
func (e *env) subscription(t *testing.T, not string) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if id := e.subscribed(t); id != "" && id != not {
			return id
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no pattern subscription from %s besides %q", e.apiVKName, not)
	return ""
}

// subscribed returns the ID of the API server's connection holding a pattern subscription
func (e *env) subscribed(t *testing.T) string {
	t.Helper()
	list, err := e.vk.Do(t.Context(), e.vk.B().ClientList().Build()).ToString()
	if err != nil {
		t.Fatalf("client list: %v", err)
	}
	for line := range strings.Lines(list) {
		f := map[string]string{}
		for _, kv := range strings.Fields(line) {
			k, v, _ := strings.Cut(kv, "=")
			f[k] = v
		}
		if f["name"] == e.apiVKName && f["psub"] != "" && f["psub"] != "0" {
			return f["id"]
		}
	}
	return ""
}

func TestGetPlacementWakesAfterResubscribe(t *testing.T) {
	for _, c := range []struct {
		name         string
		resubscribed bool
	}{
		{"ready while unsubscribed", false},
		{"ready after resubscribing", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			hold := make(chan struct{})
			release := sync.OnceFunc(func() { close(hold) })
			e := newEnv(t, options{holdReady: hold})
			t.Cleanup(release)
			old := e.subscription(t, "")
			h, _ := e.create(t)
			polled := make(chan *routerv1.Placement, 1)
			go func() {
				resp, err := e.client.GetPlacement(t.Context(), connect.NewRequest(
					&routerv1.GetPlacementRequest{
						Handle: h, KnownState: routerv1.PlacementState_PLACEMENT_STATE_PLACING,
						WaitMs: 20000,
					}))
				if err != nil {
					polled <- nil
					return
				}
				polled <- resp.Msg.GetPlacement()
			}()
			time.Sleep(300 * time.Millisecond)

			kill := e.vk.B().Arbitrary("CLIENT", "KILL").Args("ID", old).Build()
			if n, err := e.vk.Do(t.Context(), kill).AsInt64(); err != nil || n != 1 {
				t.Fatalf("kill subscription connection %s: %d %v", old, n, err)
			}
			if c.resubscribed {
				e.subscription(t, old)
			}
			release()
			released := time.Now()
			if !c.resubscribed {
				e.awaitState(t, h, store.PlacementReady)
			}

			select {
			case p := <-polled:
				if p.GetState() != routerv1.PlacementState_PLACEMENT_STATE_READY {
					t.Fatalf("long poll returned %v, want ready", p)
				}
			case <-time.After(3 * time.Second):
				t.Fatalf("long poll still waiting %v after the placement was released",
					time.Since(released))
			}
			if id := e.subscribed(t); id == "" || id == old {
				t.Fatalf("subscription is on %q after the poll, want a new connection", id)
			}
		})
	}
}

// awaitState reads the store until the placement behind h is in state
func (e *env) awaitState(t *testing.T, h, state string) {
	t.Helper()
	sha, err := handle.Hash(h)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		p, err := e.store.PlacementByHandle(t.Context(), sha)
		if err != nil {
			t.Fatalf("read placement: %v", err)
		}
		if p.State == state {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("placement never reached %s", state)
}

func TestWatchPlacementStreamsChanges(t *testing.T) {
	e := newEnv(t, options{readyDelay: 500 * time.Millisecond})
	h, _ := e.create(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	stream, err := e.client.WatchPlacement(ctx, connect.NewRequest(
		&routerv1.WatchPlacementRequest{Handle: h}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()
	var states []routerv1.PlacementState
	for stream.Receive() {
		p := stream.Msg().GetPlacement()
		states = append(states, p.GetState())
		if p.GetState() == routerv1.PlacementState_PLACEMENT_STATE_READY {
			e.stop(t, h)
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("watch ended with %v after %v", err, states)
	}
	states = slices.DeleteFunc(states, func(s routerv1.PlacementState) bool {
		return s == routerv1.PlacementState_PLACEMENT_STATE_TERMINATING
	})
	want := []routerv1.PlacementState{
		routerv1.PlacementState_PLACEMENT_STATE_PLACING,
		routerv1.PlacementState_PLACEMENT_STATE_READY,
		routerv1.PlacementState_PLACEMENT_STATE_TERMINATED,
	}
	if !slices.Equal(states, want) {
		t.Fatalf("watch streamed %v, want %v", states, want)
	}
}

func TestEstimate(t *testing.T) {
	e := newEnv(t, options{})
	resp, err := e.client.Estimate(t.Context(), connect.NewRequest(&routerv1.EstimateRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, o := range resp.Msg.GetOptions() {
		ids = append(ids, o.GetPackageId())
	}
	if want := []string{smallPkg, longPkg, gpuPkg}; !slices.Equal(ids, want) {
		t.Fatalf("options %v, want start packages %v by price", ids, want)
	}
	first := resp.Msg.GetOptions()[0]
	r := first.GetResources()
	if !first.GetAvailable() || first.GetUnavailableReason() != "" || first.GetMinutes() != 10 ||
		first.GetPriceMicros() != smallPrice || r.GetVcpu() != 2 || r.GetMemoryMib() != 2048 ||
		r.GetDiskGib() != 10 {
		t.Fatalf("small option %v", first)
	}
	gpu := resp.Msg.GetOptions()[2]
	if gpu.GetAvailable() || gpu.GetUnavailableReason() != "feature_unavailable" ||
		gpu.GetResources().GetGpuCount() != 1 || gpu.GetResources().GetGpuMemoryGib() != 24 {
		t.Fatalf("gpu option %v, want unavailable", gpu)
	}
	ssh, err := e.client.Estimate(t.Context(), connect.NewRequest(&routerv1.EstimateRequest{
		Requirements: &routerv1.Requirements{Ssh: true},
	}))
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range ssh.Msg.GetOptions() {
		if o.GetAvailable() {
			t.Fatalf("%s is available with ssh on a provider without ssh", o.GetPackageId())
		}
	}
}

type watched struct {
	stdout, stderr []byte
	exit           *routerv1.JobExit
	skips          []*routerv1.JobSkip
	cursors        []string
}

// watchJob collects a WatchJob stream, checking that offsets and cursors only move forward
func (e *env) watchJob(t *testing.T, req *routerv1.WatchJobRequest) (watched, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	stream, err := e.client.WatchJob(ctx, connect.NewRequest(req))
	if err != nil {
		return watched{}, err
	}
	defer func() { _ = stream.Close() }()
	var w watched
	offsets := map[routerv1.OutputStream]uint64{}
	for stream.Receive() {
		m := stream.Msg()
		if m.GetCursor() == "" || (len(w.cursors) > 0 &&
			!cursorBefore(w.cursors[len(w.cursors)-1], m.GetCursor())) {
			t.Fatalf("cursor %q after %v", m.GetCursor(), w.cursors)
		}
		w.cursors = append(w.cursors, m.GetCursor())
		if w.exit != nil {
			t.Fatalf("frame %v after the exit", m)
		}
		switch {
		case m.GetOutput() != nil:
			o := m.GetOutput()
			// A watch from a cursor starts each stream wherever the cursor left it
			if _, seen := offsets[o.GetStream()]; !seen && req.GetCursor() != "" {
				offsets[o.GetStream()] = o.GetOffset()
			}
			if o.GetOffset() != offsets[o.GetStream()] {
				t.Fatalf("output at offset %d, want %d", o.GetOffset(), offsets[o.GetStream()])
			}
			offsets[o.GetStream()] += uint64(len(o.GetData()))
			switch o.GetStream() {
			case routerv1.OutputStream_OUTPUT_STREAM_STDOUT:
				w.stdout = append(w.stdout, o.GetData()...)
			case routerv1.OutputStream_OUTPUT_STREAM_STDERR:
				w.stderr = append(w.stderr, o.GetData()...)
			default:
				t.Fatalf("output on stream %v", o.GetStream())
			}
		case m.GetSkip() != nil:
			sk := m.GetSkip()
			out, errOut := routerv1.OutputStream_OUTPUT_STREAM_STDOUT,
				routerv1.OutputStream_OUTPUT_STREAM_STDERR
			if sk.GetStdoutOffset() < offsets[out] || sk.GetStderrOffset() < offsets[errOut] {
				t.Fatalf("skip %v goes back from %v", sk, offsets)
			}
			offsets[out], offsets[errOut] = sk.GetStdoutOffset(), sk.GetStderrOffset()
			w.skips = append(w.skips, sk)
		case m.GetExit() != nil:
			w.exit = m.GetExit()
		}
	}
	return w, stream.Err()
}

func cursorBefore(a, b string) bool {
	parse := func(c string) (int64, int64) {
		ms, seq, _ := strings.Cut(c, "-")
		x, _ := strconv.ParseInt(ms, 10, 64)
		y, _ := strconv.ParseInt(seq, 10, 64)
		return x, y
	}
	am, as := parse(a)
	bm, bs := parse(b)
	return am < bm || (am == bm && as < bs)
}

func numbered(prefix string, n int) string {
	var b strings.Builder
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&b, "%s %d\n", prefix, i)
	}
	return b.String()
}

func TestWatchJobStreamsAllOutput(t *testing.T) {
	e := newEnv(t, options{})
	h, _ := e.readyPlacement(t)
	req := sh(h, `for i in $(seq 1 200); do echo "out $i"; echo "err $i" >&2; done
		sleep 0.3; echo "out done"; exit 7`)
	req.Until = &routerv1.Until{OutputPattern: `out 200\n`}
	res := e.exec(t, req)
	if res.GetState() != routerv1.JobState_JOB_STATE_RUNNING {
		t.Fatalf("exec until the pattern %v, want running", res)
	}
	wantOut := numbered("out", 200) + "out done\n"
	wantErr := numbered("err", 200)

	all, err := e.watchJob(t, &routerv1.WatchJobRequest{Handle: h, JobId: res.GetJobId()})
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	if string(all.stdout) != wantOut || string(all.stderr) != wantErr {
		t.Fatalf("watched stdout %q stderr %q", all.stdout, all.stderr)
	}
	if all.exit == nil || all.exit.GetExitCode() != 7 ||
		all.exit.GetStdoutBytes() != uint64(len(wantOut)) ||
		all.exit.GetStderrBytes() != uint64(len(wantErr)) {
		t.Fatalf("exit frame %v", all.exit)
	}

	rest, err := e.watchJob(t, &routerv1.WatchJobRequest{
		Handle: h, JobId: res.GetJobId(), Cursor: res.GetCursor(),
	})
	if err != nil {
		t.Fatalf("watch from the exec cursor: %v", err)
	}
	if got := string(res.GetStdoutTail()) + string(rest.stdout); got != wantOut {
		t.Fatalf("exec tail and the watch from its cursor give %q", got)
	}
	if got := string(res.GetStderrTail()) + string(rest.stderr); got != wantErr {
		t.Fatalf("exec tail and the watch from its cursor give stderr %q", got)
	}
	if rest.exit == nil || rest.exit.GetExitCode() != 7 {
		t.Fatalf("exit frame from the cursor %v", rest.exit)
	}

	early, err := e.watchJob(t, &routerv1.WatchJobRequest{
		Handle: h, JobId: res.GetJobId(), Until: &routerv1.Until{OutputPattern: `out 5\n`},
	})
	if err != nil {
		t.Fatalf("watch until a pattern: %v", err)
	}
	if early.exit != nil || !strings.Contains(string(early.stdout), "out 5\n") ||
		strings.Contains(string(early.stdout), "out done") {
		t.Fatalf("watch until out 5 gave %q and exit %v", early.stdout, early.exit)
	}
}

func TestFloodingJobReportsWhatItSkipped(t *testing.T) {
	e := newEnv(t, options{jobBudget: 128 << 10})
	h, _ := e.readyPlacement(t)
	req := sh(h, `head -c 2000000 /dev/zero | tr '\0' a; echo; echo end`)
	req.Until = &routerv1.Until{Exit: true}
	res := e.exec(t, req)
	if res.GetState() != routerv1.JobState_JOB_STATE_EXITED || !res.GetTruncated() ||
		!strings.HasSuffix(string(res.GetStdoutTail()), "a\nend\n") {
		t.Fatalf("exec of a flood gave state %v truncated %v", res.GetState(), res.GetTruncated())
	}
	w, err := e.watchJob(t, &routerv1.WatchJobRequest{Handle: h, JobId: res.GetJobId()})
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	if len(w.skips) == 0 || w.exit == nil || w.exit.GetStdoutBytes() != 2000005 ||
		!strings.HasSuffix(string(w.stdout), "a\nend\n") {
		t.Fatalf("watch of a flood gave %d skips and exit %v", len(w.skips), w.exit)
	}
	if len(w.stdout) >= 2000005 {
		t.Fatalf("watch of a flood sent all %d bytes, want it to skip most", len(w.stdout))
	}
}

func TestWatchJobMapsFrames(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	ctx := t.Context()
	id, err := uuid.Parse(p.GetPlacementId())
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = e.store.CreateJob(ctx, store.NewJob{ID: "j_frames", PlacementID: id})
	if err != nil {
		t.Fatal(err)
	}
	lease, ok, err := streams.NewBus(e.vk, e.prefix).AcquireLease(ctx, "j_frames", "writer")
	if err != nil || !ok {
		t.Fatalf("acquire: %v, %v", ok, err)
	}
	var ids []string
	for _, f := range []frame.Frame{
		{Kind: frame.KindStdout, Data: []byte("a")},
		{Kind: frame.KindHeartbeat, Out: 1},
		{Kind: frame.KindError, Reason: frame.ReasonJobExists},
		{Kind: frame.KindStderr, Data: []byte("bc")},
		{Kind: frame.KindSkip, Out: 9, Err: 2},
		{Kind: frame.KindExit, Code: 2, Out: 9, Err: 2, OOM: true, PeakMiB: 700},
	} {
		entry, err := lease.Append(ctx, f)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, entry)
	}
	stream, err := e.client.WatchJob(ctx, connect.NewRequest(
		&routerv1.WatchJobRequest{Handle: h, JobId: "j_frames"}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()
	var got []string
	for stream.Receive() {
		m := stream.Msg()
		switch {
		case m.GetOutput() != nil:
			o := m.GetOutput()
			got = append(got, fmt.Sprintf("%s %v %d %s", m.GetCursor(), o.GetStream(),
				o.GetOffset(), o.GetData()))
		case m.GetExit() != nil:
			x := m.GetExit()
			got = append(got, fmt.Sprintf("%s exit %d %d %d oom %v peak %d", m.GetCursor(),
				x.GetExitCode(), x.GetStdoutBytes(), x.GetStderrBytes(), x.GetOomKilled(),
				x.GetPeakMemoryMib()))
		case m.GetHeartbeat():
			got = append(got, m.GetCursor()+" heartbeat")
		case m.GetSkip() != nil:
			sk := m.GetSkip()
			got = append(got, fmt.Sprintf("%s skip %d %d", m.GetCursor(), sk.GetStdoutOffset(),
				sk.GetStderrOffset()))
		default:
			t.Fatalf("message without a frame %v", m)
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	want := []string{
		ids[0] + " OUTPUT_STREAM_STDOUT 0 a",
		ids[1] + " heartbeat",
		ids[3] + " OUTPUT_STREAM_STDERR 0 bc",
		ids[4] + " skip 9 2",
		ids[5] + " exit 2 9 2 oom true peak 700",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("watch sent %q, want %q", got, want)
	}
}

func TestCancelJob(t *testing.T) {
	e := newEnv(t, options{})
	h, _ := e.readyPlacement(t)
	req := sh(h, "echo up; sleep 30")
	req.Until = &routerv1.Until{OutputPattern: "up"}
	res := e.exec(t, req)
	resp, err := e.client.CancelJob(t.Context(), connect.NewRequest(
		&routerv1.CancelJobRequest{Handle: h, JobId: res.GetJobId()}))
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if resp.Msg.GetState() != routerv1.JobState_JOB_STATE_CANCELLED {
		t.Fatalf("cancelled job is %v", resp.Msg.GetState())
	}
}

func TestFileRoundTripAndLimits(t *testing.T) {
	e := newEnv(t, options{})
	h, _ := e.readyPlacement(t)
	ctx := t.Context()
	write := func(path string, data []byte, mode uint32) error {
		resp, err := e.client.WriteFile(ctx, connect.NewRequest(&routerv1.WriteFileRequest{
			Handle: h, Path: path, Data: data, Mode: mode,
		}))
		if err == nil && resp.Msg.GetSize() != uint64(len(data)) {
			t.Fatalf("write %s reported size %d, want %d", path, resp.Msg.GetSize(), len(data))
		}
		return err
	}
	read := func(path string, offset uint64, length uint32) ([]byte, bool, error) {
		resp, err := e.client.ReadFile(ctx, connect.NewRequest(&routerv1.ReadFileRequest{
			Handle: h, Path: path, Offset: offset, Length: length,
		}))
		if err != nil {
			return nil, false, err
		}
		return resp.Msg.GetData(), resp.Msg.GetEof(), nil
	}
	list := func(path string, depth uint32) ([]*routerv1.FileEntry, error) {
		resp, err := e.client.ListFiles(ctx, connect.NewRequest(&routerv1.ListFilesRequest{
			Handle: h, Path: path, Depth: depth,
		}))
		if err != nil {
			return nil, err
		}
		return resp.Msg.GetEntries(), nil
	}

	if err := write("/root/dir/a.txt", []byte("hello"), 0); err != nil {
		t.Fatal(err)
	}
	if err := write("/root/dir/sub/run.sh", []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if data, eof, err := read("/root/dir/a.txt", 0, 0); err != nil || string(data) != "hello" ||
		!eof {
		t.Fatalf("read whole file: %q %v %v", data, eof, err)
	}
	if data, eof, err := read("/root/dir/a.txt", 1, 2); err != nil || string(data) != "el" || eof {
		t.Fatalf("read a range: %q %v %v", data, eof, err)
	}

	entries, err := list("/root/dir", 0)
	if err != nil {
		t.Fatal(err)
	}
	byPath := map[string]*routerv1.FileEntry{}
	for _, en := range entries {
		byPath[en.GetPath()] = en
	}
	file, dir := byPath["/root/dir/a.txt"], byPath["/root/dir/sub"]
	if len(entries) != 2 || file == nil || dir == nil || file.GetIsDir() || !dir.GetIsDir() ||
		file.GetSize() != 5 || file.GetMode() != 0o644 || file.GetModifiedAt() == nil {
		t.Fatalf("depth 1 listing %v", entries)
	}
	deep, err := list("/root/dir", 5)
	if err != nil {
		t.Fatal(err)
	}
	var script *routerv1.FileEntry
	for _, en := range deep {
		if en.GetPath() == "/root/dir/sub/run.sh" {
			script = en
		}
	}
	if len(deep) != 3 || script == nil || script.GetMode() != 0o755 {
		t.Fatalf("depth 5 listing %v", deep)
	}
	_, err = list("/root/dir", 6)
	wantCode(t, err, connect.CodeInvalidArgument)

	if err := write("/root/max.bin", bytes.Repeat([]byte("x"), maxFile), 0); err != nil {
		t.Fatalf("write of exactly 256 KiB: %v", err)
	}
	wantCode(t, write("/root/over.bin", make([]byte, maxFile+1), 0), connect.CodeInvalidArgument)
	// Requests over 4 MiB are refused before they are read into memory
	wantCode(t, write("/root/huge.bin", make([]byte, 5<<20), 0), connect.CodeResourceExhausted)
	wantCode(t, write("/root/setuid", []byte("x"), 0o4755), connect.CodeInvalidArgument)
	_, _, err = read("/root/max.bin", 0, maxFile+1)
	wantCode(t, err, connect.CodeInvalidArgument)

	made := e.exec(t, sh(h, "head -c 300000 /dev/zero > big.bin"))
	if made.GetExitCode() != 0 {
		t.Fatalf("make a big file: %v", made)
	}
	data, eof, err := read("/root/big.bin", 0, 0)
	if err != nil || len(data) != maxFile || eof {
		t.Fatalf("default read of a big file: %d bytes, eof %v, %v", len(data), eof, err)
	}
	data, eof, err = read("/root/big.bin", maxFile, 0)
	if err != nil || len(data) != 300000-maxFile || !eof {
		t.Fatalf("read of the rest: %d bytes, eof %v, %v", len(data), eof, err)
	}

	if made := e.exec(t, sh(h, "mkdir many && cd many && seq 10001 | xargs touch")); made.
		GetExitCode() != 0 {
		t.Fatalf("make a wide directory: %v", made)
	}
	wide, err := e.client.ListFiles(ctx, connect.NewRequest(&routerv1.ListFilesRequest{
		Handle: h, Path: "/root/many", Depth: 1,
	}))
	if err != nil || len(wide.Msg.GetEntries()) != 10000 || !wide.Msg.GetTruncated() {
		t.Fatalf("listing of 10001 files gave %d entries truncated %v, %v, want 10000 truncated",
			len(wide.Msg.GetEntries()), wide.Msg.GetTruncated(), err)
	}
	if whole, err := e.client.ListFiles(ctx, connect.NewRequest(&routerv1.ListFilesRequest{
		Handle: h, Path: "/root/dir", Depth: 5,
	})); err != nil || whole.Msg.GetTruncated() {
		t.Fatalf("listing within the limit gave truncated %v, %v", whole.Msg.GetTruncated(), err)
	}

	e.stop(t, h)
	_, _, err = read("/root/dir/a.txt", 0, 0)
	wantCode(t, err, connect.CodeFailedPrecondition)
	wantCode(t, write("/root/dir/a.txt", []byte("x"), 0), connect.CodeFailedPrecondition)
	_, err = list("/root", 1)
	wantCode(t, err, connect.CodeFailedPrecondition)
}

func TestErrorCodes(t *testing.T) {
	e := newEnv(t, options{})
	h, _ := e.readyPlacement(t)
	ctx := t.Context()
	get := func(h string) error {
		_, err := e.client.GetPlacement(ctx, connect.NewRequest(
			&routerv1.GetPlacementRequest{Handle: h}))
		return err
	}
	extend := func(h, tok string) error {
		_, err := e.client.ExtendPlacement(ctx, connect.NewRequest(
			&routerv1.ExtendPlacementRequest{Handle: h, OrderToken: tok}))
		return err
	}
	checkErr := func(h, pkg string) error {
		_, err := e.checkExtend(t, h, pkg)
		return err
	}
	execErr := func(req *routerv1.ExecRequest) error {
		_, err := e.client.Exec(ctx, connect.NewRequest(req))
		return err
	}
	readErr := func(h, path string) error {
		_, err := e.client.ReadFile(ctx, connect.NewRequest(
			&routerv1.ReadFileRequest{Handle: h, Path: path}))
		return err
	}
	watchErr := func(req *routerv1.WatchJobRequest) error {
		_, err := e.watchJob(t, req)
		return err
	}
	notReady := func(t *testing.T, err error) {
		t.Helper()
		wantCode(t, err, connect.CodeFailedPrecondition)
		wantInfo(t, err, "NOT_READY")
	}

	t.Run("unknown handle is not found", func(t *testing.T) {
		unknown, _ := handle.New()
		wantCode(t, get(unknown), connect.CodeNotFound)
		wantCode(t, execErr(sh(unknown, "true")), connect.CodeNotFound)
		wantCode(t, extend(unknown, e.extendToken(t, unknown, morePkg, morePrice)),
			connect.CodeNotFound)
		wantCode(t, checkErr(unknown, morePkg), connect.CodeNotFound)
	})
	t.Run("malformed handle is not found", func(t *testing.T) {
		wantCode(t, get("sbx_not-a-handle"), connect.CodeNotFound)
		_, err := e.client.StopPlacement(ctx, connect.NewRequest(
			&routerv1.StopPlacementRequest{Handle: "nope"}))
		wantCode(t, err, connect.CodeNotFound)
		wantCode(t, readErr("", "/root"), connect.CodeNotFound)
		wantCode(t, checkErr("nope", morePkg), connect.CodeNotFound)
	})
	t.Run("bad or expired token is permission denied", func(t *testing.T) {
		_, err := e.createWith(t, "v1.garbage.token")
		wantCode(t, err, connect.CodePermissionDenied)
		expired := e.token(t, ordertoken.Claims{
			OrderID: "expired", Kind: ordertoken.KindStart, PackageID: smallPkg,
			AmountMicros: smallPrice, ExpiresAt: time.Now().Add(-time.Minute),
		})
		_, err = e.createWith(t, expired)
		wantCode(t, err, connect.CodePermissionDenied)
		_, otherKey, _ := ed25519.GenerateKey(rand.Reader)
		forged, _ := ordertoken.Sign(otherKey, ordertoken.Claims{
			OrderID: "forged", Kind: ordertoken.KindStart, PackageID: smallPkg,
			AmountMicros: smallPrice, ExpiresAt: time.Now().Add(time.Hour),
		})
		_, err = e.createWith(t, forged)
		wantCode(t, err, connect.CodePermissionDenied)
		wantCode(t, extend(h, e.startToken(t, smallPkg, smallPrice)),
			connect.CodePermissionDenied)
	})
	t.Run("underpaid order is permission denied", func(t *testing.T) {
		_, err := e.createWith(t, e.startToken(t, smallPkg, smallPrice-1))
		wantCode(t, err, connect.CodePermissionDenied)
	})
	t.Run("extend for another handle is permission denied", func(t *testing.T) {
		other, _ := handle.New()
		wantCode(t, extend(h, e.extendToken(t, other, morePkg, morePrice)),
			connect.CodePermissionDenied)
	})
	t.Run("unknown package is invalid", func(t *testing.T) {
		_, err := e.createWith(t, e.startToken(t, "no-such-package", smallPrice))
		wantCode(t, err, connect.CodeInvalidArgument)
		wantCode(t, checkErr(h, "no-such-package"), connect.CodeInvalidArgument)
	})
	t.Run("package that does not extend the placement is invalid", func(t *testing.T) {
		wantCode(t, extend(h, e.extendToken(t, h, longPkg, longPrice)),
			connect.CodeInvalidArgument)
		wantCode(t, checkErr(h, longPkg), connect.CodeInvalidArgument)
	})
	t.Run("no candidate is resource exhausted", func(t *testing.T) {
		_, err := e.createWith(t, e.startToken(t, gpuPkg, gpuPrice))
		wantCode(t, err, connect.CodeResourceExhausted)
	})
	t.Run("placing placement is a failed precondition", func(t *testing.T) {
		slow := newEnv(t, options{readyDelay: 2 * time.Second})
		ph, _ := slow.create(t)
		_, err := slow.client.Exec(ctx, connect.NewRequest(sh(ph, "true")))
		notReady(t, err)
		_, err = slow.client.ListFiles(ctx, connect.NewRequest(
			&routerv1.ListFilesRequest{Handle: ph, Path: "/root"}))
		notReady(t, err)
		_, err = slow.client.ExtendPlacement(ctx, connect.NewRequest(
			&routerv1.ExtendPlacementRequest{
				Handle: ph, OrderToken: slow.extendToken(t, ph, morePkg, morePrice),
			}))
		notReady(t, err)
		_, err = slow.checkExtend(t, ph, morePkg)
		notReady(t, err)
		_, err = slow.client.StopPlacement(ctx, connect.NewRequest(
			&routerv1.StopPlacementRequest{Handle: ph}))
		notReady(t, err)
	})
	t.Run("terminated placement is a failed precondition", func(t *testing.T) {
		th, _ := e.readyPlacement(t)
		e.stop(t, th)
		notReady(t, execErr(sh(th, "true")))
		notReady(t, readErr(th, "/root"))
		notReady(t, extend(th, e.extendToken(t, th, morePkg, morePrice)))
		notReady(t, checkErr(th, morePkg))
	})
	t.Run("extension past the provider's max lifetime is a failed precondition",
		func(t *testing.T) {
			capped := newEnv(t, options{maxLifetime: 17 * time.Minute})
			ch, _ := capped.readyPlacement(t)
			_, extendErr := capped.client.ExtendPlacement(ctx, connect.NewRequest(
				&routerv1.ExtendPlacementRequest{
					Handle: ch, OrderToken: capped.extendToken(t, ch, morePkg, morePrice),
				}))
			wantCode(t, extendErr, connect.CodeFailedPrecondition)
			wantInfo(t, extendErr, "EXCEEDS_LIFETIME")
			_, checkErr := capped.checkExtend(t, ch, morePkg)
			wantCode(t, checkErr, connect.CodeFailedPrecondition)
			wantInfo(t, checkErr, "EXCEEDS_LIFETIME")
			// The limit can come from the settings file, which the gateway never sees
			for _, err := range []error{extendErr, checkErr} {
				if strings.Contains(err.Error(), "17m") {
					t.Errorf("refusal %q quotes the provider's max lifetime", err.Error())
				}
			}
		})
	t.Run("file over 256 KiB is invalid", func(t *testing.T) {
		_, err := e.client.WriteFile(ctx, connect.NewRequest(&routerv1.WriteFileRequest{
			Handle: h, Path: "/root/big", Data: make([]byte, maxFile+1),
		}))
		wantCode(t, err, connect.CodeInvalidArgument)
		_, err = e.client.ReadFile(ctx, connect.NewRequest(&routerv1.ReadFileRequest{
			Handle: h, Path: "/root/big", Length: maxFile + 1,
		}))
		wantCode(t, err, connect.CodeInvalidArgument)
	})
	t.Run("provider error is unavailable with its code", func(t *testing.T) {
		err := readErr(h, "/fault/rate_limited")
		wantCode(t, err, connect.CodeUnavailable)
		wantInfo(t, err, "rate_limited")
		err = readErr(h, "/fault/internal")
		wantCode(t, err, connect.CodeUnavailable)
		wantInfo(t, err, "internal")
	})
	t.Run("provider error wrapping a backend's Connect error keeps its code", func(t *testing.T) {
		for code, want := range map[string]connect.Code{
			"invalid": connect.CodeInvalidArgument, "rate_limited": connect.CodeUnavailable,
		} {
			err := readErr(h, "/fault-connect/"+code)
			wantCode(t, err, want)
			wantInfo(t, err, code)
			if strings.Contains(err.Error(), "injected") {
				t.Errorf("%s error %q carries the backend's text", code, err.Error())
			}
		}
	})
	t.Run("provider error text stays in the router", func(t *testing.T) {
		for _, code := range []string{"internal", "invalid", "not_found"} {
			err := readErr(h, "/fault/"+code)
			if msg := err.Error(); strings.Contains(msg, "injected") ||
				strings.Contains(msg, "local") || strings.Contains(msg, "sbx-1") {
				t.Errorf("%s error %q carries the provider's text", code, msg)
			}
			if code == "not_found" {
				wantInfo(t, err, "FILE_NOT_FOUND")
				continue
			}
			wantInfo(t, err, code)
		}
	})
	t.Run("missing file is not found with its own reason", func(t *testing.T) {
		err := readErr(h, "/root/missing.txt")
		wantCode(t, err, connect.CodeNotFound)
		wantInfo(t, err, "FILE_NOT_FOUND")
	})
	t.Run("provider invalid is invalid", func(t *testing.T) {
		err := readErr(h, "/root")
		wantCode(t, err, connect.CodeInvalidArgument)
		wantInfo(t, err, "invalid")
	})
	t.Run("unknown job is not found", func(t *testing.T) {
		wantCode(t, watchErr(&routerv1.WatchJobRequest{Handle: h, JobId: "j_nope"}),
			connect.CodeNotFound)
		_, err := e.client.CancelJob(ctx, connect.NewRequest(
			&routerv1.CancelJobRequest{Handle: h, JobId: "j_nope"}))
		wantCode(t, err, connect.CodeNotFound)
		oh, _ := e.readyPlacement(t)
		res := e.exec(t, sh(oh, "true"))
		wantCode(t, watchErr(&routerv1.WatchJobRequest{Handle: h, JobId: res.GetJobId()}),
			connect.CodeNotFound)
	})
	t.Run("bad exec arguments are invalid", func(t *testing.T) {
		wantCode(t, execErr(&routerv1.ExecRequest{Handle: h}), connect.CodeInvalidArgument)
		bad := sh(h, "true")
		bad.Until = &routerv1.Until{OutputPattern: "("}
		wantCode(t, execErr(bad), connect.CodeInvalidArgument)
		res := e.exec(t, sh(h, "true"))
		wantCode(t, watchErr(&routerv1.WatchJobRequest{
			Handle: h, JobId: res.GetJobId(), Until: &routerv1.Until{OutputPattern: "["},
		}), connect.CodeInvalidArgument)
		wantCode(t, watchErr(&routerv1.WatchJobRequest{
			Handle: h, JobId: res.GetJobId(), Cursor: "not-a-cursor",
		}), connect.CodeInvalidArgument)
	})
	t.Run("lost job ends the watch as aborted", func(t *testing.T) {
		req := sh(h, "echo up; sleep 30")
		req.Until = &routerv1.Until{OutputPattern: "up"}
		res := e.exec(t, req)
		e.killSupervisor(t, h, res.GetJobId())
		w, err := e.watchJob(t, &routerv1.WatchJobRequest{Handle: h, JobId: res.GetJobId()})
		wantCode(t, err, connect.CodeAborted)
		wantInfo(t, err, "job_lost")
		quotesNone(t, err, h, res.GetJobId())
		if string(w.stdout) != "up\n" || w.exit != nil {
			t.Fatalf("watch of a lost job gave %q and exit %v", w.stdout, w.exit)
		}
	})
	t.Run("too many running jobs is resource exhausted", func(t *testing.T) {
		busy := newEnv(t, options{maxRunning: 1})
		bh, _ := busy.readyPlacement(t)
		req := sh(bh, "echo up; sleep 30")
		req.Until = &routerv1.Until{OutputPattern: "up"}
		busy.exec(t, req)
		_, err := busy.client.Exec(ctx, connect.NewRequest(sh(bh, "true")))
		wantCode(t, err, connect.CodeResourceExhausted)
	})
	t.Run("shutting down is unavailable", func(t *testing.T) {
		down := newEnv(t, options{})
		dh, _ := down.readyPlacement(t)
		sctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		down.jobs.Shutdown(sctx)
		_, err := down.client.Exec(ctx, connect.NewRequest(sh(dh, "true")))
		wantCode(t, err, connect.CodeUnavailable)
	})
}

// sandboxOps calls each operation the per-placement limit counts, on the handle given
func (e *env) sandboxOps(t *testing.T) map[string]func(h string) error {
	ctx := t.Context()
	return map[string]func(h string) error{
		"Exec": func(h string) error {
			_, err := e.client.Exec(ctx, connect.NewRequest(sh(h, "true")))
			return err
		},
		"CancelJob": func(h string) error {
			_, err := e.client.CancelJob(ctx, connect.NewRequest(
				&routerv1.CancelJobRequest{Handle: h, JobId: "j_nope"}))
			return err
		},
		"ReadFile": func(h string) error {
			_, err := e.client.ReadFile(ctx, connect.NewRequest(
				&routerv1.ReadFileRequest{Handle: h, Path: "/root/ops.txt"}))
			return err
		},
		"WriteFile": func(h string) error {
			_, err := e.client.WriteFile(ctx, connect.NewRequest(
				&routerv1.WriteFileRequest{Handle: h, Path: "/root/ops.txt", Data: []byte("x")}))
			return err
		},
		"ListFiles": func(h string) error {
			_, err := e.client.ListFiles(ctx, connect.NewRequest(
				&routerv1.ListFilesRequest{Handle: h, Path: "/root"}))
			return err
		},
	}
}

// opsKeys lists the operation buckets under the env's prefix
func (e *env) opsKeys(t *testing.T) []string {
	t.Helper()
	keys, err := e.vk.Do(t.Context(), e.vk.B().Keys().Pattern(e.prefix+"ops:*").Build()).
		AsStrSlice()
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(keys)
	return keys
}

// jobCount counts the job rows of a placement
func (e *env) jobCount(t *testing.T, placementID string) int {
	t.Helper()
	var n int
	err := e.pool.QueryRow(t.Context(), `select count(*) from jobs where placement_id = $1`,
		placementID).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// waitFull waits for a placement's operation bucket to refill, which expires its key
func (e *env) waitFull(t *testing.T, placementID string) {
	t.Helper()
	key := e.prefix + "ops:" + placementID
	for give := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		n, err := e.vk.Do(t.Context(), e.vk.B().Exists().Key(key).Build()).AsInt64()
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return
		}
		if time.Now().After(give) {
			t.Fatalf("operation bucket %s still not full after 10 s", key)
		}
	}
}

// watchFirst opens WatchPlacement and returns once the first message arrives
func (e *env) watchFirst(t *testing.T, h string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	stream, err := e.client.WatchPlacement(ctx, connect.NewRequest(
		&routerv1.WatchPlacementRequest{Handle: h}))
	if err != nil {
		return err
	}
	defer func() { _ = stream.Close() }()
	if !stream.Receive() {
		return cmp.Or(stream.Err(), errors.New("the watch ended without a message"))
	}
	return nil
}

func TestSandboxOperationsAreRateLimited(t *testing.T) {
	const burst = 3
	// One token a second keeps the spent bucket empty for the few calls that follow
	e := newEnv(t, options{opsPerSecond: 1, opsBurst: burst})
	ctx := t.Context()
	busy, busyMsg := e.readyPlacement(t)
	calm, calmMsg := e.readyPlacement(t)
	ended, _ := e.readyPlacement(t)
	e.stop(t, ended)
	ops := e.sandboxOps(t)
	for _, name := range []string{"Exec", "ReadFile", "WriteFile", "ListFiles"} {
		for range burst + 1 {
			wantCode(t, ops[name](ended), connect.CodeFailedPrecondition)
		}
	}
	ran := sh(busy, "echo ran")
	ran.Until = &routerv1.Until{Exit: true}
	job := e.exec(t, ran).GetJobId()
	e.waitFull(t, busyMsg.GetPlacementId())
	watchErr := func(h, jobID string) error {
		_, err := e.watchJob(t, &routerv1.WatchJobRequest{Handle: h, JobId: jobID})
		return err
	}
	uncounted := func(when string) {
		t.Helper()
		e.get(t, busy, routerv1.PlacementState_PLACEMENT_STATE_UNSPECIFIED, 0)
		if _, err := e.checkExtend(t, busy, morePkg); err != nil {
			t.Fatalf("CheckExtendPlacement %s: %v", when, err)
		}
		if _, err := e.listExtensions(t, busy); err != nil {
			t.Fatalf("ListExtensions %s: %v", when, err)
		}
		if err := e.watchFirst(t, busy); err != nil {
			t.Fatalf("WatchPlacement %s: %v", when, err)
		}
		if err := watchErr(busy, job); err != nil {
			t.Fatalf("WatchJob %s: %v", when, err)
		}
		wantCode(t, watchErr(busy, "j_nope"), connect.CodeNotFound)
	}
	for range burst + 1 {
		uncounted("before the burst")
	}

	spend := []string{"WriteFile", "ReadFile", "ListFiles"}
	for _, name := range spend[:burst] {
		if err := ops[name](busy); err != nil {
			t.Fatalf("%s within the burst: %v", name, err)
		}
	}
	// A loaded machine may refill a token between calls, so each refusal starts on an empty bucket
	drain := func() {
		t.Helper()
		for range burst + 1 {
			if err := ops["ListFiles"](busy); err != nil {
				wantCode(t, err, connect.CodeResourceExhausted)
				return
			}
		}
		t.Fatal("the bucket never ran dry")
	}
	for name, op := range ops {
		drain()
		err := op(busy)
		wantCode(t, err, connect.CodeResourceExhausted)
		wantInfo(t, err, "RATE_LIMITED")
		var ce *connect.Error
		if errors.As(err, &ce) && ce.Message() != "too many sandbox operations, slow down" {
			t.Errorf("%s past the burst says %q", name, ce.Message())
		}
		if wait := wantRetry(t, err); wait <= 0 || wait > time.Second {
			t.Errorf("%s past the burst asks to retry in %v, want up to 1s", name, wait)
		}
	}
	jobs := e.jobCount(t, busyMsg.GetPlacementId())
	drain()
	_, err := e.client.Exec(ctx, connect.NewRequest(sh(busy, `touch "$HOME/refused"`)))
	wantCode(t, err, connect.CodeResourceExhausted)
	drain()
	_, err = e.client.WriteFile(ctx, connect.NewRequest(&routerv1.WriteFileRequest{
		Handle: busy, Path: "/root/ops.txt", Data: []byte("refused"),
	}))
	wantCode(t, err, connect.CodeResourceExhausted)
	home := filepath.Join(e.sandboxFS(t, busy), "root")
	if _, err := os.Stat(filepath.Join(home, "refused")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a refused Exec ran its command (%v)", err)
	}
	if n := e.jobCount(t, busyMsg.GetPlacementId()); n != jobs {
		t.Fatalf("%d jobs after a refused Exec, want the %d before", n, jobs)
	}
	if data, err := os.ReadFile(filepath.Join(home, "ops.txt")); err != nil || string(data) != "x" {
		t.Fatalf("file after a refused WriteFile holds %q, %v, want the x from before", data, err)
	}
	uncounted("with the bucket spent")
	for _, req := range []*routerv1.ExecRequest{
		{Handle: busy},
		{Handle: busy, Argv: []string{"true"}, Env: map[string]string{"": "x"}},
		{Handle: busy, Argv: []string{"true"}, Env: map[string]string{"A=B": "x"}},
	} {
		_, err := e.client.Exec(ctx, connect.NewRequest(req))
		wantCode(t, err, connect.CodeInvalidArgument)
	}
	_, err = e.client.WriteFile(ctx, connect.NewRequest(&routerv1.WriteFileRequest{
		Handle: busy, Path: "/root/setuid", Data: []byte("x"), Mode: 0o4755,
	}))
	wantCode(t, err, connect.CodeInvalidArgument)

	for _, name := range spend[:burst] {
		if err := ops[name](calm); err != nil {
			t.Fatalf("%s on another placement: %v", name, err)
		}
	}
	unknown, _ := handle.New()
	for _, op := range ops {
		wantCode(t, op(unknown), connect.CodeNotFound)
		wantCode(t, op("sbx_not-a-handle"), connect.CodeNotFound)
	}
	want := []string{
		e.prefix + "ops:" + busyMsg.GetPlacementId(),
		e.prefix + "ops:" + calmMsg.GetPlacementId(),
	}
	slices.Sort(want)
	if keys := e.opsKeys(t); !slices.Equal(keys, want) {
		t.Fatalf("operation buckets %v, want only the two ready placements' %v", keys, want)
	}

	_, err = e.client.ExtendPlacement(ctx, connect.NewRequest(&routerv1.ExtendPlacementRequest{
		Handle: busy, OrderToken: e.extendToken(t, busy, morePkg, morePrice),
	}))
	if err != nil {
		t.Fatalf("ExtendPlacement with the bucket spent: %v", err)
	}
	e.stop(t, busy)
}

func TestSandboxOperationsGoThroughWhenTheLimitCannotBeRead(t *testing.T) {
	var logs syncBuffer
	e := newEnv(t, options{
		opsPerSecond: 1, opsBurst: 1, log: slog.New(slog.NewTextHandler(&logs, nil)),
	})
	h, _ := e.readyPlacement(t)
	e.apiVK.Close()
	for i := range 3 {
		if err := e.sandboxOps(t)["ListFiles"](h); err != nil {
			t.Fatalf("operation %d with the limit unreadable: %v", i+1, err)
		}
	}
	if !strings.Contains(logs.String(), "level=WARN msg=\"api: sandbox operation limit unread") {
		t.Fatalf("no warning that the limit was not read in the log:\n%s", logs.String())
	}
}

func TestSandboxOperationsGoThroughWhenValkeyHangs(t *testing.T) {
	var logs syncBuffer
	addr, replies := valve(t)
	e := newEnv(t, options{
		opsPerSecond: 1, opsBurst: 1, apiValkey: addr,
		log: slog.New(slog.NewTextHandler(&logs, nil)),
	})
	h, _ := e.readyPlacement(t)
	list := func(ctx context.Context) error {
		_, err := e.client.ListFiles(ctx, connect.NewRequest(
			&routerv1.ListFilesRequest{Handle: h, Path: "/root"}))
		return err
	}
	replies.Lock()
	t.Cleanup(replies.Unlock)

	short, cancelShort := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancelShort()
	wantCode(t, list(short), connect.CodeDeadlineExceeded)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	for i := range 2 {
		start := time.Now()
		if err := list(ctx); err != nil {
			t.Fatalf("operation %d with Valkey not answering: %v", i+1, err)
		}
		t.Logf("operation %d took %v", i+1, time.Since(start))
	}
	// The caller that gave up first is not Valkey's fault, so only the two others warn
	if n := strings.Count(logs.String(), "msg=\"api: sandbox operation limit unread"); n != 2 {
		t.Fatalf("%d warnings that the limit was not read, want 2:\n%s", n, logs.String())
	}
}

// valve forwards connections to Valkey, holding its replies back while the lock is held
func valve(t *testing.T) (string, *sync.RWMutex) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lis.Close() })
	shut := new(sync.RWMutex)
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			go forward(c, shut)
		}
	}()
	return lis.Addr().String(), shut
}

// forward copies c to Valkey and Valkey's replies back while shut is not locked
func forward(c net.Conn, shut *sync.RWMutex) {
	defer func() { _ = c.Close() }()
	up, err := net.Dial("tcp", valkeyAddr())
	if err != nil {
		return
	}
	defer func() { _ = up.Close() }()
	go func() {
		_, _ = io.Copy(up, c)
		_ = up.Close()
	}()
	buf := make([]byte, 32<<10)
	for {
		n, err := up.Read(buf)
		if n > 0 {
			shut.RLock()
			_, werr := c.Write(buf[:n])
			shut.RUnlock()
			if werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
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

// sandboxFS returns the host directory of the local sandbox the handle's placement runs on
func (e *env) sandboxFS(t *testing.T, h string) string {
	t.Helper()
	sha, err := handle.Hash(h)
	if err != nil {
		t.Fatal(err)
	}
	p, err := e.store.PlacementByHandle(t.Context(), sha)
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(e.root, p.ProviderRef.ID, "fs")
}

// killSupervisor kills a job's supervisor and then its command so no exit code is recorded
func (e *env) killSupervisor(t *testing.T, h, jobID string) {
	t.Helper()
	dir := filepath.Join(e.sandboxFS(t, h), "tmp", "bugnet", "jobs", jobID)
	for _, name := range []string{"spid", "pid"} {
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
		if err != nil {
			t.Fatal(err)
		}
		if name == "pid" {
			pid = -pid
		}
		if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCloseEndsStreamsAndPolls(t *testing.T) {
	e := newEnv(t, options{})
	h, _ := e.readyPlacement(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	stream, err := e.client.WatchPlacement(ctx, connect.NewRequest(
		&routerv1.WatchPlacementRequest{Handle: h}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()
	if !stream.Receive() {
		t.Fatalf("watch ended before the first message: %v", stream.Err())
	}
	polled := make(chan *routerv1.Placement, 1)
	go func() {
		resp, err := e.client.GetPlacement(ctx, connect.NewRequest(&routerv1.GetPlacementRequest{
			Handle: h, KnownState: routerv1.PlacementState_PLACEMENT_STATE_READY, WaitMs: 20000,
		}))
		if err != nil {
			polled <- nil
			return
		}
		polled <- resp.Msg.GetPlacement()
	}()
	time.Sleep(200 * time.Millisecond)
	closed := time.Now()
	e.api.Close()
	if stream.Receive() {
		t.Fatalf("watch sent %v after close", stream.Msg())
	}
	wantCode(t, stream.Err(), connect.CodeUnavailable)
	select {
	case p := <-polled:
		if p.GetState() != routerv1.PlacementState_PLACEMENT_STATE_READY {
			t.Fatalf("long poll ended with %v", p)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("long poll kept waiting after close")
	}
	if took := time.Since(closed); took > 2*time.Second {
		t.Fatalf("streams and polls took %v to end after close", took)
	}
}

func TestHealthAndReady(t *testing.T) {
	e := newEnv(t, options{})
	if code, body := httpGet(t, e.url+"/healthz"); code != http.StatusOK {
		t.Fatalf("healthz: %d %s", code, body)
	}
	if code, body := httpGet(t, e.url+"/readyz"); code != http.StatusOK {
		t.Fatalf("readyz: %d %s", code, body)
	}

	closedVK, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{valkeyAddr()}})
	if err != nil {
		t.Fatal(err)
	}
	closedVK.Close()
	closedPool, err := pgxpool.New(t.Context(), e.dbURL)
	if err != nil {
		t.Fatal(err)
	}
	closedPool.Close()
	for name, deps := range map[string]api.Deps{
		"valkey down":   {Store: e.store, Valkey: closedVK, Prefix: e.prefix},
		"postgres down": {Store: store.New(closedPool), Valkey: e.vk, Prefix: e.prefix},
	} {
		deps.Log = discard
		srv := api.New(deps)
		t.Cleanup(srv.Close)
		mux := http.NewServeMux()
		api.Mount(mux, srv, internalToken)
		hs := httptest.NewServer(mux)
		t.Cleanup(hs.Close)
		if code, body := httpGet(t, hs.URL+"/readyz"); code != http.StatusServiceUnavailable {
			t.Errorf("%s: readyz %d %s", name, code, body)
		}
		if code, _ := httpGet(t, hs.URL+"/healthz"); code != http.StatusOK {
			t.Errorf("%s: healthz %d", name, code)
		}
	}
}
