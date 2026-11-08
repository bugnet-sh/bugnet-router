package api_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	routerv1 "github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"google.golang.org/protobuf/proto"

	"github.com/oynozan/bugnet-router/internal/api"
	"github.com/oynozan/bugnet-router/internal/config"
)

const (
	edgeToken     = "api-test-edge-token-0123456789abcdef0123"
	secretsToken  = "api-test-secrets-token-0123456789abcdef"
	sshToken      = "api-test-ssh-token-0123456789abcdef01234"
	egressToken   = "api-test-egress-token-0123456789abcdef0"
	operatorToken = "api-test-operator-token-0123456789abcde"
	paymentsToken = "api-test-payments-token-0123456789abcde"
)

var allTokens = api.Tokens{
	Internal: internalToken, Edge: edgeToken, Secrets: secretsToken, SSH: sshToken,
	Egress: egressToken, Operator: operatorToken, Payments: paymentsToken,
}

// budgetDefaults are the [edge_budget] defaults with 1 GiB of egress, so edge and egress differ
var budgetDefaults = config.EdgeBudget{
	BytesPer10Minutes: 2 << 30, BytesPerSecond: 5 << 20, RequestsPerSecond: 100,
	RequestsBurst: 200, QueuedRequests: 64, WebSockets: 32, EgressBytesPer10Minutes: 1 << 30,
	EgressBytesPerSecond: 2 << 20, EgressRequestsInFlight: 32,
}

// httpClient sends every request with authorization as its bearer token, over h2c
func (e *env) httpClient(t *testing.T, token string) *http.Client {
	t.Helper()
	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	tr := &http.Transport{Protocols: &protocols}
	t.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: setHeader{value: "Bearer " + token, next: tr}}
}

func (e *env) edge(t *testing.T) routerv1connect.EdgeServiceClient {
	return routerv1connect.NewEdgeServiceClient(e.httpClient(t, edgeToken), e.url)
}

func (e *env) ssh(t *testing.T) routerv1connect.SshGatewayServiceClient {
	return routerv1connect.NewSshGatewayServiceClient(e.httpClient(t, sshToken), e.url)
}

func (e *env) egress(t *testing.T) routerv1connect.EgressServiceClient {
	return routerv1connect.NewEgressServiceClient(e.httpClient(t, egressToken), e.url)
}

func (e *env) payments(t *testing.T) routerv1connect.PaymentsServiceClient {
	return routerv1connect.NewPaymentsServiceClient(e.httpClient(t, paymentsToken), e.url)
}

// service calls one cheap RPC of a caller service with a token
type service struct {
	name string
	call func(ctx context.Context, c *http.Client, url string) error
}

var services = []service{
	{"Internal", func(ctx context.Context, c *http.Client, url string) error {
		_, err := routerv1connect.NewSandboxRouterServiceClient(c, url).PricePackage(ctx,
			connect.NewRequest(&routerv1.PricePackageRequest{PackageId: "none"}))
		return err
	}},
	{"Edge", func(ctx context.Context, c *http.Client, url string) error {
		_, err := routerv1connect.NewEdgeServiceClient(c, url).CheckEdgeLeases(ctx,
			connect.NewRequest(&routerv1.CheckEdgeLeasesRequest{}))
		return err
	}},
	{"Secrets", func(ctx context.Context, c *http.Client, url string) error {
		_, err := routerv1connect.NewSecretsRouteServiceClient(c, url).GetSecretLink(ctx,
			connect.NewRequest(&routerv1.GetSecretLinkRequest{}))
		return err
	}},
	{"SSH", func(ctx context.Context, c *http.Client, url string) error {
		_, err := routerv1connect.NewSshGatewayServiceClient(c, url).CheckSshGrants(ctx,
			connect.NewRequest(&routerv1.CheckSshGrantsRequest{}))
		return err
	}},
	{"Egress", func(ctx context.Context, c *http.Client, url string) error {
		_, err := routerv1connect.NewEgressServiceClient(c, url).CheckEgressLeases(ctx,
			connect.NewRequest(&routerv1.CheckEgressLeasesRequest{}))
		return err
	}},
	{"Operator", func(ctx context.Context, c *http.Client, url string) error {
		_, err := routerv1connect.NewOperatorServiceClient(c, url).ListProviders(ctx,
			connect.NewRequest(&routerv1.ListProvidersRequest{}))
		return err
	}},
	{"Payments", func(ctx context.Context, c *http.Client, url string) error {
		_, err := routerv1connect.NewPaymentsServiceClient(c, url).GetOrder(ctx,
			connect.NewRequest(&routerv1.GetOrderRequest{OrderId: "none"}))
		return err
	}},
}

func TestEachTokenOpensOnlyItsService(t *testing.T) {
	e := newEnv(t, options{})
	tokens := map[string]string{
		"Internal": internalToken, "Edge": edgeToken, "Secrets": secretsToken, "SSH": sshToken,
		"Egress": egressToken, "Operator": operatorToken, "Payments": paymentsToken,
	}
	eq(t, len(services), 7)
	for _, svc := range services {
		for name, tok := range tokens {
			err := svc.call(t.Context(), e.httpClient(t, tok), e.url)
			refused := connect.CodeOf(err) == connect.CodeUnauthenticated
			if refused != (name != svc.name) {
				t.Errorf("%s with the %s token gave %v", svc.name, name, err)
			}
		}
		err := svc.call(t.Context(), e.httpClient(t, ""), e.url)
		eq(t, connect.CodeOf(err), connect.CodeUnauthenticated)
	}
}

// x402Call names one Egress or Payments x402 RPC and the caller whose token opens it
type x402Call struct {
	rpc, owner string
	call       func(ctx context.Context, c *http.Client, url string) error
}

var x402Calls = []x402Call{
	{"AppendX402Events", "Egress", func(ctx context.Context, c *http.Client, url string) error {
		_, err := routerv1connect.NewEgressServiceClient(c, url).AppendX402Events(ctx,
			connect.NewRequest(&routerv1.AppendX402EventsRequest{PlacementId: "none"}))
		return err
	}},
	{"LookupPlacement", "Payments", func(ctx context.Context, c *http.Client, url string) error {
		_, err := routerv1connect.NewPaymentsServiceClient(c, url).LookupPlacement(ctx,
			connect.NewRequest(&routerv1.LookupPlacementRequest{
				Key: &routerv1.LookupPlacementRequest_StartOrderId{StartOrderId: "none"},
			}))
		return err
	}},
	{"AppendAllowanceEvent", "Payments", func(
		ctx context.Context, c *http.Client, url string,
	) error {
		_, err := routerv1connect.NewPaymentsServiceClient(c, url).AppendAllowanceEvent(ctx,
			connect.NewRequest(&routerv1.AppendAllowanceEventRequest{OrderId: "none"}))
		return err
	}},
	{"AppendX402Resolved", "Payments", func(
		ctx context.Context, c *http.Client, url string,
	) error {
		_, err := routerv1connect.NewPaymentsServiceClient(c, url).AppendX402Resolved(ctx,
			connect.NewRequest(&routerv1.AppendX402ResolvedRequest{StartOrderId: "none"}))
		return err
	}},
}

func TestNewEgressAndPaymentsRPCsNeedTheirTokens(t *testing.T) {
	e := newEnv(t, options{})
	tokens := map[string]string{
		"Internal": internalToken, "Edge": edgeToken, "Secrets": secretsToken, "SSH": sshToken,
		"Egress": egressToken, "Operator": operatorToken, "Payments": paymentsToken, "empty": "",
	}
	for _, c := range x402Calls {
		for name, tok := range tokens {
			want := connect.CodeUnauthenticated
			if name == c.owner {
				want = connect.CodeUnimplemented
			}
			err := c.call(t.Context(), e.httpClient(t, tok), e.url)
			if connect.CodeOf(err) != want {
				t.Errorf("%s with the %s token gave %v, want %v", c.rpc, name, err, want)
			}
		}
	}
}

func TestAnEmptyTokenAdmitsNothing(t *testing.T) {
	e := newEnv(t, options{})
	mux := http.NewServeMux()
	api.Mount(mux, e.api.Servers(), api.Tokens{Internal: internalToken})
	hs := newServer(t, mux)
	for _, svc := range services[1:] {
		for _, tok := range []string{"", internalToken} {
			err := svc.call(t.Context(), e.httpClient(t, tok), hs)
			eq(t, connect.CodeOf(err), connect.CodeUnauthenticated)
		}
	}
	mustCode(t, services[0].call(t.Context(), e.httpClient(t, internalToken), hs),
		connect.CodeInvalidArgument, "PACKAGE_UNKNOWN")
}

/* Usage */

func usage(id string, in, out uint64) []*routerv1.PlacementUsage {
	return []*routerv1.PlacementUsage{{PlacementId: id, BytesIn: in, BytesOut: out}}
}

func (e *env) reportEdge(
	t *testing.T, u []*routerv1.PlacementUsage,
) *routerv1.ReportEdgeUsageResponse {
	t.Helper()
	resp, err := e.edge(t).ReportEdgeUsage(t.Context(), connect.NewRequest(
		&routerv1.ReportEdgeUsageRequest{ReporterId: "edge-1", Usage: u}))
	if err != nil {
		t.Fatalf("report edge usage: %v", err)
	}
	return resp.Msg
}

func (e *env) reportSSH(
	t *testing.T, u []*routerv1.PlacementUsage,
) *routerv1.ReportSshUsageResponse {
	t.Helper()
	resp, err := e.ssh(t).ReportSshUsage(t.Context(), connect.NewRequest(
		&routerv1.ReportSshUsageRequest{ReporterId: "ssh-1", Usage: u}))
	if err != nil {
		t.Fatalf("report ssh usage: %v", err)
	}
	return resp.Msg
}

func (e *env) reportEgress(
	t *testing.T, u []*routerv1.PlacementUsage,
) *routerv1.ReportEgressUsageResponse {
	t.Helper()
	resp, err := e.egress(t).ReportEgressUsage(t.Context(), connect.NewRequest(
		&routerv1.ReportEgressUsageRequest{ReporterId: "egress-1", Usage: u}))
	if err != nil {
		t.Fatalf("report egress usage: %v", err)
	}
	return resp.Msg
}

// edgeEvents decodes every entry of the edge event stream, oldest first
func (e *env) edgeEvents(t *testing.T) []*routerv1.EdgeEvent {
	t.Helper()
	cmd := e.vk.B().Xrange().Key(e.prefix + "edge:events").Start("-").End("+").Build()
	entries, err := e.vk.Do(t.Context(), cmd).AsXRange()
	if err != nil {
		t.Fatal(err)
	}
	var out []*routerv1.EdgeEvent
	for _, en := range entries {
		ev := &routerv1.EdgeEvent{}
		if err := proto.Unmarshal([]byte(en.FieldValues["e"]), ev); err != nil {
			t.Fatal(err)
		}
		out = append(out, ev)
	}
	return out
}

func TestUsageReportsAnswerBytesLeftAndPace(t *testing.T) {
	e := newEnv(t, options{})
	_, p := e.readyPlacement(t)
	id := p.GetPlacementId()
	a := e.reportEdge(t, usage(id, 1<<20, 2<<20))
	eq(t, a.GetBytesRemaining()[id], uint64(2<<30-3<<20))
	eq(t, a.GetBytesPerSecond()[id], uint64(5<<20))
	s := e.reportSSH(t, usage(id, 0, 1<<20))
	eq(t, s.GetBytesRemaining()[id], uint64(2<<30-4<<20))
	eq(t, s.GetBytesPerSecond()[id], uint64(5<<20)/2)
	eq(t, e.reportEdge(t, usage(id, 0, 0)).GetBytesPerSecond()[id], uint64(5<<20)/2)
	g := e.reportEgress(t, usage(id, 3<<20, 0))
	eq(t, g.GetBytesRemaining()[id], uint64(1<<30-3<<20))
	eq(t, len(e.edgeEvents(t)), 0)
}

const fastPkg = "fast-10m"

// fastCatalog adds a start package with its own edge pace of 10 MiB a second
var fastCatalog = strings.TrimSuffix(catalogJSON, "\n]}") + `,
	{"id": "fast-10m", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10, "price_micros": 50000, "edge": {"bytes_per_second": "10MiB"}}
]}`

func TestAPackagesOwnPaceReachesTheReports(t *testing.T) {
	e := newEnv(t, options{catalog: fastCatalog})
	resp, err := e.createWith(t, e.startToken(t, fastPkg, smallPrice))
	if err != nil {
		t.Fatalf("create placement: %v", err)
	}
	id := e.waitReady(t, resp.Msg.GetHandle()).GetPlacementId()
	eq(t, e.reportEdge(t, usage(id, 1<<20, 0)).GetBytesPerSecond()[id], uint64(10<<20))
	eq(t, e.reportSSH(t, usage(id, 0, 1<<20)).GetBytesPerSecond()[id], uint64(5<<20))
	eq(t, e.reportEdge(t, usage(id, 0, 0)).GetBytesPerSecond()[id], uint64(5<<20))
}

func TestReportAnswersZeroOnceSpentAndNothingOnceEnded(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	id := p.GetPlacementId()
	a := e.reportEdge(t, usage(id, 1<<30, 1<<30))
	eq(t, a.GetBytesRemaining()[id], uint64(0))
	_, has := a.GetBytesRemaining()[id]
	eq(t, has, true)
	eq(t, e.reportSSH(t, usage(id, 1, 0)).GetBytesRemaining()[id], uint64(0))
	g := e.reportEgress(t, usage(id, 1<<30, 0))
	eq(t, g.GetBytesRemaining()[id], uint64(0))
	evs := e.edgeEvents(t)
	eq(t, len(evs), 2)
	eq(t, evs[0].GetBudgetSpent().GetPlacementId(), id)
	eq(t, evs[0].GetBudgetSpent().GetKind(), routerv1.BudgetKind_BUDGET_KIND_EDGE)
	eq(t, evs[1].GetBudgetSpent().GetKind(), routerv1.BudgetKind_BUDGET_KIND_EGRESS)

	e.stop(t, h)
	unknown := uuid.NewString()
	a = e.reportEdge(t, append(append(usage(id, 1, 1), usage(unknown, 1, 1)...),
		usage("not-a-uuid", 1, 1)...))
	eq(t, len(a.GetBytesRemaining()), 0)
	eq(t, len(a.GetBytesPerSecond()), 0)
	eq(t, len(e.reportEgress(t, usage(id, 1, 0)).GetBytesRemaining()), 0)
}

// replica serves a second API server over the same store, saga, Valkey and prefix
func (e *env) replica(t *testing.T) string {
	t.Helper()
	srv := api.New(api.Deps{
		Store: e.store, Saga: e.saga, Jobs: e.jobs, Catalog: loadCatalog(t, catalogJSON),
		Valkey: e.vk, Prefix: e.prefix, Log: discard, Budget: budgetDefaults,
	})
	t.Cleanup(srv.Close)
	mux := http.NewServeMux()
	api.Mount(mux, srv.Servers(), allTokens)
	return newServer(t, mux)
}

func newServer(t *testing.T, h http.Handler) string {
	t.Helper()
	hs := httptest.NewUnstartedServer(h)
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	hs.Config.Protocols = &protocols
	hs.Start()
	t.Cleanup(hs.Close)
	return hs.URL
}

func TestBudgetSpentOnceAcrossReplicas(t *testing.T) {
	e := newEnv(t, options{})
	_, p := e.readyPlacement(t)
	id := p.GetPlacementId()
	urls := []string{e.url, e.replica(t)}
	var wg sync.WaitGroup
	left := make([]uint64, 2)
	for i, url := range urls {
		edge := routerv1connect.NewEdgeServiceClient(e.httpClient(t, edgeToken), url)
		wg.Go(func() {
			resp, err := edge.ReportEdgeUsage(t.Context(), connect.NewRequest(
				&routerv1.ReportEdgeUsageRequest{Usage: usage(id, 0, 1200<<20)}))
			if err != nil {
				t.Errorf("report to replica %d: %v", i, err)
				return
			}
			left[i] = resp.Msg.GetBytesRemaining()[id]
		})
	}
	wg.Wait()
	evs := e.edgeEvents(t)
	eq(t, len(evs), 1) // two reports of 1200 MiB cross a 2 GiB budget, one writes BudgetSpent
	eq(t, evs[0].GetBudgetSpent().GetPlacementId(), id)
	eq(t, min(left[0], left[1]), uint64(0))
	eq(t, max(left[0], left[1]), uint64(2<<30-1200<<20))
}

/* Leases */

func TestCheckEdgeLeasesOmitsEnded(t *testing.T) {
	e := newEnv(t, options{})
	_, live := e.readyPlacement(t)
	h, gone := e.readyPlacement(t)
	e.stop(t, h)
	e.reportEdge(t, usage(live.GetPlacementId(), 0, 1<<20))
	resp, err := e.edge(t).CheckEdgeLeases(t.Context(), connect.NewRequest(
		&routerv1.CheckEdgeLeasesRequest{
			PlacementIds: []string{
				live.GetPlacementId(), gone.GetPlacementId(), uuid.NewString(), "bad",
			},
			Previews: []*routerv1.HeldPreview{{PreviewId: "abcdefghijklmnop", Generation: 1}},
		}))
	if err != nil {
		t.Fatal(err)
	}
	leases := resp.Msg.GetLeases()
	eq(t, len(leases), 1)
	l := leases[live.GetPlacementId()]
	eq(t, l.GetDeadline().AsTime().Equal(live.GetDeadline().AsTime()), true)
	eq(t, l.GetBytesRemaining(), uint64(2<<30-1<<20))
	// A preview the router never made can hold no route
	eq(t, resp.Msg.GetRevokedPreviewIds(), []string{"abcdefghijklmnop"})
}

func TestCheckEgressLeasesOmitsEnded(t *testing.T) {
	e := newEnv(t, options{})
	_, live := e.readyPlacement(t)
	h, gone := e.readyPlacement(t)
	e.stop(t, h)
	e.reportEgress(t, usage(live.GetPlacementId(), 1<<20, 0))
	if _, err := e.pool.Exec(t.Context(), `update placements set secrets_version = 3
		where id = $1`, live.GetPlacementId()); err != nil {
		t.Fatal(err)
	}
	resp, err := e.egress(t).CheckEgressLeases(t.Context(), connect.NewRequest(
		&routerv1.CheckEgressLeasesRequest{
			PlacementIds: []string{live.GetPlacementId(), gone.GetPlacementId()},
		}))
	if err != nil {
		t.Fatal(err)
	}
	leases := resp.Msg.GetLeases()
	eq(t, len(leases), 1)
	l := leases[live.GetPlacementId()]
	eq(t, l.GetDeadline().AsTime().Equal(live.GetDeadline().AsTime()), true)
	eq(t, l.GetBytesRemaining(), uint64(1<<30-1<<20))
	eq(t, l.GetSecretsVersion(), uint64(3))
}

/* Events */

type heard struct {
	cursor string
	ev     *routerv1.EdgeEvent
}

// watchAll opens each caller's event stream from cursor and collects n events from each
func (e *env) watchAll(
	t *testing.T, cursor string, n int,
) (wait func() map[string][]heard) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	streams := map[string]func() (string, *routerv1.EdgeEvent, bool, error){}
	edge, err := e.edge(t).WatchEdgeEvents(ctx, connect.NewRequest(
		&routerv1.WatchEdgeEventsRequest{Cursor: cursor}))
	if err != nil {
		t.Fatal(err)
	}
	streams["edge"] = func() (string, *routerv1.EdgeEvent, bool, error) {
		ok := edge.Receive()
		return edge.Msg().GetCursor(), edge.Msg().GetEvent(), ok, edge.Err()
	}
	ssh, err := e.ssh(t).WatchSshEvents(ctx, connect.NewRequest(
		&routerv1.WatchSshEventsRequest{Cursor: cursor}))
	if err != nil {
		t.Fatal(err)
	}
	streams["ssh"] = func() (string, *routerv1.EdgeEvent, bool, error) {
		ok := ssh.Receive()
		return ssh.Msg().GetCursor(), ssh.Msg().GetEvent(), ok, ssh.Err()
	}
	egress, err := e.egress(t).WatchEgressEvents(ctx, connect.NewRequest(
		&routerv1.WatchEgressEventsRequest{Cursor: cursor}))
	if err != nil {
		t.Fatal(err)
	}
	streams["egress"] = func() (string, *routerv1.EdgeEvent, bool, error) {
		ok := egress.Receive()
		return egress.Msg().GetCursor(), egress.Msg().GetEvent(), ok, egress.Err()
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	out := map[string][]heard{}
	for name, recv := range streams {
		wg.Go(func() {
			for range n {
				c, ev, ok, err := recv()
				if !ok {
					t.Errorf("%s stream ended: %v", name, err)
					return
				}
				mu.Lock()
				out[name] = append(out[name], heard{c, ev})
				mu.Unlock()
			}
		})
	}
	return func() map[string][]heard {
		wg.Wait()
		cancel()
		return out
	}
}

func TestEveryCallerWatchesTheEdgeEvents(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	wait := e.watchAll(t, "", 1)
	// The streams start from now, so the stop waits until each has read the stream's end
	time.Sleep(500 * time.Millisecond)
	e.stop(t, h)
	got := wait()
	evs := e.edgeEvents(t)
	eq(t, len(evs), 1)
	for _, name := range []string{"edge", "ssh", "egress"} {
		eq(t, len(got[name]), 1)
		ended := got[name][0].ev.GetPlacementEnded()
		eq(t, ended.GetPlacementId(), p.GetPlacementId())
		eq(t, ended.GetReason(), routerv1.EndReason_END_REASON_STOPPED)
	}

	// A cursor the stream does not hold gets a Resync first
	got = e.watchAll(t, "1-0", 1)()
	for _, name := range []string{"edge", "ssh", "egress"} {
		eq(t, got[name][0].ev.GetResync() != nil, true)
	}
}

func TestWatchRefusesABadCursorAndEndsOnClose(t *testing.T) {
	e := newEnv(t, options{})
	stream, err := e.edge(t).WatchEdgeEvents(t.Context(), connect.NewRequest(
		&routerv1.WatchEdgeEventsRequest{Cursor: "not-a-cursor"}))
	if err == nil {
		for stream.Receive() {
		}
		err = stream.Err()
	}
	wantCode(t, err, connect.CodeInvalidArgument)

	open, err := e.ssh(t).WatchSshEvents(t.Context(), connect.NewRequest(
		&routerv1.WatchSshEventsRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	ended := make(chan error, 1)
	go func() {
		for open.Receive() {
		}
		ended <- open.Err()
	}()
	time.Sleep(200 * time.Millisecond)
	e.api.Close()
	select {
	case err := <-ended:
		wantCode(t, err, connect.CodeUnavailable)
	case <-time.After(5 * time.Second):
		t.Fatal("the event stream outlived Close")
	}
}

// protocols are the client options of each protocol a caller may speak
var protocols = map[string][]connect.ClientOption{
	"connect": nil, "grpc": {connect.WithGRPC()}, "grpc-web": {connect.WithGRPCWeb()},
}

func TestWatchErrorsReachEveryProtocol(t *testing.T) {
	e := newEnv(t, options{})
	for name, opts := range protocols {
		edge := routerv1connect.NewEdgeServiceClient(e.httpClient(t, edgeToken), e.url, opts...)
		stream, err := edge.WatchEdgeEvents(t.Context(), connect.NewRequest(
			&routerv1.WatchEdgeEventsRequest{Cursor: "not-a-cursor"}))
		if err == nil {
			for stream.Receive() {
			}
			err = stream.Err()
		}
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("over %s a bad cursor gave %v, want InvalidArgument", name, err)
		}
	}

	web := routerv1connect.NewSshGatewayServiceClient(e.httpClient(t, sshToken), e.url,
		connect.WithGRPCWeb())
	ended := make(chan error, 1)
	go func() {
		stream, err := web.WatchSshEvents(t.Context(), connect.NewRequest(
			&routerv1.WatchSshEventsRequest{}))
		if err == nil {
			for stream.Receive() {
			}
			err = stream.Err()
		}
		ended <- err
	}()
	time.Sleep(200 * time.Millisecond)
	e.api.Close()
	select {
	case err := <-ended:
		wantCode(t, err, connect.CodeUnavailable)
	case <-time.After(5 * time.Second):
		t.Fatal("the gRPC-Web event stream outlived Close")
	}
}

/* Payments */

func (e *env) getOrder(t *testing.T, id string) *routerv1.GetOrderResponse {
	t.Helper()
	resp, err := e.payments(t).GetOrder(t.Context(), connect.NewRequest(
		&routerv1.GetOrderRequest{OrderId: id}))
	if err != nil {
		t.Fatalf("get order %s: %v", id, err)
	}
	return resp.Msg
}

func TestGetOrderStartAndExtend(t *testing.T) {
	e := newEnv(t, options{})
	order, tok := e.startOrder(t, smallPkg, smallPrice)
	resp, err := e.createWith(t, tok)
	if err != nil {
		t.Fatal(err)
	}
	h := resp.Msg.GetHandle()
	e.waitReady(t, h)
	got := e.getOrder(t, order)
	eq(t, proto.Equal(got, &routerv1.GetOrderResponse{
		Redeemed: true, Kind: "start", PlacementState: "ready",
	}), true)

	ext := e.extendToken(t, h, morePkg, morePrice)
	if _, err := e.client.ExtendPlacement(t.Context(), connect.NewRequest(
		&routerv1.ExtendPlacementRequest{Handle: h, OrderToken: ext})); err != nil {
		t.Fatal(err)
	}
	extID := "extend-" + strconv.FormatInt(e.orders.Load(), 10)
	got = e.getOrder(t, extID)
	eq(t, proto.Equal(got, &routerv1.GetOrderResponse{
		Redeemed: true, Kind: "extend", PlacementState: "ready",
	}), true)

	stopped := e.stop(t, h)
	got = e.getOrder(t, order)
	eq(t, got.GetPlacementState(), "terminated")
	eq(t, got.GetTerminationReason(), "stop")
	eq(t, got.GetEndedAt().AsTime().Equal(stopped.GetEndedAt().AsTime()), true)
	eq(t, got.GetRefundable(), false)
	eq(t, e.getOrder(t, extID).GetKind(), "extend")
}

func TestGetOrderUnknownAndFailedRefundable(t *testing.T) {
	e := newEnv(t, options{})
	eq(t, proto.Equal(e.getOrder(t, "order-never-taken"), &routerv1.GetOrderResponse{}), true)

	// No provider has a GPU, so the start fails at once and is refundable
	order, tok := e.startOrder(t, gpuPkg, gpuPrice)
	_, err := e.createWith(t, tok)
	wantCode(t, err, connect.CodeResourceExhausted)
	got := e.getOrder(t, order)
	eq(t, got.GetRedeemed(), true)
	eq(t, got.GetKind(), "start")
	eq(t, got.GetPlacementState(), "failed")
	eq(t, got.GetRefundable(), true)
	eq(t, got.GetFailureReason(), "no_candidate")
	eq(t, got.GetTerminationReason(), "")
	eq(t, got.GetEndedAt() != nil, true)
}

func TestGetOrderRefusesOtherCallersAndAnEmptyID(t *testing.T) {
	e := newEnv(t, options{})
	for _, tok := range []string{internalToken, edgeToken, operatorToken, ""} {
		_, err := routerv1connect.NewPaymentsServiceClient(e.httpClient(t, tok), e.url).
			GetOrder(t.Context(), connect.NewRequest(&routerv1.GetOrderRequest{OrderId: "o"}))
		wantCode(t, err, connect.CodeUnauthenticated)
	}
	_, err := e.payments(t).GetOrder(t.Context(), connect.NewRequest(
		&routerv1.GetOrderRequest{}))
	wantCode(t, err, connect.CodeInvalidArgument)
	_, err = e.payments(t).CheckPayer(t.Context(), connect.NewRequest(
		&routerv1.CheckPayerRequest{}))
	wantCode(t, err, connect.CodeUnimplemented)
}

func TestWatchOpensBeforeTheFirstEvent(t *testing.T) {
	e := newEnv(t, options{})
	for _, name := range []string{"connect", "grpc"} {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		egress := routerv1connect.NewEgressServiceClient(e.httpClient(t, egressToken), e.url,
			protocols[name]...)
		start := time.Now()
		stream, err := egress.WatchEgressEvents(ctx, connect.NewRequest(
			&routerv1.WatchEgressEventsRequest{}))
		if err != nil {
			t.Fatalf("opening a stream with no event yet over %s gave %v", name, err)
		}
		if took := time.Since(start); took > time.Second {
			t.Fatalf("opening the stream over %s took %v, want its headers at once", name, took)
		}
		_ = stream.Close()
		cancel()
	}
}

func TestAReportNeverSpendsABudgetAnExtensionGrew(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	id := p.GetPlacementId()
	extend := sync.OnceFunc(func() {
		ext := e.extendToken(t, h, morePkg, morePrice)
		if _, err := e.client.ExtendPlacement(t.Context(), connect.NewRequest(
			&routerv1.ExtendPlacementRequest{Handle: h, OrderToken: ext})); err != nil {
			t.Error(err)
		}
	})
	api.SetBeforeReport(t, extend)
	a := e.reportEdge(t, usage(id, 0, 2<<30+1))
	eq(t, a.GetBytesRemaining()[id], uint64(2<<30-1))
	for _, ev := range e.edgeEvents(t) {
		if ev.GetBudgetSpent() != nil {
			t.Fatalf("a report read before the extension wrote %v", ev)
		}
	}
	n, err := e.vk.Do(t.Context(), e.vk.B().Exists().Key(e.prefix+"edge:spent:"+id).Build()).
		AsInt64()
	eq(t, err == nil && n == 0, true)

	a = e.reportEdge(t, usage(id, 0, 2<<30))
	eq(t, a.GetBytesRemaining()[id], uint64(0))
	evs := e.edgeEvents(t)
	eq(t, evs[len(evs)-1].GetBudgetSpent().GetPlacementId(), id)
}
