package api_test

import (
	"net"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	routerv1 "github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"

	"github.com/oynozan/bugnet-router/internal/config"
	"github.com/oynozan/bugnet-router/internal/handle"
	"github.com/oynozan/bugnet-router/internal/store"
)

const previewDomain = "preview.bugnet.sh"

var previewSettings = config.Previews{MaxPerPlacement: 8, Interstitial: true}

var previewURL = regexp.MustCompile(
	`^https://([a-z2-7]{16})\.preview\.bugnet\.sh/\?bugnet_token=(bnp_[A-Za-z0-9_-]{43})$`)

func (e *env) operator(t *testing.T, token string) routerv1connect.OperatorServiceClient {
	return routerv1connect.NewOperatorServiceClient(e.httpClient(t, token), e.url)
}

func (e *env) createPreview(
	t *testing.T, h string, port uint32,
) (*routerv1.CreatePreviewResponse, error) {
	t.Helper()
	res, err := e.client.CreatePreview(t.Context(), connect.NewRequest(
		&routerv1.CreatePreviewRequest{Handle: h, Port: port}))
	if err != nil {
		return nil, err
	}
	return res.Msg, nil
}

func (e *env) mustCreatePreview(
	t *testing.T, h string, port uint32,
) *routerv1.CreatePreviewResponse {
	t.Helper()
	r, err := e.createPreview(t, h, port)
	if err != nil {
		t.Fatalf("CreatePreview of port %d: %v", port, err)
	}
	return r
}

func (e *env) resolve(t *testing.T, id, token string) (*routerv1.ResolvePreviewResponse, error) {
	t.Helper()
	res, err := e.edge(t).ResolvePreview(t.Context(), connect.NewRequest(
		&routerv1.ResolvePreviewRequest{PreviewId: id, Token: token}))
	if err != nil {
		return nil, err
	}
	return res.Msg, nil
}

func (e *env) repair(t *testing.T, id, token string) (bool, error) {
	t.Helper()
	res, err := e.edge(t).RepairPreview(t.Context(), connect.NewRequest(
		&routerv1.RepairPreviewRequest{PreviewId: id, Token: token}))
	if err != nil {
		return false, err
	}
	return res.Msg.GetRepaired(), nil
}

func (e *env) revoke(t *testing.T, id string) {
	t.Helper()
	_, err := e.operator(t, operatorToken).RevokePreview(t.Context(), connect.NewRequest(
		&routerv1.RevokePreviewRequest{PreviewId: id, Reason: "phishing page", Actor: "ops"}))
	if err != nil {
		t.Fatalf("RevokePreview: %v", err)
	}
}

// lastEvent is the newest entry of the edge event stream
func (e *env) lastEvent(t *testing.T) *routerv1.EdgeEvent {
	t.Helper()
	all := e.edgeEvents(t)
	if len(all) == 0 {
		t.Fatal("the edge event stream is empty")
	}
	return all[len(all)-1]
}

// placementOf reads the stored placement of a placement message
func (e *env) placementOf(t *testing.T, p *routerv1.Placement) store.Placement {
	t.Helper()
	got, err := e.store.PlacementByID(t.Context(), uuid.MustParse(p.GetPlacementId()))
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// freePort is a loopback port nothing listens on
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

// routeTo stores a gate route meta that points the placement's gate at port
func (e *env) routeTo(t *testing.T, p *routerv1.Placement, port int) {
	t.Helper()
	sp := e.placementOf(t, p)
	ref := *sp.ProviderRef
	ref.Meta = map[string]string{}
	for k, v := range sp.ProviderRef.Meta {
		ref.Meta[k] = v
	}
	ref.Meta["gate_port"] = strconv.Itoa(port)
	if err := e.store.SetPlacementRoute(t.Context(), sp.ID, ref, nil); err != nil {
		t.Fatal(err)
	}
}

func TestCreatePreview(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	r := e.mustCreatePreview(t, h, 5173)
	m := previewURL.FindStringSubmatch(r.GetUrl())
	if m == nil {
		t.Fatalf("preview URL %q", r.GetUrl())
	}
	eq(t, m[1], r.GetPreviewId())
	eq(t, m[2], r.GetToken())
	eq(t, r.GetExpiresAt().AsTime(), p.GetDeadline().AsTime())
	eq(t, r.GetListening(), false)
	eq(t, e.mustCreatePreview(t, h, 5173).GetUrl(), r.GetUrl())

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	open := uint32(ln.Addr().(*net.TCPAddr).Port)
	eq(t, e.mustCreatePreview(t, h, open).GetListening(), true)

	sp := e.placementOf(t, p)
	gate, _ := strconv.Atoi(sp.ProviderRef.Meta["gate_port"])
	for _, port := range []uint32{0, 65536, 29417, 22, 2022, uint32(gate)} {
		_, err := e.createPreview(t, h, port)
		mustCode(t, err, connect.CodeInvalidArgument, "PORT_REFUSED")
	}

	unknown, _ := handle.New()
	_, err = e.createPreview(t, unknown, 80)
	mustCode(t, err, connect.CodeNotFound, "HANDLE_UNKNOWN")
	e.stop(t, h)
	_, err = e.createPreview(t, h, 5173)
	mustCode(t, err, connect.CodeFailedPrecondition, "NOT_READY")
}

func TestCreatePreviewRefusesE2BsEnvdPort(t *testing.T) {
	e := newEnv(t, options{providerName: "e2b"})
	h, _ := e.readyPlacement(t)
	_, err := e.createPreview(t, h, 49983)
	mustCode(t, err, connect.CodeInvalidArgument, "PORT_REFUSED")
	e.mustCreatePreview(t, h, 49984)
}

func TestCreatePreviewNeedsTheFeature(t *testing.T) {
	e := newEnv(t, options{noPreviews: true})
	h, _ := e.readyPlacement(t)
	_, err := e.createPreview(t, h, 80)
	mustCode(t, err, connect.CodeFailedPrecondition, "FEATURE_UNAVAILABLE")
	noKeys := newEnv(t, options{noKeys: true})
	h, _ = noKeys.readyPlacement(t)
	_, err = noKeys.createPreview(t, h, 80)
	mustCode(t, err, connect.CodeFailedPrecondition, "FEATURE_UNAVAILABLE")
}

func TestNinthPreviewRefused(t *testing.T) {
	e := newEnv(t, options{})
	h, _ := e.readyPlacement(t)
	for port := uint32(3000); port < 3008; port++ {
		e.mustCreatePreview(t, h, port)
	}
	_, err := e.createPreview(t, h, 3008)
	mustCode(t, err, connect.CodeResourceExhausted, "TOO_MANY_PREVIEWS")
	e.mustCreatePreview(t, h, 3007)
	other, _ := e.readyPlacement(t)
	e.mustCreatePreview(t, other, 3008)
}

func TestRevokedPortStaysRevoked(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	r := e.mustCreatePreview(t, h, 5173)
	kept := e.mustCreatePreview(t, h, 3000)
	e.revoke(t, r.GetPreviewId())
	_, err := e.createPreview(t, h, 5173)
	mustCode(t, err, connect.CodeFailedPrecondition, "PREVIEW_REVOKED")
	if _, err := e.createPreview(t, h, 3001); err != nil {
		t.Fatalf("a preview of another port after a revoke: %v", err)
	}
	ev := e.lastEvent(t).GetPreviewRevoked()
	eq(t, ev.GetPreviewId(), r.GetPreviewId())
	eq(t, ev.GetPlacementId(), p.GetPlacementId())
	_, err = e.resolve(t, r.GetPreviewId(), r.GetToken())
	mustCode(t, err, connect.CodeFailedPrecondition, "PREVIEW_REVOKED")
	if _, err := e.resolve(t, kept.GetPreviewId(), kept.GetToken()); err != nil {
		t.Fatalf("another port's preview after a revoke: %v", err)
	}
	// A second revoke changes nothing but sends the event again, repairing a lost one
	e.revoke(t, r.GetPreviewId())
	eq(t, e.lastEvent(t).GetPreviewRevoked().GetPreviewId(), r.GetPreviewId())
	got, err := e.store.PreviewByID(t.Context(), r.GetPreviewId())
	if err != nil || got.Generation != 1 {
		t.Fatalf("after two revokes: %+v, %v, want generation 1", got, err)
	}

	op := e.operator(t, operatorToken)
	for _, req := range []*routerv1.RevokePreviewRequest{
		{PreviewId: "aaaaaaaaaaaaaaaa", Reason: "x", Actor: "ops"},
		{PreviewId: "not-an-id", Reason: "x", Actor: "ops"},
	} {
		_, err := op.RevokePreview(t.Context(), connect.NewRequest(req))
		mustCode(t, err, connect.CodeNotFound, "PREVIEW_UNKNOWN")
	}
	for _, req := range []*routerv1.RevokePreviewRequest{
		{PreviewId: kept.GetPreviewId(), Reason: "x"},
		{PreviewId: kept.GetPreviewId(), Reason: "x", Actor: strings.Repeat("a", 101)},
		{PreviewId: kept.GetPreviewId(), Reason: strings.Repeat("r", 1001), Actor: "ops"},
	} {
		_, err := op.RevokePreview(t.Context(), connect.NewRequest(req))
		mustCode(t, err, connect.CodeInvalidArgument, "")
	}
	if _, err := e.resolve(t, kept.GetPreviewId(), kept.GetToken()); err != nil {
		t.Fatalf("a refused revoke revoked: %v", err)
	}
}

func TestResolvePreviewCodes(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	r := e.mustCreatePreview(t, h, 5173)
	other := e.mustCreatePreview(t, h, 3000)
	got, err := e.resolve(t, r.GetPreviewId(), r.GetToken())
	if err != nil {
		t.Fatal(err)
	}
	sp := e.placementOf(t, p)
	eq(t, got.GetPlacementId(), p.GetPlacementId())
	eq(t, got.GetPort(), uint32(5173))
	eq(t, got.GetUpstream().GetBaseUrl(), "http://127.0.0.1:"+sp.ProviderRef.Meta["gate_port"])
	eq(t, got.GetExpiresAt().AsTime(), p.GetDeadline().AsTime())
	eq(t, got.GetStartedAt().AsTime(), p.GetReadyAt().AsTime())
	eq(t, got.GetGeneration(), uint32(0))
	eq(t, got.GetInterstitial(), true)
	eq(t, got.GetBudget().GetBytesRemaining(), uint64(2<<30))
	eq(t, got.GetBudget().GetQueuedRequests(), uint32(64))
	eq(t, got.GetBudget().GetWebsockets(), uint32(32))
	eq(t, got.GetBudget().GetRequestsPerSecond(), uint32(100))
	eq(t, got.GetBudget().GetRequestsBurst(), uint32(200))
	eq(t, got.GetBudget().GetBytesPerSecond(), uint64(5<<20))
	key, err := e.dpKeys.GateKey(sp.KeyID, sp.ID)
	if err != nil {
		t.Fatal(err)
	}
	var gate []string
	for _, hv := range got.GetUpstream().GetHeaders() {
		if hv.GetName() == "Bugnet-Gate" {
			gate = append(gate, hv.GetValue())
		}
	}
	eq(t, gate, []string{key})
	// The route the edge gets reaches the gate
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		got.GetUpstream().GetBaseUrl()+"/__bugnet/health", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, hv := range got.GetUpstream().GetHeaders() {
		req.Header.Set(hv.GetName(), hv.GetValue())
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	eq(t, resp.StatusCode, http.StatusNoContent)

	for _, id := range []string{"aaaaaaaaaaaaaaaa", "not-a-preview", ""} {
		_, err = e.resolve(t, id, r.GetToken())
		mustCode(t, err, connect.CodeNotFound, "PREVIEW_UNKNOWN")
	}
	for _, tok := range []string{"", "bnp_x", other.GetToken(), r.GetToken() + "x"} {
		_, err = e.resolve(t, r.GetPreviewId(), tok)
		mustCode(t, err, connect.CodePermissionDenied, "TOKEN_INVALID")
		quotesNone(t, err, r.GetPreviewId(), r.GetToken())
	}

	e.reportEdge(t, usage(p.GetPlacementId(), 2<<30, 0))
	_, err = e.resolve(t, r.GetPreviewId(), r.GetToken())
	mustCode(t, err, connect.CodeResourceExhausted, "BUDGET_SPENT")

	h2, p2 := e.readyPlacement(t)
	late := e.mustCreatePreview(t, h2, 5173)
	if _, err := e.pool.Exec(t.Context(), `update placements
		set deadline = now() - interval '1 second' where id = $1`,
		p2.GetPlacementId()); err != nil {
		t.Fatal(err)
	}
	_, err = e.resolve(t, late.GetPreviewId(), late.GetToken())
	mustCode(t, err, connect.CodeFailedPrecondition, "NOT_READY")
	h3, _ := e.readyPlacement(t)
	ended := e.mustCreatePreview(t, h3, 5173)
	e.stop(t, h3)
	_, err = e.resolve(t, ended.GetPreviewId(), ended.GetToken())
	mustCode(t, err, connect.CodeFailedPrecondition, "NOT_READY")
}

func TestResolveSpendsNoOperation(t *testing.T) {
	e := newEnv(t, options{opsPerSecond: 1, opsBurst: 1})
	h, _ := e.readyPlacement(t)
	r := e.mustCreatePreview(t, h, 5173)
	for range 5 {
		if _, err := e.resolve(t, r.GetPreviewId(), r.GetToken()); err != nil {
			t.Fatalf("ResolvePreview with the operation bucket spent: %v", err)
		}
	}
}

func TestCreatePreviewSpendsOneOperation(t *testing.T) {
	e := newEnv(t, options{opsPerSecond: 1, opsBurst: 1})
	h, _ := e.readyPlacement(t)
	e.mustCreatePreview(t, h, 5173)
	_, err := e.createPreview(t, h, 5173)
	mustCode(t, err, connect.CodeResourceExhausted, "RATE_LIMITED")
}

func TestCreatePreviewOpensAGateThatNeverAnswered(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	if _, err := e.pool.Exec(t.Context(), `update placements set gate_ready_at = null
		where id = $1`, p.GetPlacementId()); err != nil {
		t.Fatal(err)
	}
	r := e.mustCreatePreview(t, h, 5173)
	if e.placementOf(t, p).GateReadyAt == nil {
		t.Fatal("the gate time is still unset")
	}
	if _, err := e.resolve(t, r.GetPreviewId(), r.GetToken()); err != nil {
		t.Fatal(err)
	}
}

func TestCreatePreviewRepairsAGateThatStoppedAnswering(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	e.routeTo(t, p, freePort(t))
	if _, err := e.createPreview(t, h, 5173); err != nil {
		t.Fatalf("CreatePreview through a route the repair fixes: %v", err)
	}
	// The repair ran in the last minute, so a broken route now stays broken
	e.routeTo(t, p, freePort(t))
	start := time.Now()
	_, err := e.createPreview(t, h, 3000)
	mustCode(t, err, connect.CodeUnavailable, "GATE_UNAVAILABLE")
	if took := time.Since(start); took < 4*time.Second || took > 20*time.Second {
		t.Fatalf("the refusal took %v, want about the 5 s check", took)
	}
}

func TestRepairOnceAMinute(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	r := e.mustCreatePreview(t, h, 5173)
	var wg sync.WaitGroup
	results := make([]bool, 2)
	errs := make([]error, 2)
	for i := range results {
		wg.Go(func() { results[i], errs[i] = e.repair(t, r.GetPreviewId(), r.GetToken()) })
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	eq(t, results[0] != results[1], true)
	ttl, err := e.vk.Do(t.Context(), e.vk.B().Ttl().
		Key(e.prefix+"gate:repair:"+p.GetPlacementId()).Build()).AsInt64()
	if err != nil || ttl < 1 || ttl > 60 {
		t.Fatalf("repair guard ttl %d, %v", ttl, err)
	}
	if again, err := e.repair(t, r.GetPreviewId(), r.GetToken()); err != nil || again {
		t.Fatalf("a third repair within the minute: %v, %v", again, err)
	}
	_, err = e.repair(t, r.GetPreviewId(), "bnp_x")
	mustCode(t, err, connect.CodePermissionDenied, "TOKEN_INVALID")
	_, err = e.repair(t, "aaaaaaaaaaaaaaaa", r.GetToken())
	mustCode(t, err, connect.CodeNotFound, "PREVIEW_UNKNOWN")
	// The gate still answers after its repair
	if _, err := e.createPreview(t, h, 3000); err != nil {
		t.Fatal(err)
	}
}

func TestCheckEdgeLeasesRevokedGenerations(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	gone := e.mustCreatePreview(t, h, 5173)
	kept := e.mustCreatePreview(t, h, 3000)
	moved := e.mustCreatePreview(t, h, 3001)
	e.revoke(t, gone.GetPreviewId())
	res, err := e.edge(t).CheckEdgeLeases(t.Context(), connect.NewRequest(
		&routerv1.CheckEdgeLeasesRequest{
			PlacementIds: []string{p.GetPlacementId()},
			Previews: []*routerv1.HeldPreview{
				{PreviewId: gone.GetPreviewId(), Generation: 0},
				{PreviewId: kept.GetPreviewId(), Generation: 0},
				{PreviewId: moved.GetPreviewId(), Generation: 1},
				{PreviewId: "aaaaaaaaaaaaaaaa", Generation: 0},
				{PreviewId: "not-an-id", Generation: 0},
			},
		}))
	if err != nil {
		t.Fatal(err)
	}
	got := res.Msg.GetRevokedPreviewIds()
	slices.Sort(got)
	want := []string{gone.GetPreviewId(), moved.GetPreviewId(), "aaaaaaaaaaaaaaaa", "not-an-id"}
	slices.Sort(want)
	eq(t, got, want)
	eq(t, len(res.Msg.GetLeases()), 1)
}

func TestOperatorServiceRefusesInternalToken(t *testing.T) {
	e := newEnv(t, options{})
	h, _ := e.readyPlacement(t)
	r := e.mustCreatePreview(t, h, 5173)
	for _, token := range []string{internalToken, edgeToken} {
		_, err := e.operator(t, token).RevokePreview(t.Context(), connect.NewRequest(
			&routerv1.RevokePreviewRequest{PreviewId: r.GetPreviewId(), Actor: "ops"}))
		mustCode(t, err, connect.CodeUnauthenticated, "")
	}
	if _, err := e.resolve(t, r.GetPreviewId(), r.GetToken()); err != nil {
		t.Fatalf("a refused revoke revoked: %v", err)
	}
}
