package e2e

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/ordertoken"
	"github.com/bugnet-sh/bugnet-router/internal/telemetry"
	"github.com/bugnet-sh/bugnet-router/internal/telemetry/telemetrytest"
)

// gateway is the fake gateway, which sends a traceparent of its own span on every call
type gateway struct {
	t     *testing.T
	token string
	url   string
}

func (g *gateway) client(parent func() string) routerv1connect.SandboxRouterServiceClient {
	tr := &http.Transport{}
	g.t.Cleanup(tr.CloseIdleConnections)
	rt := headerSetter{token: g.token, parent: parent, next: tr}
	return routerv1connect.NewSandboxRouterServiceClient(&http.Client{Transport: rt}, g.url)
}

type headerSetter struct {
	token  string
	parent func() string
	next   http.RoundTripper
}

func (h headerSetter) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+h.token)
	if p := h.parent(); p != "" {
		r.Header.Set("traceparent", p)
	}
	return h.next.RoundTrip(r)
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	b[0] |= 1
	return hex.EncodeToString(b)
}

// derived is the gateway's traceparent for a call about handle h, from a fresh span of its own
func derived(h string, gatewaySpans *[]string) func() string {
	sum := sha256.Sum256([]byte(h))
	tid, _ := telemetry.OrderIDs(sum[:])
	return func() string {
		sid := randHex(8)
		*gatewaySpans = append(*gatewaySpans, sid)
		return "00-" + tid.String() + "-" + sid + "-01"
	}
}

// TestOneTracePerOrder runs two API replicas and a worker with traces on, a fake gateway sending
// derived traceparent headers, and checks DP 20.12.2's four things
func TestOneTracePerOrder(t *testing.T) {
	t.Parallel()
	recv := telemetrytest.NewReceiver(t)
	s := startStackWith(t, stackOptions{extra: fmt.Sprintf(`
[telemetry]
traces = true
otlp_endpoint = %q
environment = "e2e"
batch_timeout = "200ms"
`, recv.URL())})
	gw := &gateway{t: t, token: s.keys["BUGNET_INTERNAL_TOKEN"]}

	// The gateway's own trace carries bugnet_start, before the handle exists
	startTrace := randHex(16)
	var gatewaySpans []string
	startSpan := randHex(8)
	gatewaySpans = append(gatewaySpans, startSpan)
	gw.url = s.a.url
	start := gw.client(func() string { return "00-" + startTrace + "-" + startSpan + "-01" })

	orderID := "e2e-" + uuid.NewString()
	token, err := ordertoken.Sign(s.priv, ordertoken.Claims{
		OrderID: orderID, Kind: ordertoken.KindStart, PackageID: smallPkg,
		AmountMicros: smallPrice, ExpiresAt: time.Now().Add(10 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	res, err := start.CreatePlacement(ctx, connect.NewRequest(&routerv1.CreatePlacementRequest{
		OrderToken: token,
	}))
	if err != nil {
		t.Fatalf("CreatePlacement: %v", err)
	}
	h, p := res.Msg.GetHandle(), res.Msg.GetPlacement()
	id := p.GetPlacementId()

	a := gw.client(derived(h, &gatewaySpans))
	gw.url = s.b.url
	b := gw.client(derived(h, &gatewaySpans))
	for p.GetState() == routerv1.PlacementState_PLACEMENT_STATE_PLACING {
		r, err := a.GetPlacement(ctx, connect.NewRequest(&routerv1.GetPlacementRequest{
			Handle: h, WaitMs: pollWaitMs, KnownState: p.GetState(),
		}))
		if err != nil {
			t.Fatalf("GetPlacement: %v", err)
		}
		p = r.Msg.GetPlacement()
	}
	if p.GetState() != routerv1.PlacementState_PLACEMENT_STATE_READY {
		t.Fatalf("placement is %s (%s)", p.GetState(), p.GetFailureReason())
	}
	const secretFile = "/root/trace-secret-file.txt"
	const secretWord = "trace-secret-word"
	for i, c := range []routerv1connect.SandboxRouterServiceClient{a, b} {
		out, err := c.Exec(ctx, connect.NewRequest(&routerv1.ExecRequest{
			Handle: h, Argv: []string{"echo", secretWord}, WaitMs: 5000,
		}))
		if err != nil || out.Msg.GetExitCode() != 0 {
			t.Fatalf("Exec %d: %v", i, err)
		}
	}
	if _, err := a.WriteFile(ctx, connect.NewRequest(&routerv1.WriteFileRequest{
		Handle: h, Path: secretFile, Data: []byte("data"),
	})); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := b.ReadFile(ctx, connect.NewRequest(&routerv1.ReadFileRequest{
		Handle: h, Path: secretFile,
	})); err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	// One placement is left to its deadline, another is stopped
	h2, p2 := s.place(t, a, shortPkg, 10000)
	id2 := p2.GetPlacementId()
	sandboxIDs := sandboxNames(t, s.root)
	if _, err := b.StopPlacement(ctx, connect.NewRequest(&routerv1.StopPlacementRequest{
		Handle: h,
	})); err != nil {
		t.Fatalf("StopPlacement: %v", err)
	}
	s.events.await(t, "placement.terminated", id, eventWait)
	s.events.await(t, "placement.terminated", id2, expiryWait)

	var spans []telemetrytest.Span
	waitFor(t, "the outbox spans", func() bool {
		spans = recv.Spans()
		return len(deliveries(spans)) >= 4
	})

	// Every span of the first placement is in its order trace, parents included
	sum := sha256.Sum256([]byte(h))
	tid, sid := telemetry.OrderIDs(sum[:])
	inTrace := map[string]bool{}
	for _, sp := range spans {
		if sp.TraceID == tid.String() {
			inTrace[sp.SpanID] = true
		}
	}
	root := false
	for _, sp := range spans {
		if sp.Attrs["bugnet.placement_id"] == id && sp.TraceID != tid.String() {
			t.Errorf("%s of the placement is in trace %s, not its order trace", sp.Name, sp.TraceID)
		}
		if sp.TraceID != tid.String() {
			continue
		}
		if sp.SpanID == sid.String() {
			root = sp.Name == "bugnet.order"
			continue
		}
		if !inTrace[sp.ParentID] && !slices.Contains(gatewaySpans, sp.ParentID) {
			t.Errorf("%s has parent %s, which neither the trace nor the gateway holds", sp.Name,
				sp.ParentID)
		}
	}
	if !root {
		t.Error("the order root bugnet.order was not exported")
	}
	// The gateway's start call is its own trace, with the root linked to it
	for _, sp := range spans {
		if sp.Name == "bugnet.order" && sp.TraceID == tid.String() {
			if len(sp.Links) != 1 || sp.Links[0].TraceID != startTrace {
				t.Errorf("the order root links %v, want the gateway's start span", sp.Links)
			}
		}
	}

	// outbox.deliver spans exist for both events of the first placement and for the second's
	kinds := map[string]int{}
	for _, d := range deliveries(spans) {
		kinds[fmt.Sprint(d.Attrs["bugnet.event_type"])]++
	}
	for _, typ := range []string{"placement.ready", "placement.terminated"} {
		if kinds[typ] < 2 {
			t.Errorf("%d outbox.deliver spans for %s, want one per placement", kinds[typ], typ)
		}
	}
	for _, want := range []string{"saga.attempt", "saga.boot", "saga.commit_ready", "saga.stop",
		"saga.terminate", "provider.create", "provider.exec", "command.run", "river.deadline"} {
		if !hasSpan(spans, want) {
			t.Errorf("no %s span exported", want)
		}
	}

	// Nothing the allowlist forbids left the processes
	needles := map[string]string{
		"handle": h, "handle 2": h2, "order id": orderID, "command": secretWord,
		"path": secretFile, "order token": token, "placement 2": "",
	}
	for i, sb := range sandboxIDs {
		needles[fmt.Sprintf("sandbox %d", i)] = sb
	}
	if found := telemetrytest.Scan(spans, needles); len(found) > 0 {
		t.Errorf("exported spans hold %v", found)
	}
}

func deliveries(spans []telemetrytest.Span) []telemetrytest.Span {
	var out []telemetrytest.Span
	for _, s := range spans {
		if s.Name == "outbox.deliver" {
			out = append(out, s)
		}
	}
	return out
}

func hasSpan(spans []telemetrytest.Span, name string) bool {
	return slices.ContainsFunc(spans, func(s telemetrytest.Span) bool { return s.Name == name })
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// sandboxNames lists what the local provider keeps under root, one entry per sandbox it made
func sandboxNames(t *testing.T, root string) []string {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), ".") {
			out = append(out, e.Name())
		}
	}
	return out
}
