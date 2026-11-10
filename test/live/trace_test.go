package live

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"

	"github.com/bugnet-sh/bugnet-router/internal/telemetry"
	"github.com/bugnet-sh/bugnet-router/internal/telemetry/telemetrytest"
)

// traceRows are DP 20.12.4's rows TRACE1 and TRACE2, TRACE3 and TRACE4 being Task 42B's
var traceRows []row

// traceRun is the test the rows of one run share, which owns the receivers they point the
// stack's otlp_endpoint at
var traceRun struct {
	mu    sync.Mutex
	t     *testing.T
	recvs map[string]*telemetrytest.Receiver
}

// TestLiveTrace runs TRACE1 and TRACE2 on each provider named, the stack's spans going to a
// receiver in the test
func TestLiveTrace(t *testing.T) {
	traceRun.mu.Lock()
	traceRun.t, traceRun.recvs = t, map[string]*telemetrytest.Receiver{}
	traceRun.mu.Unlock()
	runRows(t, traceRows)
}

func init() {
	settings := func(provider string, m map[string]any) {
		traceRun.mu.Lock()
		defer traceRun.mu.Unlock()
		recv := telemetrytest.NewReceiver(traceRun.t)
		traceRun.recvs[provider] = recv
		m["telemetry"] = map[string]any{
			"traces": true, "otlp_endpoint": recv.URL(), "environment": "live",
			"batch_timeout": "200ms",
		}
	}
	traceRows = append(traceRows,
		row{id: "TRACE1", settings: settings, run: rowTrace1},
		row{id: "TRACE2", settings: settings, run: rowTrace2},
	)
}

// traceReceiver is the receiver the provider's stack exports to
func traceReceiver(t *testing.T, provider string) *telemetrytest.Receiver {
	t.Helper()
	traceRun.mu.Lock()
	defer traceRun.mu.Unlock()
	r := traceRun.recvs[provider]
	if r == nil {
		t.Fatalf("live: no trace receiver for %s", provider)
	}
	return r
}

// gatewayClient is a client that sends the derived traceparent of handle on every call
func (s *stack) gatewayClient(h string) routerv1connect.SandboxRouterServiceClient {
	sum := sha256.Sum256([]byte(h))
	tid, _ := telemetry.OrderIDs(sum[:])
	rt := tracedBearer{token: s.keys["BUGNET_INTERNAL_TOKEN"], trace: tid.String()}
	return routerv1connect.NewSandboxRouterServiceClient(&http.Client{Transport: rt}, s.apiURL)
}

type tracedBearer struct{ token, trace string }

func (b tracedBearer) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	// A span of the fake gateway's own, whose parent is the order root
	r.Header.Set("traceparent", "00-"+b.trace+"-"+randHex(8)+"-01")
	return http.DefaultTransport.RoundTrip(r)
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	b[0] |= 1
	return hex.EncodeToString(b)
}

// waitSpan polls the receiver until a span of the name arrives, or fails after a minute
func waitSpan(t *testing.T, r *telemetrytest.Receiver, name string) []telemetrytest.Span {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		spans := r.Spans()
		for _, sp := range spans {
			if sp.Name == name {
				// The other processes' batches follow within the batch timeout
				time.Sleep(time.Second)
				return r.Spans()
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("live: no %s span reached the receiver", name)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// rowTrace1 is TRACE1, create, exec echo ok, read a file, stop, with derived headers
func rowTrace1(t *testing.T, s *stack) {
	recv := traceReceiver(t, s.provider)
	h := s.sandbox(t, xferPackage)
	c := s.gatewayClient(h)
	ctx := t.Context()
	out, err := c.Exec(ctx, connect.NewRequest(&routerv1.ExecRequest{
		Handle: h, Argv: []string{"echo", "ok"}, WaitMs: 30000,
	}))
	if err != nil || strings.TrimSpace(string(out.Msg.GetStdoutTail())) != "ok" {
		t.Fatalf("live: exec echo ok: %v", err)
	}
	if _, err := c.WriteFile(ctx, connect.NewRequest(&routerv1.WriteFileRequest{
		Handle: h, Path: "~/trace1.txt", Data: []byte("trace-file-data"),
	})); err != nil {
		t.Fatalf("live: write file: %v", err)
	}
	if _, err := c.ReadFile(ctx, connect.NewRequest(&routerv1.ReadFileRequest{
		Handle: h, Path: "~/trace1.txt",
	})); err != nil {
		t.Fatalf("live: read file: %v", err)
	}
	// The provider's own view of the sandbox, which the leak scan searches for
	prov := liveAdapter(t, s.provider)
	placement := kv(s.dev(t, "get", "--handle", h))["placement_id"]
	box := s.owned(t, prov, placement)
	s.dev(t, "stop", "--handle", h)
	spans := waitSpan(t, recv, "provider.destroy")

	sum := sha256.Sum256([]byte(h))
	tid, _ := telemetry.OrderIDs(sum[:])
	want := map[string]bool{"provider.create": false, "provider.wait_ready": false,
		"provider.exec": false, "provider.destroy": false}
	for _, sp := range spans {
		if sp.Attrs["bugnet.placement_id"] == placement && sp.TraceID != tid.String() &&
			!strings.HasPrefix(sp.Name, "bugnet.router.v1.") {
			t.Errorf("live: %s of the placement is in trace %s, not %s", sp.Name, sp.TraceID, tid)
		}
		if sp.TraceID == tid.String() {
			if _, ok := want[sp.Name]; ok {
				want[sp.Name] = true
			}
		}
	}
	for name, seen := range want {
		if !seen {
			t.Errorf("live: no %s span in the order trace", name)
		}
	}
	needles := map[string]string{
		"handle": h, "provider sandbox id": box.Ref.ID, "file data": "trace-file-data",
		"path": "trace1.txt", "command text": "echo ok",
	}
	for k, v := range box.Ref.Meta {
		if len(v) >= 6 {
			needles["Ref.Meta "+k] = v
		}
	}
	if found := telemetrytest.Scan(spans, needles); len(found) > 0 {
		t.Errorf("live: exported spans hold %v", found)
	}
}

// rowTrace2 is TRACE2, no trace variable inside the sandbox
func rowTrace2(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	env := s.sh(t, h, `env`)
	for _, line := range strings.Split(env, "\n") {
		name, _, _ := strings.Cut(line, "=")
		name = strings.ToUpper(strings.TrimSpace(name))
		if name == "TRACEPARENT" || name == "TRACESTATE" || strings.HasPrefix(name, "OTEL_") {
			t.Errorf("live: the sandbox environment holds %s", name)
		}
	}
	// A root exec through the gateway client, with derived headers, adds none either
	c := s.gatewayClient(h)
	res, err := c.Exec(t.Context(), connect.NewRequest(&routerv1.ExecRequest{
		Handle: h, Argv: []string{"sh", "-c", "env"}, WaitMs: 30000,
	}))
	if err != nil {
		t.Fatalf("live: exec env: %v", err)
	}
	for _, name := range []string{"TRACEPARENT", "TRACESTATE", "OTEL_"} {
		if strings.Contains(strings.ToUpper(string(res.Msg.GetStdoutTail())), name) {
			t.Errorf("live: the job environment holds %s", name)
		}
	}
}
