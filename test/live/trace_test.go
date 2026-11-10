package live

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"github.com/pelletier/go-toml/v2"
	"golang.org/x/crypto/ssh"

	"github.com/bugnet-sh/bugnet-router/internal/telemetry"
	"github.com/bugnet-sh/bugnet-router/internal/telemetry/telemetrytest"
)

// traceRows are DP 20.12.4's rows TRACE1 to TRACE4
var traceRows []row

// traceRun is the test the rows of one run share, which owns the receivers they point the
// stack's otlp_endpoint at
var traceRun struct {
	mu    sync.Mutex
	t     *testing.T
	recvs map[string]*telemetrytest.Receiver
}

// TestLiveTrace runs TRACE1, TRACE2 and TRACE4 on each provider named, the stack's spans going to
// a receiver in the test, and TRACE3 alone, whose spans go to the dev settings' backend
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
	// TRACE3's settings come last, so its backend wins on a stack that a receiver row shares
	traceRows = append(traceRows,
		row{id: "TRACE1", settings: settings, run: rowTrace1},
		row{id: "TRACE2", settings: settings, run: rowTrace2},
		row{id: "TRACE4", vps: true, settings: settings, run: rowTrace4},
		row{id: "TRACE3", vps: true, settings: trace3Settings, run: rowTrace3},
	)
}

// traceReceiver is the receiver the stack exports to, which fails the row when the stack's
// settings point elsewhere, as TRACE3's do
func traceReceiver(t *testing.T, s *stack) *telemetrytest.Receiver {
	t.Helper()
	traceRun.mu.Lock()
	r := traceRun.recvs[s.provider]
	traceRun.mu.Unlock()
	if r == nil {
		t.Fatalf("live: no trace receiver for %s", s.provider)
	}
	if got, _ := s.telemetry["otlp_endpoint"].(string); got != r.URL() {
		t.Fatalf("live: the stack exports to the dev settings' backend, not the test's receiver, " +
			"run TRACE3 alone with --run 'LiveTrace/TRACE3'")
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
	recv := traceReceiver(t, s)
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

// trace3Settings points the stack at the dev settings' trace backend, its authorization coming
// from the environment, and fails naming whichever of the two is empty
func trace3Settings(_ string, m map[string]any) {
	t := traceRun.t
	raw, err := os.ReadFile(filepath.Join(repo(t), "config", "dev.toml"))
	if err != nil {
		t.Fatalf("live: %v", err)
	}
	var dev struct {
		Telemetry struct {
			OTLPEndpoint string `toml:"otlp_endpoint"`
		} `toml:"telemetry"`
	}
	if err := toml.Unmarshal(raw, &dev); err != nil {
		t.Fatalf("live: config/dev.toml: %v", err)
	}
	if dev.Telemetry.OTLPEndpoint == "" {
		t.Fatalf("live: telemetry.otlp_endpoint is empty in config/dev.toml, TRACE3 needs the " +
			"trace backend's URL there")
	}
	if os.Getenv("BUGNET_OTLP_AUTHORIZATION") == "" {
		t.Fatalf("live: BUGNET_OTLP_AUTHORIZATION is empty in .env, TRACE3 needs the trace " +
			"backend's Authorization header value there")
	}
	m["telemetry"] = map[string]any{
		"traces": true, "otlp_endpoint": dev.Telemetry.OTLPEndpoint, "environment": "live",
		"batch_timeout": "1s",
	}
}

// rowTrace3 is TRACE3, one order driven with the gateway's derived headers while every process
// exports to the dev settings' backend, which the founder reads in Grafana
func rowTrace3(t *testing.T, s *stack) {
	h := s.sandbox(t, xferPackage)
	c := s.gatewayClient(h)
	ctx := t.Context()
	out, err := c.Exec(ctx, connect.NewRequest(&routerv1.ExecRequest{
		Handle: h, Argv: []string{"echo", "ok"}, WaitMs: 30000,
	}))
	if err != nil || strings.TrimSpace(string(out.Msg.GetStdoutTail())) != "ok" {
		t.Fatalf("live: exec echo ok: %v", err)
	}
	dir := t.TempDir()
	local := randomFile(t, dir, "trace3", mib)
	s.upload(t, h, local, "~/trace3.bin")
	s.download(t, h, "~/trace3.bin", filepath.Join(dir, "down"))
	s.dev(t, "stop", "--handle", h)
	sum := sha256.Sum256([]byte(h))
	tid, _ := telemetry.OrderIDs(sum[:])
	t.Logf("live: TRACE3 order trace id %s, search it in the backend for the gateway's, "+
		"router-api's, the worker's and the outbox's spans", tid)
	// An export that failed logs once, after its retries run out
	time.Sleep(15 * time.Second)
	for _, p := range s.procs {
		raw, err := os.ReadFile(p.log)
		if err == nil && strings.Contains(string(raw), "trace export failed") {
			t.Errorf("live: %s failed to export spans to the backend", p.name)
		}
	}
}

// named lists the spans with name
func named(spans []telemetrytest.Span, name string) []telemetrytest.Span {
	var out []telemetrytest.Span
	for _, sp := range spans {
		if sp.Name == name {
			out = append(out, sp)
		}
	}
	return out
}

// strangerSpans counts the spans the edges and the router's edge-facing services made
func strangerSpans(spans []telemetrytest.Span) int {
	n := 0
	for _, sp := range spans {
		if strings.HasPrefix(sp.Name, "edge.") || strings.HasPrefix(sp.Name, "ssh.") ||
			strings.HasPrefix(sp.Name, "egress.") ||
			strings.HasPrefix(sp.Name, "bugnet.router.v1.EdgeService/") ||
			strings.HasPrefix(sp.Name, "bugnet.router.v1.SshGatewayService/") ||
			strings.HasPrefix(sp.Name, "bugnet.router.v1.EgressService/") {
			n++
		}
	}
	return n
}

// inOrder checks that sp is in the order trace under a span named parent, made by service
func inOrder(
	t *testing.T, sp telemetrytest.Span, tid string, byID map[string]telemetrytest.Span,
	parent, service string,
) {
	t.Helper()
	if sp.TraceID != tid {
		t.Errorf("live: %s is in trace %s, not the order's %s", sp.Name, sp.TraceID, tid)
	}
	if got := byID[sp.ParentID].Name; got != parent {
		t.Errorf("live: %s has the parent %q, want %q", sp.Name, got, parent)
	}
	if got, _ := sp.Resource["service.name"].(string); got != service {
		t.Errorf("live: %s was made by %q, want %q", sp.Name, got, service)
	}
}

// rowTrace4 is TRACE4, a download and an SSH session in the order trace behind the dev front, and
// unknown transfer tokens and SSH aliases that add no span
func rowTrace4(t *testing.T, s *stack) {
	recv := traceReceiver(t, s)
	h := s.sandbox(t, xferPackage)
	sum := sha256.Sum256([]byte(h))
	tid, _ := telemetry.OrderIDs(sum[:])
	dir := t.TempDir()
	local := randomFile(t, dir, "trace4", mib)
	s.upload(t, h, local, "~/trace4.bin")
	s.download(t, h, "~/trace4.bin", filepath.Join(dir, "down"))
	a := s.sshKey(t, h)
	s.sshMust(t, a, "true")
	spans := waitSpan(t, recv, "ssh.connection")
	byID := map[string]telemetrytest.Span{}
	for _, sp := range spans {
		byID[sp.SpanID] = sp
	}
	transfers := named(spans, "edge.transfer")
	if len(transfers) < 2 {
		t.Errorf("live: %d edge.transfer spans, want the upload's and the download's",
			len(transfers))
	}
	for _, sp := range transfers {
		inOrder(t, sp, tid.String(), byID, "bugnet.router.v1.EdgeService/OpenTransfer",
			"bugnet-edge")
	}
	conns := named(spans, "ssh.connection")
	if len(conns) != 1 {
		t.Fatalf("live: %d ssh.connection spans, want 1", len(conns))
	}
	inOrder(t, conns[0], tid.String(), byID, "bugnet.router.v1.SshGatewayService/AuthorizeSshKey",
		"bugnet-ssh")
	up := named(spans, "ssh.upstream")
	if len(up) != 1 || up[0].ParentID != conns[0].SpanID {
		t.Errorf("live: want one ssh.upstream span under the connection, got %d", len(up))
	}
	before := strangerSpans(spans)
	s.strangers(t, 100)
	// Every process flushes within its batch timeout
	time.Sleep(3 * time.Second)
	if got := strangerSpans(recv.Spans()); got != before {
		t.Errorf("live: 100 unknown tokens and aliases added %d spans", got-before)
	}
}

// strangers sends n unknown transfer tokens and n unknown SSH aliases through the dev front
func (s *stack) strangers(t *testing.T, n int) {
	t.Helper()
	c := s.client()
	url := "https://" + s.f.Names.TransferHost + "/"
	host, _, _, _, err := ssh.ParseAuthorizedKey([]byte(s.sshHost))
	if err != nil {
		t.Fatalf("live: the gateway's host key: %v", err)
	}
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	for range n {
		b := make([]byte, 32)
		_, _ = rand.Read(b)
		res, err := c.Get(url + "bnx_" + base64.RawURLEncoding.EncodeToString(b))
		if err != nil {
			t.Fatalf("live: an unknown transfer token: %v", err)
		}
		_ = res.Body.Close()
		alias := "bn-" + strings.ToLower(rand.Text()[:10])
		conn, err := ssh.Dial("tcp", s.sshAddr, &ssh.ClientConfig{
			User: alias, Auth: []ssh.AuthMethod{ssh.PublicKeys(signer)},
			HostKeyCallback: ssh.FixedHostKey(host), Timeout: 10 * time.Second,
		})
		if err == nil {
			_ = conn.Close()
			t.Fatal("live: an unknown SSH alias connected")
		}
	}
}
