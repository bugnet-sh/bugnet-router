package telemetry_test

import (
	"bytes"
	"context"
	"go/parser"
	"go/token"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/telemetry"
	"github.com/bugnet-sh/bugnet-router/internal/telemetry/attr"
	"github.com/bugnet-sh/bugnet-router/internal/telemetry/telemetrytest"
)

const createName = "bugnet.router.v1.SandboxRouterService/CreatePlacement"

var testRoot = bytes.Repeat([]byte{7}, 32)

func TestFilterDropsForbidden(t *testing.T) {
	tel := telemetrytest.New(t, "router-api", 1, nil)
	ctx, span := tel.Tracer("test").Start(t.Context(), "Bad Name With Spaces")
	_ = ctx
	span.SetAttributes(
		attribute.String("bugnet.handle_hash", "9a5bf94fe52b"),
		attribute.String("url.full", "https://x/?a"),
		attribute.String("bugnet.reason", "bnx_secret"),
		attribute.String("bugnet.state", "Ready State"),
		attribute.Int("bugnet.attempt", -1),
		attribute.String("bugnet.provider", "boat"),
	)
	span.AddEvent("exception", trace.WithAttributes(attribute.String("exception.message", "x")))
	span.RecordError(os.ErrNotExist)
	span.SetStatus(codes.Error, strings.Repeat("e", 200))
	span.End()
	got := tel.Flushed(t)
	eq(t, len(got), 1)
	s := got[0]
	keys := make([]string, 0, len(s.Attrs))
	for k := range s.Attrs {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	eq(t, keys, []string{"bugnet.handle_hash", "bugnet.provider"})
	eq(t, s.Events, 0)
	eq(t, s.StatusText, "")
	eq(t, s.Name, "unnamed")
}

func TestFilterCleansLinks(t *testing.T) {
	tel := telemetrytest.New(t, "router-api", 1, nil)
	_, other := tel.Tracer("test").Start(t.Context(), "other")
	other.End()
	_, span := tel.Tracer("test").Start(t.Context(), "has_link", trace.WithLinks(trace.Link{
		SpanContext: other.SpanContext(),
		Attributes: []attribute.KeyValue{attribute.String("bugnet.link", "order"),
			attribute.String("bugnet.reason", "sbx_abc")},
	}))
	span.End()
	for _, s := range tel.Flushed(t) {
		if s.Name == "has_link" {
			eq(t, s.Links[0].Attrs, map[string]any{"bugnet.link": "order"})
			return
		}
	}
	t.Fatal("no span")
}

func TestAttributeImportOnlyInAttr(t *testing.T) {
	root := filepath.Join("..", "..")
	var bad []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") ||
			strings.HasSuffix(path, "_test.go") {
			return err
		}
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, im := range f.Imports {
			if im.Path.Value == `"go.opentelemetry.io/otel/attribute"` &&
				!strings.HasSuffix(filepath.ToSlash(path), "internal/telemetry/attr/attr.go") {
				bad = append(bad, path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, bad, []string(nil))
}

func TestOTELEnvIgnored(t *testing.T) {
	bait := telemetrytest.NewReceiver(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", bait.URL())
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", bait.URL())
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "x-evil=1")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "false")
	t.Setenv("OTEL_EXPORTER_OTLP_COMPRESSION", "none")
	t.Setenv("OTEL_TRACES_SAMPLER", "always_off")
	t.Setenv("OTEL_SDK_DISABLED", "true")
	t.Setenv("OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT", "1")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "service.name=evil,leak=x")
	t.Setenv("OTEL_SERVICE_NAME", "evil")
	tel := telemetrytest.New(t, "router-api", 1, nil)
	_, span := tel.Tracer("test").Start(t.Context(), "one")
	span.SetAttributes(attr.Provider("boat"), attr.Size("small"), attr.Count(1))
	span.End()
	got := tel.Flushed(t)
	eq(t, len(got), 1)
	eq(t, len(got[0].Attrs), 3)
	eq(t, got[0].Resource["service.name"], any("router-api"))
	_, leaked := got[0].Resource["leak"]
	eq(t, leaked, false)
	eq(t, bait.Requests, 0)
	for _, h := range tel.Recv.Headers {
		eq(t, h.Get("X-Evil"), "")
		eq(t, h.Get("Content-Encoding"), "gzip")
	}
}

func TestResourceAttributes(t *testing.T) {
	tel := telemetrytest.New(t, "router-worker", 1, nil)
	_, span := tel.Tracer("test").Start(t.Context(), "one")
	span.End()
	res := tel.Flushed(t)[0].Resource
	eq(t, res["service.name"], any("router-worker"))
	eq(t, res["service.instance.id"], any("replica-1"))
	eq(t, res["deployment.environment.name"], any("test"))
	v, _ := res["service.version"].(string)
	if v != "unknown" && len(v) < 12 {
		t.Fatalf("version %q", v)
	}
}

func TestAuthorizationHeader(t *testing.T) {
	r := telemetrytest.NewReceiver(t)
	tel, err := telemetry.New(settings(r.URL(), 1), telemetry.Process{
		Service: "router-api", InstanceID: "r1"}, "Basic dGVzdDp0ZXN0", nil, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	_, span := tel.Tracer("test").Start(t.Context(), "one")
	span.End()
	if err := tel.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	eq(t, len(r.Headers), 1)
	eq(t, r.Headers[0].Get("Authorization"), "Basic dGVzdDp0ZXN0")
	// An empty authorization sends no header
	r2 := telemetrytest.New(t, "router-api", 1, nil)
	_, span = r2.Tracer("test").Start(t.Context(), "one")
	span.End()
	r2.Flushed(t)
	eq(t, r2.Recv.Headers[0].Get("Authorization"), "")
}

func settings(endpoint string, ratio float64) telemetry.Settings {
	return telemetry.Settings{
		Traces: true, OTLPEndpoint: endpoint, SampleRatio: ratio, Environment: "test",
		ExportTimeout: 5 * time.Second, BatchTimeout: time.Minute, BatchSize: 512,
		QueueSize: 2048,
	}
}

func TestOffBuildsNothing(t *testing.T) {
	r := telemetrytest.NewReceiver(t)
	s := settings(r.URL(), 1)
	s.Traces = false
	tel, err := telemetry.New(s, telemetry.Process{Service: "router-api", InstanceID: "r1"},
		"", nil, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	eq(t, tel.Enabled(), false)
	eq(t, len(tel.Middleware(true)), 0)
	ctx, span := tel.Tracer("x").Start(t.Context(), "never")
	span.End()
	eq(t, trace.SpanContextFromContext(ctx).IsValid(), false)
	ctx, root := tel.StartOrderRoot(t.Context(), placementOf(handleA))
	root.End()
	eq(t, trace.SpanContextFromContext(ctx).IsValid(), false)
	eq(t, tel.JoinOrder(t.Context(), placementOf(handleA)), t.Context())
	eq(t, tel.Shutdown(t.Context()), nil)
	eq(t, r.Requests, 0)
}

func TestSettingsValidation(t *testing.T) {
	for name, s := range map[string]telemetry.Settings{
		"http to a public host": settings("http://traces.example.com/v1/traces", 1),
		"ratio 1.5":             settings("http://127.0.0.1:4318/v1/traces", 1.5),
		"no endpoint":           settings("", 1),
		"credentials":           settings("https://u:p@traces.example.com/v1/traces", 1),
	} {
		if _, err := telemetry.New(s, telemetry.Process{Service: "router-api",
			InstanceID: "r1"}, "", nil, slog.Default()); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

func TestShutdownFlushes(t *testing.T) {
	tel := telemetrytest.New(t, "router-api", 1, nil)
	for range 3 {
		_, span := tel.Tracer("test").Start(t.Context(), "one")
		span.End()
	}
	// The batch timeout is a minute, so only Shutdown can have sent these
	eq(t, len(tel.Recv.Spans()), 0)
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	eq(t, len(tel.Recv.Spans()), 3)
}

func TestLogHandlerAddsTraceID(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(telemetry.LogHandler(slog.NewJSONHandler(&buf, nil)))
	tel := telemetrytest.New(t, "router-api", 1, nil)
	ctx, span := tel.Tracer("test").Start(t.Context(), "one")
	defer span.End()
	log.WarnContext(ctx, "under a span")
	line := buf.String()
	if !strings.Contains(line, `"trace_id":"`+span.SpanContext().TraceID().String()+`"`) ||
		!strings.Contains(line, `"span_id":"`+span.SpanContext().SpanID().String()+`"`) {
		t.Fatalf("no ids in %s", line)
	}
	buf.Reset()
	log.Warn("no span")
	eq(t, strings.Contains(buf.String(), "trace_id"), false)
}

func TestChildOnlyMakesNoRoots(t *testing.T) {
	tel := telemetrytest.New(t, "router-api", 1, nil)
	tr := tel.Tracer("test")
	ctx, span := telemetry.Child(t.Context(), tr, "provider.create")
	span.End()
	eq(t, trace.SpanContextFromContext(ctx).IsValid(), false)
	eq(t, len(tel.Flushed(t)), 0)
	// Under a span of any trace it makes a child, sampled or not
	pctx, parent := tr.Start(t.Context(), "parent")
	_, child := telemetry.Child(pctx, tr, "provider.create")
	child.End()
	parent.End()
	eq(t, len(tel.Flushed(t)), 2)
}

func TestOrderRootIgnoresTheCallersTrace(t *testing.T) {
	tel := telemetrytest.New(t, "router-api", 1, testRoot)
	ctx, caller := tel.Tracer("gateway").Start(t.Context(), createName)
	p := store.Placement{ID: placementOf(handleA).ID, HandleSHA256: sha(handleA),
		OrderID: "ord-secret-1", PackageID: "small-10m"}
	_, root := tel.StartOrderRoot(ctx, p)
	eq(t, root.SpanContext().TraceID().String(), "55a4dee648a7a4d0a70cd0a8b24c672f")
	eq(t, root.SpanContext().SpanID().String(), "69923b8439c47854")
	root.End()
	caller.End()
	var rootSpan, callerSpan telemetrytest.Span
	for _, s := range tel.Flushed(t) {
		switch s.Name {
		case "bugnet.order":
			rootSpan = s
		case createName:
			callerSpan = s
		}
	}
	eq(t, rootSpan.TraceID, "55a4dee648a7a4d0a70cd0a8b24c672f")
	eq(t, rootSpan.ParentID, "")
	eq(t, len(rootSpan.Links), 1)
	eq(t, rootSpan.Links[0].SpanID, callerSpan.SpanID)
	eq(t, len(callerSpan.Links), 1)
	eq(t, callerSpan.Links[0].SpanID, "69923b8439c47854")
	eq(t, at(rootSpan.Attrs, "bugnet.placement_id"), p.ID.String())
	eq(t, at(rootSpan.Attrs, "bugnet.package_id"), "small-10m")
	eq(t, at(rootSpan.Attrs, "bugnet.handle_hash"), "9a5bf94fe52b")
}

func TestOrderIDsExportOnlyHashed(t *testing.T) {
	tel := telemetrytest.New(t, "router-api", 1, testRoot)
	p := store.Placement{ID: placementOf(handleA).ID, HandleSHA256: sha(handleA),
		OrderID: "ord-secret-1", PackageID: "small-10m"}
	_, root := tel.StartOrderRoot(t.Context(), p)
	root.End()
	hash := tel.OrderHash("ord-secret-1")
	eq(t, len(hash), 12)
	eq(t, tel.OrderHash("ord-secret-2") != hash, true)
	for _, s := range tel.Flushed(t) {
		eq(t, at(s.Attrs, "bugnet.order_hash"), hash)
		for k, v := range s.Attrs {
			if strings.Contains(k+at(s.Attrs, k), "ord-secret") || strings.Contains(
				at(s.Attrs, k), handleA) {
				t.Fatalf("exported %s=%v", k, v)
			}
		}
	}
	// Without data-plane keys the attribute is left out
	bare := telemetrytest.New(t, "router-api", 1, nil)
	_, root = bare.StartOrderRoot(t.Context(), p)
	root.End()
	_, has := bare.Flushed(t)[0].Attrs["bugnet.order_hash"]
	eq(t, has, false)
}

func TestSandboxHashIsKeyed(t *testing.T) {
	a := telemetrytest.New(t, "router-api", 1, testRoot)
	b := telemetrytest.New(t, "router-api", 1, bytes.Repeat([]byte{8}, 32))
	ref := provRef("boat", "sandbox-1")
	eq(t, len(a.SandboxHash(ref)), 12)
	eq(t, a.SandboxHash(ref), a.SandboxHash(ref))
	eq(t, a.SandboxHash(ref) != b.SandboxHash(ref), true)
	eq(t, a.SandboxHash(ref) != a.SandboxHash(provRef("e2b", "sandbox-1")), true)
}

func TestPinnedWireValues(t *testing.T) {
	eq(t, attr.Keys(), []string{
		"bugnet.attempt", "bugnet.bytes_in", "bugnet.bytes_out", "bugnet.can_fail_over",
		"bugnet.count", "bugnet.direction", "bugnet.egress_host", "bugnet.end_cause",
		"bugnet.estimate_id", "bugnet.event_type", "bugnet.exit_code",
		"bugnet.extend_order_hash", "bugnet.forwards", "bugnet.gate_errors",
		"bugnet.handle_hash", "bugnet.job.attempt", "bugnet.job.kind", "bugnet.job.outcome",
		"bugnet.job.queue", "bugnet.job.wait_ms", "bugnet.job_id", "bugnet.link",
		"bugnet.order_hash", "bugnet.package_id", "bugnet.placement_id", "bugnet.port",
		"bugnet.preview_id", "bugnet.provider", "bugnet.provider_code", "bugnet.reason",
		"bugnet.refused", "bugnet.repaired", "bugnet.requests", "bugnet.responses_4xx",
		"bugnet.responses_5xx", "bugnet.sandbox_hash", "bugnet.sessions", "bugnet.size",
		"bugnet.size_bytes", "bugnet.state", "bugnet.swaps", "bugnet.transfer_hash",
		"bugnet.transport", "bugnet.truncated", "bugnet.websockets", "error.type",
		"http.response.status_code", "rpc.method", "rpc.response.status_code",
		"rpc.system.name",
	})
	eq(t, telemetry.NeverTraced, []string{
		"/bugnet.router.v1.EdgeService/ReportEdgeUsage",
		"/bugnet.router.v1.EdgeService/CheckEdgeLeases",
		"/bugnet.router.v1.EdgeService/WatchEdgeEvents",
		"/bugnet.router.v1.SshGatewayService/RenewSshConnection",
		"/bugnet.router.v1.SshGatewayService/CheckSshGrants",
		"/bugnet.router.v1.SshGatewayService/ReportSshUsage",
		"/bugnet.router.v1.SshGatewayService/WatchSshEvents",
		"/bugnet.router.v1.EgressService/ReportEgressUsage",
		"/bugnet.router.v1.EgressService/CheckEgressLeases",
		"/bugnet.router.v1.EgressService/WatchEgressEvents",
		"/bugnet.router.v1.EgressService/AppendX402Events",
	})
}
