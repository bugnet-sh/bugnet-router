package api_test

import (
	"crypto/sha256"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/telemetry"
	"github.com/bugnet-sh/bugnet-router/internal/telemetry/telemetrytest"
)

const routerService = "bugnet.router.v1.SandboxRouterService/"

func tracedEnv(t *testing.T) (*env, *telemetrytest.Telemetry) {
	t.Helper()
	tel := telemetrytest.New(t, "router-api", 1, []byte("0123456789abcdef0123456789abcdef"))
	return newEnv(t, options{tel: tel.Telemetry}), tel
}

func spanAttr(s telemetrytest.Span, key string) string {
	v, _ := s.Attrs[key].(string)
	return v
}

// TestEveryPlacementRPCJoinsOrder calls each operation that resolves a handle, and finds one
// span of it in the order trace, tagged with the placement
func TestEveryPlacementRPCJoinsOrder(t *testing.T) {
	e, tel := tracedEnv(t)
	h, p := e.readyPlacement(t)
	sum := sha256.Sum256([]byte(h))
	tid, sid := telemetry.OrderIDs(sum[:])
	ops := e.sandboxOps(t)
	for name, call := range ops {
		_ = call(h)
		var found []telemetrytest.Span
		for _, s := range tel.Flushed(t) {
			if s.Name == routerService+name {
				found = append(found, s)
			}
		}
		if len(found) != 1 {
			t.Fatalf("%s made %d spans, want 1", name, len(found))
		}
		s := found[0]
		if name == "WatchJob" {
			// A stream starts a root of its own, which the handler links to the order
			eq(t, len(s.Links), 1)
			eq(t, s.Links[0].SpanID, sid.String())
		} else {
			eq(t, s.TraceID, tid.String())
			eq(t, s.ParentID, sid.String())
		}
		eq(t, spanAttr(s, "bugnet.placement_id"), p.GetPlacementId())
		tel.Recv.Reset()
	}
	_ = e.stop(t, h)
	var stop int
	for _, s := range tel.Flushed(t) {
		if s.Name == routerService+"StopPlacement" || s.Name == "saga.stop" {
			stop++
			eq(t, s.TraceID, tid.String())
		}
	}
	if stop < 2 {
		t.Fatalf("the stop made %d spans in the order trace, want the call and saga.stop", stop)
	}
}

func TestResolvePreviewAnswersOrderTraceparent(t *testing.T) {
	e, tel := tracedEnv(t)
	h, _ := e.readyPlacement(t)
	r := e.mustCreatePreview(t, h, 5173)
	sum := sha256.Sum256([]byte(h))
	tid, sid := telemetry.OrderIDs(sum[:])
	got, err := e.resolve(t, r.GetPreviewId(), r.GetToken())
	if err != nil {
		t.Fatal(err)
	}
	want := "00-" + tid.String() + "-"
	if len(got.GetOrderTraceparent()) != 55 || got.GetOrderTraceparent()[:36] != want {
		t.Fatalf("order_traceparent %q, want one in trace %s", got.GetOrderTraceparent(), tid)
	}
	var span telemetrytest.Span
	for _, s := range tel.Flushed(t) {
		if s.Name == "bugnet.router.v1.EdgeService/ResolvePreview" {
			span = s
		}
	}
	eq(t, span.ParentID, sid.String())
	eq(t, got.GetOrderTraceparent()[36:52], span.SpanID)
	// A stranger's token makes no span and gets no answer
	tel.Recv.Reset()
	if _, err := e.resolve(t, "aaaaaaaaaaaaaaaa", r.GetToken()); err == nil {
		t.Fatal("an unknown preview resolved")
	}
	eq(t, len(tel.Flushed(t)), 0)
}

func TestOrderTraceparentEmptyWhileTracesAreOff(t *testing.T) {
	e := newEnv(t, options{})
	h, _ := e.readyPlacement(t)
	r := e.mustCreatePreview(t, h, 5173)
	got, err := e.resolve(t, r.GetPreviewId(), r.GetToken())
	if err != nil {
		t.Fatal(err)
	}
	eq(t, got.GetOrderTraceparent(), "")
}

func TestOperatorPlacementHasOrderTraceID(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	sum := sha256.Sum256([]byte(h))
	tid, _ := telemetry.OrderIDs(sum[:])
	got := e.mustFind(t, byPlacement(p.GetPlacementId()))
	eq(t, got.GetOrderTraceId(), tid.String())
}
