package saga_test

import (
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/provider/traced"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/telemetry"
	"github.com/bugnet-sh/bugnet-router/internal/telemetry/telemetrytest"
)

const createCall = "bugnet.router.v1.SandboxRouterService/CreatePlacement"

// tracedEnv is a saga on the local provider that exports its spans, through the traced wrapper
func tracedEnv(t *testing.T) (*env, *telemetrytest.Telemetry) {
	t.Helper()
	tel := telemetrytest.New(t, "router-api", 1, []byte("0123456789abcdef0123456789abcdef"))
	prov := traced.Wrap(newLocal(t, local.Options{Name: "local"}),
		tel.Tracer("provider"), tel.SandboxHash)
	e := newEnvWith(t, func(d *saga.Deps) { d.Telemetry = tel.Telemetry }, prov)
	return e, tel
}

func spansNamed(spans []telemetrytest.Span, name string) []telemetrytest.Span {
	var out []telemetrytest.Span
	for _, s := range spans {
		if s.Name == name {
			out = append(out, s)
		}
	}
	return out
}

func TestOrderRootOncePerOrder(t *testing.T) {
	e, tel := tracedEnv(t)
	in := saga.CreateInput{
		Token: e.startToken(t, "ord-once"), Requirements: placement.Requirements{},
	}
	for range 2 {
		if _, _, err := e.saga.Create(t.Context(), in); err != nil {
			t.Fatal(err)
		}
	}
	e.saga.Wait()
	eq(t, len(spansNamed(tel.Flushed(t), "bugnet.order")), 1)
}

func TestOrderRootIgnoresTheCallersTrace(t *testing.T) {
	e, tel := tracedEnv(t)
	ctx, call := tel.Tracer("gateway").Start(t.Context(), createCall)
	p, h, err := e.saga.Create(ctx, saga.CreateInput{
		Token: e.startToken(t, "ord-gw"), Requirements: placement.Requirements{},
	})
	if err != nil {
		t.Fatal(err)
	}
	e.saga.Wait()
	call.End()
	sum := sha256.Sum256([]byte(h))
	tid, sid := telemetry.OrderIDs(sum[:])
	spans := tel.Flushed(t)
	root := spansNamed(spans, "bugnet.order")
	eq(t, len(root), 1)
	eq(t, root[0].TraceID, tid.String())
	eq(t, root[0].SpanID, sid.String())
	eq(t, len(root[0].Links), 1)
	eq(t, root[0].Links[0].SpanID, call.SpanContext().SpanID().String())
	eq(t, at(root[0].Attrs, "bugnet.placement_id"), p.ID.String())
	eq(t, at(root[0].Attrs, "bugnet.state"), "ready")
	eq(t, len(at(root[0].Attrs, "bugnet.order_hash")), 12)
	// The order id never leaves, only its keyed hash
	for _, s := range spans {
		for k, v := range s.Attrs {
			if strings.Contains(at(s.Attrs, k), "ord-gw") || strings.Contains(at(s.Attrs, k), h) {
				t.Fatalf("%s exported %s=%v", s.Name, k, v)
			}
		}
	}
}

func TestSagaSpansUnderRoot(t *testing.T) {
	e, tel := tracedEnv(t)
	_, h := e.placeReady(t, "ord-spans")
	sum := sha256.Sum256([]byte(h))
	tid, sid := telemetry.OrderIDs(sum[:])
	spans := tel.Flushed(t)
	byID := map[string]telemetrytest.Span{}
	for _, s := range spans {
		byID[s.SpanID] = s
	}
	for _, name := range []string{"bugnet.order", "saga.attempt", "saga.boot", "saga.commit_ready",
		"saga.setup", "saga.prepare_gate", "saga.gate_health", "provider.create",
		"provider.wait_ready"} {
		if len(spansNamed(spans, name)) == 0 {
			t.Errorf("no %s span", name)
		}
	}
	for _, s := range spans {
		eq(t, s.TraceID, tid.String())
		if s.SpanID == sid.String() {
			continue
		}
		// Every other span's chain of parents reaches the order root inside the trace
		cur := s
		for range 10 {
			if cur.ParentID == sid.String() {
				break
			}
			next, ok := byID[cur.ParentID]
			if !ok {
				t.Fatalf("%s has parent %s, which was not exported", cur.Name, cur.ParentID)
			}
			cur = next
		}
		eq(t, cur.ParentID, sid.String())
	}
	attempt := spansNamed(spans, "saga.attempt")[0]
	eq(t, attempt.ParentID, sid.String())
	eq(t, at(attempt.Attrs, "bugnet.provider"), "local")
	boot := spansNamed(spans, "saga.boot")[0]
	eq(t, boot.ParentID, attempt.SpanID)
	eq(t, spansNamed(spans, "saga.commit_ready")[0].ParentID, attempt.SpanID)
	eq(t, spansNamed(spans, "saga.setup")[0].ParentID, boot.SpanID)
}

func TestTracesOffPlacesWithoutSpans(t *testing.T) {
	// The same saga with traces off exports nothing and still places
	e := newEnv(t, newLocal(t, local.Options{Name: "local"}))
	e.placeReady(t, "ord-off")
}

// at reads one exported attribute as text
func at(attrs map[string]any, key string) string {
	v, ok := attrs[key]
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
}
