package koyeb

import (
	"errors"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// gateRoutes lists the executor's port routes the adapter called, oldest first
func gateRoutes(f *fakeKoyeb) []string {
	var out []string
	for _, r := range f.routesCalled() {
		if r == "POST /bind_port" || r == "POST /unbind_port" {
			out = append(out, r)
		}
	}
	return out
}

func TestKoyebPrepareGateBindsTheGatePort(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	ref := ready(t, p, "key-gate-bind")
	meta, err := p.PrepareGate(t.Context(), ref, 29417)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, meta, map[string]string{
		"executor_url": ref.Meta["executor_url"], "routing_key": ref.Meta["routing_key"],
	})
	eq(t, gateRoutes(f), []string{"POST /bind_port"})
	eq(t, f.bindBodies(), []string{`{"port":"29417"}`})
	eq(t, f.boundPort(ref.ID), "29417")

	ref.Meta = meta
	r, err := p.GateRoute(ref, 29417)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, r, provider.GateRoute{
		BaseURL:         "https://" + f.domain(ref.ID),
		Header:          map[string]string{"X-Routing-Key": meta["routing_key"]},
		BlockedPrefixes: []string{"/koyeb-sandbox/"},
	})
}

func TestKoyebPrepareGateRebindsOn409(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	ref := ready(t, p, "key-gate-409")
	f.bind(ref.ID, "8080")
	if _, err := p.PrepareGate(t.Context(), ref, 29417); err != nil {
		t.Fatal(err)
	}
	eq(t, gateRoutes(f), []string{"POST /bind_port", "POST /unbind_port", "POST /bind_port"})
	eq(t, f.boundPort(ref.ID), "29417")
}

func TestKoyebPrepareGateFailsWhenTheRebindConflicts(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	ref := ready(t, p, "key-gate-stolen")
	f.bind(ref.ID, "8080")
	f.stealBind = "8081"
	_, err := p.PrepareGate(t.Context(), ref, 29417)
	e, ok := provider.AsError(err)
	if !ok || e.Code != provider.CodeInternal || e.CanFailOver {
		t.Fatalf("prepare whose rebind got 409 gave %v, want internal without failover", err)
	}
	eq(t, gateRoutes(f), []string{"POST /bind_port", "POST /unbind_port", "POST /bind_port"})
	eq(t, f.boundPort(ref.ID), "8081")
}

func TestKoyebPrepareGateFailsWhenTheBindFails(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	ref := ready(t, p, "key-gate-fail")
	f.refuseBinds = true
	_, err := p.PrepareGate(t.Context(), ref, 29417)
	if err == nil {
		t.Fatal("prepare passed while the executor refused every bind")
	}
	eq(t, f.boundPort(ref.ID), "")
}

func TestKoyebPrepareGateOnABareRef(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	full := ready(t, p, "key-gate-bare")
	meta, err := p.PrepareGate(t.Context(), provider.Ref{Provider: "koyeb", ID: full.ID}, 29417)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, meta["executor_url"], full.Meta["executor_url"])
	eq(t, meta["routing_key"], full.Meta["routing_key"])
}

func TestKoyebGateRouteWithoutRoutingKey(t *testing.T) {
	f := newFake(t)
	f.noMetadata = true
	p := f.provider(t, fakeToken)
	ref := ready(t, p, "key-gate-domain")
	meta, err := p.PrepareGate(t.Context(), ref, 29417)
	if err != nil {
		t.Fatal(err)
	}
	r, err := p.GateRoute(provider.Ref{Provider: "koyeb", ID: ref.ID, Meta: meta}, 29417)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, r.BaseURL, "https://"+f.domain(ref.ID))
	eq(t, len(r.Header), 0)
}

func TestKoyebGateRouteNeedsTheExecutor(t *testing.T) {
	p := newFake(t).provider(t, fakeToken)
	ref := provider.Ref{Provider: "koyeb", ID: "app-1"}
	if _, err := p.GateRoute(ref, 29417); !errors.Is(err, provider.ErrGateUnprepared) {
		t.Fatalf("route without an executor gave %v, want unprepared", err)
	}
	ref.Meta = map[string]string{"executor_url": "https://elsewhere.example/api"}
	if _, err := p.GateRoute(ref, 29417); !isCode(err, provider.CodeInternal) {
		t.Fatalf("route from a foreign executor URL gave %v, want internal", err)
	}
	ref.Meta = map[string]string{"executor_url": "https://a.koyeb.app/koyeb-sandbox"}
	for _, port := range []int{0, 65536} {
		if _, err := p.GateRoute(ref, port); !isCode(err, provider.CodeInvalid) {
			t.Errorf("route to port %d gave %v, want invalid", port, err)
		}
		if _, err := p.PrepareGate(t.Context(), ref, port); !isCode(err, provider.CodeInvalid) {
			t.Errorf("prepare of port %d gave %v, want invalid", port, err)
		}
	}
}

func TestPrepareGateTwiceIsSafe(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	ref := ready(t, p, "key-gate-twice")
	first, err := p.PrepareGate(t.Context(), ref, 29417)
	if err != nil {
		t.Fatal(err)
	}
	second, err := p.PrepareGate(t.Context(), ref, 29417)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, second, first)
	eq(t, f.boundPort(ref.ID), "29417")
}

func TestGateRouteNeverCallsTheProvider(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	ref := ready(t, p, "key-gate-route")
	if _, err := p.PrepareGate(t.Context(), ref, 29417); err != nil {
		t.Fatal(err)
	}
	calls := f.totalCalls()
	if _, err := p.GateRoute(ref, 29417); err != nil {
		t.Fatal(err)
	}
	eq(t, f.totalCalls(), calls)
}
