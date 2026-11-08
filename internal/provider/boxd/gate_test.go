package boxd

import (
	"errors"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// routed builds an adapter on f that reaches the gate by route
func routed(t *testing.T, f *fakeBoxd, route string) *Provider {
	t.Helper()
	p, err := New(Options{APIKey: fakeKey, Endpoint: f.addr, Insecure: true, GateRoute: route})
	if err != nil {
		t.Fatal(err)
	}
	p.pollInterval = 20 * time.Millisecond
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func TestBoxdPrepareGateSubdomainAndDefault(t *testing.T) {
	f := newFake(t)
	p := routed(t, f, "subdomain")
	ref := mustCreate(t, p, "key-gate-sub")
	vm, _ := f.vm(ref.ID)
	meta, err := p.PrepareGate(t.Context(), ref, 29417)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, meta, map[string]string{"gate_host": "bn." + vm.name + "." + fakeAccessDomain})
	eq(t, f.proxiesOf(ref.ID), map[string]uint32{"bn": 29417})
	eq(t, f.callCount("SetBotProtection"), 0)
	ref.Meta["gate_host"] = meta["gate_host"]
	r, err := p.GateRoute(ref, 29417)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, r, provider.GateRoute{BaseURL: "https://bn." + vm.name + "." + fakeAccessDomain})

	p = routed(t, f, "default")
	ref = mustCreate(t, p, "key-gate-default")
	f.setVM(ref.ID, func(vm *fakeVM) { vm.proxyPort = "auto" })
	vm, _ = f.vm(ref.ID)
	meta, err = p.PrepareGate(t.Context(), ref, 29417)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, meta, map[string]string{"gate_host": vm.name + "." + fakeAccessDomain})
	eq(t, f.botModes(), []string{"off"})
	vm, _ = f.vm(ref.ID)
	eq(t, vm.proxyPort, "29417")
	eq(t, len(f.proxiesOf(ref.ID)), 0)
	ref.Meta["gate_host"] = meta["gate_host"]
	r, err = p.GateRoute(ref, 29417)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, r, provider.GateRoute{BaseURL: "https://" + vm.name + "." + fakeAccessDomain})

	if _, err := p.PrepareGate(t.Context(), ref, 29417); err != nil {
		t.Fatal(err)
	}
	eq(t, f.botModes(), []string{"off"})
}

func TestBoxdPrepareGateNeedsTheMachinesDomain(t *testing.T) {
	f := newFake(t)
	p := routed(t, f, "subdomain")
	ref := mustCreate(t, p, "key-gate-nodomain")
	f.setVM(ref.ID, func(vm *fakeVM) { vm.accessDomain = "" })
	_, err := p.PrepareGate(t.Context(), ref, 29417)
	if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeInternal {
		t.Fatalf("prepare without an access domain gave %v, want internal", err)
	}
	eq(t, len(f.proxiesOf(ref.ID)), 0)
}

func TestBoxdPrepareGateOfAGoneMachineIsNotFound(t *testing.T) {
	f := newFake(t)
	p := routed(t, f, "subdomain")
	_, err := p.PrepareGate(t.Context(), provider.Ref{Provider: "boxd", ID: "vm-none"}, 29417)
	if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeNotFound {
		t.Fatalf("prepare of a gone machine gave %v, want not_found", err)
	}
}

func TestBoxdGateRouteNeedsThePreparedHost(t *testing.T) {
	_, p := newTestProvider(t)
	ref := provider.Ref{Provider: "boxd", ID: "vm-0001"}
	_, err := p.GateRoute(ref, 29417)
	if !errors.Is(err, provider.ErrGateUnprepared) {
		t.Fatalf("route without a gate host gave %v, want unprepared", err)
	}
	ref.Meta = map[string]string{"gate_host": "bn.m.boxd.sh"}
	for _, port := range []int{0, 65536} {
		if _, err := p.GateRoute(ref, port); !isCode(err, provider.CodeInvalid) {
			t.Errorf("route to port %d gave %v, want invalid", port, err)
		}
		if _, err := p.PrepareGate(t.Context(), ref, port); !isCode(err, provider.CodeInvalid) {
			t.Errorf("prepare of port %d gave %v, want invalid", port, err)
		}
	}
}

func TestBoxdRefusesAnUnknownGateRoute(t *testing.T) {
	if _, err := New(Options{APIKey: fakeKey, GateRoute: "tunnel"}); err == nil {
		t.Fatal("gate route tunnel was accepted")
	}
	p, err := New(Options{APIKey: fakeKey})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	eq(t, p.gateRoute, "default")
}

func TestPrepareGateTwiceIsSafe(t *testing.T) {
	f := newFake(t)
	p := routed(t, f, "subdomain")
	ref := mustCreate(t, p, "key-gate-twice")
	first, err := p.PrepareGate(t.Context(), ref, 29417)
	if err != nil {
		t.Fatal(err)
	}
	second, err := p.PrepareGate(t.Context(), ref, 29417)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, second, first)
	eq(t, f.proxiesOf(ref.ID), map[string]uint32{"bn": 29417})
	eq(t, f.callCount("CreateProxy"), 2)
}

func TestGateRouteNeverCallsTheProvider(t *testing.T) {
	f := newFake(t)
	p := routed(t, f, "subdomain")
	ref := mustCreate(t, p, "key-gate-route")
	meta, err := p.PrepareGate(t.Context(), ref, 29417)
	if err != nil {
		t.Fatal(err)
	}
	ref.Meta["gate_host"] = meta["gate_host"]
	calls := len(f.callOrder())
	if _, err := p.GateRoute(ref, 29417); err != nil {
		t.Fatal(err)
	}
	eq(t, len(f.callOrder()), calls)
}
