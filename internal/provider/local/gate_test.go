package local_test

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/provider/local"
)

// ports reads the gate and sshd ports from ref's meta, failing unless both are TCP ports
func ports(t *testing.T, ref provider.Ref) (int, int) {
	t.Helper()
	gate, err1 := strconv.Atoi(ref.Meta["gate_port"])
	sshd, err2 := strconv.Atoi(ref.Meta["sshd_port"])
	if err1 != nil || err2 != nil || gate < 1 || gate > 65535 || sshd < 1 || sshd > 65535 {
		t.Fatalf("ref meta %v, want a gate_port and an sshd_port", ref.Meta)
	}
	return gate, sshd
}

func TestLocalGatePortsInMeta(t *testing.T) {
	root := t.TempDir()
	p := newLocal(t, local.Options{Root: root})
	req := provider.CreateRequest{IdempotencyKey: "key-gate", Size: small}
	ref, err := p.Create(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	gate, sshd := ports(t, ref)
	if gate == sshd {
		t.Fatalf("gate and sshd share port %d", gate)
	}

	again, err := newLocal(t, local.Options{Root: root}).Create(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, again, ref)
	owned, err := p.ListOwned(t.Context(), nil)
	if err != nil || len(owned) != 1 {
		t.Fatalf("ListOwned gave %v, %v", owned, err)
	}
	eq(t, owned[0].Ref, ref)

	meta, err := p.PrepareGate(t.Context(), ref, provider.DefaultGatePort)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, meta, ref.Meta)
	r, err := p.GateRoute(ref, provider.DefaultGatePort)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, r, provider.GateRoute{BaseURL: "http://127.0.0.1:" + strconv.Itoa(gate)})
}

func TestLocalPrepareGateGivesPortsToAnOlderSandbox(t *testing.T) {
	root := t.TempDir()
	p := newLocal(t, local.Options{Root: root})
	ref := createReady(t, p, nil)
	name := filepath.Join(root, ref.ID, "meta.json")
	raw, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	delete(m, "gate_port")
	delete(m, "sshd_port")
	raw, _ = json.Marshal(m)
	if err := os.WriteFile(name, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	bare := provider.Ref{Provider: ref.Provider, ID: ref.ID}
	meta, err := p.PrepareGate(t.Context(), bare, provider.DefaultGatePort)
	if err != nil {
		t.Fatal(err)
	}
	ports(t, provider.Ref{Meta: meta})
	again, err := p.PrepareGate(t.Context(), bare, provider.DefaultGatePort)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, again, meta)
}

func TestLocalPrepareGateOfAGoneSandboxIsNotFound(t *testing.T) {
	p := newLocal(t, local.Options{})
	ref := createReady(t, p, nil)
	if err := p.Destroy(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	_, err := p.PrepareGate(t.Context(), ref, provider.DefaultGatePort)
	requireCode(t, "prepare of a gone sandbox", err, provider.CodeNotFound)
}

func TestLocalGateRouteNeedsAGatePort(t *testing.T) {
	p := newLocal(t, local.Options{})
	ref := provider.Ref{Provider: "local", ID: "00112233445566778899aabb"}
	if _, err := p.GateRoute(ref, provider.DefaultGatePort); !errors.Is(err,
		provider.ErrGateUnprepared) {
		t.Fatalf("route without a gate port gave %v, want unprepared", err)
	}
	ref.Meta = map[string]string{"gate_port": "seventy"}
	_, err := p.GateRoute(ref, provider.DefaultGatePort)
	requireCode(t, "route with a broken gate port", err, provider.CodeInternal)
	ref.Meta = map[string]string{"gate_port": "30001"}
	for _, port := range []int{0, 65536} {
		_, err := p.GateRoute(ref, port)
		requireCode(t, "route to port "+strconv.Itoa(port), err, provider.CodeInvalid)
		_, err = p.PrepareGate(t.Context(), ref, port)
		requireCode(t, "prepare of port "+strconv.Itoa(port), err, provider.CodeInvalid)
	}
}

func TestPrepareGateTwiceIsSafe(t *testing.T) {
	p := newLocal(t, local.Options{})
	ref := createReady(t, p, nil)
	first, err := p.PrepareGate(t.Context(), ref, provider.DefaultGatePort)
	if err != nil {
		t.Fatal(err)
	}
	second, err := p.PrepareGate(t.Context(), ref, provider.DefaultGatePort)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, second, first)
}

func TestGateRouteNeverCallsTheProvider(t *testing.T) {
	p := newLocal(t, local.Options{})
	ref := createReady(t, p, nil)
	gate, _ := ports(t, ref)
	if err := p.Destroy(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	r, err := p.GateRoute(ref, provider.DefaultGatePort)
	if err != nil {
		t.Fatalf("route from the ref of a sandbox gone from disk gave %v", err)
	}
	eq(t, r.BaseURL, "http://127.0.0.1:"+strconv.Itoa(gate))
}
