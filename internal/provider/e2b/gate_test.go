package e2b

import (
	"errors"
	"testing"
	"time"

	"github.com/oynozan/bugnet-router/internal/provider"
)

func TestE2BCreateKeepsTheTrafficToken(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := mustCreateRef(t, p)
	box, _ := f.box(ref.ID)
	if box.traffic == "" || ref.Meta["traffic_token"] != box.traffic {
		t.Fatalf("ref meta %v, want the traffic token %q", ref.Meta, box.traffic)
	}
}

func TestE2BGateRoute(t *testing.T) {
	p, err := New(Options{APIKey: fakeKey, Templates: fakeTemplates})
	if err != nil {
		t.Fatal(err)
	}
	ref := provider.Ref{Provider: "e2b", ID: "isbx1", Meta: map[string]string{
		"traffic_token": "tt-1",
	}}
	r, err := p.GateRoute(ref, 29417)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, r.BaseURL, "https://29417-"+ref.ID+".e2b.app")
	eq(t, r.Header, map[string]string{"e2b-traffic-access-token": "tt-1"})
	eq(t, len(r.Cookies)+len(r.BlockedPrefixes), 0)

	ref.Meta["domain"] = "e2b-juliett.dev"
	r, err = p.GateRoute(ref, 30001)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, r.BaseURL, "https://30001-"+ref.ID+".e2b-juliett.dev")
}

func TestE2BGateRouteNeedsThePreparedToken(t *testing.T) {
	p := newFake(t).provider(t, fakeKey)
	_, err := p.GateRoute(provider.Ref{Provider: "e2b", ID: "isbx1"}, 29417)
	if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeInvalid ||
		!errors.Is(err, provider.ErrGateUnprepared) {
		t.Fatalf("route without a traffic token gave %v, want unprepared", err)
	}
	for _, port := range []int{0, 65536} {
		ref := provider.Ref{Provider: "e2b", ID: "isbx1", Meta: map[string]string{
			"traffic_token": "tt-1",
		}}
		if _, err := p.GateRoute(ref, port); !isInvalid(err) {
			t.Errorf("route to port %d gave %v, want invalid", port, err)
		}
		if _, err := p.PrepareGate(t.Context(), ref, port); !isInvalid(err) {
			t.Errorf("prepare of port %d gave %v, want invalid", port, err)
		}
	}
}

func TestE2BPrepareGateOnAdoptedSandbox(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := f.add(nil)
	before, _ := f.box(ref.ID)
	meta, err := p.PrepareGate(t.Context(), ref, 29417)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, meta, map[string]string{"traffic_token": before.traffic})
	eq(t, f.connectBodies(), []string{`{"timeout":1}`})
	after, _ := f.box(ref.ID)
	if !after.endAt.Equal(before.endAt) {
		t.Fatalf("connect moved the deadline from %v to %v", before.endAt, after.endAt)
	}
}

func TestE2BPrepareGateFillsWhatTheAdoptedRefLacks(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := f.add(nil)
	box, _ := f.box(ref.ID)
	bare := provider.Ref{Provider: "e2b", ID: ref.ID}
	meta, err := p.PrepareGate(t.Context(), bare, 29417)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, meta, map[string]string{
		"traffic_token": box.traffic, "envd_token": box.token, "domain": fakeDomain,
	})
}

func TestE2BPrepareGateKeepsAKnownToken(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := mustCreateRef(t, p)
	calls := f.requests.Load()
	meta, err := p.PrepareGate(t.Context(), ref, 29417)
	if err != nil || len(meta) != 0 {
		t.Fatalf("prepare with a known token gave %v, %v, want nothing to store", meta, err)
	}
	eq(t, f.requests.Load(), calls)
}

func TestE2BPrepareGateRefusesAPublicSandbox(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := f.add(nil)
	f.setBox(ref.ID, func(b *fakeSandbox) { b.traffic = "" })
	_, err := p.PrepareGate(t.Context(), ref, 29417)
	if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeInternal {
		t.Fatalf("prepare of a sandbox without a traffic token gave %v, want internal", err)
	}
}

func TestE2BPrepareGateOfAGoneSandboxIsNotFound(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	_, err := p.PrepareGate(t.Context(), provider.Ref{Provider: "e2b", ID: "inone"}, 29417)
	if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeNotFound {
		t.Fatalf("prepare of a gone sandbox gave %v, want not_found", err)
	}
}

func TestPrepareGateTwiceIsSafe(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := f.add(nil)
	first, err := p.PrepareGate(t.Context(), ref, 29417)
	if err != nil {
		t.Fatal(err)
	}
	second, err := p.PrepareGate(t.Context(), ref, 29417)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, second, first)
}

func TestGateRouteNeverCallsTheProvider(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := f.add(nil)
	meta, err := p.PrepareGate(t.Context(), ref, 29417)
	if err != nil {
		t.Fatal(err)
	}
	ref.Meta["traffic_token"] = meta["traffic_token"]
	calls := f.requests.Load()
	if _, err := p.GateRoute(ref, 29417); err != nil {
		t.Fatal(err)
	}
	eq(t, f.requests.Load(), calls)
}

func mustCreateRef(t *testing.T, p *Provider) provider.Ref {
	t.Helper()
	ref, err := p.Create(t.Context(), provider.CreateRequest{
		Size: provider.Size{Name: "2x2"}, Lifetime: 10 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func isInvalid(err error) bool {
	e, ok := provider.AsError(err)
	return ok && e.Code == provider.CodeInvalid
}
