package boat

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/oynozan/bugnet-router/internal/provider"
)

func TestBoatPrepareGateKeepsTokenNeverLogsURL(t *testing.T) {
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))
	t.Cleanup(func() { slog.SetDefault(old) })

	f := newFake(t)
	p := f.provider(t, fakeKey)
	s := f.add(nil)
	ref := newRef(s.id, s.created)
	meta, err := p.PrepareGate(t.Context(), ref, 29417)
	if err != nil {
		t.Fatal(err)
	}
	token := f.hostToken(s.id, 29417)
	eq(t, meta, map[string]string{"gate_host": s.id + "-29417.on.boat.dev", "_token": token})
	eq(t, f.hostBodies(), []string{`{"port":29417}`})

	for k, v := range meta {
		ref.Meta[k] = v
	}
	r, err := p.GateRoute(ref, 29417)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, r, provider.GateRoute{
		BaseURL: "https://" + s.id + "-29417.on.boat.dev",
		Cookies: map[string]string{"_port_auth": token},
	})

	for _, reply := range []string{
		"https://" + s.id + "-29417.on.boat.dev",
		"http://" + s.id + "-29417.on.boat.dev?_token=" + token,
		"https://%zz-29417.on.boat.dev?_token=" + token,
		"https://u:" + token + "@" + s.id + "-29417.on.boat.dev?_token=" + token,
		"",
	} {
		f.setHostURL(reply)
		_, err := p.PrepareGate(t.Context(), ref, 29417)
		if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeInternal {
			t.Errorf("reply %q gave %v, want internal", reply, err)
			continue
		}
		if strings.Contains(err.Error(), token) || strings.Contains(err.Error(), "boat.dev") {
			t.Errorf("reply %q gave an error quoting the URL, %v", reply, err)
		}
	}
	if strings.Contains(logs.String(), token) || strings.Contains(logs.String(), "on.boat.dev") {
		t.Fatalf("the logs hold the hosted URL, %s", logs.String())
	}
}

func TestBoatPrepareGateOfAGoneSandboxIsNotFound(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	_, err := p.PrepareGate(t.Context(), provider.Ref{Provider: "boat", ID: "bx_none"}, 29417)
	if !isCode(err, provider.CodeNotFound) {
		t.Fatalf("prepare of a gone sandbox gave %v, want not_found", err)
	}
}

func TestBoatGateRouteNeedsTheHostAndToken(t *testing.T) {
	p := newFake(t).provider(t, fakeKey)
	for _, meta := range []map[string]string{
		nil, {"gate_host": "a-29417.on.boat.dev"}, {"_token": "tok"},
	} {
		ref := provider.Ref{Provider: "boat", ID: "bx_1", Meta: meta}
		if _, err := p.GateRoute(ref, 29417); !errors.Is(err, provider.ErrGateUnprepared) {
			t.Errorf("route with meta %v gave %v, want unprepared", meta, err)
		}
	}
	ref := provider.Ref{Provider: "boat", ID: "bx_1", Meta: map[string]string{
		"gate_host": "a-29417.on.boat.dev", "_token": "tok",
	}}
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
	p := f.provider(t, fakeKey)
	s := f.add(nil)
	ref := newRef(s.id, s.created)
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
	s := f.add(nil)
	ref := newRef(s.id, s.created)
	meta, err := p.PrepareGate(t.Context(), ref, 29417)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range meta {
		ref.Meta[k] = v
	}
	calls := f.totalCalls()
	if _, err := p.GateRoute(ref, 29417); err != nil {
		t.Fatal(err)
	}
	eq(t, f.totalCalls(), calls)
}
