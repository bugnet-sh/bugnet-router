package gateclient_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/oynozan/bugnet-router/internal/gateclient"
	"github.com/oynozan/bugnet-router/internal/provider"
)

const key = "k0123456789abcdefghijklmnopqrstuvwxyzABCDEF"

// fakeGate answers as bugnet-run's gate does, and records what each request carried
type fakeGate struct {
	got  []*http.Request
	open map[string]bool
}

func (g *fakeGate) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g.got = append(g.got, r)
	w.Header().Set(gateclient.HeaderResponse, "1")
	if r.Header.Get(gateclient.HeaderKey) != key {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	switch {
	case r.URL.Path == gateclient.PathHealth:
		w.Header().Set(gateclient.HeaderVersion, "1")
		w.WriteHeader(http.StatusNoContent)
	case strings.HasPrefix(r.URL.Path, gateclient.PathPort):
		if g.open[strings.TrimPrefix(r.URL.Path, gateclient.PathPort)] {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func serve(t *testing.T, h http.Handler) provider.GateRoute {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return provider.GateRoute{
		BaseURL: srv.URL,
		Header:  map[string]string{"E2b-Traffic-Access-Token": "traffic"},
		Cookies: map[string]string{"_port_auth": "cookie"},
	}
}

func TestHealthSendsTheKeyAndTheProviderCredentials(t *testing.T) {
	g := &fakeGate{}
	r := serve(t, g)
	if err := gateclient.Health(t.Context(), r, key); err != nil {
		t.Fatal(err)
	}
	req := g.got[0]
	if req.Method != http.MethodGet || req.URL.Path != gateclient.PathHealth ||
		req.Header.Get(gateclient.HeaderKey) != key ||
		req.Header.Get("E2b-Traffic-Access-Token") != "traffic" {
		t.Fatalf("the gate got %s %s with %v", req.Method, req.URL.Path, req.Header)
	}
	if c, err := req.Cookie("_port_auth"); err != nil || c.Value != "cookie" {
		t.Fatalf("the gate got the cookie %v, %v", c, err)
	}
}

func TestHealthRefusesAWrongKeyAndAnswersNotFromTheGate(t *testing.T) {
	r := serve(t, &fakeGate{})
	if err := gateclient.Health(t.Context(), r, "wrong"); !errors.Is(err,
		gateclient.ErrKeyRefused) || strings.Contains(err.Error(), "wrong") {
		t.Fatalf("a wrong key: %v", err)
	}
	proxy := serve(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	err := gateclient.Health(t.Context(), proxy, key)
	if !errors.Is(err, gateclient.ErrNotGate) || strings.Contains(err.Error(), key) {
		t.Fatalf("a 204 without the gate's header: %v", err)
	}
}

func TestHealthNeverFollowsARedirect(t *testing.T) {
	var followed bool
	target := serve(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		followed = true
		w.Header().Set(gateclient.HeaderResponse, "1")
		w.WriteHeader(http.StatusNoContent)
	}))
	r := serve(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		http.Redirect(w, req, target.BaseURL+gateclient.PathHealth, http.StatusFound)
	}))
	if err := gateclient.Health(t.Context(), r, key); err == nil || followed {
		t.Fatalf("a redirect: %v, followed %v", err, followed)
	}
}

func TestHealthErrorsQuoteNoRoute(t *testing.T) {
	r := provider.GateRoute{BaseURL: "http://127.0.0.1:1"}
	err := gateclient.Health(t.Context(), r, key)
	if err == nil || strings.Contains(err.Error(), "127.0.0.1:1") {
		t.Fatalf("a refused route: %v", err)
	}
	for _, base := range []string{"", "ftp://x", "/relative", "http://"} {
		err := gateclient.Health(t.Context(), provider.GateRoute{BaseURL: base}, key)
		if err == nil {
			t.Fatalf("base URL %q answered nil", base)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := gateclient.Health(ctx, serve(t, &fakeGate{}), key); err == nil {
		t.Fatal("a cancelled context answered nil")
	}
}

func TestPortListening(t *testing.T) {
	g := &fakeGate{open: map[string]bool{"8000": true}}
	r := serve(t, g)
	for port, want := range map[int]bool{8000: true, 3000: false} {
		got, err := gateclient.PortListening(t.Context(), r, key, port)
		if err != nil || got != want {
			t.Fatalf("port %d: %v, %v, want %v", port, got, err, want)
		}
	}
	if _, err := gateclient.PortListening(t.Context(), r, key, 0); err == nil {
		t.Fatal("port 0 answered nil")
	}
	proxy := serve(t, http.NotFoundHandler())
	if _, err := gateclient.PortListening(t.Context(), proxy, key, 8000); !errors.Is(err,
		gateclient.ErrNotGate) {
		t.Fatalf("a provider's own 404: %v", err)
	}
	// The gate answers a wrong key with the same 404 as a closed port, so health tells them apart
	for _, port := range []int{8000, 3000} {
		got, err := gateclient.PortListening(t.Context(), r, "wrong", port)
		if got || !errors.Is(err, gateclient.ErrKeyRefused) {
			t.Fatalf("port %d with a wrong key: %v, %v", port, got, err)
		}
	}
}

func TestWireNames(t *testing.T) {
	got := []string{gateclient.HeaderKey, gateclient.HeaderResponse, gateclient.HeaderVersion,
		gateclient.PathHealth, gateclient.PathPort}
	want := []string{"Bugnet-Gate", "Bugnet-Gate-Response", "Bugnet-Gate-Version",
		"/__bugnet/health", "/__bugnet/port/"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("wire name %q, want %q", got[i], want[i])
		}
	}
}
