package hf

import (
	"strings"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

func TestHFGateRoute(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	var _ provider.Gater = p
	j := f.addJob("RUNNING", map[string]string{"bugnet": "1"})
	f.set(j, func(j *fakeJob) { j.token = strings.Repeat("ab", 32) })
	ref := refTo(j)
	meta, err := p.PrepareGate(t.Context(), ref, provider.DefaultGatePort)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, meta, map[string]string{})
	route, err := p.GateRoute(ref, provider.DefaultGatePort)
	if err != nil {
		t.Fatal(err)
	}
	// The gate key is the route's only credential, so it carries no header and no cookie
	eq(t, route, provider.GateRoute{BaseURL: "https://" + j.id + "--29417.hf.jobs"})
	if err := checkSBXURL(route.BaseURL + "/"); err != nil {
		t.Fatalf("the route %s is outside HF's Job hosts: %v", route.BaseURL, err)
	}
	// The port is public from the Job's create, so neither step calls HF
	eq(t, len(f.all()), 0)
}

func TestHFGateRouteRefuses(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	j := f.addJob("RUNNING", nil)
	ref := refTo(j)
	cases := []struct {
		name string
		ref  provider.Ref
		port int
		code provider.Code
	}{
		{"sbx-server's port", ref, sbxPort, provider.CodeInvalid},
		{"a port the Job does not expose", ref, 8080, provider.CodeInvalid},
		{"no port", ref, 0, provider.CodeInvalid},
		{"a port past TCP's", ref, 70000, provider.CodeInvalid},
		{"another provider's sandbox", provider.Ref{Provider: "e2b", ID: j.id},
			provider.DefaultGatePort, provider.CodeNotFound},
		{"no Job id", provider.Ref{Provider: "hf"}, provider.DefaultGatePort,
			provider.CodeNotFound},
		{"an id that leaves the host", provider.Ref{Provider: "hf", ID: "evil.example/x"},
			provider.DefaultGatePort, provider.CodeInvalid},
		{"an id with a dot", provider.Ref{Provider: "hf", ID: "a.b"}, provider.DefaultGatePort,
			provider.CodeInvalid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := p.GateRoute(tc.ref, tc.port)
			if e, ok := provider.AsError(err); !ok || e.Code != tc.code {
				t.Fatalf("GateRoute = %v, want %s", err, tc.code)
			}
			meta, err := p.PrepareGate(t.Context(), tc.ref, tc.port)
			if e, ok := provider.AsError(err); !ok || e.Code != tc.code || meta != nil {
				t.Fatalf("PrepareGate = %v, %v, want %s", meta, err, tc.code)
			}
		})
	}
	eq(t, len(f.all()), 0)
}
