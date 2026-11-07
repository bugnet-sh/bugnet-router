package hostrules

import (
	"errors"
	"strings"
	"testing"
)

// rules is a set like the router's, with the provider domains of DP 10.3
func rules() Rules {
	return Rules{
		BugnetNames: []string{"bugnet.sh"},
		ProviderDomains: []string{
			"e2b.app", "e2b.dev", "e2b-juliett.dev", "boxd.sh", "koyeb.app", "boat.dev",
			"ascii.dev",
		},
		SharedSuffixes: []string{"workers.dev", "up.railway.app", "github.io"},
	}
}

func refused(t *testing.T, host string, r Rules) {
	t.Helper()
	err := Check(host, r)
	if !errors.Is(err, ErrRefused) {
		t.Fatalf("%q answered %v, want ErrRefused", host, err)
	}
	if host != "" && strings.Contains(err.Error(), host) {
		t.Fatalf("the error for %q quotes the host: %v", host, err)
	}
}

func TestHostRules(t *testing.T) {
	r := rules()
	for _, host := range []string{
		"Agent402.tools", "a.example.", "1.2.3.4", "*.x.com", "x.com:443", "co.uk",
		"e2b.app", "x.boxd.sh", "preview.bugnet.sh",
		"", "localhost", "com", "bugnet.sh", "a.b.e2b-juliett.dev", "https://x.com",
		"x.com/a", "[::1]", "::1", "127.1", "a b.com", "xn--bcher-kva", "github.io",
		strings.Repeat("a", 64) + ".com",
	} {
		refused(t, host, r)
	}
	for _, host := range []string{
		"agent402.tools", "api.example.com", "x.com", "xn--bcher-kva.example", "a.co.uk",
		"notbugnet.sh", "e2b.application.com", "myboxd.sh", "a.railway.app",
	} {
		if err := Check(host, r); err != nil {
			t.Fatalf("%q refused: %v", host, err)
		}
	}
}

func TestSharedAndBlockedHosts(t *testing.T) {
	r := rules()
	refused(t, "api.workers.dev", r)
	refused(t, "a.b.workers.dev", r)
	refused(t, "x.up.railway.app", r)
	refused(t, "oynozan.github.io", r)

	r.AllowedShared = []string{"api.workers.dev"}
	if err := Check("api.workers.dev", r); err != nil {
		t.Fatalf("an allowed shared host refused: %v", err)
	}
	refused(t, "b.api.workers.dev", r)
	refused(t, "other.workers.dev", r)

	r.Blocked = []string{"bad.example.com", "api.workers.dev"}
	refused(t, "bad.example.com", r)
	refused(t, "api.workers.dev", r)
	if err := Check("sub.bad.example.com", r); err != nil {
		t.Fatalf("a name under a blocked host refused: %v", err)
	}
	if err := Check("good.example.com", r); err != nil {
		t.Fatalf("a plain host refused: %v", err)
	}
}

func TestEmptyRulesStillCheckTheName(t *testing.T) {
	for _, host := range []string{"Upper.com", "1.2.3.4", "co.uk", "com", "*.x.com"} {
		refused(t, host, Rules{})
	}
	if err := Check("api.example.com", Rules{}); err != nil {
		t.Fatal(err)
	}
}

func TestUnderSuffix(t *testing.T) {
	list := []string{"workers.dev", "boxd.sh"}
	for _, c := range []struct {
		host string
		want bool
	}{
		{"workers.dev", true},
		{"a.workers.dev", true},
		{"a.b.workers.dev", true},
		{"boxd.sh", true},
		{"x.boxd.sh", true},
		{"myworkers.dev", false},
		{"workers.dev.evil.com", false},
		{"dev", false},
		{"", false},
		{"orkers.dev", false},
	} {
		eq(t, UnderSuffix(c.host, list), c.want)
	}
	eq(t, UnderSuffix("a.workers.dev", nil), false)
}
