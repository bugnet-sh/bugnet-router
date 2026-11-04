package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/oynozan/bugnet-router/internal/config"
	"github.com/oynozan/bugnet-router/internal/handle"
	"github.com/oynozan/bugnet-router/internal/ordertoken"
)

const catalogJSON = `{"packages": [
	{"id": "small-10m", "resources": {"vcpu": 1, "memory_mib": 512}, "minutes": 10,
		"price_micros": 50000},
	{"id": "small-10m-more", "resources": {"vcpu": 1, "memory_mib": 512}, "minutes": 10,
		"price_micros": 45000, "extension_of": "small-10m"}
]}`

func env(vars map[string]string) func(string) string {
	return func(name string) string { return vars[name] }
}

func runCLI(t *testing.T, getenv func(string) string, args ...string) (string, string, int) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := run(t.Context(), args, &stdout, &stderr, getenv)
	return stdout.String(), stderr.String(), code
}

func newKey(t *testing.T) (ed25519.PublicKey, string) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return pub, base64.StdEncoding.EncodeToString(priv.Seed())
}

func TestKeysMakeAValidConfig(t *testing.T) {
	out, stderr, code := runCLI(t, env(nil), "keys")
	if code != 0 {
		t.Fatalf("keys exited %d: %s", code, stderr)
	}
	vars := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		k, v, ok := strings.Cut(line, "=")
		if !ok || v == "" {
			t.Fatalf("line %q is not KEY=value", line)
		}
		vars[k] = v
	}
	vars["BUGNET_DATABASE_URL"] = "postgres://localhost/unused"
	vars["BUGNET_PACKAGES_FILE"] = "packages.json"
	cfg, err := config.LoadFrom(func(name string) (string, bool) {
		v, ok := vars[name]
		return v, ok
	})
	if err != nil {
		t.Fatalf("config from the printed keys: %v", err)
	}
	if cfg.EventSigningKey == nil {
		t.Fatal("no event signing key")
	}

	pub := base64.StdEncoding.EncodeToString(cfg.EventSigningKey.Public().(ed25519.PublicKey))
	if vars["BUGNET_EVENT_PUBLIC_KEY"] != pub {
		t.Error("BUGNET_EVENT_PUBLIC_KEY does not belong to BUGNET_EVENT_SIGNING_KEY")
	}
	token, _, code := runCLI(t, env(nil), "token", "--priv", vars["BUGNET_ORDER_PRIVATE_KEY"],
		"--package", "small-10m", "--amount", "1")
	if code != 0 {
		t.Fatalf("token exited %d", code)
	}
	_, err = ordertoken.Verify(cfg.OrderTokenPublicKey, strings.TrimSpace(token), time.Now())
	if err != nil {
		t.Errorf("token signed with BUGNET_ORDER_PRIVATE_KEY: %v", err)
	}
}

func TestTokenStartWithFlags(t *testing.T) {
	pub, priv := newKey(t)
	out, stderr, code := runCLI(t, env(nil), "token", "--priv", priv, "--kind", "start",
		"--package", "small-10m", "--order", "order-1", "--amount", "123", "--ttl", "2m")
	if code != 0 {
		t.Fatalf("token exited %d: %s", code, stderr)
	}
	c, err := ordertoken.Verify(pub, strings.TrimSpace(out), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if c.OrderID != "order-1" || c.Kind != ordertoken.KindStart ||
		c.PackageID != "small-10m" || c.AmountMicros != 123 || c.HandleSHA256 != "" {
		t.Errorf("claims %+v", c)
	}
	if left := time.Until(c.ExpiresAt); left < time.Minute || left > 2*time.Minute {
		t.Errorf("token expires in %s, want about 2m", left)
	}
}

func TestTokenExtendFromEnvironment(t *testing.T) {
	pub, priv := newKey(t)
	packages := filepath.Join(t.TempDir(), "packages.json")
	if err := os.WriteFile(packages, []byte(catalogJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	h, sum := handle.New()
	out, stderr, code := runCLI(t, env(map[string]string{
		"BUGNET_ORDER_PRIVATE_KEY": priv, "BUGNET_PACKAGES_FILE": packages,
	}), "token", "--kind", "extend", "--package", "small-10m-more", "--handle", h)
	if code != 0 {
		t.Fatalf("token exited %d: %s", code, stderr)
	}
	c, err := ordertoken.Verify(pub, strings.TrimSpace(out), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if c.Kind != ordertoken.KindExtend || c.AmountMicros != 45000 ||
		c.HandleSHA256 != hex.EncodeToString(sum) || c.OrderID == "" {
		t.Errorf("claims %+v, want the extension price, the handle hash and an order id", c)
	}
	if left := time.Until(c.ExpiresAt); left < 9*time.Minute || left > 10*time.Minute {
		t.Errorf("token expires in %s, want about 10m", left)
	}
}

func TestTokenRefusesBadArguments(t *testing.T) {
	_, priv := newKey(t)
	h, _ := handle.New()
	for name, args := range map[string][]string{
		"no package":            {"--amount", "1"},
		"unknown kind":          {"--kind", "renew", "--package", "p", "--amount", "1"},
		"extend without handle": {"--kind", "extend", "--package", "p", "--amount", "1"},
		"handle on a start":     {"--package", "p", "--amount", "1", "--handle", h},
		"no amount or catalog":  {"--package", "p"},
		"extra argument":        {"--package", "p", "--amount", "1", "more"},
	} {
		args := append([]string{"token", "--priv", priv}, args...)
		if _, _, code := runCLI(t, env(nil), args...); code != 2 {
			t.Errorf("%s: exited %d, want 2", name, code)
		}
	}
	if _, _, code := runCLI(t, env(nil), "token", "--package", "p", "--amount", "1"); code != 2 {
		t.Errorf("no private key: exited %d, want 2", code)
	}
}

func TestUsage(t *testing.T) {
	if _, stderr, code := runCLI(t, env(nil), "launch"); code != 2 ||
		!strings.Contains(stderr, "usage") {
		t.Errorf("unknown command exited %d with %q, want 2 and the usage", code, stderr)
	}
	if _, _, code := runCLI(t, env(nil)); code != 2 {
		t.Errorf("no command exited %d, want 2", code)
	}
	if out, _, code := runCLI(t, env(nil), "help"); code != 0 || !strings.Contains(out, "usage") {
		t.Errorf("help exited %d with %q, want 0 and the usage", code, out)
	}
}
