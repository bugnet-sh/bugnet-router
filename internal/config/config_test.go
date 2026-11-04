package config

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const secretToken = "0123456789abcdef0123456789abcdef"

func pubB64(t *testing.T) string {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(pub)
}

func env(t *testing.T, over map[string]string) func(string) (string, bool) {
	m := map[string]string{
		"BUGNET_DATABASE_URL":           "postgres://localhost/x",
		"BUGNET_INTERNAL_TOKEN":         secretToken,
		"BUGNET_ORDER_TOKEN_PUBLIC_KEY": pubB64(t),
		"BUGNET_PACKAGES_FILE":          "/etc/packages.json",
	}
	for k, v := range over {
		if v == "-" {
			delete(m, k)
		} else {
			m[k] = v
		}
	}
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func TestLoadDefaultsAndRequired(t *testing.T) {
	c, err := LoadFrom(env(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	if c.ValkeyAddr != "127.0.0.1:6379" || c.ValkeyPrefix != "bn:" || c.ListenAddr != ":8080" {
		t.Fatalf("defaults: %+v", c)
	}
	if strings.Join(c.Providers, ",") != "koyeb,boat,e2b,boxd" || c.MarginFloor != 0.5 {
		t.Fatalf("defaults: %+v", c)
	}
	host, _ := os.Hostname()
	if !strings.HasPrefix(c.ReplicaID, host) || c.ReplicaID == host {
		t.Fatalf("replica id %q", c.ReplicaID)
	}
	if c.EventSigningKey != nil || c.LocalEnabled || len(c.ProviderKeys) != 0 {
		t.Fatalf("optional fields set: %+v", c)
	}
	if c.LocalRoot != filepath.Join(os.TempDir(), "bugnet-local") {
		t.Fatalf("local root %q", c.LocalRoot)
	}
	if len(c.OrderTokenPublicKey) != ed25519.PublicKeySize {
		t.Fatal("public key not parsed")
	}
	for _, name := range []string{
		"BUGNET_DATABASE_URL", "BUGNET_INTERNAL_TOKEN",
		"BUGNET_ORDER_TOKEN_PUBLIC_KEY", "BUGNET_PACKAGES_FILE",
	} {
		_, err := LoadFrom(env(t, map[string]string{name: "-"}))
		if err == nil || !strings.Contains(err.Error(), name) {
			t.Errorf("missing %s: %v", name, err)
		}
	}
}

func TestLoadValidation(t *testing.T) {
	shortKey := base64.StdEncoding.EncodeToString([]byte("short"))
	short := "BUGNET_INTERNAL_TOKEN"
	bad := map[string]map[string]string{
		short:                           {short: "tooshort-secret"},
		"BUGNET_ORDER_TOKEN_PUBLIC_KEY": {"BUGNET_ORDER_TOKEN_PUBLIC_KEY": shortKey},
		"BUGNET_MARGIN_FLOOR":           {"BUGNET_MARGIN_FLOOR": "11"},
		"BUGNET_EVENT_SIGNING_KEY":      {"BUGNET_EVENT_SIGNING_KEY": "AAAA"},
		"BUGNET_PROVIDERS":              {"BUGNET_PROVIDERS": "koyeb,nope"},
		"local":                         {"BUGNET_PROVIDERS": "boat,local"},
	}
	for name, over := range bad {
		_, err := LoadFrom(env(t, over))
		if err == nil {
			t.Errorf("%s: expected error", name)
			continue
		}
		for _, v := range over {
			if strings.Contains(err.Error(), v) && len(v) > 8 {
				t.Errorf("%s: error echoes value: %v", name, err)
			}
		}
	}
	if _, err := LoadFrom(env(t, map[string]string{"BUGNET_MARGIN_FLOOR": "-0.1"})); err == nil {
		t.Error("negative margin accepted")
	}
	if _, err := LoadFrom(env(t, map[string]string{"BUGNET_MARGIN_FLOOR": "abc"})); err == nil {
		t.Error("non-numeric margin accepted")
	}
}

func TestLoadOptional(t *testing.T) {
	seed := base64.StdEncoding.EncodeToString(make([]byte, 32))
	c, err := LoadFrom(env(t, map[string]string{
		"BUGNET_EVENT_SIGNING_KEY":     seed,
		"BUGNET_EVENTS_URL":            "https://events.example",
		"BUGNET_ENABLE_LOCAL_PROVIDER": "1",
		"BUGNET_LOCAL_ROOT":            "/var/bn",
		"BUGNET_PROVIDERS":             "local,e2b",
		"BOXD_API_KEY":                 "bk",
		"E2B_API_KEY":                  "ek",
		"KOYEB_API_TOKEN":              "",
		"BOAT_API_KEY":                 "ok",
		"BUGNET_RUNNER_LINUX_AMD64":    "/r/amd",
		"BUGNET_RUNNER_LINUX_ARM64":    "/r/arm",
		"BUGNET_MARGIN_FLOOR":          "2.5",
		"BUGNET_REPLICA_ID":            "r1",
		"BUGNET_VALKEY_ADDR":           "v:1",
		"BUGNET_LISTEN_ADDR":           ":9",
		"BUGNET_VALKEY_PREFIX":         "t:",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.EventSigningKey) != ed25519.PrivateKeySize || c.EventsURL != "https://events.example" {
		t.Fatalf("event config: %+v", c)
	}
	if !c.LocalEnabled || c.LocalRoot != "/var/bn" || c.MarginFloor != 2.5 || c.ReplicaID != "r1" {
		t.Fatalf("%+v", c)
	}
	want := map[string]string{"boxd": "bk", "e2b": "ek", "boat": "ok"}
	if len(c.ProviderKeys) != 3 {
		t.Fatalf("keys: %v", c.ProviderKeys)
	}
	for k, v := range want {
		if c.ProviderKeys[k] != v {
			t.Fatalf("key %s: %v", k, c.ProviderKeys)
		}
	}
	if c.RunnerLinuxAMD64 != "/r/amd" || c.RunnerLinuxARM64 != "/r/arm" {
		t.Fatalf("%+v", c)
	}
}

func TestLoadUsesProcessEnv(t *testing.T) {
	t.Setenv("BUGNET_DATABASE_URL", "postgres://localhost/x")
	t.Setenv("BUGNET_INTERNAL_TOKEN", secretToken)
	t.Setenv("BUGNET_ORDER_TOKEN_PUBLIC_KEY", pubB64(t))
	t.Setenv("BUGNET_PACKAGES_FILE", "/p.json")
	if _, err := Load(); err != nil {
		t.Fatal(err)
	}
}
