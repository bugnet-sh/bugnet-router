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

var handleKeyB64 = base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))

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
		"BUGNET_HANDLE_KEY":             handleKeyB64,
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
	if c.LocalRunnerPath != "" || c.LocalHourlyMicros != 0 {
		t.Fatalf("local runner settings: %+v", c)
	}
	if c.LocalRoot != filepath.Join(os.TempDir(), "bugnet-local") {
		t.Fatalf("local root %q", c.LocalRoot)
	}
	if string(c.HandleKey) != "0123456789abcdef0123456789abcdef" {
		t.Fatal("handle key not parsed")
	}
	if len(c.OrderTokenPublicKey) != ed25519.PublicKeySize {
		t.Fatal("public key not parsed")
	}
	for _, name := range []string{
		"BUGNET_DATABASE_URL", "BUGNET_INTERNAL_TOKEN",
		"BUGNET_ORDER_TOKEN_PUBLIC_KEY", "BUGNET_PACKAGES_FILE", "BUGNET_HANDLE_KEY",
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
		"BUGNET_HANDLE_KEY":             {"BUGNET_HANDLE_KEY": shortKey},
		"handle key not base64":         {"BUGNET_HANDLE_KEY": "!!not-base64!!"},
		"BUGNET_MARGIN_FLOOR":           {"BUGNET_MARGIN_FLOOR": "11"},
		"negative margin":               {"BUGNET_MARGIN_FLOOR": "-0.1"},
		"non-numeric margin":            {"BUGNET_MARGIN_FLOOR": "abc"},
		"NaN margin":                    {"BUGNET_MARGIN_FLOOR": "NaN"},
		"+Inf margin":                   {"BUGNET_MARGIN_FLOOR": "+Inf"},
		"-Inf margin":                   {"BUGNET_MARGIN_FLOOR": "-Inf"},
		"BUGNET_EVENT_SIGNING_KEY":      {"BUGNET_EVENT_SIGNING_KEY": "AAAA"},
		"BUGNET_PROVIDERS":              {"BUGNET_PROVIDERS": "koyeb,nope"},
		"duplicate providers":           {"BUGNET_PROVIDERS": "boat,e2b,boat"},
		"trailing comma":                {"BUGNET_PROVIDERS": "boat,"},
		"empty entry":                   {"BUGNET_PROVIDERS": "boat,,e2b"},
		"local":                         {"BUGNET_PROVIDERS": "boat,local"},
		"BUGNET_LOCAL_RUNNER": {
			"BUGNET_PROVIDERS": "e2b,local", "BUGNET_ENABLE_LOCAL_PROVIDER": "1",
		},
		"BUGNET_LOCAL_HOURLY_MICROS":    {"BUGNET_LOCAL_HOURLY_MICROS": "-1"},
		"non-numeric local hourly cost": {"BUGNET_LOCAL_HOURLY_MICROS": "1.5"},
	}
	secrets := map[string]string{
		"BUGNET_DATABASE_URL":   "postgres://user:dbpass@localhost/x",
		"BUGNET_INTERNAL_TOKEN": secretToken,
		"BUGNET_HANDLE_KEY":     handleKeyB64,
		"BOXD_API_KEY":          "bk",
		"E2B_API_KEY":           "ek",
		"KOYEB_API_TOKEN":       "kt",
		"BOAT_API_KEY":          "ok",
	}
	for name, over := range bad {
		all := map[string]string{}
		for k, v := range secrets {
			all[k] = v
		}
		for k, v := range over {
			all[k] = v
		}
		_, err := LoadFrom(env(t, all))
		if err == nil {
			t.Errorf("%s: expected error", name)
			continue
		}
		if strings.HasPrefix(name, "BUGNET_") && !strings.Contains(err.Error(), name) {
			t.Errorf("%s: error does not name the variable: %v", name, err)
		}
		for k, v := range all {
			if strings.Contains(err.Error(), v) {
				t.Errorf("%s: error echoes value of %s: %v", name, k, err)
			}
		}
	}
	for _, miss := range []string{"BUGNET_INTERNAL_TOKEN", "BUGNET_HANDLE_KEY"} {
		over := map[string]string{miss: "-"}
		for k, v := range secrets {
			if k != miss {
				over[k] = v
			}
		}
		_, err := LoadFrom(env(t, over))
		if err == nil {
			t.Fatalf("missing %s accepted", miss)
		}
		for _, v := range secrets {
			if strings.Contains(err.Error(), v) {
				t.Errorf("missing %s: error echoes a secret: %v", miss, err)
			}
		}
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
		"BUGNET_LOCAL_RUNNER":          "/r/local",
		"BUGNET_LOCAL_HOURLY_MICROS":   "1500",
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
	if c.LocalRunnerPath != "/r/local" || c.LocalHourlyMicros != 1500 {
		t.Fatalf("local runner settings: %+v", c)
	}
}

func TestLoadUsesProcessEnv(t *testing.T) {
	t.Setenv("BUGNET_DATABASE_URL", "postgres://localhost/x")
	t.Setenv("BUGNET_INTERNAL_TOKEN", secretToken)
	t.Setenv("BUGNET_ORDER_TOKEN_PUBLIC_KEY", pubB64(t))
	t.Setenv("BUGNET_PACKAGES_FILE", "/p.json")
	t.Setenv("BUGNET_HANDLE_KEY", handleKeyB64)
	if _, err := Load(); err != nil {
		t.Fatal(err)
	}
}
