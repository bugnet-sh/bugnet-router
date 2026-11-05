package config

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/oynozan/bugnet-router/internal/jobs"
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
	if c.WorkerListenAddr != ":8081" {
		t.Fatalf("worker listen addr %q, want :8081", c.WorkerListenAddr)
	}
	if c.DeploymentID != "main" {
		t.Fatalf("deployment id %q, want main", c.DeploymentID)
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
	if c.JobOutputBudget != jobs.DefaultJobBudget ||
		c.PlacementOutputBudget != jobs.DefaultPlacementBudget ||
		c.PlacementTailReserve != jobs.DefaultTailReserve ||
		c.MaxRunningJobs != jobs.DefaultMaxRunning {
		t.Fatalf("output limits %d, %d, %d and %d jobs, want the jobs service's defaults",
			c.JobOutputBudget, c.PlacementOutputBudget, c.PlacementTailReserve, c.MaxRunningJobs)
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
		"BUGNET_DEPLOYMENT_ID":          {"BUGNET_DEPLOYMENT_ID": "Prod"},
		"deployment with underscore":    {"BUGNET_DEPLOYMENT_ID": "blue_green"},
		"deployment with a space":       {"BUGNET_DEPLOYMENT_ID": "blue green"},
		"long deployment":               {"BUGNET_DEPLOYMENT_ID": strings.Repeat("d", 33)},
		"BUGNET_JOB_OUTPUT_BUDGET":      {"BUGNET_JOB_OUTPUT_BUDGET": "0"},
		"negative job budget":           {"BUGNET_JOB_OUTPUT_BUDGET": "-5MiB"},
		"job budget in GB":              {"BUGNET_JOB_OUTPUT_BUDGET": "8GB"},
		"fractional job budget":         {"BUGNET_JOB_OUTPUT_BUDGET": "1.5MiB"},
		"job budget with a space":       {"BUGNET_JOB_OUTPUT_BUDGET": "8 MiB"},
		"lowercase suffix":              {"BUGNET_JOB_OUTPUT_BUDGET": "8mib"},
		"BUGNET_PLACEMENT_OUTPUT_BUDGET": {
			"BUGNET_PLACEMENT_OUTPUT_BUDGET": "9223372036854775807MiB",
		},
		"zero placement budget":         {"BUGNET_PLACEMENT_OUTPUT_BUDGET": "0KiB"},
		"BUGNET_PLACEMENT_TAIL_RESERVE": {"BUGNET_PLACEMENT_TAIL_RESERVE": "lots"},
		"negative tail reserve":         {"BUGNET_PLACEMENT_TAIL_RESERVE": "-1"},
		"BUGNET_MAX_RUNNING_JOBS":       {"BUGNET_MAX_RUNNING_JOBS": "0"},
		"negative running jobs":         {"BUGNET_MAX_RUNNING_JOBS": "-3"},
		"running jobs with a suffix":    {"BUGNET_MAX_RUNNING_JOBS": "16KiB"},
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
		"BUGNET_EVENT_SIGNING_KEY":       seed,
		"BUGNET_EVENTS_URL":              "https://events.example",
		"BUGNET_ENABLE_LOCAL_PROVIDER":   "1",
		"BUGNET_LOCAL_ROOT":              "/var/bn",
		"BUGNET_PROVIDERS":               "local,e2b",
		"BOXD_API_KEY":                   "bk",
		"E2B_API_KEY":                    "ek",
		"KOYEB_API_TOKEN":                "",
		"BOAT_API_KEY":                   "ok",
		"BUGNET_RUNNER_LINUX_AMD64":      "/r/amd",
		"BUGNET_RUNNER_LINUX_ARM64":      "/r/arm",
		"BUGNET_MARGIN_FLOOR":            "2.5",
		"BUGNET_REPLICA_ID":              "r1",
		"BUGNET_VALKEY_ADDR":             "v:1",
		"BUGNET_LISTEN_ADDR":             ":9",
		"BUGNET_WORKER_LISTEN_ADDR":      ":10",
		"BUGNET_VALKEY_PREFIX":           "t:",
		"BUGNET_LOCAL_RUNNER":            "/r/local",
		"BUGNET_LOCAL_HOURLY_MICROS":     "1500",
		"BUGNET_DEPLOYMENT_ID":           "staging-2",
		"BUGNET_JOB_OUTPUT_BUDGET":       "4096KiB",
		"BUGNET_PLACEMENT_OUTPUT_BUDGET": "64MiB",
		"BUGNET_PLACEMENT_TAIL_RESERVE":  "1000000",
		"BUGNET_MAX_RUNNING_JOBS":        "4",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if c.JobOutputBudget != 4<<20 || c.PlacementOutputBudget != 64<<20 ||
		c.PlacementTailReserve != 1_000_000 || c.MaxRunningJobs != 4 {
		t.Fatalf("output limits %d, %d, %d and %d jobs, want 4 MiB, 64 MiB, 1000000 and 4",
			c.JobOutputBudget, c.PlacementOutputBudget, c.PlacementTailReserve, c.MaxRunningJobs)
	}
	if len(c.EventSigningKey) != ed25519.PrivateKeySize || c.EventsURL != "https://events.example" {
		t.Fatalf("event config: %+v", c)
	}
	if !c.LocalEnabled || c.LocalRoot != "/var/bn" || c.MarginFloor != 2.5 || c.ReplicaID != "r1" {
		t.Fatalf("%+v", c)
	}
	if c.ListenAddr != ":9" || c.WorkerListenAddr != ":10" {
		t.Fatalf("listen addrs %q and %q", c.ListenAddr, c.WorkerListenAddr)
	}
	if c.DeploymentID != "staging-2" {
		t.Fatalf("deployment id %q, want staging-2", c.DeploymentID)
	}
	long := strings.Repeat("d", 32)
	if c, err := LoadFrom(env(t, map[string]string{"BUGNET_DEPLOYMENT_ID": long})); err != nil ||
		c.DeploymentID != long {
		t.Fatalf("32 character deployment id %q, %v", c.DeploymentID, err)
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

func TestByteSizes(t *testing.T) {
	sizes := map[string]int64{
		"1": 1, "8388608": 8 << 20, "8KiB": 8 << 10, "8MiB": 8 << 20, "+2MiB": 2 << 20,
		"8796093022207MiB": 8796093022207 << 20,
	}
	for _, raw := range []string{
		"", "KiB", "MiB", "0", "0MiB", "-1KiB", "8 KiB", "8kib", "8GiB", "8KB", "1e3", "0x10",
		"8796093022208MiB",
	} {
		sizes[raw] = 0
	}
	for raw, want := range sizes {
		got, err := byteSize("BUGNET_X", raw)
		if want == 0 {
			if err == nil || !strings.Contains(err.Error(), "BUGNET_X") {
				t.Errorf("%q gave %d, %v, want an error naming the variable", raw, got, err)
			}
			continue
		}
		if err != nil || got != want {
			t.Errorf("%q gave %d, %v, want %d", raw, got, err, want)
		}
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

func TestDatabaseURLNeedsNothingElse(t *testing.T) {
	only := func(k string) (string, bool) {
		if k == "BUGNET_DATABASE_URL" {
			return "postgres://localhost/x", true
		}
		return "", false
	}
	if url, err := DatabaseURLFrom(only); err != nil || url != "postgres://localhost/x" {
		t.Fatalf("database url %q, %v", url, err)
	}
	for name, getenv := range map[string]func(string) (string, bool){
		"unset": func(string) (string, bool) { return "", false },
		"empty": func(string) (string, bool) { return "", true },
	} {
		url, err := DatabaseURLFrom(getenv)
		if err == nil || url != "" || !strings.Contains(err.Error(), "BUGNET_DATABASE_URL") {
			t.Errorf("%s: %q, %v, want an error naming BUGNET_DATABASE_URL", name, url, err)
		}
	}
	t.Setenv("BUGNET_DATABASE_URL", "postgres://localhost/y")
	if url, err := DatabaseURL(); err != nil || url != "postgres://localhost/y" {
		t.Fatalf("database url from the process %q, %v", url, err)
	}
}
