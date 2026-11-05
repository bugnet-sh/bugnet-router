package config

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/oynozan/bugnet-router/internal/jobs"
)

const secretToken = "0123456789abcdef0123456789abcdef"

var handleKeyB64 = base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))

// secrets are the environment of a valid Load, with a password in the database URL
var secrets = map[string]string{
	"BUGNET_DATABASE_URL":   "postgres://user:dbpass@localhost/x",
	"BUGNET_INTERNAL_TOKEN": secretToken,
	"BUGNET_HANDLE_KEY":     handleKeyB64,
	"BOXD_API_KEY":          "bk-secret",
	"E2B_API_KEY":           "ek-secret",
	"KOYEB_API_TOKEN":       "kt-secret",
	"BOAT_API_KEY":          "ok-secret",
}

func pubB64(t *testing.T) string {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(pub)
}

// writeFile writes a settings file into a fresh directory and returns its path
func writeFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "settings.toml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// serving is the start of a file Load accepts, two top-level lines so tables may follow
func serving(t *testing.T) string {
	t.Helper()
	return `order_token_public_key = "` + pubB64(t) + `"
packages_file = "/etc/packages.json"
`
}

// env looks up vars, with "-" dropping a variable from secrets
func env(over map[string]string) func(string) (string, bool) {
	m := maps.Clone(secrets)
	for k, v := range over {
		if v == "-" {
			delete(m, k)
		} else {
			m[k] = v
		}
	}
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func TestLoadFileDefaults(t *testing.T) {
	c, err := LoadFile(writeFile(t, ""))
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
	if c.OrderTokenPublicKey != nil || c.PackagesFile != "" || c.EventsURL != "" {
		t.Fatalf("optional settings set: %+v", c)
	}
	if c.DatabaseURL != "" || c.InternalToken != "" || c.HandleKey != nil ||
		c.EventSigningKey != nil || len(c.ProviderKeys) != 0 {
		t.Fatalf("secrets set by LoadFile: %+v", c)
	}
	if c.LocalEnabled || c.LocalRunnerPath != "" || c.LocalHourlyMicros != 0 {
		t.Fatalf("local runner settings: %+v", c)
	}
	if c.LocalRoot != filepath.Join(os.TempDir(), "bugnet-local") {
		t.Fatalf("local root %q", c.LocalRoot)
	}
	if c.RunnerLinuxAMD64 != "" || c.RunnerLinuxARM64 != "" {
		t.Fatalf("runner builds: %+v", c)
	}
	if c.JobOutputBudget != jobs.DefaultJobBudget ||
		c.PlacementOutputBudget != jobs.DefaultPlacementBudget ||
		c.PlacementTailReserve != jobs.DefaultTailReserve ||
		c.MaxRunningJobs != jobs.DefaultMaxRunning {
		t.Fatalf("output limits %d, %d, %d and %d jobs, want the jobs service's defaults",
			c.JobOutputBudget, c.PlacementOutputBudget, c.PlacementTailReserve, c.MaxRunningJobs)
	}
	if c.Boxd != (Boxd{Region: "fra"}) || c.Koyeb.Region != "fra" ||
		c.Koyeb.InstanceTypes != nil || c.Boat.Types != nil {
		t.Fatalf("provider defaults: %+v, %+v, %+v", c.Boxd, c.Koyeb, c.Boat)
	}
	if c.E2B.Domain != "" || c.E2B.MaxLifetime != 0 || c.E2B.Templates != nil {
		t.Fatalf("e2b defaults: %+v", c.E2B)
	}
}

func TestLoadFileEmptyStringsMeanDefaults(t *testing.T) {
	c, err := LoadFile(writeFile(t, `deployment_id = ""
replica_id = ""
[api]
listen_addr = ""
[worker]
listen_addr = ""
[valkey]
addr = ""
prefix = ""
[limits]
job_output_budget = ""
[local]
root = ""
[boxd]
region = ""
[koyeb]
region = ""
`))
	if err != nil {
		t.Fatal(err)
	}
	empty, err := LoadFile(writeFile(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c, empty) {
		t.Fatalf("empty strings gave %+v, want the defaults %+v", c, empty)
	}
}

func TestLoadFileSettings(t *testing.T) {
	pub := pubB64(t)
	c, err := LoadFile(writeFile(t, `order_token_public_key = "`+pub+`"
packages_file = "/p/packages.json"
providers = ["local", "e2b"]
margin_floor = 2
deployment_id = "staging-2"
replica_id = "r1"
events_url = "https://events.example"

[api]
listen_addr = ":9"

[worker]
listen_addr = ":10"

[valkey]
addr = "v:1"
prefix = "t:"

[runner]
linux_amd64 = "/r/amd"
linux_arm64 = "/r/arm"

[limits]
job_output_budget = "4096KiB"
placement_output_budget = "64MiB"
placement_tail_reserve = "1000000"
max_running_jobs = 4

[local]
enabled = true
root = "/var/bn"
runner = "/r/local"
hourly_micros = 1500

[boxd]
region = "nyc"
endpoint = "boxd.example:9443"

[e2b]
domain = "e2b-juliett.dev"
max_lifetime = "24h"

[[e2b.templates]]
size = "2x2"
template = "bugnet-2x2"
vcpu = 2
memory_mib = 2048
disk_gib = 10

[[e2b.templates]]
size = "4x8"
template = "team/bugnet-4x8:v1"
vcpu = 4
memory_mib = 8192
disk_gib = 20

[koyeb]
region = "par"

[koyeb.instance_types]
cpu = "medium"
gpu = "gpu-nvidia-rtx-4000-sff-ada"

[boat.types]
cpu2 = "small"
cpu8 = "large"
`))
	if err != nil {
		t.Fatal(err)
	}
	if base64.StdEncoding.EncodeToString(c.OrderTokenPublicKey) != pub ||
		c.PackagesFile != "/p/packages.json" || c.EventsURL != "https://events.example" {
		t.Fatalf("top-level settings: %+v", c)
	}
	if !slices.Equal(c.Providers, []string{"local", "e2b"}) || c.MarginFloor != 2 ||
		c.DeploymentID != "staging-2" || c.ReplicaID != "r1" {
		t.Fatalf("placement settings: %+v", c)
	}
	if c.ListenAddr != ":9" || c.WorkerListenAddr != ":10" || c.ValkeyAddr != "v:1" ||
		c.ValkeyPrefix != "t:" {
		t.Fatalf("addresses: %+v", c)
	}
	if c.RunnerLinuxAMD64 != "/r/amd" || c.RunnerLinuxARM64 != "/r/arm" {
		t.Fatalf("runner builds: %+v", c)
	}
	if c.JobOutputBudget != 4<<20 || c.PlacementOutputBudget != 64<<20 ||
		c.PlacementTailReserve != 1_000_000 || c.MaxRunningJobs != 4 {
		t.Fatalf("output limits %d, %d, %d and %d jobs, want 4 MiB, 64 MiB, 1000000 and 4",
			c.JobOutputBudget, c.PlacementOutputBudget, c.PlacementTailReserve, c.MaxRunningJobs)
	}
	if !c.LocalEnabled || c.LocalRoot != "/var/bn" || c.LocalRunnerPath != "/r/local" ||
		c.LocalHourlyMicros != 1500 {
		t.Fatalf("local settings: %+v", c)
	}
	if c.Boxd != (Boxd{Region: "nyc", Endpoint: "boxd.example:9443"}) {
		t.Fatalf("boxd settings: %+v", c.Boxd)
	}
	wantTemplates := []E2BTemplate{
		{Size: "2x2", Template: "bugnet-2x2", VCPU: 2, MemoryMiB: 2048, DiskGiB: 10},
		{Size: "4x8", Template: "team/bugnet-4x8:v1", VCPU: 4, MemoryMiB: 8192, DiskGiB: 20},
	}
	if c.E2B.Domain != "e2b-juliett.dev" || c.E2B.MaxLifetime != 24*time.Hour ||
		!slices.Equal(c.E2B.Templates, wantTemplates) {
		t.Fatalf("e2b settings: %+v", c.E2B)
	}
	wantTypes := map[string]string{"cpu": "medium", "gpu": "gpu-nvidia-rtx-4000-sff-ada"}
	if c.Koyeb.Region != "par" || !maps.Equal(c.Koyeb.InstanceTypes, wantTypes) {
		t.Fatalf("koyeb settings: %+v", c.Koyeb)
	}
	if !maps.Equal(c.Boat.Types, map[string]string{"cpu2": "small", "cpu8": "large"}) {
		t.Fatalf("boat settings: %+v", c.Boat)
	}
	long := strings.Repeat("d", 32)
	if c, err := LoadFile(writeFile(t, `deployment_id = "`+long+`"`)); err != nil ||
		c.DeploymentID != long {
		t.Fatalf("32 character deployment id %q, %v", c.DeploymentID, err)
	}
}

func TestLoadFileMakesRelativePathsAbsolute(t *testing.T) {
	path := writeFile(t, `packages_file = "config/packages.json"
providers = ["local"]
[runner]
linux_amd64 = "../dist/amd"
linux_arm64 = "dist/arm"
[local]
enabled = true
root = "bin/local"
runner = "./bin/bugnet-run"
`)
	t.Chdir(t.TempDir())
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	c, err := LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for got, rel := range map[string]string{
		c.PackagesFile: "config/packages.json", c.RunnerLinuxAMD64: "../dist/amd",
		c.RunnerLinuxARM64: "dist/arm", c.LocalRoot: "bin/local",
		c.LocalRunnerPath: "bin/bugnet-run",
	} {
		if want := filepath.Join(wd, rel); got != want {
			t.Errorf("%s resolved to %s, want %s", rel, got, want)
		}
	}
}

func TestLoadNeedsThePublicKeyAndCatalog(t *testing.T) {
	pubLine := `order_token_public_key = "` + pubB64(t) + `"` + "\n"
	catalogLine := `packages_file = "/etc/packages.json"` + "\n"
	if _, err := LoadFile(writeFile(t, catalogLine)); err != nil {
		t.Fatalf("LoadFile without the public key: %v", err)
	}
	for key, content := range map[string]string{
		"order_token_public_key": catalogLine,
		"packages_file":          pubLine,
	} {
		path := writeFile(t, content)
		_, err := Load(path, env(nil))
		if err == nil || !strings.Contains(err.Error(), key+" is required") ||
			!strings.Contains(err.Error(), path) {
			t.Errorf("Load without %s: %v, want an error naming it and the file", key, err)
		}
	}
	c, err := Load(writeFile(t, pubLine+catalogLine), env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if c.DatabaseURL != secrets["BUGNET_DATABASE_URL"] || c.InternalToken != secretToken ||
		string(c.HandleKey) != "0123456789abcdef0123456789abcdef" ||
		len(c.OrderTokenPublicKey) != ed25519.PublicKeySize || c.EventSigningKey != nil {
		t.Fatalf("secrets: %+v", c)
	}
	want := map[string]string{
		"boxd": "bk-secret", "e2b": "ek-secret", "koyeb": "kt-secret", "boat": "ok-secret",
	}
	if !maps.Equal(c.ProviderKeys, want) {
		t.Fatalf("provider keys %v", c.ProviderKeys)
	}
}

func TestLoadSecrets(t *testing.T) {
	path := writeFile(t, serving(t))
	seed := base64.StdEncoding.EncodeToString(make([]byte, ed25519.SeedSize))
	c, err := Load(path, env(map[string]string{
		"BUGNET_EVENT_SIGNING_KEY": seed, "KOYEB_API_TOKEN": "", "BOXD_API_KEY": "-",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.EventSigningKey) != ed25519.PrivateKeySize {
		t.Fatalf("event signing key %v", c.EventSigningKey)
	}
	if !maps.Equal(c.ProviderKeys, map[string]string{"e2b": "ek-secret", "boat": "ok-secret"}) {
		t.Fatalf("provider keys %v, want only the ones set", c.ProviderKeys)
	}

	shortKey := base64.StdEncoding.EncodeToString([]byte("short"))
	bad := []struct {
		name, env string
		over      map[string]string
	}{
		{"missing database url", "BUGNET_DATABASE_URL",
			map[string]string{"BUGNET_DATABASE_URL": "-"}},
		{"empty database url", "BUGNET_DATABASE_URL", map[string]string{"BUGNET_DATABASE_URL": ""}},
		{"missing token", "BUGNET_INTERNAL_TOKEN", map[string]string{"BUGNET_INTERNAL_TOKEN": "-"}},
		{"empty token", "BUGNET_INTERNAL_TOKEN", map[string]string{"BUGNET_INTERNAL_TOKEN": ""}},
		{"short token", "BUGNET_INTERNAL_TOKEN",
			map[string]string{"BUGNET_INTERNAL_TOKEN": "tooshort-secret"}},
		{"one character token", "BUGNET_INTERNAL_TOKEN",
			map[string]string{"BUGNET_INTERNAL_TOKEN": "x"}},
		{"missing handle key", "BUGNET_HANDLE_KEY", map[string]string{"BUGNET_HANDLE_KEY": "-"}},
		{"empty handle key", "BUGNET_HANDLE_KEY", map[string]string{"BUGNET_HANDLE_KEY": ""}},
		{"short handle key", "BUGNET_HANDLE_KEY", map[string]string{"BUGNET_HANDLE_KEY": shortKey}},
		{"handle key not base64", "BUGNET_HANDLE_KEY",
			map[string]string{"BUGNET_HANDLE_KEY": "!!not-base64!!"}},
		{"short event signing key", "BUGNET_EVENT_SIGNING_KEY",
			map[string]string{"BUGNET_EVENT_SIGNING_KEY": "AAAA"}},
	}
	for _, c := range bad {
		_, err := Load(path, env(c.over))
		if err == nil {
			t.Errorf("%s: expected an error", c.name)
			continue
		}
		if !strings.Contains(err.Error(), c.env) {
			t.Errorf("%s: error does not name %s: %v", c.name, c.env, err)
		}
		noSecrets(t, c.name, err, c.over)
	}
}

// noSecrets fails the test when err carries the value of a secret
func noSecrets(t *testing.T, name string, err error, over map[string]string) {
	t.Helper()
	for k, v := range secrets {
		if strings.Contains(err.Error(), v) {
			t.Errorf("%s: error echoes the value of %s: %v", name, k, err)
		}
	}
	for k, v := range over {
		if len(v) > 1 && strings.Contains(err.Error(), v) {
			t.Errorf("%s: error echoes the value of %s: %v", name, k, err)
		}
	}
}

func TestLoadFileValidation(t *testing.T) {
	cases := []struct {
		name, toml, want string
	}{
		{"margin above 10", `margin_floor = 11`, "margin_floor must be a finite number"},
		{"negative margin", `margin_floor = -0.1`, "margin_floor"},
		{"non-numeric margin", `margin_floor = "abc"`, "margin_floor must be a number"},
		{"NaN margin", `margin_floor = nan`, "margin_floor"},
		{"+Inf margin", `margin_floor = +inf`, "margin_floor"},
		{"-Inf margin", `margin_floor = -inf`, "margin_floor"},
		{"unknown provider", `providers = ["koyeb", "nope"]`, `unknown provider "nope"`},
		{"duplicate providers", `providers = ["boat", "e2b", "boat"]`, `"boat" more than once`},
		{"empty entry", `providers = ["boat", "", "e2b"]`, "providers has an empty entry"},
		{"empty providers", `providers = []`, "providers must name at least one provider"},
		{"providers as a string", `providers = "boat,e2b"`, "providers must be an array"},
		{"local not enabled", `providers = ["boat", "local"]`, "local.enabled is not true"},
		{"local without runner", "providers = [\"e2b\", \"local\"]\n[local]\nenabled = true",
			"local.runner is not set"},
		{"negative local hourly cost", "[local]\nhourly_micros = -1", "local.hourly_micros"},
		{"fractional local hourly cost", "[local]\nhourly_micros = 1.5",
			"line 4: local.hourly_micros must be a whole number"},
		{"local enabled as a number", "[local]\nenabled = 1", "local.enabled must be true or"},
		{"uppercase deployment", `deployment_id = "Prod"`, "deployment_id"},
		{"deployment with underscore", `deployment_id = "blue_green"`, "deployment_id"},
		{"deployment with a space", `deployment_id = "blue green"`, "deployment_id"},
		{"long deployment", `deployment_id = "` + strings.Repeat("d", 33) + `"`,
			"deployment_id"},
		{"zero job budget", "[limits]\njob_output_budget = \"0\"",
			"limits.job_output_budget must be 1 byte to 1 TiB"},
		{"negative job budget", "[limits]\njob_output_budget = \"-5MiB\"",
			"limits.job_output_budget"},
		{"job budget in GB", "[limits]\njob_output_budget = \"8GB\"", "limits.job_output_budget"},
		{"fractional job budget", "[limits]\njob_output_budget = \"1.5MiB\"",
			"limits.job_output_budget"},
		{"job budget with a space", "[limits]\njob_output_budget = \"8 MiB\"",
			"limits.job_output_budget"},
		{"lowercase suffix", "[limits]\njob_output_budget = \"8mib\"", "limits.job_output_budget"},
		{"job budget as a number", "[limits]\njob_output_budget = 8388608",
			"limits.job_output_budget must be a string"},
		{"huge placement budget",
			"[limits]\nplacement_output_budget = \"9223372036854775807MiB\"",
			"limits.placement_output_budget"},
		{"zero placement budget", "[limits]\nplacement_output_budget = \"0KiB\"",
			"limits.placement_output_budget"},
		{"tail reserve in words", "[limits]\nplacement_tail_reserve = \"lots\"",
			"limits.placement_tail_reserve"},
		{"negative tail reserve", "[limits]\nplacement_tail_reserve = \"-1\"",
			"limits.placement_tail_reserve"},
		{"zero running jobs", "[limits]\nmax_running_jobs = 0",
			"limits.max_running_jobs must be a positive whole number"},
		{"negative running jobs", "[limits]\nmax_running_jobs = -3", "limits.max_running_jobs"},
		{"running jobs with a suffix", "[limits]\nmax_running_jobs = \"16KiB\"",
			"limits.max_running_jobs must be a whole number"},
		{"lifetime in words", "[e2b]\nmax_lifetime = \"an hour\"", "e2b.max_lifetime"},
		{"negative lifetime", "[e2b]\nmax_lifetime = \"-1h\"", "e2b.max_lifetime"},
		{"zero lifetime", "[e2b]\nmax_lifetime = \"0s\"", "e2b.max_lifetime"},
		{"template vcpu in words", "[[e2b.templates]]\nsize = \"2x2\"\nvcpu = \"two\"",
			"e2b.templates.vcpu must be a whole number"},
		{"instance type as a number", "[koyeb.instance_types]\ncpu = 1",
			"line 4: koyeb.instance_types.cpu must be a string"},
		{"size named twice", "[boat.types]\ncpu = \"small\"\ncpu = \"large\"",
			"line 5: boat.types.cpu is set twice"},
		{"listen address as a number", "[api]\nlisten_addr = 8080",
			"api.listen_addr must be a string"},
		{"top-level listen address", `listen_addr = ":8080"`, "listen_addr is not a setting"},
		{"misspelt key", "[limits]\nmax_jobs = 4", "line 4: limits.max_jobs is not a setting"},
		{"unknown table", "[nimbus]\nregion = \"fra\"", "nimbus is not a setting"},
		{"template key", "[[e2b.templates]]\nmemory = 2048", "e2b.templates.memory is not"},
		{"syntax error", "margin_floor = ", "line 3"},
	}
	for _, c := range cases {
		path := writeFile(t, serving(t)+c.toml)
		_, err := Load(path, env(nil))
		if err == nil || !strings.Contains(err.Error(), c.want) ||
			!strings.HasPrefix(err.Error(), "config: "+path+": ") {
			t.Errorf("%s: %v, want an error on %s with %q", c.name, err, path, c.want)
			continue
		}
		noSecrets(t, c.name, err, nil)
	}
	shortKey := base64.StdEncoding.EncodeToString([]byte("short"))
	for _, key := range []string{shortKey, "!!"} {
		_, err := LoadFile(writeFile(t, `order_token_public_key = "`+key+`"`))
		if err == nil ||
			!strings.Contains(err.Error(), "order_token_public_key must be base64 of 32 bytes") {
			t.Errorf("public key %q: %v, want an error naming order_token_public_key", key, err)
		}
	}
}

func TestLoadFileRefusesSecrets(t *testing.T) {
	cases := []struct {
		toml, key, env, value string
	}{
		{`internal_token = "` + secretToken + `"`, "internal_token",
			"BUGNET_INTERNAL_TOKEN", secretToken},
		{`BUGNET_HANDLE_KEY = "` + handleKeyB64 + `"`, "BUGNET_HANDLE_KEY",
			"BUGNET_HANDLE_KEY", handleKeyB64},
		{`database_url = "postgres://user:dbpass@localhost/x"`, "database_url",
			"BUGNET_DATABASE_URL", "dbpass"},
		{"[e2b]\napi_key = \"ek-secret\"", "e2b.api_key", "E2B_API_KEY", "ek-secret"},
		{"[koyeb]\napi_token = \"kt-secret\"", "koyeb.api_token", "KOYEB_API_TOKEN",
			"kt-secret"},
		{`event_signing_key = "c2VlZA=="`, "event_signing_key", "BUGNET_EVENT_SIGNING_KEY",
			"c2VlZA=="},
		{`order_private_key = "cHJpdg=="`, "order_private_key", "BUGNET_ORDER_PRIVATE_KEY",
			"cHJpdg=="},
		{"[limits]\nE2B_API_KEY = \"ek-secret\"", "limits.E2B_API_KEY", "E2B_API_KEY",
			"ek-secret"},
		{"[boat.types]\nBOAT_API_KEY=boat_unquoted", "boat.types.BOAT_API_KEY", "BOAT_API_KEY",
			"boat_unquoted"},
	}
	for _, c := range cases {
		_, err := LoadFile(writeFile(t, c.toml))
		want := c.key + " is a secret, set " + c.env + " in the environment instead"
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s in the file gave %v, want %q", c.key, err, want)
			continue
		}
		if strings.Contains(err.Error(), c.value) {
			t.Errorf("%s in the file: error echoes its value: %v", c.key, err)
		}
	}
	_, err := LoadFile(writeFile(t, "internal_token = \""+secretToken+"\"\n[limits]\nfoo = 1"))
	if err == nil || !strings.Contains(err.Error(), "line 1: internal_token is a secret") ||
		!strings.Contains(err.Error(), "line 3: limits.foo is not a setting") ||
		strings.Contains(err.Error(), secretToken) {
		t.Fatalf("two unknown keys gave %v, want both named and no value", err)
	}
}

func TestLoadFileKeysAreCaseSensitive(t *testing.T) {
	cases := []struct{ name, toml, want string }{
		{"top-level key", `Providers = ["boat"]`,
			"line 3: Providers is not a setting, did you mean providers"},
		{"table", "[API]\nlisten_addr = \":9\"", "line 3: API is not a setting, did you mean api"},
		{"key in a table", "[api]\nlisten_Addr = \":9\"",
			"line 4: api.listen_Addr is not a setting, did you mean api.listen_addr"},
		{"key set again in capitals", `PACKAGES_FILE = "/b"`,
			"line 3: PACKAGES_FILE is not a setting, did you mean packages_file"},
		{"dotted key", "Limits.max_running_jobs = 2", "line 3: Limits.max_running_jobs is not"},
		{"template key", "[[e2b.templates]]\nSize = \"2x2\"", "line 4: e2b.templates.Size is not"},
		{"table header in capitals", "[Boat.types]\ncpu = \"small\"", "line 3: Boat.types is not"},
	}
	for _, c := range cases {
		path := writeFile(t, serving(t)+c.toml)
		_, err := LoadFile(path)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want an error with %q", c.name, err, c.want)
		}
	}
}

func TestLoadFileChecksSizeNames(t *testing.T) {
	example, err := os.ReadFile(filepath.Join("..", "..", "config", "example.toml"))
	if err != nil {
		t.Fatal(err)
	}
	const secret = "e2b_pastedsecretvalue"
	pasted := `E2B_API_KEY = "` + secret + `"`
	head, tail, ok := strings.Cut(string(example), "[koyeb.instance_types]")
	if !ok {
		t.Fatal("config/example.toml has no [koyeb.instance_types] table")
	}
	header, rest, _ := strings.Cut(tail, "\n")
	for table, content := range map[string]string{
		"boat.types": string(example) + "\n" + pasted + "\n",
		"koyeb.instance_types": head + "[koyeb.instance_types]" + header + "\n" + pasted + "\n" +
			rest,
	} {
		_, err := LoadFile(writeFile(t, content))
		want := table + ".E2B_API_KEY is not a size name, size names are lowercase letters, " +
			"digits and hyphens"
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("a secret pasted into %s gave %v, want %q", table, err, want)
			continue
		}
		if strings.Contains(err.Error(), secret) {
			t.Errorf("a secret pasted into %s: error echoes its value: %v", table, err)
		}
	}
	for _, key := range []string{
		"CPU", "cpu_2", "cpu2 ", `""`, strings.Repeat("c", 33), `"cpu.2"`, "events_url",
	} {
		name := strings.Trim(key, `"`)
		_, err := LoadFile(writeFile(t, "[boat.types]\n"+quoteKey(key)+` = "small"`))
		if err == nil || !strings.Contains(err.Error(), "boat.types."+name+" is not a size name") {
			t.Errorf("size %q gave %v, want an error naming it", key, err)
		}
	}
	long := strings.Repeat("c", 32)
	c, err := LoadFile(writeFile(t, "[boat.types]\ncpu-2 = \"small\"\n"+long+" = \"large\"\n"+
		"[koyeb]\ninstance_types = { gpu-1 = \"gpu-nvidia-rtx-4000-sff-ada\" }"))
	if err != nil || len(c.Boat.Types) != 2 || c.Koyeb.InstanceTypes["gpu-1"] == "" {
		t.Fatalf("valid size names gave %+v, %+v, %v", c.Boat, c.Koyeb, err)
	}
}

// quoteKey quotes a TOML key that a bare key cannot spell
func quoteKey(key string) string {
	if strings.ContainsAny(key, " ") {
		return `"` + key + `"`
	}
	return key
}

func TestLoadFileTypeErrorsNameTheKey(t *testing.T) {
	for _, key := range []string{"limits.max_running_jobs", "local.hourly_micros"} {
		table, name, _ := strings.Cut(key, ".")
		for _, value := range []string{
			"1.5", "true", "99999999999999999999", "-9999999999999999999",
		} {
			path := writeFile(t, serving(t)+"["+table+"]\n"+name+" = "+value+"\n")
			_, err := LoadFile(path)
			if err == nil {
				t.Errorf("%s = %s loaded", key, value)
				continue
			}
			msg, ok := strings.CutPrefix(err.Error(), "config: "+path+": line 4: ")
			if !ok || !strings.HasPrefix(msg, key+" must be a whole number") {
				t.Errorf("%s = %s gave %v, want line 4 and %s named", key, value, err, key)
			}
			digits := strings.Map(func(r rune) rune {
				if r < '0' || r > '9' {
					return -1
				}
				return r
			}, value)
			if digits != "" && strings.ContainsAny(msg, digits) {
				t.Errorf("%s = %s: error carries digits of the value: %v", key, value, err)
			}
		}
	}
	cases := []struct{ name, toml, want string }{
		{"template vcpu as a fraction", "[[e2b.templates]]\nvcpu = 2.5",
			"line 4: e2b.templates.vcpu must be a whole number"},
		{"inline template", "[e2b]\ntemplates = [{ size = \"2x2\", disk_gib = false }]",
			"line 4: e2b.templates.disk_gib must be a whole number"},
		{"size type as a number", "[boat.types]\ncpu = 1",
			"line 4: boat.types.cpu must be a string"},
		{"inline size type", "[boat]\ntypes = { cpu = 1 }",
			"line 4: boat.types.cpu must be a string"},
		{"provider as a number", "providers = [\n  \"boat\",\n  7,\n]",
			"line 5: providers must be an array of strings"},
		{"margin in hex", "margin_floor = 0x10", "line 3: margin_floor must be a number"},
		{"table as a value", `api = "x"`, "line 3: api must be a table"},
		{"templates as a table", "[e2b.templates]\nsize = \"2x2\"",
			"line 3: e2b.templates must be an array of tables"},
		{"api as an array of tables", "[[api]]\nlisten_addr = \":9\"",
			"line 3: api must be a table"},
		{"setting as a table", "[margin_floor]", "line 3: margin_floor must be a number"},
		{"key below a setting", "margin_floor.x = 1", "line 3: margin_floor must be a number"},
		{"key below a size", "[boat.types]\ncpu.x = \"small\"",
			"line 4: boat.types.cpu must be a string"},
		{"size set twice", "[boat.types]\ncpu = \"small\"\ncpu = \"large\"",
			"line 5: boat.types.cpu is set twice"},
		{"table set twice", "[api]\nlisten_addr = \":9\"\n[api]", "line 5: api is set twice"},
		{"key set twice", "[limits]\nmax_running_jobs = 2\nmax_running_jobs = 3",
			"line 5: limits.max_running_jobs is set twice"},
		{"dotted table given a header", "api.listen_addr = \":9\"\n[api]",
			"line 4: api is set twice"},
		{"table given dotted keys", "[koyeb.instance_types]\ncpu = \"large\"\n[koyeb]\n" +
			"instance_types.gpu = \"small\"", "line 6: koyeb.instance_types is set twice"},
		{"value given dotted keys", "[api]\nlisten_addr = \":9\"\nlisten_addr.port = 9",
			"line 5: api.listen_addr is set twice"},
		{"array given tables", "[e2b]\ntemplates = [{ size = \"a\" }]\n[[e2b.templates]]",
			"line 5: e2b.templates is set twice"},
		{"table given array tables", "[e2b.templates]\n[[e2b.templates]]",
			"line 3: e2b.templates must be an array of tables"},
		{"syntax error", "margin_floor = 0.5 0.6", "line 3: margin_floor is not valid TOML"},
		{"syntax error in a table", "[limits]\nmax_running_jobs = 2 3",
			"line 4: limits.max_running_jobs is not valid TOML"},
		{"broken header", "[limits\nmax_running_jobs = 2", "line 3: not valid TOML"},
		{"pasted environment line", "E2B_API_KEY=e2b_pastedsecretvalue",
			"line 3: E2B_API_KEY is a secret, set E2B_API_KEY in the environment instead"},
	}
	for _, c := range cases {
		path := writeFile(t, serving(t)+c.toml)
		_, err := LoadFile(path)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want an error with %q", c.name, err, c.want)
			continue
		}
		for _, v := range []string{"0x10", "0.6", "e2b_pastedsecretvalue", "2.5", "strconv"} {
			if strings.Contains(err.Error(), v) {
				t.Errorf("%s: error quotes %q: %v", c.name, v, err)
			}
		}
	}
	for _, ok := range []string{
		"[e2b]\ntemplates = [{ size = \"a\", vcpu = 1 }, { size = \"b\", vcpu = 2 }]",
		"[[e2b.templates]]\nsize = \"a\"\n[[e2b.templates]]\nsize = \"b\"",
		"api.listen_addr = \":9\"\nworker.listen_addr = \":10\"",
		"[boat]\ntypes.cpu = \"small\"\ntypes.gpu = \"large\"",
		"[[e2b.templates]]\nsize = \"a\"\n[e2b]\ndomain = \"x\"",
		"[limits]\n[api]\n[limits.x]",
	} {
		_, err := LoadFile(writeFile(t, ok))
		if strings.HasSuffix(ok, "[limits.x]") {
			if err == nil || !strings.Contains(err.Error(), "limits.x is not a setting") ||
				strings.Contains(err.Error(), "twice") {
				t.Errorf("%q gave %v, want only limits.x named", ok, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q gave %v", ok, err)
		}
	}
}

// problem loads the serving lines and content as a file and returns its error after the path
func problem(t *testing.T, content string) string {
	t.Helper()
	path := writeFile(t, serving(t)+content)
	_, err := LoadFile(path)
	if err == nil {
		return ""
	}
	msg, _ := strings.CutPrefix(err.Error(), "config: "+path+": ")
	return msg
}

func TestLoadFileRefusesDatesAndTimes(t *testing.T) {
	cases := []struct{ name, toml, want string }{
		{"date as a whole number", "[limits]\nmax_running_jobs = 1979-05-27",
			"line 4: limits.max_running_jobs must be a whole number"},
		{"date and time as a number", "margin_floor = 1979-05-27T07:32:00Z",
			"line 3: margin_floor must be a number"},
		{"date as true or false", "[local]\nenabled = 1979-05-27",
			"line 4: local.enabled must be true or false"},
		{"date as a provider", "providers = [1979-05-27]",
			"line 3: providers must be an array of strings"},
		{"date as a size type", "[boat.types]\ncpu = 1979-05-27",
			"line 4: boat.types.cpu must be a string"},
		{"date as an instance type", "[koyeb.instance_types]\ncpu = 1979-05-27",
			"line 4: koyeb.instance_types.cpu must be a string"},
		{"date as a template vcpu", "[[e2b.templates]]\nvcpu = 1979-05-27",
			"line 4: e2b.templates.vcpu must be a whole number"},
		{"date as a template size", "[[e2b.templates]]\nsize = 1979-05-27",
			"line 4: e2b.templates.size must be a string"},
		{"date and time as a lifetime", "[e2b]\nmax_lifetime = 1979-05-27T07:32:00Z",
			"line 4: e2b.max_lifetime must be a string"},
		{"local time", "[e2b]\nmax_lifetime = 07:32:00",
			"line 4: e2b.max_lifetime must be a string"},
		{"local time with fractions", "[limits]\nmax_running_jobs = 07:32:00.999",
			"line 4: limits.max_running_jobs must be a whole number"},
		{"local date and time", "[local]\nhourly_micros = 1979-05-27T07:32:00",
			"line 4: local.hourly_micros must be a whole number"},
		{"date and time with a space", "[api]\nlisten_addr = 1979-05-27 07:32:00-07:00",
			"line 4: api.listen_addr must be a string"},
		{"date in an inline table", "[boat]\ntypes = { cpu = 1979-05-27 }",
			"line 4: boat.types.cpu must be a string"},
		{"date in an inline template", "[e2b]\ntemplates = [{ size = \"a\", disk_gib = 07:32:00 }]",
			"line 4: e2b.templates.disk_gib must be a whole number"},
		{"date on its own line", "providers = [\n  \"boat\",\n  1979-05-27T07:32:00,\n]",
			"line 5: providers must be an array of strings"},
		{"date as a template", "[e2b]\ntemplates = [1979-05-27]",
			"line 4: e2b.templates must be an array of tables"},
		{"date as a table", "api = 1979-05-27", "line 3: api must be a table"},
		{"date under an unknown key", "nimbus = 1979-05-27", "line 3: nimbus is not a setting"},
	}
	for _, c := range cases {
		msg := problem(t, c.toml)
		if msg != c.want {
			t.Errorf("%s: %q, want %q", c.name, msg, c.want)
		}
		for _, v := range []string{"1979", "05-27", "07:32", "999"} {
			if strings.Contains(msg, v) {
				t.Errorf("%s: error quotes %q: %s", c.name, v, msg)
			}
		}
	}
}

func TestLoadFileSyntaxErrorsNameOnlyKnownKeys(t *testing.T) {
	const pasted = "QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVphYmNkZWZn="
	for _, table := range []string{
		"", "[limits]\n", "[boat.types]\n", "[koyeb.instance_types]\n", "[[e2b.templates]]\n",
		"[nimbus]\n",
	} {
		msg := problem(t, table+pasted+"\n")
		want := "line " + strconv.Itoa(3+strings.Count(table, "\n")) + ": not valid TOML"
		if msg != want {
			t.Errorf("a pasted secret after %q gave %q, want %q", table, msg, want)
		}
		for i := 0; i+8 <= len(pasted); i++ {
			if strings.Contains(msg, pasted[i:i+8]) {
				t.Errorf("a pasted secret after %q: error carries %q: %s", table, pasted[i:i+8], msg)
			}
		}
	}
	cases := []struct{ name, toml, want string }{
		{"setting", "margin_floor = 0.5 0.6", "line 3: margin_floor is not valid TOML"},
		{"template setting", "[[e2b.templates]]\nvcpu = 2 3",
			"line 4: e2b.templates.vcpu is not valid TOML"},
		{"unknown key", "nimbus_region = fra", "line 3: not valid TOML"},
		{"key below a setting", "[limits]\nmax_running_jobs.x = 2 3", "line 4: not valid TOML"},
		{"size", "[boat.types]\ncpu = \"small\" \"large\"", "line 4: not valid TOML"},
		{"secret", "[boat.types]\nBOAT_API_KEY=boat_unquoted",
			"line 4: boat.types.BOAT_API_KEY is a secret, set BOAT_API_KEY in the environment " +
				"instead"},
		{"secret under an unknown table", "[QUJDREVGR0hJSktM]\nE2B_API_KEY=e2b_pastedsecretvalue",
			"line 4: E2B_API_KEY is a secret, set E2B_API_KEY in the environment instead"},
		{"secret below a dotted key", "QUJDREVGR0hJSktM.e2b_api_key=e2b_pastedsecretvalue",
			"line 3: e2b_api_key is a secret, set E2B_API_KEY in the environment instead"},
	}
	for _, c := range cases {
		if msg := problem(t, c.toml); msg != c.want {
			t.Errorf("%s: %q, want %q", c.name, msg, c.want)
		}
	}
}

func TestDecodeRecoversFromPanics(t *testing.T) {
	f, err := recovered(func() (file, error) {
		panic("reflect.Set: e2b_pastedsecretvalue")
	})
	if err == nil || err.Error() != "the file could not be decoded" ||
		!reflect.DeepEqual(f, file{}) {
		t.Fatalf("a panic while decoding gave %+v, %v, want a generic error", f, err)
	}
	f, err = recovered(func() (file, error) { return defaults(), nil })
	if err != nil || !reflect.DeepEqual(f, defaults()) {
		t.Fatalf("a decode without a panic gave %+v, %v", f, err)
	}
}

func TestLoadFileValueKinds(t *testing.T) {
	cases := []struct{ name, toml, want string }{
		{"whole number as an array", "[limits]\nmax_running_jobs = [16]",
			"line 4: limits.max_running_jobs must be a whole number"},
		{"string as an array", "[api]\nlisten_addr = [\":9\"]",
			"line 4: api.listen_addr must be a string"},
		{"boolean as an array", "[local]\nenabled = [true]",
			"line 4: local.enabled must be true or false"},
		{"number as an array", "margin_floor = [0.5]", "line 3: margin_floor must be a number"},
		{"size type as an array", "[boat.types]\ncpu = [\"small\"]",
			"line 4: boat.types.cpu must be a string"},
		{"providers in an array", "providers = [[\"boat\"]]",
			"line 3: providers must be an array of strings"},
		{"template size as an array", "[[e2b.templates]]\nsize = [\"a\"]",
			"line 4: e2b.templates.size must be a string"},
		{"whole number as a table", "[limits]\nmax_running_jobs = { a = 1 }",
			"line 4: limits.max_running_jobs must be a whole number"},
		{"whole number as an array of tables", "[limits]\nmax_running_jobs = [{ a = 1 }]",
			"line 4: limits.max_running_jobs must be a whole number"},
		{"provider as a table", "providers = [{ name = \"boat\" }]",
			"line 3: providers must be an array of strings"},
		{"template as a string", "[e2b]\ntemplates = [\"2x2\"]",
			"line 4: e2b.templates must be an array of tables"},
		{"size types as an array", "[boat]\ntypes = [\"small\"]",
			"line 4: boat.types must be a table of strings"},
		{"a float as a whole number", "[limits]\nmax_running_jobs = 2.0",
			"line 4: limits.max_running_jobs must be a whole number"},
		{"two templates", "[[e2b.templates]]\nsize = [\"a\"]\n[[e2b.templates]]\nvcpu = [1]",
			"line 4: e2b.templates.size must be a string, " +
				"line 6: e2b.templates.vcpu must be a whole number"},
	}
	for _, c := range cases {
		if msg := problem(t, c.toml); msg != c.want {
			t.Errorf("%s: %q, want %q", c.name, msg, c.want)
		}
	}
}

func TestLoadFileMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent.toml")
	_, err := LoadFile(path)
	if err == nil || !strings.Contains(err.Error(), path) {
		t.Fatalf("missing file gave %v, want an error naming it", err)
	}
}

// committed reads a settings file of the repository and checks that it loads as it is
func committed(t *testing.T, name string) string {
	t.Helper()
	path := filepath.Join("..", "..", "config", name)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := LoadFile(path); err != nil {
		t.Fatalf("config/%s as committed: %v", name, err)
	}
	return string(raw)
}

// orderKeyLine is a top-level order_token_public_key line, which dev-up.sh drops and replaces
var orderKeyLine = regexp.MustCompile(`(?m)^[ \t]*order_token_public_key[ \t]*=`)

func TestCommittedFilesLoad(t *testing.T) {
	raw := committed(t, "dev.toml")
	if orderKeyLine.MatchString(raw) {
		t.Fatal("config/dev.toml sets order_token_public_key, which dev-up.sh adds")
	}
	// dev-up.sh writes the key line, a blank line and the file, and both binaries Load the result
	line := `order_token_public_key = "` + pubB64(t) + `"`
	dev, err := Load(writeFile(t, line+"\n\n"+raw), env(nil))
	if err != nil {
		t.Fatalf("config/dev.toml as dev-up.sh writes it: %v", err)
	}
	if !slices.Equal(dev.Providers, []string{"local"}) || !dev.LocalEnabled ||
		dev.ValkeyPrefix != "bn:dev:" || dev.ListenAddr != "127.0.0.1:8080" ||
		dev.WorkerListenAddr != "127.0.0.1:8081" || dev.DeploymentID != "dev" {
		t.Fatalf("config/dev.toml: %+v", dev)
	}
	for path, suffix := range map[string]string{
		dev.PackagesFile: "/config/dev-packages.json", dev.LocalRoot: "/bin/local",
		dev.LocalRunnerPath:  "/bin/bugnet-run",
		dev.RunnerLinuxAMD64: "/bugnet-run/dist/bugnet-run-linux-amd64",
	} {
		if !filepath.IsAbs(path) || !strings.HasSuffix(path, suffix) {
			t.Errorf("config/dev.toml path %s, want an absolute path ending in %s", path, suffix)
		}
	}
	if len(dev.E2B.Templates) != 2 {
		t.Errorf("config/dev.toml e2b templates %+v, want both deploy/e2b builds", dev.E2B)
	}

	content := committed(t, "example.toml")
	if !strings.Contains(content, `order_token_public_key = ""`) {
		t.Fatal(`config/example.toml lacks order_token_public_key = ""`)
	}
	content = strings.Replace(content, `order_token_public_key = ""`, line, 1)
	example, err := LoadFile(writeFile(t, content))
	if err != nil {
		t.Fatalf("config/example.toml with an order token key: %v", err)
	}
	if len(example.E2B.Templates) != 2 {
		t.Errorf("config/example.toml e2b templates %+v, want both deploy/e2b builds",
			example.E2B)
	}
	empty, err := LoadFile(writeFile(t, line))
	if err != nil {
		t.Fatal(err)
	}
	example.E2B.Templates = nil
	if len(example.Koyeb.InstanceTypes) == 0 && len(example.Boat.Types) == 0 {
		example.Koyeb.InstanceTypes, example.Boat.Types = nil, nil
	}
	if !reflect.DeepEqual(example, empty) {
		t.Errorf("config/example.toml gave %+v, want the defaults %+v", example, empty)
	}
}

func TestByteSizes(t *testing.T) {
	sizes := map[string]int64{
		"1": 1, "8388608": 8 << 20, "8KiB": 8 << 10, "8MiB": 8 << 20, "+2MiB": 2 << 20,
		"1048576MiB": 1 << 40,
	}
	for _, raw := range []string{
		"", "KiB", "MiB", "0", "0MiB", "-1KiB", "8 KiB", "8kib", "8GiB", "8KB", "1e3", "0x10",
		"1048577MiB", "1099511627777",
	} {
		sizes[raw] = 0
	}
	for raw, want := range sizes {
		got, err := byteSize("limits.x", raw)
		if want == 0 {
			if err == nil || !strings.Contains(err.Error(), "limits.x") {
				t.Errorf("%q gave %d, %v, want an error naming the key", raw, got, err)
			}
			continue
		}
		if err != nil || got != want {
			t.Errorf("%q gave %d, %v, want %d", raw, got, err, want)
		}
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
