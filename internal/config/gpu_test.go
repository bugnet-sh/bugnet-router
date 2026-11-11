package config

import (
	"encoding/base64"
	"strings"
	"testing"
	"time"
)

// euCodes are the 27 EU member states, design 3.12's lium.countries
var euCodes = []string{
	"AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR", "HU", "IE", "IT", "LV",
	"LT", "LU", "MT", "NL", "PL", "PT", "RO", "SK", "SI", "ES", "SE",
}

const sbxSHA256 = "eb2b04f79fbe765195300aaf1582be2a2a9ea698ff067d0cab428171d5584932"

// liumDefaults is the [lium] table of an empty file, design 3.12, sizes aside
var liumDefaults = Lium{
	Countries: euCodes, OtherCountries: true,
	BlockedCountries: []string{"BY", "CU", "IR", "KP", "RU", "SY"},
	MaxLifetime:      2 * time.Hour, StartBilled: 7 * time.Minute, SSHPath: "native",
	KeyDailyBudgetMicros: 2_300_000_000, TemplateVolume: "/workspace",
	Termination: "max_lifetime",
}

// hfDefaults is the [hf] table of an empty file, design 4.12
var hfDefaults = HF{
	MaxLifetime: 2 * time.Hour, ExtendRoom: time.Hour, TimeoutMargin: 10 * time.Minute,
	StartBilled: 3 * time.Minute, PollEvery: 3 * time.Second, SBXServer: "fetch",
	SBXServerVersion: "0.7.0", SBXServerSHA256: sbxSHA256, TokenPlan: "free",
}

// rtx3090 is design 3.5's lium-rtx3090 row
var rtx3090 = LiumSize{
	Size: "lium-rtx3090", Model: "NVIDIA RTX 3090", Match: []string{"NVIDIA GeForce RTX 3090"},
	GPUs: 1, GPUMemoryGiB: 24, VCPU: 8, MemoryMiB: 28672, DiskGiB: 200, MaxHourlyMicros: 350_000,
}

func TestGPUProviderDefaults(t *testing.T) {
	for name, c := range map[string]Config{"example.toml": loadExample(t), "empty": loadEmpty(t)} {
		t.Run(name, func(t *testing.T) {
			lium := c.Lium
			lium.Sizes = nil
			eq(t, lium, liumDefaults)
			eq(t, c.HF, hfDefaults)
			eq(t, c.ReadyTimeouts["lium"], 6*time.Minute)
			eq(t, c.ReadyTimeouts["hf"], 5*time.Minute)
			eq(t, c.ProviderOps["lium"], 5)
			eq(t, c.ProviderOps["hf"], 3)
			for _, p := range []string{"lium", "hf"} {
				eq(t, c.MaxConcurrent[p], 10)
				eq(t, c.CreatesPerSecond[p], 1)
			}
			eq(t, c.Hardening.Rules["lium"],
				HardenRule{Required: true, Allow: []string{}, NoGateway: true})
			eq(t, c.Hardening.Rules["hf"], HardenRule{Allow: []string{}})
			eq(t, c.NativeSSH(), []string{"lium"})
			// GPU providers serve only when listed
			eq(t, c.Providers, []string{"koyeb", "boat", "e2b", "boxd"})
		})
	}
	eq(t, len(loadEmpty(t).Lium.Sizes), 0)
}

func TestGPUProviderSettings(t *testing.T) {
	c, err := LoadFile(writeFile(t, `gpu_placing_budget = "15m"

[lium]
countries = ["DE", "NL"]
other_countries = false
blocked_countries = []
min_download_mbps = 500
max_lifetime = "3h"
ready_timeout = "7m"
start_billed = "8m"
ssh_path = "gate"
harden_required = true
harden_allow = ["10.0.0.0/24"]
harden_no_gateway = true
ops_per_second = 4
max_concurrent = 2
key_daily_budget_micros = 20000000
creates_per_second = 2
template_volume = "/root"
termination = "lifetime"

[[lium.sizes]]
size = "lium-rtx3090"
model = "NVIDIA RTX 3090"
match = ["NVIDIA GeForce RTX 3090"]
gpus = 1
gpu_memory_gib = 24
vcpu = 8
memory_mib = 28672
disk_gib = 200
max_hourly_micros = 350000

[hf]
namespace = "bugnet-sh"
flavors = ["t4-small", "a100-large"]
max_lifetime = "90m"
extend_room = "30m"
timeout_margin = "5m"
ready_timeout = "4m"
start_billed = "2m"
poll_every = "2s"
sbx_server = "baked"
sbx_server_version = "0.8.0"
sbx_server_sha256 = "`+strings.Repeat("a", 64)+`"
harden_required = true
harden_allow = ["192.168.1.1"]
token_plan = "pro"
ops_per_second = 8
max_concurrent = 3
creates_per_second = 2
`))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.Lium, Lium{
		Countries: []string{"DE", "NL"}, MinDownloadMbps: 500, MaxLifetime: 3 * time.Hour,
		StartBilled: 8 * time.Minute, SSHPath: "gate", KeyDailyBudgetMicros: 20_000_000,
		TemplateVolume: "/root", Termination: "lifetime", Sizes: []LiumSize{rtx3090},
	})
	eq(t, c.HF, HF{
		Namespace: "bugnet-sh", Flavors: []string{"t4-small", "a100-large"},
		MaxLifetime: 90 * time.Minute, ExtendRoom: 30 * time.Minute,
		TimeoutMargin: 5 * time.Minute, StartBilled: 2 * time.Minute, PollEvery: 2 * time.Second,
		SBXServer: "baked", SBXServerVersion: "0.8.0", SBXServerSHA256: strings.Repeat("a", 64),
		TokenPlan: "pro",
	})
	eq(t, c.ReadyTimeouts["lium"], 7*time.Minute)
	eq(t, c.ReadyTimeouts["hf"], 4*time.Minute)
	eq(t, c.ProviderOps["lium"], 4)
	eq(t, c.ProviderOps["hf"], 8)
	eq(t, c.MaxConcurrent["lium"], 2)
	eq(t, c.MaxConcurrent["hf"], 3)
	eq(t, c.CreatesPerSecond["lium"], 2)
	eq(t, c.CreatesPerSecond["hf"], 2)
	eq(t, c.Hardening.Rules["lium"],
		HardenRule{Required: true, Allow: []string{"10.0.0.0/24"}, NoGateway: true})
	eq(t, c.Hardening.Rules["hf"], HardenRule{Required: true, Allow: []string{"192.168.1.1/32"}})
	eq(t, len(c.NativeSSH()), 0)
}

func TestGPUProviderRefusals(t *testing.T) {
	upper := strings.Repeat("A", 64)
	for name, c := range map[string]struct{ doc, want string }{
		"lium lifetime": {"[lium]\nmax_lifetime = \"0s\"", "lium.max_lifetime"},
		"lium billed":   {"[lium]\nstart_billed = \"soon\"", "lium.start_billed"},
		"lium ready":    {"[lium]\nready_timeout = \"16m\"", "lium.ready_timeout"},
		"lium ssh path": {"[lium]\nssh_path = \"direct\"", "lium.ssh_path must be one of"},
		"lium end":      {"[lium]\ntermination = \"never\"", "lium.termination must be one of"},
		"lium country":  {"[lium]\ncountries = [\"de\"]", "lium.countries entry 1"},
		"lium blocked": {"[lium]\nblocked_countries = [\"RU\", \"Russia\"]",
			"lium.blocked_countries entry 2"},
		"lium download":  {"[lium]\nmin_download_mbps = -1", "lium.min_download_mbps"},
		"lium budget":    {"[lium]\nkey_daily_budget_micros = -1", "lium.key_daily_budget_micros"},
		"lium volume":    {"[lium]\ntemplate_volume = \"workspace\"", "lium.template_volume"},
		"lium ops":       {"[lium]\nops_per_second = -1", "lium.ops_per_second"},
		"lium cap":       {"[lium]\nmax_concurrent = -1", "lium.max_concurrent"},
		"lium allow":     {"[lium]\nharden_allow = [\"host\"]", "lium.harden_allow entry 1"},
		"lium gateway":   {"[lium]\nharden_no_gateway = \"yes\"", "lium.harden_no_gateway"},
		"lium size kind": {"[[lium.sizes]]\ngpus = \"one\"", "lium.sizes.gpus"},
		"lium size key":  {"[[lium.sizes]]\nprice = 1", "lium.sizes.price is not a setting"},
		"hf lifetime":    {"[hf]\nmax_lifetime = \"-1h\"", "hf.max_lifetime"},
		"hf room":        {"[hf]\nextend_room = \"later\"", "hf.extend_room"},
		"hf margin":      {"[hf]\ntimeout_margin = \"-1m\"", "hf.timeout_margin"},
		"hf ready":       {"[hf]\nready_timeout = \"59s\"", "hf.ready_timeout"},
		"hf billed":      {"[hf]\nstart_billed = \"-1m\"", "hf.start_billed"},
		"hf poll":        {"[hf]\npoll_every = \"0s\"", "hf.poll_every"},
		"hf sbx":         {"[hf]\nsbx_server = \"pip\"", "hf.sbx_server must be one of"},
		"hf sbx sha":     {"[hf]\nsbx_server_sha256 = \"abc\"", "hf.sbx_server_sha256"},
		"hf sbx upper":   {"[hf]\nsbx_server_sha256 = \"" + upper + "\"", "hf.sbx_server_sha256"},
		"hf plan":        {"[hf]\ntoken_plan = \"team\"", "hf.token_plan must be one of"},
		"hf ops":         {"[hf]\nops_per_second = -1", "hf.ops_per_second"},
		"hf creates":     {"[hf]\ncreates_per_second = -1", "hf.creates_per_second"},
		"hf flavors":     {"[hf]\nflavors = \"t4-small\"", "hf.flavors"},
		"hf required":    {"[hf]\nharden_required = 1", "hf.harden_required"},
	} {
		_, err := LoadFile(writeFile(t, c.doc+"\n"))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: got %v, want an error naming %q", name, err, c.want)
		}
	}
	// A country entry is never quoted, since a secret could have been pasted there
	_, err := LoadFile(writeFile(t, "[lium]\ncountries = [\"sk-pasted-secret\"]\n"))
	if err == nil || strings.Contains(err.Error(), "pasted") {
		t.Fatalf("an unknown country gave %v", err)
	}
}

func TestGPUProvidersAreKnown(t *testing.T) {
	c, err := LoadFile(writeFile(t, `providers = ["lium", "hf", "e2b"]
[ssh]
providers = ["lium"]
[previews]
providers = ["hf"]
[hooks]
providers = ["lium"]
`+gpuTables))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.Providers, []string{"lium", "hf", "e2b"})
	eq(t, c.SSH.Providers, []string{"lium"})
	eq(t, c.Previews.Providers, []string{"hf"})
	eq(t, c.Hooks.Providers, []string{"lium"})
	for name, want := range map[string]string{"lium": "LIUM_API_KEY", "hf": "HF_JOBS_TOKEN"} {
		got, ok := ProviderKeyEnv(name)
		eq(t, got, want)
		eq(t, ok, true)
	}
}

func TestNativeSSHNamesBothProviders(t *testing.T) {
	c, err := LoadFile(writeFile(t, "[boat]\nssh_path = \"native\"\n"))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.NativeSSH(), []string{"boat", "lium"})
	c, err = LoadFile(writeFile(t, "[lium]\nssh_path = \"gate\"\n"))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(c.NativeSSH()), 0)
}

func TestLoadReadsProviderSeeds(t *testing.T) {
	liumSeed := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("l", 32)))
	hfSeed := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("h", 32)))
	listed := writeFile(t, serving(t)+`providers = ["lium", "hf"]`+"\n"+gpuTables)
	c, err := Load(listed, env(map[string]string{
		"LIUM_SSH_SEED": liumSeed, "HF_SANDBOX_SEED": hfSeed,
		"LIUM_API_KEY": "lium-key", "HF_JOBS_TOKEN": "hf_jobs",
	}))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.ProviderSeeds, map[string][]byte{
		"lium": []byte(strings.Repeat("l", 32)), "hf": []byte(strings.Repeat("h", 32)),
	})
	eq(t, c.ProviderKeys["lium"], "lium-key")
	eq(t, c.ProviderKeys["hf"], "hf_jobs")

	// A seed is required only while its provider is listed
	c, err = Load(writeFile(t, serving(t)), env(nil))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(c.ProviderSeeds), 0)
	for _, over := range []struct{ missing, name string }{
		{"LIUM_SSH_SEED", "lium"}, {"HF_SANDBOX_SEED", "hf"},
	} {
		vars := map[string]string{"LIUM_SSH_SEED": liumSeed, "HF_SANDBOX_SEED": hfSeed}
		delete(vars, over.missing)
		_, err := Load(listed, env(vars))
		want := over.missing + " is required while providers lists " + over.name
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("no %s gave %v, want %q", over.missing, err, want)
		}
	}

	// A seed of the wrong length or in bad base64 is refused even unlisted, and never quoted
	short := base64.StdEncoding.EncodeToString([]byte("short-seed-but-secret"))
	long := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("s", 33)))
	url := base64.URLEncoding.EncodeToString([]byte(strings.Repeat("\xff", 32)))
	for _, name := range []string{"LIUM_SSH_SEED", "HF_SANDBOX_SEED"} {
		for _, v := range []string{short, long, url, "not base64 at all!", liumSeed[:40]} {
			_, err := Load(writeFile(t, serving(t)), env(map[string]string{name: v}))
			want := name + " must be base64 of 32 bytes"
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("%s of %d characters gave %v, want %q", name, len(v), err, want)
				continue
			}
			if strings.Contains(err.Error(), v) {
				t.Errorf("%s: the error quotes the value: %v", name, err)
			}
		}
	}
}

func TestLoadReadsLiumWebhookSecret(t *testing.T) {
	c, err := Load(writeFile(t, serving(t)), env(map[string]string{
		"LIUM_WEBHOOK_SECRET": "lium-now", "LIUM_WEBHOOK_SECRET_PREVIOUS": "lium-old",
	}))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.HookSecrets["lium"],
		HookSecret{Current: []byte("lium-now"), Previous: []byte("lium-old")})
}

func TestLoadFileRefusesGPUSecrets(t *testing.T) {
	for _, c := range []struct{ toml, key, env string }{
		{"[lium]\napi_key = \"lium-secret-value\"", "lium.api_key", "LIUM_API_KEY"},
		{"[lium]\nssh_seed = \"lium-secret-value\"", "lium.ssh_seed", "LIUM_SSH_SEED"},
		{"[lium]\nwebhook_secret = \"lium-secret-value\"", "lium.webhook_secret",
			"LIUM_WEBHOOK_SECRET"},
		{"[lium]\nwebhook_secret_previous = \"lium-secret-value\"",
			"lium.webhook_secret_previous", "LIUM_WEBHOOK_SECRET_PREVIOUS"},
		{"[lium]\nbudget_test_key = \"lium-secret-value\"", "lium.budget_test_key",
			"LIUM_BUDGET_TEST_KEY"},
		{"[hf]\njobs_token = \"lium-secret-value\"", "hf.jobs_token", "HF_JOBS_TOKEN"},
		{"[hf]\nsandbox_seed = \"lium-secret-value\"", "hf.sandbox_seed", "HF_SANDBOX_SEED"},
		{"LIUM_SSH_SEED = \"lium-secret-value\"", "LIUM_SSH_SEED", "LIUM_SSH_SEED"},
	} {
		_, err := LoadFile(writeFile(t, c.toml))
		want := c.key + " is a secret, set " + c.env + " in the environment instead"
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s in the file gave %v, want %q", c.key, err, want)
			continue
		}
		if strings.Contains(err.Error(), "lium-secret-value") {
			t.Errorf("%s in the file: error echoes its value: %v", c.key, err)
		}
	}
}
