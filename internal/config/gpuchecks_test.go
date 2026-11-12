package config

import (
	"fmt"
	"strings"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/gpu"
)

// gpuCheck is one start-up check of the GPU design's 5.3, a file it takes and one it refuses
type gpuCheck struct {
	pass, refuse, want string
}

// runGPUChecks loads each check's two files and wants the refusal to say want
func runGPUChecks(t *testing.T, checks map[string]gpuCheck) {
	t.Helper()
	for name, c := range checks {
		if _, err := LoadFile(writeFile(t, c.pass)); err != nil {
			t.Errorf("%s: the passing file gave %v", name, err)
		}
		_, err := LoadFile(writeFile(t, c.refuse))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: the refused file gave %v, want an error saying %q", name, err, c.want)
		}
	}
}

// TestConfigGPUDataPlane is checks 1 and 3 of 5.3 and the previews rule, lium having no gate
func TestConfigGPUDataPlane(t *testing.T) {
	runGPUChecks(t, map[string]gpuCheck{
		"1 secrets": {
			pass:   "[secrets]\nproviders = [\"hf\", \"e2b\"]\n",
			refuse: "[secrets]\nproviders = [\"e2b\", \"lium\"]\n",
			want: "secrets.providers may not list lium, lium pods can be read by their host " +
				"operator, so secrets never go there",
		},
		"3 ssh": {
			pass:   "[ssh]\nproviders = [\"lium\"]\n[lium]\nssh_path = \"native\"\n",
			refuse: "[ssh]\nproviders = [\"lium\"]\n[lium]\nssh_path = \"gate\"\n",
			want:   `ssh.providers may list lium only while lium.ssh_path is "native"`,
		},
		"3 gate unlisted": {
			pass:   "[ssh]\nproviders = [\"e2b\"]\n[lium]\nssh_path = \"gate\"\n",
			refuse: "[ssh]\nproviders = [\"e2b\", \"lium\"]\n[lium]\nssh_path = \"gate\"\n",
			want:   `ssh.providers may list lium only while lium.ssh_path is "native"`,
		},
		"previews": {
			pass:   "[previews]\nproviders = [\"hf\"]\n",
			refuse: "[previews]\nproviders = [\"hf\", \"lium\"]\n",
			want:   "previews.providers may not list lium, whose pods run no gate",
		},
	})
}

// TestConfigLiumHarden is checks 2 and 12 of 5.3, harden on every Lium pod with its gateway
func TestConfigLiumHarden(t *testing.T) {
	runGPUChecks(t, map[string]gpuCheck{
		"2 required": {
			pass:   "[lium]\nharden_required = true\n",
			refuse: "[lium]\nharden_required = false\n",
			want:   "lium.harden_required must stay true",
		},
		"12 no gateway": {
			pass:   "[lium]\nharden_no_gateway = true\n",
			refuse: "[lium]\nharden_no_gateway = false\n",
			want:   "lium.harden_no_gateway must stay true",
		},
	})
}

// TestConfigGPUNotBeforePayment is check 4 of 5.3, no GPU sandbox starts before payment
func TestConfigGPUNotBeforePayment(t *testing.T) {
	pool := "[[pools.pool]]\nprovider = %q\nsize = \"lium-rtx3090\"\nmax = 1\n"
	checks := map[string]gpuCheck{}
	for _, name := range []string{"lium", "hf"} {
		checks["4 reserve "+name] = gpuCheck{
			pass:   "[reserve]\nspeculative_providers = [\"e2b\", \"boxd\"]\n",
			refuse: fmt.Sprintf("[reserve]\nspeculative_providers = [\"e2b\", %q]\n", name),
			want: "reserve.speculative_providers cannot list " + name + ", no GPU sandbox " +
				"starts before payment",
		}
		checks["4 pool "+name] = gpuCheck{
			pass:   fmt.Sprintf(pool, "boxd"),
			refuse: fmt.Sprintf(pool, name),
			want: "pools.pool[0].provider " + name + " cannot keep a pool, no GPU sandbox " +
				"starts before payment",
		}
	}
	runGPUChecks(t, checks)
}

// liumSize writes one [[lium.sizes]] table, rtx3090's but for the name, model and cap given
func liumSize(name, model string, capMicros int64) string {
	return fmt.Sprintf(`
[[lium.sizes]]
size = %q
model = %q
match = ["NVIDIA GeForce RTX 3090"]
gpus = 1
gpu_memory_gib = 24
vcpu = 8
memory_mib = 28672
disk_gib = 200
max_hourly_micros = %d
`, name, model, capMicros)
}

// gpuTables are the [hf] and [lium] settings a file listing both providers needs, appended last
var gpuTables = "\n[hf]\nnamespace = \"bugnet-sh\"\n" +
	liumSize("lium-rtx3090", gpu.RTX3090, 350_000)

// TestConfigGPUTables is check 6 of 5.3, the [lium] and [hf] tables a listed provider needs
func TestConfigGPUTables(t *testing.T) {
	size := liumSize("lium-rtx3090", gpu.RTX3090, 350_000)
	runGPUChecks(t, map[string]gpuCheck{
		"6 hf namespace": {
			pass:   "providers = [\"hf\"]\n[hf]\nnamespace = \"bugnet-sh\"\n",
			refuse: "providers = [\"hf\"]\n",
			want:   "hf.namespace is required while providers lists hf",
		},
		"6 lium size": {
			pass:   "providers = [\"lium\"]\n" + size,
			refuse: "providers = [\"lium\"]\n",
			want:   "lium.sizes must name at least one size while providers lists lium",
		},
		"6 size twice": {
			pass:   size + liumSize("lium-rtx3090-big", gpu.RTX3090, 400_000),
			refuse: size + liumSize("lium-rtx3090", gpu.RTX3090, 400_000),
			want:   "lium.sizes entry 2 repeats the size of entry 1",
		},
		"6 size name": {
			pass:   size,
			refuse: size + liumSize("Lium RTX 3090", gpu.RTX3090, 350_000),
			want: "lium.sizes entry 2 size must be 1 to 32 lowercase letters, digits or " +
				"hyphens",
		},
		"6 model": {
			pass:   liumSize("lium-a100", gpu.A100, 1_500_000),
			refuse: liumSize("lium-a100", "NVIDIA A100", 1_500_000),
			want:   "lium.sizes entry 1 model must be a canonical GPU model, one of NVIDIA T4,",
		},
		"6 cap zero": {
			pass:   liumSize("lium-rtx3090", gpu.RTX3090, 1),
			refuse: liumSize("lium-rtx3090", gpu.RTX3090, 0),
			want:   "lium.sizes entry 1 max_hourly_micros must be above 0",
		},
		"6 cap below": {
			pass:   size,
			refuse: liumSize("lium-rtx3090", gpu.RTX3090, -350_000),
			want:   "lium.sizes entry 1 max_hourly_micros must be above 0",
		},
		"6 gpus": {
			pass:   size,
			refuse: strings.Replace(size, "gpus = 1", "gpus = 0", 1),
			want:   "lium.sizes entry 1 gpus must be 1 or more",
		},
		"6 match": {
			pass: size,
			refuse: strings.Replace(size, `match = ["NVIDIA GeForce RTX 3090"]`,
				"match = []", 1),
			want: "lium.sizes entry 1 match must name at least one gpu_class",
		},
		"6 flavor": {
			pass:   "[hf]\nflavors = [\"t4-small\", \"h200x8\"]\n",
			refuse: "[hf]\nflavors = [\"t4-small\", \"cpu-basic\"]\n",
			want:   "hf.flavors entry 2 is not an HF GPU flavor, use one of t4-small,",
		},
		"6 countries empty": {
			pass:   "[lium]\ncountries = [\"DE\"]\nother_countries = false\n",
			refuse: "[lium]\ncountries = []\n",
			want:   "lium.countries must name at least one country",
		},
		"6 country blocked": {
			pass:   "[lium]\ncountries = [\"DE\"]\nblocked_countries = [\"RU\"]\n",
			refuse: "[lium]\ncountries = [\"DE\", \"RU\"]\nblocked_countries = [\"BY\", \"RU\"]\n",
			want:   "lium.countries entry 2 is also in lium.blocked_countries",
		},
	})
	// A flavor entry is never quoted, since a secret could have been pasted there
	_, err := LoadFile(writeFile(t, "[hf]\nflavors = [\"hf_pastedsecret\"]\n"))
	if err == nil || strings.Contains(err.Error(), "pasted") {
		t.Fatalf("an unknown flavor gave %v", err)
	}
}

// TestConfigGPUOrder is check 5 of 5.3, which may name a provider that providers leaves out
func TestConfigGPUOrder(t *testing.T) {
	runGPUChecks(t, map[string]gpuCheck{
		"5 unknown": {
			pass:   "providers = [\"e2b\"]\ngpu_order = [\"hf\", \"lium\"]\n",
			refuse: "gpu_order = [\"hf\", \"runpod\"]\n",
			want:   "gpu_order entry 2 is not one of",
		},
		"5 twice": {
			pass:   "gpu_order = [\"hf\"]\n",
			refuse: "gpu_order = [\"hf\", \"lium\", \"hf\"]\n",
			want:   `gpu_order lists "hf" more than once`,
		},
		"5 empty entry": {
			pass:   "gpu_order = []\n",
			refuse: "gpu_order = [\"lium\", \"\"]\n",
			want:   "gpu_order has an empty entry",
		},
	})
}

// TestConfigReadyTimeouts is check 7 of 5.3, every provider's ready_timeout from 60 s to 15 min
func TestConfigReadyTimeouts(t *testing.T) {
	// A budget over lium's slowest failing boot at a 15 minute ready_timeout
	budget := "gpu_placing_budget = \"22m\"\n"
	checks := map[string]gpuCheck{}
	for _, p := range []string{"local", "boxd", "e2b", "koyeb", "boat", "lium", "hf"} {
		table := budget + "[" + p + "]\nready_timeout = "
		want := p + ".ready_timeout must be a Go duration from 1m0s to 15m0s"
		checks["7 "+p+" short"] = gpuCheck{table + `"60s"`, table + `"59s"`, want}
		checks["7 "+p+" long"] = gpuCheck{table + `"15m"`, table + `"15m1s"`, want}
	}
	runGPUChecks(t, checks)
}

// TestConfigLiumKeyBudget is check 10 of 5.3, a key budget above what the router spends in a day
func TestConfigLiumKeyBudget(t *testing.T) {
	sizes := liumSize("lium-rtx3090", gpu.RTX3090, 350_000) +
		liumSize("lium-b200", gpu.B200, 9_500_000)
	budget := func(slots, micros int64) string {
		return fmt.Sprintf("[lium]\nmax_concurrent = %d\nkey_daily_budget_micros = %d\n",
			slots, micros) + sizes
	}
	// 10 pods at B200's $9.50 cap for 24 hours is $2,280
	want := "lium.key_daily_budget_micros must be 0 or at least 2280000000"
	runGPUChecks(t, map[string]gpuCheck{
		"10 at the bound": {budget(10, 2_280_000_000), budget(10, 2_279_999_999), want},
		"10 default":      {budget(10, 2_300_000_000), budget(10, 1_000_000), want},
		"10 none":         {budget(10, 0), budget(10, 1), want},
		// Without its own cap Lium gets the 50 slots of a provider with no cap at all
		"10 no cap": {budget(0, 11_400_000_000), budget(0, 11_399_999_999),
			"lium.key_daily_budget_micros must be 0 or at least 11400000000"},
		// The dev key, 2 pods at the $0.35 cap for a day is $16.80
		"10 dev": {
			"[lium]\nmax_concurrent = 2\nkey_daily_budget_micros = 16800000\n" +
				liumSize("lium-rtx3090", gpu.RTX3090, 350_000),
			"[lium]\nmax_concurrent = 2\nkey_daily_budget_micros = 16799999\n" +
				liumSize("lium-rtx3090", gpu.RTX3090, 350_000),
			"lium.key_daily_budget_micros must be 0 or at least 16800000",
		},
	})
	// With no size the router rents nothing on Lium, so any budget stands
	c, err := LoadFile(writeFile(t, "[lium]\nkey_daily_budget_micros = 1\n"))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.Lium.KeyDailyBudgetMicros, int64(1))
	// A cap near the int64 limit is compared without overflow
	_, err = LoadFile(writeFile(t, "[lium]\nkey_daily_budget_micros = 9223372036854775807\n"+
		liumSize("lium-b200", gpu.B200, 9_000_000_000_000_000_000)))
	over := "must be 0 or at least 2160000000000000000000"
	if err == nil || !strings.Contains(err.Error(), over) {
		t.Fatalf("a budget under an overflowing bound gave %v", err)
	}
}

// TestConfigHFPace is check 11 of 5.3, HF's calls paced under the token user's Hub rate limit
func TestConfigHFPace(t *testing.T) {
	pace := func(plan string, ops int) string {
		return fmt.Sprintf("[hf]\ntoken_plan = %q\nops_per_second = %d\n", plan, ops)
	}
	free := `hf.ops_per_second must be 1 to 3 while hf.token_plan is "free"`
	pro := `hf.ops_per_second must be 1 to 8 while hf.token_plan is "pro"`
	runGPUChecks(t, map[string]gpuCheck{
		"11 free":         {pace("free", 3), pace("free", 4), free},
		"11 free slow":    {pace("free", 1), pace("free", 8), free},
		"11 pro":          {pace("pro", 8), pace("pro", 9), pro},
		"11 pro slow":     {pace("pro", 4), pace("pro", 50), pro},
		"11 free unpaced": {pace("free", 2), pace("free", 0), free},
		"11 pro unpaced":  {pace("pro", 1), pace("pro", 0), pro},
	})
}

// TestConfigHFGatePort refuses a gate port but 29417 while hf serves previews, the one HF exposes
func TestConfigHFGatePort(t *testing.T) {
	previews := func(port int, providers ...string) string {
		return fmt.Sprintf("[previews]\ngate_port = %d\nproviders = [%s]\n", port,
			strings.Join(providers, ", "))
	}
	want := "previews.gate_port must be 29417 while previews.providers lists hf, the one " +
		"gate port every HF Job exposes"
	runGPUChecks(t, map[string]gpuCheck{
		"hf on the gate port": {previews(29417, `"hf"`), previews(29418, `"hf"`), want},
		"hf beside another":   {previews(29418, `"e2b"`), previews(8080, `"e2b"`, `"hf"`), want},
		"hf off previews":     {previews(8080), previews(1, `"hf"`), want},
		"hf with the default": {"[previews]\nproviders = [\"hf\"]\n", previews(443, `"hf"`), want},
		"hf among the providers": {"providers = [\"hf\"]\n[hf]\nnamespace = \"bugnet-test\"\n" +
			previews(8080), previews(65535, `"hf"`), want},
	})
}
