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
