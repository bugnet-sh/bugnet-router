package config

import (
	"fmt"
	"strings"
	"testing"
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
