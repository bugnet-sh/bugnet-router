package config

import (
	"slices"
	"strings"
	"testing"
	"time"
)

var readyDefaults = map[string]time.Duration{
	"local": time.Minute, "boxd": time.Minute, "e2b": time.Minute, "koyeb": time.Minute,
	"boat": time.Minute, "lium": 6 * time.Minute, "hf": 5 * time.Minute,
}

func TestPlacingDefaults(t *testing.T) {
	c, err := LoadFile(writeFile(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.ReadyTimeouts, readyDefaults)
	eq(t, c.GPUOrder, []string{"lium", "hf"})
	eq(t, c.GPUPlacingBudget, 13*time.Minute)
	eq(t, c.Hedge.OntoGPU, false)
}

func TestPlacingSettings(t *testing.T) {
	c, err := LoadFile(writeFile(t, `gpu_order = ["hf", "koyeb", "lium"]
gpu_placing_budget = "20m"

[local]
ready_timeout = "61s"

[boxd]
ready_timeout = "2m"

[e2b]
ready_timeout = "90s"

[koyeb]
ready_timeout = "15m"

[boat]
ready_timeout = "60s"

[hedge]
onto_gpu = true
`))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.ReadyTimeouts, map[string]time.Duration{
		"local": 61 * time.Second, "boxd": 2 * time.Minute, "e2b": 90 * time.Second,
		"koyeb": 15 * time.Minute, "boat": time.Minute, "lium": 6 * time.Minute,
		"hf": 5 * time.Minute,
	})
	eq(t, c.GPUOrder, []string{"hf", "koyeb", "lium"})
	eq(t, c.GPUPlacingBudget, 20*time.Minute)
	eq(t, c.Hedge.OntoGPU, true)
	// A GPU order may name a provider the providers setting leaves out, which has no candidates
	c, err = LoadFile(writeFile(t, "providers = [\"e2b\"]\ngpu_order = [\"lium\"]\n"))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.GPUOrder, []string{"lium"})
	c, err = LoadFile(writeFile(t, "providers = [\"e2b\"]\ngpu_order = []\n"))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(c.GPUOrder), 0)
}

// TestConfigPlacingBudget is check 9 of the GPU design's 5.3
func TestConfigPlacingBudget(t *testing.T) {
	// Lium's create, ready wait, two runner pings, setup and harden, then the next create
	eq(t, MinGPUPlacingBudget(6*time.Minute), 12*time.Minute+10*time.Second)
	eq(t, MinGPUPlacingBudget(7*time.Minute), 13*time.Minute+10*time.Second)
	c, err := LoadFile(writeFile(t, `gpu_placing_budget = "12m10s"`))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.GPUPlacingBudget, 12*time.Minute+10*time.Second)
	_, err = LoadFile(writeFile(t, `gpu_placing_budget = "12m9s"`))
	if err == nil || !strings.Contains(err.Error(), "gpu_placing_budget must be at least 12m10s") {
		t.Fatalf("a budget a second short of lium's slowest failing boot gave %v", err)
	}
}

func TestPlacingRefusals(t *testing.T) {
	for name, c := range map[string]struct{ doc, want string }{
		"ready short": {"[e2b]\nready_timeout = \"59s\"\n", "e2b.ready_timeout"},
		"ready long":  {"[boat]\nready_timeout = \"16m\"\n", "boat.ready_timeout"},
		"ready word":  {"[koyeb]\nready_timeout = \"soon\"\n", "koyeb.ready_timeout"},
		"ready local": {"[local]\nready_timeout = \"0s\"\n", "local.ready_timeout"},
		"ready boxd":  {"[boxd]\nready_timeout = 60\n", "boxd.ready_timeout"},
		"budget long": {"gpu_placing_budget = \"61m\"\n", "gpu_placing_budget"},
		"budget word": {"gpu_placing_budget = \"later\"\n", "gpu_placing_budget"},
		"order twice": {"gpu_order = [\"lium\", \"hf\", \"lium\"]\n",
			`gpu_order lists "lium" more than once`},
		"order unknown": {"gpu_order = [\"lium\", \"runpod\"]\n",
			"gpu_order entry 2 is not one of boat, boxd, e2b, hf, koyeb, lium, local"},
		"order empty": {"gpu_order = [\"\"]\n", "gpu_order has an empty entry"},
		"order kind":  {"gpu_order = \"lium\"\n", "gpu_order"},
		"hedge kind":  {"[hedge]\nonto_gpu = \"yes\"\n", "hedge.onto_gpu"},
	} {
		_, err := LoadFile(writeFile(t, c.doc))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: got %v, want an error naming %q", name, err, c.want)
		}
	}
	// An unknown entry is never quoted, since a secret could have been pasted there
	_, err := LoadFile(writeFile(t, `gpu_order = ["sk-pasted-secret"]`))
	if err == nil || strings.Contains(err.Error(), "pasted") {
		t.Fatalf("an unknown gpu_order entry gave %v", err)
	}
}

func TestCommittedFilesWritePlacingSettings(t *testing.T) {
	want := [][]string{{"gpu_order"}, {"gpu_placing_budget"}, {"hedge", "onto_gpu"}}
	for _, p := range []string{"local", "boxd", "e2b", "koyeb", "boat", "lium", "hf"} {
		want = append(want, []string{p, "ready_timeout"})
	}
	doc, err := outline([]byte(committed(t, "example.toml")))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range want {
		if !slices.ContainsFunc(doc, func(s setting) bool { return slices.Equal(s.path, path) }) {
			t.Errorf("config/example.toml does not write %s", strings.Join(path, "."))
		}
	}
	doc, err = outline([]byte(committed(t, "dev.toml")))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"gpu_order", "gpu_placing_budget"} {
		if !slices.ContainsFunc(doc, func(s setting) bool {
			return slices.Equal(s.path, []string{key})
		}) {
			t.Errorf("config/dev.toml does not write %s", key)
		}
	}
}
