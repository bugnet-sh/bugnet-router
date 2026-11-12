package config

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/gpu"
)

// liumSizes are design 3.5's starting sizes, as config/example.toml writes them
var liumSizes = []struct {
	size, model                   string
	gpuGiB, vcpu, ramGiB, diskGiB int
	capMicros                     int64
}{
	{"lium-t4", gpu.T4, 16, 3, 14, 200, 300_000},
	{"lium-rtx3090", gpu.RTX3090, 24, 8, 28, 200, 350_000},
	{"lium-rtxa6000", gpu.RTXA6000, 48, 4, 22, 200, 600_000},
	{"lium-rtx4090", gpu.RTX4090, 24, 8, 28, 200, 700_000},
	{"lium-rtx5090", gpu.RTX5090, 32, 8, 20, 400, 950_000},
	{"lium-rtx6000ada", gpu.RTX6000Ada, 48, 8, 56, 250, 1_100_000},
	{"lium-a100", gpu.A100, 80, 12, 110, 900, 1_500_000},
	{"lium-rtxpro6000", gpu.RTXPro6000, 96, 6, 56, 180, 1_800_000},
	{"lium-h100", gpu.H100, 80, 12, 180, 180, 2_900_000},
	{"lium-h200", gpu.H200, 141, 16, 240, 900, 4_900_000},
	{"lium-b200", gpu.B200, 180, 16, 200, 400, 9_500_000},
}

// writes reports whether an outlined document writes the key at path
func writes(doc []setting, path []string) bool {
	return slices.ContainsFunc(doc, func(s setting) bool { return slices.Equal(s.path, path) })
}

func TestCommittedFilesWriteGPUSettings(t *testing.T) {
	doc, err := outline([]byte(committed(t, "example.toml")))
	if err != nil {
		t.Fatal(err)
	}
	for table, schema := range map[string]reflect.Type{
		"lium": reflect.TypeFor[liumFile](), "hf": reflect.TypeFor[hfFile](),
		"lium.sizes": reflect.TypeFor[LiumSize](),
	} {
		for i := range schema.NumField() {
			path := slices.Concat(strings.Split(table, "."),
				[]string{schema.Field(i).Tag.Get("toml")})
			if !writes(doc, path) {
				t.Errorf("config/example.toml does not write %s", strings.Join(path, "."))
			}
		}
	}
	for _, path := range [][]string{
		{"images", "lium", "ref"}, {"images", "hf", "ref"},
		{"images", "lium", "version"}, {"images", "hf", "version"},
	} {
		if !writes(doc, path) {
			t.Errorf("config/example.toml does not write %s", strings.Join(path, "."))
		}
	}
	example := loadExample(t)
	eq(t, len(example.Lium.Sizes), len(liumSizes))
	for i, want := range liumSizes {
		got := example.Lium.Sizes[i]
		eq(t, got.Size, want.size)
		eq(t, got.Model, want.model)
		eq(t, gpu.Known(got.Model), true)
		eq(t, got.GPUs, 1)
		eq(t, []int{got.GPUMemoryGiB, got.VCPU, got.MemoryMiB, got.DiskGiB},
			[]int{want.gpuGiB, want.vcpu, want.ramGiB * 1024, want.diskGiB})
		eq(t, got.MaxHourlyMicros, want.capMicros)
		eq(t, len(got.Match) > 0, true)
	}
	eq(t, example.Lium.Sizes[1], rtx3090)

	dev := loadDev(t)
	eq(t, dev.Lium.Sizes, []LiumSize{rtx3090})
	eq(t, dev.MaxConcurrent["lium"], 2)
	eq(t, dev.Lium.KeyDailyBudgetMicros, int64(20_000_000))
	eq(t, dev.HF, hfDefaults)
	eq(t, dev.Providers, []string{"local"})
	doc, err = outline([]byte(committed(t, "dev.toml")))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range [][]string{{"hf", "namespace"}, {"lium", "max_concurrent"}} {
		if !writes(doc, path) {
			t.Errorf("config/dev.toml does not write %s", strings.Join(path, "."))
		}
	}
}

// TestHFSBXServerDefaultIsTheImagePin keeps hf's default build the one deploy/image bakes
func TestHFSBXServerDefaultIsTheImagePin(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "deploy", "image", "pins.conf"))
	if err != nil {
		t.Fatal(err)
	}
	pins := map[string]string{}
	for line := range strings.SplitSeq(string(b), "\n") {
		if k, v, ok := strings.Cut(line, "="); ok {
			pins[k] = v
		}
	}
	d := defaultHF()
	eq(t, pins["SBX_SERVER_SHA256_AMD64"], d.SBXServerSHA256)
	eq(t, pins["SBX_SERVER_VERSION"], d.SBXServerVersion)
}
