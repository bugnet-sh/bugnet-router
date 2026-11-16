package providers_test

import (
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/providers"
)

// gpuServers builds lium with config/example.toml's sizes and hf on its compiled list, with no
// node listed and HF's hardware list out of reach
func gpuServers(t *testing.T) (config.Config, []provider.Provider) {
	t.Helper()
	swapNet(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Host == "lium.io" {
			_, _ = io.WriteString(w, `[]`)
			return
		}
		http.Error(w, "offline", http.StatusServiceUnavailable)
	})
	raw, err := os.ReadFile("../../config/example.toml")
	if err != nil {
		t.Fatal(err)
	}
	// example.toml's [lium] table and sizes, which run to its [hf] table
	start := strings.Index(string(raw), "\n[lium]")
	end := strings.Index(string(raw), "\n[hf]")
	if start < 0 || end < start {
		t.Fatal("config/example.toml has no [lium] table before [hf]")
	}
	content := `providers = ["lium", "hf"]` + string(raw[start:end]) +
		"\n[hf]\nnamespace = \"bugnet-test\"\n"
	cfg := load(t, content, map[string]string{"lium": "lium_test", "hf": "hf_test"})
	cfg.ProviderSeeds = seeds("lium", "hf")
	reg, err := providers.Build(cfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range reg.Ordered() {
		closeOnCleanup(t, p)
	}
	sold := map[string]int{}
	for _, p := range reg.Ordered() {
		if sizer, ok := provider.As[provider.Sizer](p); ok {
			sold[p.Name()] = len(sizer.Sizes())
		}
	}
	// Every starting Lium size and every HF GPU flavor
	if sold["lium"] != 11 || sold["hf"] != 22 {
		t.Fatalf("sizes sold %v, want lium's 11 and hf's 22", sold)
	}
	return cfg, reg.Ordered()
}

// TestGPUCatalogsPassCheck8 runs check 8 on both catalogs against every Lium size at its cap and
// every HF flavor, and holds each dev GPU price to the least the markup allows
func TestGPUCatalogsPassCheck8(t *testing.T) {
	cfg, provs := gpuServers(t)
	for _, file := range []string{"dev-packages.json", "packages.json"} {
		c, err := catalog.Load("../../config/" + file)
		if err != nil {
			t.Fatal(err)
		}
		if err := cfg.CheckGPUMargin(c.All(), provs); err != nil {
			t.Errorf("%s: %v", file, err)
		}
	}
	c, err := catalog.Load("../../config/dev-packages.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range c.All() {
		if p.Resources.GPUCount == 0 {
			continue
		}
		under := p
		under.PriceMicros--
		if err := cfg.CheckGPUMargin([]catalog.Package{under}, provs); err == nil {
			t.Errorf("package %s passes at %d, so its price is over the least the markup "+
				"allows", p.ID, under.PriceMicros)
		}
	}
}
