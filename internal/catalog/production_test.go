package catalog

import (
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/providers"
)

// liveProviders builds production's providers from its settings, each with a stand-in key
func liveProviders(t *testing.T) []provider.Provider {
	t.Helper()
	cfg, err := config.LoadFile("../../deploy/vps/config/router.toml")
	if err != nil {
		t.Fatal(err)
	}
	cfg.ProviderKeys = map[string]string{}
	for _, name := range cfg.Providers {
		cfg.ProviderKeys[name] = "stand-in-key"
	}
	reg, err := providers.Build(cfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(reg.Ordered()) == 0 {
		t.Fatal("production runs no provider")
	}
	return reg.Ordered()
}

// TestProductionCatalog holds config/packages.json to production's providers, so every package
// loads, fits a size they sell and keeps the 50% margin floor at the cheapest adapter's rate
func TestProductionCatalog(t *testing.T) {
	c, err := Load("../../config/packages.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(c.All()) == 0 {
		t.Fatal("the production catalog sells nothing")
	}
	live := liveProviders(t)
	for _, p := range c.All() {
		var cheapest provider.Provider
		var size provider.Size
		for _, prov := range live {
			s, ok := prov.SizeFor(p.Resources)
			if ok && (cheapest == nil ||
				prov.HourlyCostMicros(s) < cheapest.HourlyCostMicros(size)) {
				cheapest, size = prov, s
			}
		}
		if cheapest == nil {
			t.Errorf("package %s fits no live provider size", p.ID)
			continue
		}
		cost := cheapest.HourlyCostMicros(size) * int64(p.Minutes) / 60
		if p.PriceMicros < 2*cost {
			t.Errorf("package %s at %d micros is under the 50%% margin floor on %s %s (cost %d)",
				p.ID, p.PriceMicros, cheapest.Name(), size.Name, cost)
		}
	}
}

// TestProductionExtensions holds every base package to one extension at its own rate
func TestProductionExtensions(t *testing.T) {
	c, err := Load("../../config/packages.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, base := range c.All() {
		if base.ExtensionOf != "" {
			continue
		}
		var found []Package
		for _, p := range c.All() {
			if p.ExtensionOf == base.ID {
				found = append(found, p)
			}
		}
		if len(found) != 1 {
			t.Errorf("package %s has %d extensions, want 1", base.ID, len(found))
			continue
		}
		ext := found[0]
		if ext.ID != base.ID+"-more" || ext.Resources != base.Resources ||
			ext.PriceMicros != base.PriceMicros || ext.Minutes != base.Minutes {
			t.Errorf("extension %s differs from %s in id, resources, price or minutes",
				ext.ID, base.ID)
		}
	}
}
