package catalog_test

import (
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/providers"
)

// upHealth reports every provider healthy
type upHealth struct{}

func (upHealth) Healthy(string) bool           { return true }
func (upHealth) ReadyP95(string) time.Duration { return 0 }

// productionEngine builds production's providers from its settings, each with a stand-in key
func productionEngine(t *testing.T) (*placement.Engine, config.Config) {
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
	return placement.New(reg, upHealth{}, nil), cfg
}

// TestProductionCatalog holds each package to a live price that lands on a production server
func TestProductionCatalog(t *testing.T) {
	c, err := catalog.Load("../../config/packages.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(c.All()) == 0 {
		t.Fatal("the production catalog sells nothing")
	}
	e, cfg := productionEngine(t)
	if cfg.Pricing.Rule.MarkupBP != 1000 {
		t.Fatalf("production marks cost up %d bp, want 1000", cfg.Pricing.Rule.MarkupBP)
	}
	for _, p := range c.All() {
		if p.FixedPrice {
			t.Errorf("package %s sets price_micros, production quotes every package live", p.ID)
		}
		in := placement.Inputs{
			Package: p, Rule: cfg.Pricing.Rule, ImageAccept: cfg.Images.Accept,
		}
		if len(e.Lands(in)) == 0 {
			t.Errorf("package %s lands on no production server", p.ID)
		}
	}
}

// TestProductionExtensions holds every base package to one extension at its own rate
func TestProductionExtensions(t *testing.T) {
	c, err := catalog.Load("../../config/packages.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, base := range c.All() {
		if base.ExtensionOf != "" {
			continue
		}
		var found []catalog.Package
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
			ext.Minutes != base.Minutes {
			t.Errorf("extension %s differs from %s in id, resources or minutes",
				ext.ID, base.ID)
		}
	}
}
