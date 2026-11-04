package providers_test

import (
	"strings"
	"testing"
	"time"

	"github.com/oynozan/bugnet-router/internal/config"
	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/providers"
)

func localConfig(t *testing.T) config.Config {
	t.Helper()
	return config.Config{
		Providers:         []string{"local"},
		LocalEnabled:      true,
		LocalRoot:         t.TempDir(),
		LocalRunnerPath:   "/opt/bugnet-run",
		LocalHourlyMicros: 2500,
	}
}

func TestBuildLocal(t *testing.T) {
	t.Setenv("BUGNET_ENABLE_LOCAL_PROVIDER", "1")
	reg, err := providers.Build(localConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	ordered := reg.Ordered()
	if len(ordered) != 1 || ordered[0].Name() != "local" {
		t.Fatalf("registry %v, want only local", ordered)
	}
	p := ordered[0]
	huge := provider.Resources{
		VCPU: 256, MemoryMiB: 1 << 20, DiskGiB: 1 << 14, GPUCount: 8, GPUMemoryGiB: 640,
	}
	size, ok := p.SizeFor(huge)
	if !ok || size.Name != "local" {
		t.Fatalf("size for a large package: %+v, %v", size, ok)
	}
	if p.HourlyCostMicros(size) != 2500 || p.BillingIncrement() != time.Second {
		t.Fatalf("cost %d per hour in %v increments", p.HourlyCostMicros(size),
			p.BillingIncrement())
	}
}

func TestBuildLocalRefusesWithoutFlag(t *testing.T) {
	t.Setenv("BUGNET_ENABLE_LOCAL_PROVIDER", "")
	_, err := providers.Build(localConfig(t))
	if err == nil || !strings.Contains(err.Error(), "BUGNET_ENABLE_LOCAL_PROVIDER") {
		t.Fatalf("build without the flag: %v", err)
	}
}

func TestBuildRejectsProvidersWithoutAdapter(t *testing.T) {
	for _, name := range []string{"koyeb", "boat", "e2b", "boxd"} {
		_, err := providers.Build(config.Config{Providers: []string{name}})
		if err == nil || !strings.Contains(err.Error(), name) {
			t.Errorf("build %s: %v", name, err)
		}
	}
}
