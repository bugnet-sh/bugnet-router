package saga_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/gpu"
	"github.com/bugnet-sh/bugnet-router/internal/ordertoken"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// t4 is a one-GPU size, which serves the GPU package and the small CPU one alike
var t4 = provider.Size{Name: "t4", Resources: provider.Resources{
	VCPU: 4, MemoryMiB: 16384, DiskGiB: 40, GPUCount: 1, GPUMemoryGiB: 16, GPUModel: gpu.T4,
}}

const gpuCatalogJSON = `{"packages": [
	{"id": "small-10m", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10, "price_micros": 50000},
	{"id": "gpu-t4-10m", "resources": {"vcpu": 4, "memory_mib": 12288, "disk_gib": 40,
		"gpu_count": 1, "gpu_memory_gib": 16, "gpu_model": "NVIDIA T4"},
		"minutes": 10, "price_micros": 50000}
]}`

func loadCatalog(t *testing.T, raw string) *catalog.Catalog {
	t.Helper()
	path := filepath.Join(t.TempDir(), "packages.json")
	if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
		t.Fatal(err)
	}
	cat, err := catalog.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	return cat
}

// gpuLocal is a local provider that takes GPU packages
type gpuLocal struct{ *local.Provider }

func (g gpuLocal) Capabilities() provider.Capabilities {
	c := g.Provider.Capabilities()
	c.GPU = true
	return c
}

// failsLate is a GPU provider whose sandboxes fail to become ready once wait has passed
type failsLate struct {
	gpuLocal
	wait time.Duration
}

func (f failsLate) WaitReady(ctx context.Context, _ provider.Ref) error {
	timer := time.NewTimer(f.wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return &provider.Error{Code: provider.CodeInternal, Err: errors.New("boot failed")}
	}
}

// TestGPUPackagePlacesUnderTheGPUBudget lets a second GPU candidate start after a failed boot that
// outran the CPU budget, which a CPU package never does
func TestGPUPackagePlacesUnderTheGPUBudget(t *testing.T) {
	saga.SetPlacingBudget(t, 500*time.Millisecond)
	lium := failsLate{gpuLocal{newLocal(t, local.Options{
		Name: "lium", Sizes: []provider.Size{t4},
	})}, 2 * time.Second}
	hf := gpuLocal{newLocal(t, local.Options{
		Name: "hf", Sizes: []provider.Size{t4}, HourlyCostMicros: 2000,
	})}
	e := newEnvWith(t, func(d *saga.Deps) {
		d.Catalog = loadCatalog(t, gpuCatalogJSON)
		d.Engine = d.Engine.WithGPUOrder([]string{"lium", "hf"})
		d.GPUPlacingBudget = time.Minute
	}, lium, hf)
	for _, c := range []struct {
		pkg   string
		state string
		tried []string
	}{
		{"gpu-t4-10m", store.PlacementReady, []string{"lium", "hf"}},
		{"small-10m", store.PlacementFailed, []string{"lium"}},
	} {
		p, _, err := e.saga.Create(t.Context(), saga.CreateInput{Token: e.token(t,
			ordertoken.Claims{
				OrderID: "order-" + c.pkg, Kind: ordertoken.KindStart, PackageID: c.pkg,
				AmountMicros: 50000,
			})})
		if err != nil {
			t.Fatalf("create %s: %v", c.pkg, err)
		}
		e.saga.Wait()
		p = e.reload(t, p.ID)
		var tried []string
		for _, a := range e.attempts(t, p.ID) {
			tried = append(tried, a.provider)
		}
		if p.State != c.state || !slices.Equal(tried, c.tried) {
			t.Errorf("%s ended %s (%s) after trying %v, want %s after %v", c.pkg, p.State,
				p.FailureReason, tried, c.state, c.tried)
		}
	}
}
