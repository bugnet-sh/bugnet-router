package catalog_test

import (
	"slices"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// devGPU are design 5.4's GPU packages without the two Q4 keeps out of the EU
var devGPU = []string{
	"gpu-t4-10m", "gpu-t4-medium-10m", "gpu-l4-10m", "gpu-a10g-10m", "gpu-a10g-large-10m",
	"gpu-l40s-10m", "gpu-a100-10m", "gpu-rtxpro6000-10m", "gpu-h200-10m", "gpu-l4x4-10m",
	"gpu-a10g-largex2-10m", "gpu-a10g-largex4-10m", "gpu-l40sx4-10m", "gpu-l40sx8-10m",
	"gpu-a100x4-10m", "gpu-a100x8-10m", "gpu-h200x2-10m", "gpu-h200x4-10m", "gpu-h200x8-10m",
	"gpu-rtxpro6000x2-10m", "gpu-rtxpro6000x4-10m", "gpu-rtxpro6000x8-10m", "gpu-rtx3090-10m",
	"gpu-rtx4090-10m", "gpu-rtx5090-10m", "gpu-rtx6000ada-10m", "gpu-h100-10m",
}

// TestDevGPUPackages holds the dev catalog to 5.4's GPU packages, each at a fixed price with one
// extension of the same floor and minutes
func TestDevGPUPackages(t *testing.T) {
	c, err := catalog.Load("../../config/dev-packages.json")
	if err != nil {
		t.Fatal(err)
	}
	var starts []string
	for _, p := range c.All() {
		if p.Resources.GPUCount == 0 {
			continue
		}
		if !p.FixedPrice || p.PriceMicros <= 0 || p.Minutes != 10 || p.GPUModel == "" {
			t.Errorf("package %s is not a priced 10-minute GPU package: %+v", p.ID, p)
		}
		if p.ExtensionOf != "" {
			base, ok := c.Get(p.ExtensionOf)
			if !ok || p.ID != base.ID+"-more" || p.Resources != base.Resources ||
				p.Minutes != base.Minutes {
				t.Errorf("extension %s differs from %s in id, resources or minutes", p.ID,
					p.ExtensionOf)
			}
			continue
		}
		starts = append(starts, p.ID)
		if more, ok := c.Get(p.ID + "-more"); !ok || more.ExtensionOf != p.ID {
			t.Errorf("package %s has no extension %s-more", p.ID, p.ID)
		}
	}
	slices.Sort(starts)
	want := slices.Sorted(slices.Values(devGPU))
	if !slices.Equal(starts, want) {
		t.Errorf("GPU start packages %v, want %v", starts, want)
	}
	// No EU node of either type and no HF flavor, so neither could ever be placed (Q4)
	for _, id := range []string{"gpu-rtxa6000-10m", "gpu-b200-10m"} {
		if _, ok := c.Get(id); ok {
			t.Errorf("package %s is listed while Q4 keeps it out", id)
		}
	}
}

// TestProductionGPUPackagesLandOnLium holds each production GPU package to a production Lium size
func TestProductionGPUPackagesLandOnLium(t *testing.T) {
	c, err := catalog.Load("../../config/packages.json")
	if err != nil {
		t.Fatal(err)
	}
	reg, _ := productionRegistry(t)
	var lium provider.Provider
	for _, p := range reg.Ordered() {
		if p.Name() == "lium" {
			lium = p
		}
	}
	if lium == nil {
		t.Fatal("production lists no lium provider")
	}
	sizer, ok := lium.(interface{ Sizes() []provider.Size })
	if !ok {
		t.Fatal("the lium provider lists no sizes")
	}
	gpu := 0
	for _, p := range c.All() {
		f := p.Resources
		if f.GPUCount == 0 {
			continue
		}
		gpu++
		// SizeFor's own rule without its live node list, which a test has none of
		fits := slices.ContainsFunc(sizer.Sizes(), func(s provider.Size) bool {
			r := s.Resources
			return r.GPUModel == f.GPUModel && r.VCPU >= f.VCPU && r.MemoryMiB >= f.MemoryMiB &&
				r.DiskGiB >= f.DiskGiB && r.GPUCount >= f.GPUCount &&
				r.GPUMemoryGiB >= f.GPUMemoryGiB
		})
		if !fits {
			t.Errorf("package %s fits no production lium size", p.ID)
		}
	}
	if gpu != 16 {
		t.Errorf("production lists %d GPU packages, want 16", gpu)
	}
}
