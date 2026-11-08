package catalog

import (
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// liveSize is one size a live provider sells and what it costs us an hour, in micro-USD
type liveSize struct {
	provider   string
	res        provider.Resources
	hourMicros int64
}

func live(name string, vcpu, memGiB, diskGiB int, hourMicros int64) liveSize {
	return liveSize{name, provider.Resources{
		VCPU: vcpu, MemoryMiB: memGiB << 10, DiskGiB: diskGiB,
	}, hourMicros}
}

// liveSizes copies the live providers' sizes: boat.go machineTypes, boxd.go sizes (EUR, taken at
// 1.10 USD so the margin check stays safe) and the paid stack's E2B templates priced as e2b.go
// prices them. Koyeb is not live, so a GPU package has nowhere to run yet.
var liveSizes = []liveSize{
	live("boat", 2, 4, 51, 18_000),
	live("boat", 4, 8, 51, 36_000),
	live("boat", 8, 16, 111, 72_000),
	live("boat", 16, 32, 251, 200_000),
	live("boxd", 1, 4, 100, 110_000),
	live("boxd", 2, 8, 100, 198_000),
	live("boxd", 4, 16, 100, 396_000),
	live("boxd", 8, 32, 100, 770_000),
	live("e2b", 2, 2, 10, 2*50_400+2*16_200),
	live("e2b", 4, 8, 10, 4*50_400+8*16_200),
}

func fits(size, floor provider.Resources) bool {
	return size.VCPU >= floor.VCPU && size.MemoryMiB >= floor.MemoryMiB &&
		size.DiskGiB >= floor.DiskGiB && size.GPUCount >= floor.GPUCount &&
		size.GPUMemoryGiB >= floor.GPUMemoryGiB
}

// TestProductionCatalog holds config/packages.json to what the live providers can run: every
// package loads, fits at least one live size, and keeps the 50% margin floor on the cheapest one
func TestProductionCatalog(t *testing.T) {
	c, err := Load("../../config/packages.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(c.All()) == 0 {
		t.Fatal("the production catalog sells nothing")
	}
	for _, p := range c.All() {
		var cheapest *liveSize
		for i, s := range liveSizes {
			if fits(s.res, p.Resources) && (cheapest == nil || s.hourMicros < cheapest.hourMicros) {
				cheapest = &liveSizes[i]
			}
		}
		if cheapest == nil {
			t.Errorf("package %s fits no live provider size", p.ID)
			continue
		}
		cost := cheapest.hourMicros * int64(p.Minutes) / 60
		if p.PriceMicros < 2*cost {
			t.Errorf("package %s at %d micros is under the 50%% margin floor on %s (cost %d)",
				p.ID, p.PriceMicros, cheapest.provider, cost)
		}
	}
}
