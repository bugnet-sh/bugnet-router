package config

import (
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// CheckGPUMargin refuses a GPU package that placement would drop from a built gpu_order provider
func (c Config) CheckGPUMargin(pkgs []catalog.Package, provs []provider.Provider) error {
	for _, pkg := range pkgs {
		if pkg.Resources.GPUCount == 0 {
			continue
		}
		for _, name := range c.GPUOrder {
			i := slices.IndexFunc(provs, func(p provider.Provider) bool { return p.Name() == name })
			if i < 0 {
				continue
			}
			size, cost, ok := cheapestFor(provs[i], pkg)
			if !ok || clearsMargin(pkg.PriceMicros, cost, c.MarginFloor) {
				continue
			}
			return fmt.Errorf("package %q at %d micros misses margin_floor %g on %s, whose "+
				"size %s costs %d micros for its %d minutes and start, so placement would never "+
				"send it there%s", pkg.ID, pkg.PriceMicros, c.MarginFloor, name, size.Name, cost,
				pkg.Minutes, leastPrice(cost, c.MarginFloor))
		}
	}
	return nil
}

// cheapestFor is the size of p covering pkg's floor that costs least for pkg's minutes and start
func cheapestFor(p provider.Provider, pkg catalog.Package) (provider.Size, int64, bool) {
	var sizes []provider.Size
	// Sizes and Lium's cap, since Lium's SizeFor reads a node listing that start has not filled
	if s, ok := provider.As[provider.Sizer](p); ok {
		sizes = s.Sizes()
	} else if size, ok := p.SizeFor(pkg.Resources); ok {
		sizes = []provider.Size{size}
	}
	var best provider.Size
	cost, found := int64(0), false
	for _, size := range sizes {
		if !covers(size.Resources, pkg.Resources) {
			continue
		}
		got, ok := estimate(p, size, pkg.Minutes)
		if ok && (!found || got < cost) {
			best, cost, found = size, got, true
		}
	}
	return best, cost, found
}

// estimate is what minutes on size cost at p as placement bills them, false for a refused start
func estimate(p provider.Provider, size provider.Size, minutes int) (int64, bool) {
	used := time.Duration(minutes) * time.Minute
	cost := placement.CostMicros(p.HourlyCostMicros(size), used, p.BillingIncrement())
	if sc, ok := provider.As[provider.StartCoster](p); ok {
		fee, ok := sc.StartCostMicros(size)
		if !ok {
			return 0, false
		}
		cost += fee
	}
	return cost, true
}

// covers reports whether a size offers at least floor, of floor's GPU model when it names one
func covers(size, floor provider.Resources) bool {
	return size.VCPU >= floor.VCPU && size.MemoryMiB >= floor.MemoryMiB &&
		size.DiskGiB >= floor.DiskGiB && size.GPUCount >= floor.GPUCount &&
		size.GPUMemoryGiB >= floor.GPUMemoryGiB &&
		(floor.GPUModel == "" || size.GPUModel == floor.GPUModel)
}

// clearsMargin is placement's own test of a candidate's price against the floor
func clearsMargin(price, cost int64, floor float64) bool {
	if price == 0 {
		return cost == 0
	}
	return float64(price-cost)/float64(price) >= floor
}

// leastPrice names the lowest price that clears floor at cost, nothing when none can
func leastPrice(cost int64, floor float64) string {
	if floor >= 1 {
		return ""
	}
	least := int64(math.Ceil(float64(cost) / (1 - floor)))
	for !clearsMargin(least, cost, floor) {
		least++
	}
	return fmt.Sprintf(", price it at %d micros or more", least)
}
