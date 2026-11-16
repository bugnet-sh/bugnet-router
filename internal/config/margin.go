package config

import (
	"fmt"
	"slices"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/pricing"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// CheckGPUMargin refuses a fixed-price GPU package placement would drop from a gpu_order provider
func (c Config) CheckGPUMargin(pkgs []catalog.Package, provs []provider.Provider) error {
	r := c.Pricing.Rule
	for _, pkg := range pkgs {
		// A live package is priced at its server's marked up cost, so no cost drops a candidate
		// Placement never starts an extension, so no start fee or candidate drop applies to one
		if pkg.Resources.GPUCount == 0 || pkg.ExtensionOf != "" || !pkg.FixedPrice {
			continue
		}
		for _, name := range c.GPUOrder {
			i := slices.IndexFunc(provs, func(p provider.Provider) bool { return p.Name() == name })
			if i < 0 {
				continue
			}
			size, cost, ok := cheapestFor(provs[i], pkg, r)
			if !ok || r.Fits(cost, pkg.PriceMicros) {
				continue
			}
			return fmt.Errorf("package %q at %d micros misses pricing.markup %g on %s, whose "+
				"size %s costs %d micros for its %d minutes and start, so placement would never "+
				"send it there, price it at %d micros or more", pkg.ID, pkg.PriceMicros,
				float64(r.MarkupBP)/10_000, name, size.Name, cost, pkg.Minutes, r.Price(cost))
		}
	}
	return nil
}

// cheapestFor is the size of p covering pkg's floor that costs least for pkg's minutes and start
func cheapestFor(
	p provider.Provider, pkg catalog.Package, r pricing.Rule,
) (provider.Size, int64, bool) {
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
		got, ok := estimate(p, size, pkg.Minutes, r)
		if ok && (!found || got < cost) {
			best, cost, found = size, got, true
		}
	}
	return best, cost, found
}

// estimate is what minutes on size cost at p as placement bills them, false for a refused start
func estimate(p provider.Provider, size provider.Size, minutes int, r pricing.Rule) (int64, bool) {
	used := r.Billed(p.Name(), time.Duration(minutes)*time.Minute)
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
