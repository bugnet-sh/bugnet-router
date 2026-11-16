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

// CheckGPUMargin refuses a GPU package a covering gpu_order provider cannot price, or whose
// fixed price misses the markup on the dearest of them
func (c Config) CheckGPUMargin(pkgs []catalog.Package, provs []provider.Provider) error {
	r := c.Pricing.Rule
	for _, pkg := range pkgs {
		if pkg.Resources.GPUCount == 0 {
			continue
		}
		for _, name := range c.GPUOrder {
			i := slices.IndexFunc(provs, func(p provider.Provider) bool { return p.Name() == name })
			if i < 0 {
				continue
			}
			if err := checkGPUPrice(provs[i], pkg, r); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkGPUPrice refuses pkg where p covers it at no hourly cost or under its fixed price
func checkGPUPrice(p provider.Provider, pkg catalog.Package, r pricing.Rule) error {
	sizes := covering(p, pkg)
	for _, size := range sizes {
		if hourlyCost(p, size) <= 0 {
			return fmt.Errorf("package %q is covered by %s size %s, which has no hourly cost, "+
				"so %s cannot price it", pkg.ID, p.Name(), size.Name, p.Name())
		}
	}
	if !pkg.FixedPrice {
		return nil
	}
	size, cost, ok := cheapest(p, sizes, pkg, r)
	if !ok || r.Fits(cost, pkg.PriceMicros) {
		return nil
	}
	if pkg.ExtensionOf != "" {
		return fmt.Errorf("extension %q at %d micros misses pricing.markup %g on %s, whose "+
			"size %s costs %d micros for its %d minutes, price it at %d micros or more", pkg.ID,
			pkg.PriceMicros, float64(r.MarkupBP)/10_000, p.Name(), size.Name, cost, pkg.Minutes,
			r.Price(cost))
	}
	return fmt.Errorf("package %q at %d micros misses pricing.markup %g on %s, whose "+
		"size %s costs %d micros for its %d minutes and start, so placement would never "+
		"send it there, price it at %d micros or more", pkg.ID, pkg.PriceMicros,
		float64(r.MarkupBP)/10_000, p.Name(), size.Name, cost, pkg.Minutes, r.Price(cost))
}

// covering are the sizes of p that cover pkg's floor
func covering(p provider.Provider, pkg catalog.Package) []provider.Size {
	var sizes []provider.Size
	// Sizes and Lium's cap, since Lium's SizeFor reads a node listing that start has not filled
	if s, ok := provider.As[provider.Sizer](p); ok {
		sizes = s.Sizes()
	} else if size, ok := p.SizeFor(pkg.Resources); ok {
		sizes = []provider.Size{size}
	}
	return slices.DeleteFunc(slices.Clone(sizes), func(s provider.Size) bool {
		return !covers(s.Resources, pkg.Resources)
	})
}

// hourlyCost is the most an hour of size costs at p, a per-node provider's cap since a node
// listing may not be read yet and no rent goes above the cap
func hourlyCost(p provider.Provider, size provider.Size) int64 {
	if np, ok := provider.As[provider.NodePricer](p); ok {
		return np.CapHourlyMicros(size)
	}
	return p.HourlyCostMicros(size)
}

// cheapest is the size among sizes that costs least for pkg, a start with its start fee
func cheapest(
	p provider.Provider, sizes []provider.Size, pkg catalog.Package, r pricing.Rule,
) (provider.Size, int64, bool) {
	var best provider.Size
	cost, found := int64(0), false
	for _, size := range sizes {
		got, ok := estimate(p, size, pkg, r)
		if ok && (!found || got < cost) {
			best, cost, found = size, got, true
		}
	}
	return best, cost, found
}

// estimate is what pkg on size costs at p as placement bills it, a start with its overhead and
// start fee and an extension its minutes alone, false for a refused start
func estimate(
	p provider.Provider, size provider.Size, pkg catalog.Package, r pricing.Rule,
) (int64, bool) {
	run := time.Duration(pkg.Minutes) * time.Minute
	hourly := hourlyCost(p, size)
	if pkg.ExtensionOf != "" {
		return placement.CostMicros(hourly, run, p.BillingIncrement()), true
	}
	cost := placement.CostMicros(hourly, r.Billed(p.Name(), run), p.BillingIncrement())
	var fee int64
	ok := true
	if np, isNode := provider.As[provider.NodePricer](p); isNode {
		fee, ok = np.StartCostAt(size, hourly)
	} else if sc, isFee := provider.As[provider.StartCoster](p); isFee {
		fee, ok = sc.StartCostMicros(size)
	}
	if !ok {
		return 0, false
	}
	return cost + fee, true
}

// covers reports whether a size offers at least floor, of floor's GPU model when it names one
func covers(size, floor provider.Resources) bool {
	return size.VCPU >= floor.VCPU && size.MemoryMiB >= floor.MemoryMiB &&
		size.DiskGiB >= floor.DiskGiB && size.GPUCount >= floor.GPUCount &&
		size.GPUMemoryGiB >= floor.GPUMemoryGiB &&
		(floor.GPUModel == "" || size.GPUModel == floor.GPUModel)
}
