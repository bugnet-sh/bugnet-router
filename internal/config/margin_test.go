package config

import (
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/gpu"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// gpuSeller is a GPU provider with fixed sizes and prices whose SizeFor finds no free node
type gpuSeller struct {
	provider.Provider // nil, so a call check 8 should not make panics
	name              string
	sizes             []provider.Size
	hourly            map[string]int64
	increment         time.Duration
	// startBilled is the time each start bills at the size's hourly cost
	startBilled time.Duration
	// refused names the sizes whose next start would be refused
	refused map[string]bool
}

func (g gpuSeller) Name() string                           { return g.name }
func (g gpuSeller) Sizes() []provider.Size                 { return g.sizes }
func (g gpuSeller) BillingIncrement() time.Duration        { return g.increment }
func (g gpuSeller) HourlyCostMicros(s provider.Size) int64 { return g.hourly[s.Name] }

// SizeFor finds nothing, as Lium's does before its first node listing
func (g gpuSeller) SizeFor(provider.Resources) (provider.Size, bool) {
	return provider.Size{}, false
}

func (g gpuSeller) StartCostMicros(s provider.Size) (int64, bool) {
	whole := g.hourly[s.Name] * int64(g.startBilled)
	return (whole + int64(time.Hour) - 1) / int64(time.Hour), !g.refused[s.Name]
}

// unsized is a GPU provider without Sizes, whose SizeFor answers its one size for any floor
type unsized struct {
	provider.Provider
	name   string
	size   provider.Size
	hourly int64
}

func (u unsized) Name() string                                     { return u.name }
func (u unsized) BillingIncrement() time.Duration                  { return time.Second }
func (u unsized) HourlyCostMicros(provider.Size) int64             { return u.hourly }
func (u unsized) SizeFor(provider.Resources) (provider.Size, bool) { return u.size, true }

// gpuSize is a size of count GPUs of model with the resources given
func gpuSize(name, model string, count, gpuGiB, vcpu, memMiB, diskGiB int) provider.Size {
	return provider.Size{Name: name, Resources: provider.Resources{
		VCPU: vcpu, MemoryMiB: memMiB, DiskGiB: diskGiB, GPUCount: count,
		GPUMemoryGiB: gpuGiB, GPUModel: model,
	}}
}

// liumFake holds design 3.5's H200 and T4 sizes at their caps, 7 billed start minutes, 1 s billing
var liumFake = gpuSeller{
	name: "lium", increment: time.Second, startBilled: 7 * time.Minute,
	sizes: []provider.Size{
		gpuSize("lium-t4", gpu.T4, 1, 16, 3, 14<<10, 200),
		gpuSize("lium-h200", gpu.H200, 1, 141, 16, 240<<10, 900),
	},
	hourly: map[string]int64{"lium-t4": 300_000, "lium-h200": 4_900_000},
}

// hfFake holds design 4.5's flavors for the packages below, 3 billed start minutes, by the minute
var hfFake = gpuSeller{
	name: "hf", increment: time.Minute, startBilled: 3 * time.Minute,
	sizes: []provider.Size{
		gpuSize("t4-small", gpu.T4, 1, 16, 4, 14_305, 46),
		gpuSize("t4-medium", gpu.T4, 1, 16, 8, 28_610, 93),
		gpuSize("l4x1", gpu.L4, 1, 24, 8, 28_610, 372),
		gpuSize("h200", gpu.H200, 1, 141, 23, 244_140, 2793),
	},
	hourly: map[string]int64{
		"t4-small": 410_020, "t4-medium": 610_000, "l4x1": 809_980, "h200": 5_009_980,
	},
}

// gpuPackage is a 10-minute package of one GPU of model with design 5.4's floor at a fixed price
func gpuPackage(id, model string, gpuGiB int, price int64) catalog.Package {
	return catalog.Package{ID: id, Minutes: 10, PriceMicros: price, FixedPrice: true,
		GPUModel: model, Resources: provider.Resources{
			VCPU: 4, MemoryMiB: 12 << 10, DiskGiB: 40, GPUCount: 1, GPUMemoryGiB: gpuGiB,
			GPUModel: model,
		}}
}

// t4Package is design 5.4's gpu-t4-10m floor, 3 vCPU so lium-t4 covers it too
func t4Package(id string, price int64) catalog.Package {
	p := gpuPackage(id, gpu.T4, 16, price)
	p.Resources.VCPU = 3
	return p
}

// TestConfigGPUMargin is check 8 of 5.3, each fixed GPU price over the markup on gpu_order sellers
func TestConfigGPUMargin(t *testing.T) {
	both := []provider.Provider{liumFake, hfFake}
	c := loadEmpty(t)
	// Lium's estimate for gpu-h200-10m is 816,667 for 10 minutes and 571,667 for 7 at the cap
	h200 := gpuPackage("gpu-h200-10m", gpu.H200, 141, 2_780_000)
	cpu := catalog.Package{ID: "cpu-10m", Minutes: 10, PriceMicros: 1, FixedPrice: true,
		Resources: provider.Resources{VCPU: 2, MemoryMiB: 2048, DiskGiB: 10}}
	// Placement never starts an extension, so its price is never held to the start fee
	more := gpuPackage("gpu-h200-10m-more", gpu.H200, 141, 1)
	more.ExtensionOf = h200.ID
	// A live package is priced at the marked up cost of the server it lands on
	live := gpuPackage("gpu-h200-10m", gpu.H200, 141, 0)
	live.FixedPrice = false
	for name, pkgs := range map[string][]catalog.Package{
		"5.4's lowest price":       {h200},
		"Q15's pick":               {gpuPackage("gpu-h200-10m", gpu.H200, 141, 3_060_000)},
		"hf alone serves l4":       {gpuPackage("gpu-l4-10m", gpu.L4, 24, 360_000)},
		"the cheapest flavor":      {t4Package("gpu-t4-10m", 180_000)},
		"no package without a GPU": {cpu, h200},
		"no extension":             {h200, more},
		"a live package":           {live},
	} {
		if err := c.CheckGPUMargin(pkgs, both); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}

	// 1.5 USDC clears hf's 1,194,046 but not lium's 1,527,168, so lium never serves it
	old := gpuPackage("gpu-h200-10m", gpu.H200, 141, 1_500_000)
	err := c.CheckGPUMargin([]catalog.Package{old}, both)
	for _, want := range []string{
		`package "gpu-h200-10m" at 1500000 micros misses pricing.markup 0.1 on lium`,
		"lium-h200 costs 1388334 micros", "price it at 1527168 micros or more",
	} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("Q15's old price gave %v, want an error saying %q", err, want)
		}
	}
	// The same price clears hf alone, and a provider outside gpu_order is never checked
	if err := c.CheckGPUMargin([]catalog.Package{old}, []provider.Provider{hfFake}); err != nil {
		t.Fatalf("hf alone: %v", err)
	}
	outside := c
	outside.GPUOrder = []string{"hf"}
	if err := outside.CheckGPUMargin([]catalog.Package{old}, both); err != nil {
		t.Fatalf("lium outside gpu_order: %v", err)
	}
	// A lower markup lets it through
	low := c
	low.Pricing.Rule.MarkupBP = 500
	if err := low.CheckGPUMargin([]catalog.Package{old}, both); err != nil {
		t.Fatalf("a 5%% markup: %v", err)
	}
	// A provider's billed overhead counts as placement counts it
	slow := c
	slow.Pricing.Rule.Overhead = map[string]time.Duration{"hf": 10 * time.Minute}
	if err := slow.CheckGPUMargin([]catalog.Package{old}, []provider.Provider{hfFake}); err == nil {
		t.Fatal("hf's 10 minute overhead let 1.5 USDC through")
	}

	for name, tc := range map[string]struct {
		pkg  catalog.Package
		prov []provider.Provider
		want string
	}{
		"hf under its markup": {gpuPackage("gpu-l4-10m", gpu.L4, 24, 190_000), both,
			`package "gpu-l4-10m" at 190000 micros misses pricing.markup 0.1 on hf, ` +
				`whose size l4x1 costs 175496 micros`},
		"a free package": {t4Package("gpu-t4-free", 0), both,
			`package "gpu-t4-free" at 0 micros misses pricing.markup 0.1 on lium`},
		"a size by SizeFor": {t4Package("gpu-t4-10m", 180_000),
			[]provider.Provider{unsized{name: "hf", hourly: 2_000_000,
				size: gpuSize("big", gpu.T4, 1, 16, 8, 32<<10, 100)}},
			`misses pricing.markup 0.1 on hf, whose size big costs 333334 micros`},
	} {
		err := c.CheckGPUMargin([]catalog.Package{tc.pkg}, tc.prov)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got %v, want an error saying %q", name, err, tc.want)
		}
	}

	// A size whose start would be refused is not one placement can use
	refusing := liumFake
	refusing.refused = map[string]bool{"lium-h200": true}
	err = c.CheckGPUMargin([]catalog.Package{old}, []provider.Provider{refusing, hfFake})
	if err != nil {
		t.Fatalf("a refused start: %v", err)
	}
}
