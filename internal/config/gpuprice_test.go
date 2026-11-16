package config

import (
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/gpu"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// nodeSeller is a gpuSeller that prices nodes as Lium does, its live quote far under its caps
type nodeSeller struct {
	gpuSeller
	quote int64
}

func (n nodeSeller) HourlyCostMicros(provider.Size) int64 { return n.quote }

func (n nodeSeller) StartCostMicros(s provider.Size) (int64, bool) {
	return n.StartCostAt(s, n.quote)
}

func (n nodeSeller) CapHourlyMicros(s provider.Size) int64 { return n.hourly[s.Name] }

func (n nodeSeller) StartCostAt(s provider.Size, hourly int64) (int64, bool) {
	whole := hourly * int64(n.startBilled)
	return (whole + int64(time.Hour) - 1) / int64(time.Hour), !n.refused[s.Name]
}

func (nodeSeller) RentedHourlyMicros(provider.Ref) (int64, bool) { return 0, false }

// liumNodes is liumFake quoting a $1 node live, which check 8 never reads
var liumNodes = nodeSeller{gpuSeller: liumFake, quote: 1_000_000}

// liveGPU is a 10-minute live package of one GPU of model
func liveGPU(id, model string, gpuGiB int) catalog.Package {
	p := gpuPackage(id, model, gpuGiB, 0)
	p.FixedPrice = false
	return p
}

// TestCheckGPUPricesNeedACost is check 8, a gpu_order provider covering a package can price it
func TestCheckGPUPricesNeedACost(t *testing.T) {
	c := loadEmpty(t)
	h200 := liveGPU("gpu-h200-10m", gpu.H200, 141)
	if err := c.CheckGPUPrices([]catalog.Package{h200}, []provider.Provider{liumFake,
		hfFake}); err != nil {
		t.Fatalf("both price h200: %v", err)
	}
	free := hfFake
	free.hourly = maps.Clone(hfFake.hourly)
	free.hourly["h200"] = 0
	for name, pkg := range map[string]catalog.Package{
		"a live package":  h200,
		"a fixed package": gpuPackage("gpu-h200-10m", gpu.H200, 141, 9_000_000),
		"its extension":   withExtension(liveGPU("gpu-h200-10m-more", gpu.H200, 141)),
		"a fixed extension": withExtension(gpuPackage("gpu-h200-10m-more", gpu.H200, 141,
			9_000_000)),
	} {
		err := c.CheckGPUPrices([]catalog.Package{pkg}, []provider.Provider{liumFake, free})
		want := `package "` + pkg.ID + `" is covered by hf size h200, which has no hourly cost`
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: got %v, want an error saying %q", name, err, want)
		}
	}
	// A package no size of the provider covers needs no price from it
	if err := c.CheckGPUPrices([]catalog.Package{liveGPU("gpu-t4-10m", gpu.T4, 16)},
		[]provider.Provider{liumFake, free}); err != nil {
		t.Fatalf("hf's free h200 does not cover a T4 package: %v", err)
	}
	// A provider outside gpu_order is never asked
	hfOnly := c
	hfOnly.GPUOrder = []string{"lium"}
	if err := hfOnly.CheckGPUPrices([]catalog.Package{h200},
		[]provider.Provider{liumFake, free}); err != nil {
		t.Fatalf("hf outside gpu_order: %v", err)
	}
}

// withExtension makes p the extension of the package its id names without -more
func withExtension(p catalog.Package) catalog.Package {
	p.ExtensionOf = strings.TrimSuffix(p.ID, "-more")
	return p
}

// TestCheckGPUPricesReadTheCap holds a fixed price to a per-node provider's cap, never its quote
func TestCheckGPUPricesReadTheCap(t *testing.T) {
	c := loadEmpty(t)
	// At the $4.90 cap 17 minutes cost 1,388,334, priced at 1,527,168, whatever the live quote
	for price, want := range map[int64]string{
		1_527_168: "",
		1_527_167: `package "gpu-h200-10m" at 1527167 micros misses pricing.markup 0.1 on lium`,
	} {
		err := c.CheckGPUPrices([]catalog.Package{gpuPackage("gpu-h200-10m", gpu.H200, 141,
			price)}, []provider.Provider{liumNodes, hfFake})
		switch {
		case want == "" && err != nil:
			t.Errorf("at %d: %v", price, err)
		case want != "" && (err == nil || !strings.Contains(err.Error(), want)):
			t.Errorf("at %d: got %v, want an error saying %q", price, err, want)
		}
	}
	// A quote of nothing, as before the first node listing, still prices at the cap
	cold := liumNodes
	cold.quote = 0
	if err := c.CheckGPUPrices([]catalog.Package{liveGPU("gpu-h200-10m", gpu.H200, 141)},
		[]provider.Provider{cold, hfFake}); err != nil {
		t.Fatalf("lium before its listing: %v", err)
	}
}

// TestCheckGPUFixedExtension holds a fixed extension to its minutes on the dearest provider
func TestCheckGPUFixedExtension(t *testing.T) {
	c := loadEmpty(t)
	// 10 minutes cost 816,667 on lium's cap and 834,997 on hf, priced at 898,334 and 918,497
	for price, want := range map[int64]string{
		918_497: "",
		918_496: `extension "gpu-h200-10m-more" at 918496 micros misses pricing.markup 0.1 ` +
			`on hf, whose size h200 costs 834997 micros for its 10 minutes, price it at ` +
			`918497 micros or more`,
	} {
		ext := withExtension(gpuPackage("gpu-h200-10m-more", gpu.H200, 141, price))
		err := c.CheckGPUPrices([]catalog.Package{ext}, []provider.Provider{liumNodes, hfFake})
		switch {
		case want == "" && err != nil:
			t.Errorf("at %d: %v", price, err)
		case want != "" && (err == nil || !strings.Contains(err.Error(), want)):
			t.Errorf("at %d: got %v, want an error saying %q", price, err, want)
		}
	}
}
