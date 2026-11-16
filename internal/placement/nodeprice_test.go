package placement

import (
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/gpu"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// t4Size is a one-GPU T4 size, which the T4 package below fits
var t4Size = provider.Size{Name: "t4", Resources: provider.Resources{
	VCPU: 4, MemoryMiB: 16384, DiskGiB: 40, GPUCount: 1, GPUMemoryGiB: 16, GPUModel: gpu.T4,
}}

// nodePriced is a GPU provider that prices nodes as Lium does, a live quote under a cap and a
// start of 7 billed minutes at the hour it is priced at
type nodePriced struct {
	*capsProvider
	quote *atomic.Int64
	limit int64
}

func newNodePriced(t *testing.T, name string, quote, limit int64) nodePriced {
	t.Helper()
	n := nodePriced{capsProvider: &capsProvider{
		Provider: newLocal(t, name, 0, time.Second, t4Size),
		caps:     provider.Capabilities{GPU: true, MaxLifetime: time.Hour},
	}, quote: &atomic.Int64{}, limit: limit}
	n.quote.Store(quote)
	return n
}

func (n nodePriced) HourlyCostMicros(provider.Size) int64 { return n.quote.Load() }

func (n nodePriced) StartCostMicros(s provider.Size) (int64, bool) {
	return n.StartCostAt(s, n.quote.Load())
}

func (n nodePriced) CapHourlyMicros(provider.Size) int64 { return n.limit }

func (n nodePriced) StartCostAt(_ provider.Size, hourly int64) (int64, bool) {
	return (hourly*420 + 3599) / 3600, true
}

func (n nodePriced) RentedHourlyMicros(ref provider.Ref) (int64, bool) {
	h, err := strconv.ParseInt(ref.Meta["hourly_micros"], 10, 64)
	return h, err == nil && h > 0
}

// t4Live is a live 10-minute T4 package
func t4Live() catalog.Package {
	p := live(10)
	p.Resources = provider.Resources{
		VCPU: 4, MemoryMiB: 12288, DiskGiB: 40, GPUCount: 1, GPUMemoryGiB: 16, GPUModel: gpu.T4,
	}
	p.GPUModel = gpu.T4
	return p
}

// gpuRule marks cost up 10% with no overhead, as for lium and hf
var gpuRule = boatRule

func TestPerNodeCandidateAtTheLiveQuote(t *testing.T) {
	lium := newNodePriced(t, "lium", 280_000, 350_000)
	e := engine(fakeHealth{}, lium)
	got, err := e.Candidates(Inputs{Package: t4Live(), Rule: gpuRule})
	eq(t, err, nil)
	// 10 minutes at $0.28 cost 46,667 and the 7-minute start 32,667, 79,334 priced at 87,268
	c := got[0]
	eq(t, []int64{c.HourlyMicros, c.StartFeeMicros, c.CostMicros, c.PriceMicros},
		[]int64{280_000, 32_667, 79_334, 87_268})
	eq(t, e.PricesPerNode("lium"), true)
	eq(t, e.PricesPerNode("boat"), false)

	// A provider that prices per node and quotes nothing is not one placement can price on
	lium.quote.Store(0)
	_, err = e.Candidates(Inputs{Package: t4Live(), Rule: gpuRule})
	eq(t, errors.Is(err, ErrNoCandidate), true)
	eq(t, len(e.Lands(Inputs{Package: t4Live(), Rule: gpuRule})), 0)
}

func TestHeldPerNodeServerPricedAtItsQuote(t *testing.T) {
	// The quote was $0.25, 41,667 and 29,167, priced at 77,918, and nodes rose to $0.30
	lium := newNodePriced(t, "lium", 300_000, 350_000)
	e := engine(fakeHealth{}, lium).WithGPUOrder([]string{"lium"})
	held := &Server{Provider: "lium", Size: "t4"}
	in := Inputs{
		Package: t4Live(), Rule: gpuRule, BudgetMicros: 77_918, Prefer: held,
		PreferHourlyMicros: 250_000, PreferRiseMicros: 30_000,
	}
	got, err := e.Candidates(in)
	eq(t, err, nil)
	c := got[0]
	eq(t, []int64{c.HourlyMicros, c.StartFeeMicros, c.CostMicros, c.PriceMicros},
		[]int64{250_000, 29_167, 70_834, 77_918})
	// A quote that kept no hourly figure holds nothing on a per-node server
	in.PreferHourlyMicros = 0
	_, err = e.Candidates(in)
	eq(t, errors.Is(err, ErrNoCandidate), true)
	// Without the hold the live quote prices it and the payment is short
	in.Prefer = nil
	_, err = e.Candidates(in)
	eq(t, errors.Is(err, ErrNoCandidate), true)
	// A held figure on another server or size prices nothing
	in.Prefer, in.PreferHourlyMicros = &Server{Provider: "lium", Size: "a100"}, 250_000
	_, err = e.Candidates(in)
	eq(t, errors.Is(err, ErrNoCandidate), true)
}

func TestHeldFigureLeavesOtherProvidersLive(t *testing.T) {
	// A server that prices no node keeps its own cost whatever the hold names
	hf := capsProvider{
		Provider: newLocal(t, "hf", 600_000, time.Minute, t4Size),
		caps:     provider.Capabilities{GPU: true, MaxLifetime: time.Hour},
	}
	e := engine(fakeHealth{}, hf)
	got, err := e.Candidates(Inputs{
		Package: t4Live(), Rule: gpuRule, Prefer: &Server{Provider: "hf", Size: "t4"},
		PreferHourlyMicros: 1,
	})
	eq(t, err, nil)
	eq(t, []int64{got[0].HourlyMicros, got[0].CostMicros}, []int64{600_000, 100_000})
}

// TestRentedExtensionPrice is review focus 4 on the engine, the rented node prices an extension
func TestRentedExtensionPrice(t *testing.T) {
	lium := newNodePriced(t, "lium", 250_000, 350_000)
	hf := capsProvider{
		Provider: newLocal(t, "hf", 600_000, time.Minute, t4Size),
		caps:     provider.Capabilities{GPU: true, MaxLifetime: time.Hour},
	}
	e := engine(fakeHealth{}, lium, hf)
	for _, c := range []struct {
		name   string
		server string
		rented int64
		want   int64
	}{
		// 10 minutes at the rented $0.30 cost 50,000, over today's $0.25 quote
		{"the rented node", "lium", 300_000, 55_000},
		// Nothing recorded falls back to the $0.35 cap, 58,334 priced at 64,168
		{"no node recorded", "lium", 0, 64_168},
		// A provider that prices no node prices the extension at its own hour
		{"another provider", "hf", 300_000, 110_000},
	} {
		price, err := e.RentedExtensionPrice(t4Live(), c.server, "t4", c.rented, gpuRule)
		eq(t, []any{c.name, price, err}, []any{c.name, c.want, nil})
	}
	price, err := e.ExtensionPrice(t4Live(), "lium", "t4", gpuRule)
	eq(t, []any{price, err}, []any{int64(64_168), nil})
	fixed := pkg(10, 45_000)
	price, err = e.RentedExtensionPrice(fixed, "lium", "t4", 300_000, gpuRule)
	eq(t, []any{price, err}, []any{int64(45_000), nil})
	_, err = e.RentedExtensionPrice(t4Live(), "lium", "a100", 300_000, gpuRule)
	eq(t, errors.Is(err, ErrNoServer), true)
}
