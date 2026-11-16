package hf

import (
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/gpu"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/pricing"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/spend"
)

// healthy lets every provider be tried
type healthy struct{}

func (healthy) Healthy(string) bool           { return true }
func (healthy) ReadyP95(string) time.Duration { return 0 }

// t4Start and t4More are design 5.4's gpu-t4-10m and its extension, quoted live
var (
	t4Start = catalog.Package{ID: "gpu-t4-10m", Minutes: 10, GPUModel: gpu.T4,
		Resources: provider.Resources{
			VCPU: 3, MemoryMiB: 12 << 10, DiskGiB: 40, GPUCount: 1, GPUMemoryGiB: 16,
			GPUModel: gpu.T4,
		}}
	t4More = func() catalog.Package {
		p := t4Start
		p.ID, p.ExtensionOf = "gpu-t4-10m-more", t4Start.ID
		return p
	}()
)

// TestHFPricesUnderTheRule is R5, HF's list quotes, extends and meters at the official cost + 10%
func TestHFPricesUnderTheRule(t *testing.T) {
	p, err := newProvider(testOptions())
	if err != nil {
		t.Fatal(err)
	}
	var _ provider.StartCoster = p
	if _, perNode := provider.As[provider.NodePricer](p); perNode {
		t.Fatal("hf prices its flavors from a list, never per node")
	}
	reg, err := provider.NewRegistry([]provider.Provider{p}, provider.Features{})
	if err != nil {
		t.Fatal(err)
	}
	e := placement.New(reg, healthy{}, nil)
	rule := pricing.Rule{MarkupBP: 1000}

	// Pricing plan 4.3, $0.41002 an hour for 10 minutes and 3 billed start minutes
	q, ok := e.Quote(placement.Inputs{Package: t4Start, Rule: rule})
	eq(t, []any{ok, q.Size.Name}, []any{true, "t4-small"})
	eq(t, []int64{q.HourlyMicros, q.StartFeeMicros, q.CostMicros, q.PriceMicros},
		[]int64{410_020, 20_501, 88_838, 97_722})
	// An extension bills its 10 minutes on the sandbox's flavor, 68,337 priced at 75,171
	for _, rented := range []int64{0, 1} {
		price, err := e.RentedExtensionPrice(t4More, "hf", "t4-small", rented, rule)
		eq(t, []any{price, err}, []any{int64(75_171), nil})
	}
	price, err := q.ExtensionPrice(t4More, rule)
	eq(t, []any{price, err}, []any{int64(75_171), nil})
	// The meter bills the same hour and start fee per whole minute, under the markup paid
	start := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	cost := spend.AttemptCost(start, start.Add(9*time.Minute+time.Second), p.BillingIncrement(),
		q.HourlyMicros, q.StartFeeMicros)
	eq(t, cost, int64(88_838))
	eq(t, spend.Judge(cost, q.PriceMicros, rule), spend.Verdict{})

	// An hourly read that moves the list moves the quote and the extension with it
	var entries []json.RawMessage
	if err := json.Unmarshal(compiledHardware, &entries); err != nil {
		t.Fatal(err)
	}
	for i, raw := range entries {
		var e map[string]any
		if err := json.Unmarshal(raw, &e); err != nil {
			t.Fatal(err)
		}
		if e["name"] == "t4-small" {
			e["unitCostMicroUSD"] = 20_000
			entries[i], _ = json.Marshal(e)
		}
	}
	if _, err := p.hw.update(entries, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatal(err)
	}
	// t4-small at $1.21 an hour is now dearer than t4-medium's $0.61, 132,167 priced at 145,384
	q, _ = e.Quote(placement.Inputs{Package: t4Start, Rule: rule})
	eq(t, q.Size.Name, "t4-medium")
	eq(t, []int64{q.HourlyMicros, q.CostMicros, q.PriceMicros}, []int64{610_000, 132_167, 145_384})
	// A sandbox on t4-small extends at its new hour, 201,667 priced at 221,834
	price, err = e.RentedExtensionPrice(t4More, "hf", "t4-small", 0, rule)
	eq(t, []any{price, err}, []any{int64(221_834), nil})
}
