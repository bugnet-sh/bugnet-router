package api_test

import (
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"

	"github.com/bugnet-sh/bugnet-router/internal/gpu"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
)

// l4Node is a one-GPU L4 size, which the live catalog's GPU package fits
var l4Node = provider.Size{Name: "l4", Resources: provider.Resources{
	VCPU: 4, MemoryMiB: 8192, DiskGiB: 20, GPUCount: 1, GPUMemoryGiB: 24, GPUModel: gpu.L4,
}}

// perNode is a GPU server that prices nodes as Lium does, its live quote a test may move
type perNode struct {
	*local.Provider
	quote *atomic.Int64
}

func (p perNode) Capabilities() provider.Capabilities {
	c := p.Provider.Capabilities()
	c.GPU = true
	return c
}

func (p perNode) HourlyCostMicros(provider.Size) int64 { return p.quote.Load() }

func (p perNode) StartCostMicros(s provider.Size) (int64, bool) {
	return p.StartCostAt(s, p.quote.Load())
}

func (perNode) CapHourlyMicros(provider.Size) int64 { return 1_000_000 }

// StartCostAt bills 7 minutes at hourly
func (perNode) StartCostAt(_ provider.Size, hourly int64) (int64, bool) {
	return (hourly*420 + 3599) / 3600, true
}

func (perNode) RentedHourlyMicros(ref provider.Ref) (int64, bool) {
	h, err := strconv.ParseInt(ref.Meta["hourly_micros"], 10, 64)
	return h, err == nil && h > 0
}

// TestQuotesHoldAPerNodeServerByItsHour stores each quote's hourly figure, which alone lets a
// per-node server's quote hold
func TestQuotesHoldAPerNodeServerByItsHour(t *testing.T) {
	boatHour, liumHour := &atomic.Int64{}, &atomic.Int64{}
	boatHour.Store(boatRate)
	liumHour.Store(280_000)
	boat := metered{Provider: local.New(local.Options{Name: "boat", Root: t.TempDir(),
		Sizes: []provider.Size{small}, Increment: time.Second}), hourly: boatHour}
	lium := perNode{Provider: local.New(local.Options{Name: "lium", Root: t.TempDir(),
		Sizes: []provider.Size{l4Node}, Increment: time.Second}), quote: liumHour}
	reg, err := provider.NewRegistry([]provider.Provider{boat, lium}, provider.Features{})
	if err != nil {
		t.Fatal(err)
	}
	engine := placement.New(reg, placement.NewTracker(time.Now), nil)
	e := newEnv(t, options{
		apiCatalog: liveCatalog, apiEngine: engine, apiRule: &tenPercent,
		quoteHold: 45 * time.Minute, quoteRise: 30_000, estimator: true,
	})
	q, err := e.quote(t, &routerv1.EstimateRequest{})
	eq(t, err, nil)
	id := q.GetEstimateId()
	hourly := func(pkg string) int64 {
		var h int64
		if err := e.pool.QueryRow(t.Context(), `select hourly_micros from price_quotes
			where estimate_id = $1 and package_id = $2`, id, pkg).Scan(&h); err != nil {
			t.Fatal(err)
		}
		return h
	}
	eq(t, []int64{hourly("gpu-10m"), hourly(smallPkg)}, []int64{280_000, boatRate})

	// At $0.28 10 minutes and a 7-minute start cost 79,334, priced at 87,268, and at $0.30 93,500
	liumHour.Store(300_000)
	eq(t, offer(e.lockedPrice(t, "gpu-10m", id)), []any{int64(87_268), true, ""})
	eq(t, offer(e.price(t, "gpu-10m", nil)), []any{int64(93_500), true, ""})
	// A row from before the figure was kept holds nothing on a per-node server
	if _, err := e.pool.Exec(t.Context(), `update price_quotes set hourly_micros = 0
		where estimate_id = $1`, id); err != nil {
		t.Fatal(err)
	}
	eq(t, offer(e.lockedPrice(t, "gpu-10m", id)), []any{int64(93_500), true, ""})
	// and holds a server that prices no node as it did
	boatHour.Store(2 * boatRate)
	eq(t, offer(e.lockedPrice(t, smallPkg, id)), []any{int64(3355), true, ""})
}
