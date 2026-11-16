package lium

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

var rtx3090 = provider.Size{Name: "lium-rtx3090"}

// startAt is 7 billed minutes at hourly, rounded up to a micro
func startAt(hourly int64) int64 { return ceilDiv(hourly*420, 3600) }

func TestLiumQuotesTheSecondCheapestNode(t *testing.T) {
	var _ provider.NodePricer = (*Provider)(nil)
	// liveFake lists nodes at $0.25, $0.28 and $0.30, so one rent still leaves $0.28
	f, p := liveFake(t)
	eq(t, p.HourlyCostMicros(rtx3090), int64(280_000))
	fee, ok := p.StartCostMicros(rtx3090)
	eq(t, []any{fee, ok}, []any{startAt(280_000), true})
	eq(t, fee, int64(32_667))

	// A node outside the first countries never sets the quote while one inside serves
	us := euNode("node-us")
	us.country, us.price = "US", 0.20
	a, b := euNode("node-a"), euNode("node-b")
	a.price, b.price = 0.25, 0.28
	f.setNodes(us, a, b)
	relist(t, p)
	eq(t, p.HourlyCostMicros(rtx3090), int64(280_000))

	// One node quotes its own price
	f.setNodes(a)
	relist(t, p)
	eq(t, p.HourlyCostMicros(rtx3090), int64(250_000))
	fee, ok = p.StartCostMicros(rtx3090)
	eq(t, []any{fee, ok}, []any{startAt(250_000), true})

	// Ties keep the listing's order by id, and the second of two equal prices is that price
	b.price = 0.25
	f.setNodes(b, a)
	relist(t, p)
	eq(t, p.HourlyCostMicros(rtx3090), int64(250_000))

	// A size not sold costs nothing and starts nothing
	eq(t, p.HourlyCostMicros(provider.Size{Name: "lium-b200"}), int64(0))
}

// TestLiumStaleListingQuotesNothingUnderTheOldPrice is review focus 3
func TestLiumStaleListingQuotesNothingUnderTheOldPrice(t *testing.T) {
	f := newFake(t)
	clk := newClock()
	a, b := euNode("node-a"), euNode("node-b")
	a.price, b.price = 0.25, 0.28
	p := listed(t, f, clk, a, b)
	floor := provider.Resources{GPUCount: 1, GPUModel: "NVIDIA RTX 3090"}
	if _, ok := p.SizeFor(floor); !ok {
		t.Fatal("a fresh listing with two nodes offers no size")
	}
	eq(t, p.HourlyCostMicros(rtx3090), int64(280_000))

	// Past 2 minutes the size is not offered and the cost is the cap, never the old quote
	clk.add(2*time.Minute + time.Second)
	if _, ok := p.SizeFor(floor); ok {
		t.Fatal("a stale listing offered the size")
	}
	eq(t, p.HourlyCostMicros(rtx3090), int64(350_000))
	fee, _ := p.StartCostMicros(rtx3090)
	eq(t, fee, startAt(350_000))
	_, err := p.Create(context.Background(), createReq(testKey))
	wantCode(t, err, provider.CodeCapacity, true)

	// An empty listing offers nothing either, at the cap
	f.setNodes()
	relist(t, p)
	if _, ok := p.SizeFor(floor); ok {
		t.Fatal("an empty listing offered the size")
	}
	eq(t, p.HourlyCostMicros(rtx3090), int64(350_000))

	// No listing read yet is the same
	cold := f.provider(t, testOptions(), clk.now)
	eq(t, cold.HourlyCostMicros(rtx3090), int64(350_000))
	eq(t, len(f.rents()), 0)
}

func TestLiumNodePricer(t *testing.T) {
	p, err := newProvider(testOptions(), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, p.CapHourlyMicros(rtx3090), int64(350_000))
	eq(t, p.CapHourlyMicros(provider.Size{Name: "lium-h200"}), int64(4_900_000))
	eq(t, p.CapHourlyMicros(provider.Size{Name: "lium-b200"}), int64(0))

	fee, ok := p.StartCostAt(rtx3090, 280_000)
	eq(t, []any{fee, ok}, []any{int64(32_667), true})
	fee, ok = p.StartCostAt(provider.Size{Name: "lium-b200"}, 280_000)
	eq(t, []any{fee, ok}, []any{int64(0), false})

	for raw, want := range map[string]int64{"250000": 250_000, "0": 0, "-1": 0, "x": 0} {
		got, ok := p.RentedHourlyMicros(provider.Ref{Meta: map[string]string{
			"hourly_micros": raw,
		}})
		eq(t, []any{raw, got, ok}, []any{raw, want, want > 0})
	}
	got, ok := p.RentedHourlyMicros(provider.Ref{})
	eq(t, []any{got, ok}, []any{int64(0), false})
}

func TestLiumCreateRentsUnderTheOrderCap(t *testing.T) {
	capped := func(key string, hourly int64) provider.CreateRequest {
		req := createReq(key)
		req.MaxHourlyMicros = hourly
		return req
	}
	t.Run("the cheapest node under the cap", func(t *testing.T) {
		f, p := liveFake(t)
		ref := mustCreate(t, p, capped(testKey, 280_000))
		eq(t, []string{ref.Meta["node"], ref.Meta["hourly_micros"]},
			[]string{"node-a", "250000"})
		eq(t, ref.Meta["max_hourly_micros"], "280000")
		eq(t, len(f.rents()), 1)
	})
	// Review focus 2, the node the quote counted on goes first and the next still fits
	t.Run("the cheapest node taken since the quote", func(t *testing.T) {
		f, p := liveFake(t)
		mustCreate(t, p, createReq(freshKey()))
		relist(t, p)
		ref := mustCreate(t, p, capped(testKey, 280_000))
		eq(t, []string{ref.Meta["node"], ref.Meta["hourly_micros"]},
			[]string{"node-b", "280000"})
		eq(t, len(f.rents()), 2)
	})
	t.Run("a refused node leaves only nodes over the cap", func(t *testing.T) {
		f, p := liveFake(t)
		f.rentFault("node-a", fault{status: 400, code: "node_unavailable"})
		_, err := p.Create(context.Background(), capped(testKey, 260_000))
		wantCode(t, err, provider.CodeCapacity, true)
		for _, r := range f.rents() {
			if n := rentNode(r); n != "node-a" {
				t.Fatalf("rented %s, over the order's cap", n)
			}
		}
	})
	// Review focus 1, every node rose over what the order was priced at
	t.Run("every node over the cap", func(t *testing.T) {
		f, p := liveFake(t)
		_, err := p.Create(context.Background(), capped(testKey, 240_000))
		wantCode(t, err, provider.CodeCapacity, true)
		if err == nil || !strings.Contains(err.Error(), "at or under 240000 micros an hour") {
			t.Fatalf("err %v, want one naming the order's cap", err)
		}
		eq(t, len(f.rents()), 0)
		eq(t, f.podCount(), 0)
	})
	t.Run("a cap over the size's keeps the size's", func(t *testing.T) {
		f, p := liveFake(t)
		c := euNode("node-c")
		c.price = 0.36
		a := euNode("node-a")
		a.price = 0.355
		f.setNodes(c, a)
		relist(t, p)
		_, err := p.Create(context.Background(), capped(testKey, 400_000))
		wantCode(t, err, provider.CodeCapacity, true)
		eq(t, len(f.rents()), 0)
	})
	t.Run("no cap rents under the size's", func(t *testing.T) {
		_, p := liveFake(t)
		ref := mustCreate(t, p, createReq(testKey))
		eq(t, ref.Meta["node"], "node-a")
		_, has := ref.Meta["max_hourly_micros"]
		eq(t, has, false)
	})
}

func TestLiumMoveOverTheOrderCapRefused(t *testing.T) {
	for name, c := range map[string]struct {
		price float64
		pass  bool
	}{"at the cap": {0.28, true}, "over the cap": {0.29, false}} {
		t.Run(name, func(t *testing.T) {
			f, p := liveFake(t)
			req := createReq(freshKey())
			req.MaxHourlyMicros = 280_000
			ref := mustCreate(t, p, req)
			f.setPod(ref.ID, func(fp *fakePod) {
				fp.status = "RUNNING"
				fp.node, fp.machine, fp.tier, fp.country = "node-moved",
					"NVIDIA GeForce RTX 3090", "secure", "FR"
				fp.nodeGPUs, fp.gpus, fp.price = 1, 1, c.price
			})
			meta, err := p.ReadyMeta(context.Background(), ref)
			if c.pass {
				if err != nil {
					t.Fatal(err)
				}
				eq(t, meta["hourly_micros"], "280000")
				return
			}
			wantCode(t, err, provider.CodeCapacity, true)
			if !strings.Contains(err.Error(), "costs more than the order's cap") {
				t.Fatalf("err %v, want one naming the order's cap", err)
			}
		})
	}
}
