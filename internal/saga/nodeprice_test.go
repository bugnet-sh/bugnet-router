package saga_test

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/ordertoken"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

const (
	gpuPkg  = "gpu-t4-10m"
	gpuMore = "gpu-t4-10m-more"
)

// gpuLiveJSON is a live T4 package and its extension, which both GPU providers serve
const gpuLiveJSON = `{"packages": [
	{"id": "gpu-t4-10m", "resources": {"vcpu": 4, "memory_mib": 12288, "disk_gib": 40,
		"gpu_count": 1, "gpu_memory_gib": 16, "gpu_model": "NVIDIA T4"}, "minutes": 10},
	{"id": "gpu-t4-10m-more", "resources": {"vcpu": 4, "memory_mib": 12288, "disk_gib": 40,
		"gpu_count": 1, "gpu_memory_gib": 16, "gpu_model": "NVIDIA T4"}, "minutes": 10,
		"extension_of": "gpu-t4-10m"}
]}`

// noded rents nodes as Lium does, quoting the second cheapest node left and renting the cheapest
// at or under the order's cap and the size's, its start fee billed boot time
type noded struct {
	gpuLocal
	mu    *sync.Mutex
	nodes *[]int64
	limit int64
	// asked is the cap each create named
	asked *[]int64
	// fails makes every sandbox it rents fail to become ready
	fails *atomic.Bool
}

func newNoded(t *testing.T, limit int64, nodes ...int64) noded {
	t.Helper()
	return newNodedOf(t, t4, limit, nodes...)
}

// newNodedOf is newNoded selling size
func newNodedOf(t *testing.T, size provider.Size, limit int64, nodes ...int64) noded {
	t.Helper()
	n := noded{
		gpuLocal: gpuLocal{newLocal(t, local.Options{
			Name: "lium", Sizes: []provider.Size{size},
		})},
		mu: &sync.Mutex{}, nodes: &[]int64{}, limit: limit, asked: &[]int64{},
		fails: &atomic.Bool{},
	}
	n.set(nodes...)
	return n
}

func (noded) StartFeeIsBootTime() bool { return true }

func (n noded) WaitReady(ctx context.Context, ref provider.Ref) error {
	if n.fails.Load() {
		return &provider.Error{Code: provider.CodeInternal, Err: errors.New("boot failed")}
	}
	return n.gpuLocal.WaitReady(ctx, ref)
}

// set lists the free nodes at these prices
func (n noded) set(prices ...int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	*n.nodes = slices.Sorted(slices.Values(prices))
}

// caps are the caps the creates named, in order
func (n noded) caps() []int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Clone(*n.asked)
}

func (n noded) free() []int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Clone(*n.nodes)
}

func (n noded) HourlyCostMicros(provider.Size) int64 {
	switch f := n.free(); len(f) {
	case 0:
		return n.limit
	case 1:
		return f[0]
	default:
		return f[1]
	}
}

func (n noded) StartCostMicros(s provider.Size) (int64, bool) {
	return n.StartCostAt(s, n.HourlyCostMicros(s))
}

func (n noded) CapHourlyMicros(provider.Size) int64 { return n.limit }

// StartCostAt bills 7 minutes at hourly
func (n noded) StartCostAt(_ provider.Size, hourly int64) (int64, bool) {
	return (hourly*420 + 3599) / 3600, true
}

func (n noded) RentedHourlyMicros(ref provider.Ref) (int64, bool) {
	h, err := strconv.ParseInt(ref.Meta["hourly_micros"], 10, 64)
	return h, err == nil && h > 0
}

func (n noded) SizeFor(floor provider.Resources) (provider.Size, bool) {
	if len(n.free()) == 0 {
		return provider.Size{}, false
	}
	return n.gpuLocal.SizeFor(floor)
}

func (n noded) Create(ctx context.Context, req provider.CreateRequest) (provider.Ref, error) {
	n.mu.Lock()
	*n.asked = append(*n.asked, req.MaxHourlyMicros)
	limit := n.limit
	if req.MaxHourlyMicros > 0 {
		limit = min(limit, req.MaxHourlyMicros)
	}
	i := slices.IndexFunc(*n.nodes, func(p int64) bool { return p <= limit })
	if i < 0 {
		n.mu.Unlock()
		return provider.Ref{}, &provider.Error{Code: provider.CodeCapacity, CanFailOver: true,
			Err: errors.New("no node at or under the cap")}
	}
	price := (*n.nodes)[i]
	*n.nodes = slices.Delete(*n.nodes, i, i+1)
	n.mu.Unlock()
	ref, err := n.gpuLocal.Create(ctx, req)
	if err != nil {
		return ref, err
	}
	ref.Meta = maps.Clone(ref.Meta)
	if ref.Meta == nil {
		ref.Meta = map[string]string{}
	}
	ref.Meta["hourly_micros"] = strconv.FormatInt(price, 10)
	return ref, nil
}

// newHF is a GPU provider at a fixed hour and no start fee
func newHF(t *testing.T, hourly int64) gpuLocal {
	t.Helper()
	return gpuLocal{newLocal(t, local.Options{
		Name: "hf", Sizes: []provider.Size{t4}, HourlyCostMicros: hourly,
	})}
}

// gpuEnv gives the saga the live T4 catalog at 10%, lium first, a 45 m hold and a rise of 30,000
func gpuEnv(t *testing.T, ps ...provider.Provider) *env {
	t.Helper()
	return gpuEnvOf(t, gpuLiveJSON, nil, ps...)
}

// gpuEnvOf is gpuEnv with the catalog raw and adjust applied last
func gpuEnvOf(
	t *testing.T, raw string, adjust func(*saga.Deps), ps ...provider.Provider,
) *env {
	t.Helper()
	return newEnvWith(t, func(d *saga.Deps) {
		if adjust != nil {
			defer adjust(d)
		}
		d.Catalog = loadCatalog(t, raw)
		d.Engine = d.Engine.WithGPUOrder([]string{"lium", "hf"})
		d.Rule, d.QuoteHold, d.QuoteRise = tenPercent, 45*time.Minute, 30_000
		d.GPUPlacingBudget = time.Minute
	}, ps...)
}

// holdGPU stores an estimate's quote of the T4 package on lium at hourly
func (e *env) holdGPU(t *testing.T, cost, price, hourly int64) uuid.UUID {
	t.Helper()
	id := uuid.New()
	err := e.store.PutQuotes(t.Context(), id, []store.Quote{{
		PackageID: gpuPkg, Provider: "lium", SizeName: t4.Name, CostMicros: cost,
		PriceMicros: price, HourlyMicros: hourly,
	}})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// startGPU orders the T4 package for amount under the estimate and waits for its placing
func (e *env) startGPU(
	t *testing.T, order string, amount int64, estimate uuid.UUID,
) (store.Placement, string, error) {
	t.Helper()
	p, h, err := e.saga.Create(t.Context(), saga.CreateInput{
		Token: e.token(t, ordertoken.Claims{
			OrderID: order, Kind: ordertoken.KindStart, PackageID: gpuPkg, AmountMicros: amount,
		}),
		EstimateID: estimate,
	})
	e.saga.Wait()
	if p.ID == uuid.Nil {
		t.Fatalf("create %s recorded nothing: %v", order, err)
	}
	return e.reload(t, p.ID), h, err
}

// attemptPrices are each attempt's provider, hour and start fee, as the spend meter reads them
func (e *env) attemptPrices(t *testing.T, id uuid.UUID) [][]any {
	t.Helper()
	rows, err := e.pool.Query(t.Context(), `select provider, hourly_micros, start_fee_micros
		from placement_attempts where placement_id = $1 order by attempt`, id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out [][]any
	for rows.Next() {
		var name string
		var hourly, fee int64
		if err := rows.Scan(&name, &hourly, &fee); err != nil {
			t.Fatal(err)
		}
		out = append(out, []any{name, hourly, fee})
	}
	return out
}

// At $0.28 an hour 10 minutes cost 46,667 and the 7-minute start 32,667, 79,334 priced at 87,268
const (
	quotedHourly = 280_000
	quotedCost   = 79_334
	quotedPrice  = 87_268
)

func TestGPUQuoteIsTheSecondCheapestNode(t *testing.T) {
	lium := newNoded(t, 350_000, 250_000, 280_000, 300_000)
	e := gpuEnv(t, lium, newHF(t, 600_000))
	p, _, err := e.startGPU(t, "order-1", quotedPrice, uuid.Nil)
	if err != nil || p.State != store.PlacementReady || p.Provider != "lium" {
		t.Fatalf("placement %+v %v, want ready on lium", p, err)
	}
	// The order was priced at $0.28, so its rent is capped there and takes the $0.25 node
	eq(t, lium.caps(), []int64{quotedHourly})
	eq(t, p.RentedHourlyMicros, int64(250_000))
	// The start fee is boot time the span bills, so the meter's row adds none
	eq(t, e.attemptPrices(t, p.ID), [][]any{{"lium", int64(250_000), int64(0)}})
	eq(t, lium.free(), []int64{280_000, 300_000})
}

// TestGPUHeldQuoteRefusesTheDearerNode is review focus 1, a rise past the held quote's cap
func TestGPUHeldQuoteRefusesTheDearerNode(t *testing.T) {
	t.Run("hf inside the payment", func(t *testing.T) {
		// hf's 40,000 for 10 minutes is priced at 44,000, which the payment covers
		lium := newNoded(t, 350_000, 250_000, 280_000, 300_000)
		hf := newHF(t, 240_000)
		e := gpuEnv(t, lium, hf)
		est := e.holdGPU(t, quotedCost, quotedPrice, quotedHourly)
		lium.set(300_000, 320_000)
		p, _, err := e.startGPU(t, "order-1", quotedPrice, est)
		if err != nil || p.State != store.PlacementReady || p.Provider != "hf" {
			t.Fatalf("placement %+v %v, want ready on hf", p, err)
		}
		eq(t, lium.caps(), []int64{quotedHourly})
		eq(t, lium.free(), []int64{300_000, 320_000})
		eq(t, e.attempts(t, p.ID)[0].provider, "lium")
		eq(t, p.RentedHourlyMicros, int64(0))
	})
	t.Run("hf past the payment", func(t *testing.T) {
		// hf's 90,000 is priced at 99,000, over the whole payment lium's refusal left
		lium := newNoded(t, 350_000, 250_000, 280_000, 300_000)
		hf := newHF(t, 540_000)
		e := gpuEnv(t, lium, hf)
		est := e.holdGPU(t, quotedCost, quotedPrice, quotedHourly)
		lium.set(300_000, 320_000)
		p, _, err := e.startGPU(t, "order-1", quotedPrice, est)
		if err != nil || p.State != store.PlacementFailed || !p.Refundable {
			t.Fatalf("placement %+v %v, want failed refundable", p, err)
		}
		ev := e.oneEvent(t, saga.EventFailed)
		eq(t, ev.Refundable && ev.BilledSeconds == 0, true)
		eq(t, lium.caps(), []int64{quotedHourly})
		eq(t, lium.free(), []int64{300_000, 320_000})
		eq(t, len(owned(t, hf)), 0)
		eq(t, len(owned(t, lium)), 0)
	})
}

// TestGPUHeldQuoteStartsOnTheNextNode is review focus 2, the cheapest node went since the quote
func TestGPUHeldQuoteStartsOnTheNextNode(t *testing.T) {
	lium := newNoded(t, 350_000, 250_000, 280_000, 300_000)
	e := gpuEnv(t, lium, newHF(t, 600_000))
	est := e.holdGPU(t, quotedCost, quotedPrice, quotedHourly)
	// The $0.25 node is rented, so the live quote is $0.30, priced at 93,500
	lium.set(280_000, 300_000)
	p, _, err := e.startGPU(t, "order-1", quotedPrice, est)
	if err != nil || p.State != store.PlacementReady || p.Provider != "lium" {
		t.Fatalf("placement %+v %v, want ready on lium", p, err)
	}
	eq(t, lium.caps(), []int64{quotedHourly})
	eq(t, p.RentedHourlyMicros, int64(quotedHourly))
	eq(t, e.attemptPrices(t, p.ID), [][]any{{"lium", int64(quotedHourly), int64(0)}})
	// The same payment without the quote is short of the live price
	lium.set(280_000, 300_000)
	p, _, err = e.startGPU(t, "order-2", quotedPrice, uuid.Nil)
	var ue *saga.UnderpaidError
	if !errors.As(err, &ue) || ue.PriceMicros != 93_500 || p.FailureReason !=
		saga.ReasonPriceChanged {
		t.Fatalf("placement %+v %v, want refused at 93500", p, err)
	}
	eq(t, len(lium.caps()), 1)
}

// TestGPUExtensionAtTheRentedNode is review focus 4, a node dearer than today's live quote
func TestGPUExtensionAtTheRentedNode(t *testing.T) {
	lium := newNoded(t, 350_000, 280_000)
	e := gpuEnv(t, lium, newHF(t, 600_000))
	p, h, err := e.startGPU(t, "order-1", quotedPrice, uuid.Nil)
	if err != nil || p.State != store.PlacementReady || p.RentedHourlyMicros != quotedHourly {
		t.Fatalf("placement %+v %v, want ready on the $0.28 node", p, err)
	}
	// Nodes fell to $0.20, a quote of 35,000 priced at 38,500, while this one costs 46,667
	lium.set(200_000, 210_000)
	exts, err := e.saga.ListExtensions(t.Context(), h)
	eq(t, []any{err, priced(exts)}, []any{nil, []any{gpuMore, int64(51_334), "ok"}})
	_, _, price, err := e.saga.CheckExtend(t.Context(), h, gpuMore)
	eq(t, []any{price, err}, []any{int64(51_334), nil})
	_, err = e.saga.Extend(t.Context(), h, e.extendToken(t, "short", gpuMore, 38_500, h))
	var ue *saga.UnderpaidError
	eq(t, errors.As(err, &ue) && ue.PriceMicros == 51_334, true)
	got, err := e.saga.Extend(t.Context(), h, e.extendToken(t, "ext-1", gpuMore, 51_334, h))
	if err != nil || !got.Deadline.Equal(p.Deadline.Add(10*time.Minute)) {
		t.Fatalf("extend paying 51334 gave %v, %v", got.Deadline, err)
	}

	// A placement that recorded no node is priced at the size's cap, 58,334 priced at 64,168
	if _, err := e.pool.Exec(t.Context(), `update placements set rented_hourly_micros = 0
		where id = $1`, p.ID); err != nil {
		t.Fatal(err)
	}
	_, _, price, err = e.saga.CheckExtend(t.Context(), h, gpuMore)
	eq(t, []any{price, err}, []any{int64(64_168), nil})
}

// TestGPUQuoteWithoutHourlyPricesLive is review focus 5, a held row from before the migration
func TestGPUQuoteWithoutHourlyPricesLive(t *testing.T) {
	t.Run("a rise inside the hold's room is refused", func(t *testing.T) {
		lium := newNoded(t, 350_000, 250_000, 280_000, 300_000)
		e := gpuEnv(t, lium, newHF(t, 600_000))
		est := e.holdGPU(t, quotedCost, quotedPrice, 0)
		// $0.29 costs 82,168, 3,117 over the payment priced, inside the 30,000 a hold absorbs
		lium.set(250_000, 290_000, 300_000)
		p, _, err := e.startGPU(t, "order-1", quotedPrice, est)
		var ue *saga.UnderpaidError
		if !errors.As(err, &ue) || ue.PriceMicros != 90_385 ||
			p.FailureReason != saga.ReasonPriceChanged || !p.Refundable {
			t.Fatalf("placement %+v %v, want refused at 90385", p, err)
		}
		eq(t, len(lium.caps()), 0)
		q, ok, err := e.store.QuoteFor(t.Context(), est, gpuPkg, time.Hour)
		eq(t, []any{q.HourlyMicros, ok, err}, []any{int64(0), true, nil})
	})
	t.Run("a live price the payment covers starts capped", func(t *testing.T) {
		lium := newNoded(t, 350_000, 250_000, 280_000, 300_000)
		e := gpuEnv(t, lium, newHF(t, 600_000))
		est := e.holdGPU(t, quotedCost, quotedPrice, 0)
		p, _, err := e.startGPU(t, "order-1", quotedPrice, est)
		if err != nil || p.State != store.PlacementReady || p.Provider != "lium" {
			t.Fatalf("placement %+v %v, want ready on lium", p, err)
		}
		eq(t, lium.caps(), []int64{quotedHourly})
	})
}
