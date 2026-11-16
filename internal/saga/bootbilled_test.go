package saga_test

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/gpu"
	"github.com/bugnet-sh/bugnet-router/internal/ordertoken"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/boat"
	"github.com/bugnet-sh/bugnet-router/internal/provider/boxd"
	"github.com/bugnet-sh/bugnet-router/internal/provider/e2b"
	"github.com/bugnet-sh/bugnet-router/internal/provider/koyeb"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// hfBilled is a GPU provider priced as HF is, by the minute with billed start minutes as its fee
type hfBilled struct {
	gpuLocal
	hourly, fee int64
}

// newHFBilled sells size at hourly, its fee 3 minutes of it
func newHFBilled(t *testing.T, size provider.Size, hourly int64) hfBilled {
	t.Helper()
	return hfBilled{
		gpuLocal: gpuLocal{newLocal(t, local.Options{Name: "hf", Sizes: []provider.Size{size}})},
		hourly:   hourly, fee: (hourly*3 + 59) / 60,
	}
}

func (h hfBilled) HourlyCostMicros(provider.Size) int64 { return h.hourly }

func (h hfBilled) StartCostMicros(provider.Size) (int64, bool) { return h.fee, true }

func (hfBilled) BillingIncrement() time.Duration { return time.Minute }

func (hfBilled) StartFeeIsBootTime() bool { return true }

// syncLog is a log buffer several goroutines write
type syncLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *syncLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *syncLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// TestGPUMeterBillsBootOnce meters an HF run's boot in its span alone, never again as a fee
func TestGPUMeterBillsBootOnce(t *testing.T) {
	logs := &syncLog{}
	hf := newHFBilled(t, t4, 410_020)
	e := gpuEnvOf(t, gpuLiveJSON, func(d *saga.Deps) {
		d.Log = slog.New(slog.NewTextHandler(logs, nil))
	}, hf)
	// 10 minutes and 3 start minutes at $0.41002 cost 88,838, priced at 97,722
	p, _, err := e.startGPU(t, "order-1", 97_722, uuid.Nil)
	if err != nil || p.State != store.PlacementReady || p.Provider != "hf" {
		t.Fatalf("placement %+v %v, want ready on hf", p, err)
	}
	eq(t, e.attemptPrices(t, p.ID), [][]any{{"hf", int64(410_020), int64(0)}})
	// A 2-minute boot and the paid 10 minutes, as HF bills them, are 12 minutes from the create
	if _, err := e.pool.Exec(t.Context(), `update placement_attempts
		set created_at = now() - interval '11 minutes 30 seconds' where placement_id = $1`,
		p.ID); err != nil {
		t.Fatal(err)
	}
	if err := e.saga.Terminate(t.Context(), p.ID, *p.Deadline); err != nil {
		t.Fatal(err)
	}
	ended := e.reload(t, p.ID)
	// 12 billed minutes cost 82,004, under the 97,722 paid and its markup
	eq(t, []any{ended.State, ended.CostMicros}, []any{store.PlacementTerminated, int64(82_004)})
	if got := logs.String(); strings.Contains(got, "placement cost is over") {
		t.Fatalf("log %q, want no loss and no cost over the markup", got)
	}
}

// h200 is a one-GPU H200 size, which the H200 package below fits
var h200 = provider.Size{Name: "h200", Resources: provider.Resources{
	VCPU: 16, MemoryMiB: 245760, DiskGiB: 900, GPUCount: 1, GPUMemoryGiB: 141,
	GPUModel: gpu.H200,
}}

// h200JSON is a live H200 package
const h200JSON = `{"packages": [
	{"id": "gpu-h200-10m", "resources": {"vcpu": 4, "memory_mib": 12288, "disk_gib": 40,
		"gpu_count": 1, "gpu_memory_gib": 141, "gpu_model": "NVIDIA H200"}, "minutes": 10}
]}`

// On a $4.15 node 10 minutes cost 691,667 and 7 start minutes 484,167, priced at 1,293,418, and
// HF's h200 at $5.00998 costs 834,997 and 250,499, priced at 1,194,046
const (
	h200Hourly = 4_150_000
	h200Paid   = 1_293_418
	hfH200     = 5_009_980
)

// startH200 orders the H200 package paying h200Paid under the estimate
func (e *env) startH200(t *testing.T, order string, estimate uuid.UUID) (store.Placement, error) {
	t.Helper()
	p, _, err := e.saga.Create(t.Context(), saga.CreateInput{
		Token: e.token(t, ordertoken.Claims{
			OrderID: order, Kind: ordertoken.KindStart, PackageID: "gpu-h200-10m",
			AmountMicros: h200Paid,
		}),
		EstimateID: estimate,
	})
	e.saga.Wait()
	if p.ID == uuid.Nil {
		t.Fatalf("create %s recorded nothing: %v", order, err)
	}
	return e.reload(t, p.ID), err
}

// TestGPUCapRefusalSpendsNothing fails over to HF with the whole payment after Lium's create
// refused every node over the held quote's cap, since nothing was rented
func TestGPUCapRefusalSpendsNothing(t *testing.T) {
	lium := newNodedOf(t, h200, 4_900_000, h200Hourly)
	e := gpuEnvOf(t, h200JSON, nil, lium, newHFBilled(t, h200, hfH200))
	est := uuid.New()
	if err := e.store.PutQuotes(t.Context(), est, []store.Quote{{
		PackageID: "gpu-h200-10m", Provider: "lium", SizeName: h200.Name,
		CostMicros: 1_175_834, PriceMicros: h200Paid, HourlyMicros: h200Hourly,
	}}); err != nil {
		t.Fatal(err)
	}
	lium.set(4_300_000)
	p, err := e.startH200(t, "order-1", est)
	if err != nil || p.State != store.PlacementReady || p.Provider != "hf" {
		t.Fatalf("placement %+v %v, want ready on hf", p, err)
	}
	eq(t, lium.caps(), []int64{h200Hourly})
	eq(t, lium.free(), []int64{4_300_000})
}

// TestGPURentedThenFailedSpendsItsSpan counts a Lium attempt that rented and failed at its span's
// hours, its start fee being boot time the span holds
func TestGPURentedThenFailedSpendsItsSpan(t *testing.T) {
	t.Run("hf inside what is left", func(t *testing.T) {
		lium := newNodedOf(t, h200, 4_900_000, h200Hourly)
		lium.fails.Store(true)
		e := gpuEnvOf(t, h200JSON, nil, lium, newHFBilled(t, h200, hfH200))
		p, err := e.startH200(t, "order-1", uuid.Nil)
		if err != nil || p.State != store.PlacementReady || p.Provider != "hf" {
			t.Fatalf("placement %+v %v, want ready on hf", p, err)
		}
		eq(t, lium.free(), []int64{})
		eq(t, e.attempts(t, p.ID)[0].provider, "lium")
	})
	t.Run("hf at the whole payment", func(t *testing.T) {
		// $5.426922 an hour prices HF at exactly the payment, which any second on lium spent
		lium := newNodedOf(t, h200, 4_900_000, h200Hourly)
		lium.fails.Store(true)
		hf := newHFBilled(t, h200, 5_426_922)
		e := gpuEnvOf(t, h200JSON, nil, lium, hf)
		p, err := e.startH200(t, "order-1", uuid.Nil)
		if err != nil || p.State != store.PlacementFailed || !p.Refundable {
			t.Fatalf("placement %+v %v, want failed refundable", p, err)
		}
		eq(t, lium.free(), []int64{})
		eq(t, len(owned(t, hf)), 0)
	})
}

// cpuAdapters builds every CPU provider adapter offline, each pointed at a closed loopback port
func cpuAdapters(t *testing.T) []provider.Provider {
	t.Helper()
	const closed = "127.0.0.1:1"
	bx, err := boxd.New(boxd.Options{APIKey: "bxd_test", Endpoint: closed, Insecure: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bx.Close() })
	eb, err := e2b.New(e2b.Options{
		APIKey: "e2b_test", BaseURL: "http://" + closed, EnvdURL: "http://" + closed,
		Templates: []e2b.Template{
			{Name: "2x2", TemplateID: "bugnet-2x2", VCPU: 2, MemoryMiB: 2048, DiskGiB: 10},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ky, err := koyeb.New(koyeb.Options{APIToken: "koyeb_test", BaseURL: "http://" + closed})
	if err != nil {
		t.Fatal(err)
	}
	bt, err := boat.New(boat.Options{APIKey: "boat_test", BaseURL: "http://" + closed})
	if err != nil {
		t.Fatal(err)
	}
	return []provider.Provider{newLocal(t, local.Options{Name: "local"}), bx, eb, ky, bt}
}

// TestCPUMeterUnchanged keeps every CPU provider's start fee on its attempt row, its own charge
func TestCPUMeterUnchanged(t *testing.T) {
	floor := provider.Resources{VCPU: 1, MemoryMiB: 1024, DiskGiB: 5}
	for _, p := range cpuAdapters(t) {
		t.Run(p.Name(), func(t *testing.T) {
			size, ok := p.SizeFor(floor)
			if !ok {
				t.Fatalf("%s sells no size for %+v", p.Name(), floor)
			}
			var fee int64
			if sc, ok := provider.As[provider.StartCoster](p); ok {
				if f, ok := sc.StartCostMicros(size); ok {
					fee = f
				}
			}
			hourly, recorded := saga.AttemptQuote(p, size)
			eq(t, []int64{hourly, recorded}, []int64{p.HourlyCostMicros(size), fee})
			_, boot := provider.As[provider.BootBiller](p)
			eq(t, boot, false)
			eq(t, provider.SeparateStartFee(p, 30_000), int64(30_000))
		})
	}
}
