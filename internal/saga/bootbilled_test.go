package saga_test

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

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
