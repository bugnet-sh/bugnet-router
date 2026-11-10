package saga_test

import (
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/ratelimit"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

// fixedTotals answers every provider with the same whole-attempt figures
type fixedTotals struct {
	cold, pooled time.Duration
	samples      int
}

func (f fixedTotals) Totals(string) (time.Duration, time.Duration, int) {
	return f.cold, f.pooled, f.samples
}

func hedgeSettings() config.Hedge { return config.DefaultHedge() }

func newHedger(t *testing.T, h config.Hedge, tot saga.Totals, creates map[string]int) (
	*saga.Hedger, string,
) {
	t.Helper()
	vk, prefix := testenv.Valkey(t)
	return saga.NewHedger(saga.HedgerDeps{
		Settings: h, Health: tot, Valkey: vk, Prefix: prefix, CreatesPerSecond: creates,
		MarginFloor: 0.5,
	}), prefix
}

func TestHedgerAfter(t *testing.T) {
	h, _ := newHedger(t, hedgeSettings(), fixedTotals{cold: 8 * time.Second, samples: 4}, nil)
	// Under min_samples the default stands
	d, ok := h.After("e2b", saga.PathCold)
	eq(t, ok && d == 5*time.Second, true)

	h, _ = newHedger(t, hedgeSettings(), fixedTotals{cold: 8 * time.Second, samples: 5}, nil)
	d, ok = h.After("e2b", saga.PathCold)
	eq(t, ok && d == 8*time.Second, true)

	// Never under min_after, and the pooled figure serves the pooled path
	h, _ = newHedger(t, hedgeSettings(), fixedTotals{
		cold: 8 * time.Second, pooled: 900 * time.Millisecond, samples: 5,
	}, nil)
	d, ok = h.After("boxd", saga.PathPooled)
	eq(t, ok, true)
	eq(t, d, 1500*time.Millisecond)

	// A path without a figure falls back to the default
	h, _ = newHedger(t, hedgeSettings(), fixedTotals{cold: 8 * time.Second, samples: 9}, nil)
	d, ok = h.After("boxd", saga.PathPooled)
	eq(t, ok && d == 5*time.Second, true)

	off := hedgeSettings()
	off.Enabled = false
	h, _ = newHedger(t, off, nil, nil)
	_, ok = h.After("e2b", saga.PathCold)
	eq(t, ok, false)
	var none *saga.Hedger
	_, ok = none.After("e2b", saga.PathCold)
	eq(t, ok, false)
}

func hedgeCand(t *testing.T, name string, hourly int64) placement.Candidate {
	t.Helper()
	p := newLocal(t, local.Options{Name: name})
	return placement.Candidate{Provider: p, Size: small, HourlyMicros: hourly}
}

var hedgePkg = catalog.Package{ID: "p", Minutes: 10, PriceMicros: 50_000}

func TestHedgerAllowRefusals(t *testing.T) {
	ctx := t.Context()
	running := hedgeCand(t, "running", 1000)
	next := hedgeCand(t, "next", 1000)
	left := 20 * time.Second
	// Every refusal comes before a token is taken, so the router-wide bucket keeps its whole burst
	h, _ := newHedger(t, hedgeSettings(), nil, nil)
	eq(t, h.Allow(ctx, hedgePkg, running, hedgeCand(t, "boat", 1000), 0, left), false)
	eq(t, h.Allow(ctx, hedgePkg, running, next, 0, 5*time.Second), false)
	eq(t, h.Allow(ctx, hedgePkg, hedgeCand(t, "dear", 400_000), next, 0, left), false)
	for i := range 5 {
		if !h.Allow(ctx, hedgePkg, running, next, 0, left) {
			t.Fatalf("hedge %d was refused, a refusal took a token", i+1)
		}
	}
}

func TestHedgerAllowOntoBoatWhenAllowed(t *testing.T) {
	s := hedgeSettings()
	s.OntoBoat = true
	h, _ := newHedger(t, s, nil, nil)
	eq(t, h.Allow(t.Context(), hedgePkg, hedgeCand(t, "running", 1000),
		hedgeCand(t, "boat", 1000), 0, 20*time.Second), true)
}

func TestHedgerAllowRefusesAPausedProvider(t *testing.T) {
	vk, prefix := testenv.Valkey(t)
	h := saga.NewHedger(saga.HedgerDeps{
		Settings: hedgeSettings(), Valkey: vk, Prefix: prefix, MarginFloor: 0.5,
	})
	running, next := hedgeCand(t, "running", 1000), hedgeCand(t, "next", 1000)
	eq(t, h.Allow(t.Context(), hedgePkg, running, next, 0, 20*time.Second), true)
	err := vk.Do(t.Context(), vk.B().Set().Key(prefix+"pause:next:create").Value("1").
		PxMilliseconds(time.Minute.Milliseconds()).Build()).Error()
	eq(t, err, nil)
	eq(t, h.Allow(t.Context(), hedgePkg, running, next, 0, 20*time.Second), false)
}

func TestHedgerAllowRefusesALowCreateBucket(t *testing.T) {
	vk, prefix := testenv.Valkey(t)
	h := saga.NewHedger(saga.HedgerDeps{
		Settings: hedgeSettings(), Valkey: vk, Prefix: prefix, MarginFloor: 0.5,
		CreatesPerSecond: map[string]int{"next": 1},
	})
	running, next := hedgeCand(t, "running", 1000), hedgeCand(t, "next", 1000)
	eq(t, h.Allow(t.Context(), hedgePkg, running, next, 0, 20*time.Second), true)
	// The bucket of a create a second holds one token, and the create takes it
	emptyCreateBucket(t, vk, prefix, "next")
	eq(t, h.Allow(t.Context(), hedgePkg, running, next, 0, 20*time.Second), false)
}

func TestHedgerAllowSpendsTheRouterWideBucket(t *testing.T) {
	h, _ := newHedger(t, hedgeSettings(), nil, nil)
	running, next := hedgeCand(t, "running", 1000), hedgeCand(t, "next", 1000)
	// The burst is 5 and refills 30 a minute
	for i := range 5 {
		if !h.Allow(t.Context(), hedgePkg, running, next, 0, 20*time.Second) {
			t.Fatalf("hedge %d was refused inside the burst", i+1)
		}
	}
	eq(t, h.Allow(t.Context(), hedgePkg, running, next, 0, 20*time.Second), false)
}

func TestHedgerDisabledAndNil(t *testing.T) {
	off := hedgeSettings()
	off.Enabled = false
	h, _ := newHedger(t, off, nil, nil)
	running, next := hedgeCand(t, "running", 1000), hedgeCand(t, "next", 1000)
	eq(t, h.Allow(t.Context(), hedgePkg, running, next, 0, 20*time.Second), false)
	var none *saga.Hedger
	eq(t, none.Allow(t.Context(), hedgePkg, running, next, 0, 20*time.Second), false)
}

// emptyCreateBucket takes the one token of a create a second bucket
func emptyCreateBucket(t *testing.T, vk valkey.Client, prefix, name string) {
	t.Helper()
	l := ratelimit.New(vk, prefix, 0, 0)
	ok, _, err := l.TakeKey(t.Context(), ratelimit.Bucket{
		Key: "rl:create:" + name, PerSecond: 1, Burst: 1,
	})
	if err != nil || !ok {
		t.Fatalf("take the create token of %s = %v, %v", name, ok, err)
	}
}
