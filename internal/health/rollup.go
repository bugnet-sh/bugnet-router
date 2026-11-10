package health

import (
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// Rules are the thresholds a rollup judges a provider by
type Rules struct {
	// MinSamples is the fewest samples that can mark a provider down or slow
	MinSamples int
	// DownBelow is the Wilson lower bound under which a provider is down
	DownBelow float64
	// SlowReadyP95 is the ready p95 over which a provider is slow
	SlowReadyP95 time.Duration
}

// Counts is what the window's samples say about one provider
type Counts = store.HealthCounts

// canaryRun is how many failed canaries in a row mark a provider down
const canaryRun = 2

// canaryFailed reports whether the last two canaries that count both failed
func canaryFailed(c Counts) bool {
	if len(c.LastCanaries) < canaryRun {
		return false
	}
	for _, o := range c.LastCanaries[:canaryRun] {
		if o != store.OutcomeFailed {
			return false
		}
	}
	return true
}

// State judges one provider from its counts
func (r Rules) State(c Counts) store.HealthState {
	n := c.OK + c.Failed
	lb := LowerBound(c.OK, n)
	state := store.HealthOK
	switch {
	case canaryFailed(c), n >= r.MinSamples && lb < r.DownBelow:
		state = store.HealthDown
	case n >= r.MinSamples && c.ReadyP95 > r.SlowReadyP95:
		state = store.HealthSlow
	}
	return store.HealthState{
		State: state, Samples: n, SuccessLB: lb, ReadyP95: c.ReadyP95,
		ColdTotalP95: c.ColdTotalP95, PooledTotalP95: c.PooledTotalP95,
	}
}

// Rollup judges every provider, keeping each one's previous state when canary failures alone
// would mark all of two or more down, since the probe is then the likelier thing to be broken.
// It reports whether the guard held.
func (r Rules) Rollup(
	counts map[string]Counts, previous map[string]string,
) (map[string]store.HealthState, bool) {
	out := make(map[string]store.HealthState, len(counts))
	allCanary := len(counts) > 1
	for name, c := range counts {
		out[name] = r.State(c)
		allCanary = allCanary && canaryFailed(c)
	}
	if !allCanary {
		return out, false
	}
	for name, h := range out {
		h.State = store.HealthOK
		if prev := previous[name]; prev != "" {
			h.State = prev
		}
		out[name] = h
	}
	return out, true
}
