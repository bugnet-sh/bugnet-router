// Package simulate replays recorded start samples through the saga's failover rules to tune the
// failover budget and the slow-provider rule offline. It is pure Go over the samples and calls no
// provider.
package simulate

import (
	"math/rand/v2"
	"slices"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/bench"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// Settings are one setting the replay tries
type Settings struct {
	// Providers lists the providers in the order the engine tries them, the cheapest first
	Providers []string
	// Budget is the time the saga has to place an order before it gives up
	Budget time.Duration
	// MaxAttempts is how many providers an order may start a sandbox on, 3 when zero
	MaxAttempts int
	// SlowReadyP95 sends a provider whose recorded ready p95 is over it to the end of the
	// order, none when zero
	SlowReadyP95 time.Duration
	// Orders is how many orders the replay places, 1000 when zero
	Orders int
	// Seed makes a replay repeat
	Seed uint64
}

// Result is what one replay of Settings found
type Result struct {
	Orders int
	// Ready is the orders ready within the budget
	Ready int
	// FailedOver is the orders whose first attempt failed
	FailedOver int
	// Refunded is the orders with no candidate left within the budget
	Refunded int
	// ReadyP50 and ReadyP90 are the nearest-rank times to ready of the ready orders
	ReadyP50, ReadyP90 time.Duration
}

// ReadyShare is the fraction of the orders that were ready within the budget
func (r Result) ReadyShare() float64 { return share(r.Ready, r.Orders) }

// FailoverShare is the fraction of the orders whose first attempt failed
func (r Result) FailoverShare() float64 { return share(r.FailedOver, r.Orders) }

// RefundShare is the fraction of the orders refunded for no candidate
func (r Result) RefundShare() float64 { return share(r.Refunded, r.Orders) }

func share(n, of int) float64 {
	if of == 0 {
		return 0
	}
	return float64(n) / float64(of)
}

// attempt is the outcome of one recorded start
type attempt struct {
	ok bool
	// refused is a capacity, rate limit or quota answer, which costs no attempt
	refused bool
	took    time.Duration
}

// Run places s.Orders orders. Each attempt takes a recorded start of the provider the order
// tries, drawn at random. An ok start is ready after its ready, runner and gate times, a failed
// one after the time its last step took. An attempt that runs past the budget is cut there.
func Run(samples []store.StartSample, s Settings) Result {
	if s.MaxAttempts == 0 {
		s.MaxAttempts = 3
	}
	if s.Orders == 0 {
		s.Orders = 1000
	}
	pools := map[string][]attempt{}
	for _, m := range samples {
		pools[m.Provider] = append(pools[m.Provider], attemptOf(m))
	}
	order := tryOrder(s, pools)
	rng := rand.New(rand.NewPCG(s.Seed, s.Seed^0x9e3779b97f4a7c15))
	res := Result{Orders: s.Orders}
	var times []int
	for range s.Orders {
		took, ready, failedOver := place(rng, pools, order, s)
		if failedOver {
			res.FailedOver++
		}
		if ready {
			res.Ready++
			times = append(times, int(took/time.Millisecond))
		} else {
			res.Refunded++
		}
	}
	res.ReadyP50 = percentile(times, 0.5)
	res.ReadyP90 = percentile(times, 0.9)
	return res
}

// place runs one order and reports when it was ready, whether it was, and whether its first
// attempt failed
func place(
	rng *rand.Rand, pools map[string][]attempt, order []string, s Settings,
) (took time.Duration, ready, failedOver bool) {
	attempts := 0
	for _, name := range order {
		pool := pools[name]
		if attempts >= s.MaxAttempts || took >= s.Budget {
			break
		}
		a := pool[rng.IntN(len(pool))]
		if a.refused {
			took += a.took
			continue
		}
		attempts++
		if took+a.took > s.Budget {
			return s.Budget, false, failedOver || attempts > 1
		}
		took += a.took
		if a.ok {
			return took, true, failedOver
		}
		failedOver = true
	}
	return took, false, failedOver
}

// tryOrder lists the providers with recorded starts in s.Providers' order, the slow ones last
func tryOrder(s Settings, pools map[string][]attempt) []string {
	var fast, slow []string
	for _, name := range s.Providers {
		pool := pools[name]
		if len(pool) == 0 {
			continue
		}
		if s.SlowReadyP95 > 0 && readyP95(pool) > s.SlowReadyP95 {
			slow = append(slow, name)
			continue
		}
		fast = append(fast, name)
	}
	return append(fast, slow...)
}

// readyP95 is the 95th percentile of the time an ok start took
func readyP95(pool []attempt) time.Duration {
	var ms []int
	for _, a := range pool {
		if a.ok {
			ms = append(ms, int(a.took/time.Millisecond))
		}
	}
	return percentile(ms, 0.95)
}

func attemptOf(m store.StartSample) attempt {
	switch m.Outcome {
	case store.OutcomeOK:
		return attempt{ok: true, took: ms(m.ReadyMS) + ms(m.RunnerMS) + ms(m.GateMS)}
	case store.OutcomeFailed:
		return attempt{took: lastStep(m)}
	}
	return attempt{refused: true, took: ms(m.CreateMS)}
}

// lastStep is how long a failed start ran before it failed, the longest step it timed
func lastStep(m store.StartSample) time.Duration {
	return ms(slices.Max([]int{m.CreateMS, m.ReadyMS, m.RunnerMS, m.GateMS, 0}))
}

// ms is a recorded step as a duration, 0 for a step that did not run
func ms(v int) time.Duration { return time.Duration(max(v, 0)) * time.Millisecond }

func percentile(ms []int, p float64) time.Duration {
	return time.Duration(bench.Percentile(ms, p)) * time.Millisecond
}
