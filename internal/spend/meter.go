// Package spend is the router's own meter of what its sandboxes cost at their providers
// (CP 10.7). An attempt's provider time runs from its create call to its destroy, is rounded up
// to the provider's billing increment and priced at its size's hourly rate, and the start fee is
// added once.
package spend

import (
	"math/big"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/pool"
	"github.com/bugnet-sh/bugnet-router/internal/pricing"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// AttemptCost is the provider cost in micros of one attempt from start to end. The billed time is
// rounded up to whole increments, the product with the hourly rate is rounded up to a micro, and
// fee is added. An end before start bills no time. A zero increment bills the time as it ran.
func AttemptCost(start, end time.Time, incr time.Duration, hourly, fee int64) int64 {
	ran := max(end.Sub(start), 0)
	if incr > 0 {
		ran = (ran + incr - 1) / incr * incr
	}
	// The product of a day in nanoseconds and a dear hour overflows 64 bits
	n := new(big.Int).Mul(big.NewInt(int64(ran)), big.NewInt(hourly))
	h := big.NewInt(int64(time.Hour))
	n.Add(n, h.Sub(h, big.NewInt(1)))
	n.Div(n, big.NewInt(int64(time.Hour)))
	return n.Int64() + fee
}

// Verdict is how a placement's cost compares with what its orders paid
type Verdict struct {
	// UnderMarkup is true when the price of the cost is more than what was paid
	UnderMarkup bool
	// Loss is true when the cost is more than what was paid
	Loss bool
}

// Judge compares cost with paid, the sum of the placement's start and extension orders
func Judge(cost, paid int64, r pricing.Rule) Verdict {
	return Verdict{
		UnderMarkup: r.Price(cost) > paid,
		Loss:        cost > paid,
	}
}

// SpansCost prices attempts that made a sandbox, a sandbox still alive running to end, and
// incrOf giving the billing increment of a provider by name
func SpansCost(spans []store.AttemptSpan, end time.Time, incrOf func(string) time.Duration) int64 {
	var total int64
	for _, a := range spans {
		to := end
		if a.DestroyedAt != nil {
			to = *a.DestroyedAt
		}
		total += AttemptCost(a.CreatedAt, to, incrOf(a.Provider), a.HourlyMicros, a.StartFeeMicros)
	}
	return total
}

// MemberCost is the provider cost in micros of one pool member's own time up to end: filling at
// the hourly rate, waiting parked at the parked rate, and, for a member held for a reservation or
// ended unbound, the running time after its claim. A bound member's running time is its
// placement's, which the attempt spans price.
func MemberCost(m store.Member, hourly, parked int64, end time.Time) int64 {
	return pool.OverheadMicros(m, hourly, parked, m.CreatedAt, end)
}
