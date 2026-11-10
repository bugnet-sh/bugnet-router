// Package spend is the router's own meter of what its sandboxes cost at their providers
// (CP 10.7). An attempt's provider time runs from its create call to its destroy, is rounded up
// to the provider's billing increment and priced at its size's hourly rate, and the start fee is
// added once.
package spend

import (
	"math/big"
	"time"
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
	// BelowFloor is true when the cost leaves less than the margin floor of what was paid
	BelowFloor bool
	// Loss is true when the cost is more than what was paid
	Loss bool
}

// Judge compares cost with paid, the sum of the placement's start and extension orders
func Judge(cost, paid int64, floor float64) Verdict {
	return Verdict{
		BelowFloor: float64(cost) > (1-floor)*float64(paid),
		Loss:       cost > paid,
	}
}
