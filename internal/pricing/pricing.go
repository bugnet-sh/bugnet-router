// Package pricing holds the rule that turns a provider's official cost into the price Bugnet quotes
package pricing

import (
	"math"
	"time"
)

// bpBase is 100% in basis points
const bpBase = 10_000

// Rule is the markup on official cost and the time each provider bills beyond a run
type Rule struct {
	// MarkupBP is the markup in basis points, 1000 is 10%
	MarkupBP int64
	// Overhead is the time a provider bills beyond a run, none for a provider not listed
	Overhead map[string]time.Duration
}

// Price is the cost plus the markup, rounded up to a whole micro, MaxInt64 on overflow
func (r Rule) Price(cost int64) int64 {
	if cost <= 0 {
		return 0
	}
	factor := bpBase + r.MarkupBP
	if cost > (math.MaxInt64-(bpBase-1))/factor {
		return math.MaxInt64
	}
	return (cost*factor + bpBase - 1) / bpBase
}

// Fits reports whether the price of cost is within budget
func (r Rule) Fits(cost, budget int64) bool { return r.Price(cost) <= budget }

// Billed is the run plus the provider's overhead
func (r Rule) Billed(provider string, run time.Duration) time.Duration {
	return run + r.Overhead[provider]
}
