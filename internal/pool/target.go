// Package pool keeps warm sandboxes for the providers that can park them
//
// A pool is one provider and size with a minimum, a maximum and a longest age. The manager runs
// in router-worker. pool_tick sizes each pool from the claims and misses it saw, stores the
// target, fills the deficit one pool_fill job a member, retires old members, inspects warm ones
// and holds every target at its minimum while the day's waste and idle cost reach the cap. A fill
// creates the sandbox, boots it in its pool phase, parks it and marks it warm, and a claim takes
// the oldest warm member in one statement under skip locked. Warmth is every replica's view of
// the warm members and targets, read from Postgres, so router-api needs nothing of the worker's
// memory.
package pool

import (
	"math"
	"time"
)

// Key names one pool
type Key struct{ Provider, Size string }

// Config is one [[pools.pool]] table
type Config struct {
	Key
	Min, Max int
	MaxAge   time.Duration
}

// Target is claims a second times the refill p95, plus headroom, within min and max
func Target(c Config, claimsPerSecond float64, refillP95 time.Duration, headroom int) int {
	want := int(math.Ceil(claimsPerSecond*refillP95.Seconds())) + headroom
	return min(max(want, c.Min), c.Max)
}
