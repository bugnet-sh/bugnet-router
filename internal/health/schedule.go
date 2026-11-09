package health

import (
	"hash/fnv"
	"strconv"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// jitter is how far a canary gap may stray from its setting, as a share of it
const jitter = 0.2

// Gap spreads a canary interval over 80 to 120 percent by a hash of the provider and the last
// canary, so every tick gives the same answer
func Gap(every time.Duration, name string, last time.Time) time.Duration {
	h := fnv.New64a()
	_, _ = h.Write([]byte(name + "\x00" + strconv.FormatInt(last.UnixNano(), 10)))
	unit := float64(h.Sum64()>>11) / float64(1<<53) // [0, 1)
	return time.Duration(float64(every) * (1 - jitter + 2*jitter*unit))
}

// Schedule holds the intervals the canary tick uses
type Schedule struct {
	Every, EveryDown, BoatEvery, PassiveWindow time.Duration
}

// Inputs are the facts one provider's canary is judged by
type Inputs struct {
	Name  string
	Facts store.CanaryFacts
	// Costly says the provider charges per start, so it uses BoatEvery
	Costly bool
	// StartFree says the next start of the canary's size costs nothing
	StartFree bool
}

// Due reports whether a canary of the provider should be queued at now
func (s Schedule) Due(in Inputs, now time.Time) bool {
	f := in.Facts
	every := s.Every
	switch {
	case f.State == store.HealthDown:
		every = s.EveryDown
	case in.Costly:
		every = s.BoatEvery
	}
	if f.State != store.HealthDown {
		every = Gap(every, in.Name, f.LastCanary)
	}
	switch {
	case f.Blocked != "":
		return false
	case !in.StartFree:
		return false
	case !f.LastCanary.IsZero() && now.Sub(f.LastCanary) < every:
		return false
	case !f.LastReady.IsZero() && now.Sub(f.LastReady) < s.PassiveWindow:
		return false
	}
	return true
}
