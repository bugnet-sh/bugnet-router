package main

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// lateIntervals is how many canary intervals late a canary must be before /monitor says so
const lateIntervals = 3

// canaryCheck says what /monitor reads of the canaries and the shared provider states
type canaryCheck struct {
	// every is each provider's canary interval
	every map[string]time.Duration
	// downAfter is how long a provider is down before /monitor reports it
	downAfter time.Duration
	// since is when this worker started, before which a missing canary proves nothing
	since time.Time
	// due says whether the provider gets canaries at all, false while its next start costs
	due func(name string) bool
	// oldProbes lists the providers whose sweep listing held a probe sandbox over 15 minutes old
	oldProbes func() map[string]time.Duration
}

type providerAge struct {
	provider string
	age      time.Duration
}

// canaryHealth is what the health rows and canary samples showed
type canaryHealth struct {
	down, late, probes []providerAge
}

// checkCanaries finds providers down for long, canaries 3 intervals late and old probe sandboxes
func checkCanaries(
	ctx context.Context, log *slog.Logger, st *store.Store, c canaryCheck,
) canaryHealth {
	var h canaryHealth
	now := time.Now()
	rows, err := st.ProviderHealths(ctx)
	if err != nil {
		log.Warn("monitor cannot read the provider health rows", "err", err)
	}
	blocked := map[string]bool{}
	for _, r := range rows {
		blocked[r.Provider] = r.Blocked != ""
		if r.State == store.HealthDown && now.Sub(r.StateSince) > c.downAfter {
			h.down = append(h.down, providerAge{r.Provider, now.Sub(r.StateSince)})
		}
	}
	for _, name := range slices.Sorted(maps.Keys(c.every)) {
		if blocked[name] || c.due == nil || !c.due(name) {
			continue
		}
		late := lateIntervals * c.every[name]
		f, err := st.CanaryFactsOf(ctx, name)
		if err != nil {
			log.Warn("monitor cannot read the canary facts", "provider", name, "err", err)
			continue
		}
		// A placement that got ready stands in for a canary, and a restart restarts the count
		last := latest(f.LastCanary, f.LastReady, c.since)
		if age := now.Sub(last); age > late {
			h.late = append(h.late, providerAge{name, age})
		}
	}
	if c.oldProbes != nil {
		old := c.oldProbes()
		for _, name := range slices.Sorted(maps.Keys(old)) {
			h.probes = append(h.probes, providerAge{name, old[name]})
		}
	}
	return h
}

func (h canaryHealth) problems() []string {
	var out []string
	for _, p := range h.down {
		out = append(out, fmt.Sprintf("provider %s is down for %d minutes", p.provider,
			int(p.age.Minutes())))
	}
	for _, p := range h.late {
		out = append(out, fmt.Sprintf("provider %s has had no canary for %d minutes", p.provider,
			int(p.age.Minutes())))
	}
	for _, p := range h.probes {
		out = append(out, fmt.Sprintf("provider %s has a probe sandbox %d minutes old",
			p.provider, int(p.age.Minutes())))
	}
	return out
}

// latest returns the latest of ts
func latest(ts ...time.Time) time.Time {
	var out time.Time
	for _, t := range ts {
		if t.After(out) {
			out = t
		}
	}
	return out
}
