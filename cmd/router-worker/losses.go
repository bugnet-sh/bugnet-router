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

// lossWindow is how far back /monitor counts a provider's lost placements
const lossWindow = time.Hour

// lossCheck holds the threshold of abuse.lost_alert_per_hour, zero reporting no lost ends
type lossCheck struct {
	alertPerHour int
}

// lossHealth is what the last hour showed of placements a provider lost
type lossHealth struct {
	// unasked counts each provider's kills no router asked for
	unasked map[string]int
	// failed counts each provider's provider_failed ends over the hourly alert
	failed map[string]int
}

// checkLosses reads the last hour's kills no router asked for and provider_failed ends
func checkLosses(ctx context.Context, log *slog.Logger, st *store.Store, c lossCheck) lossHealth {
	var h lossHealth
	since := time.Now().Add(-lossWindow)
	unasked, err := st.UnaskedKills(ctx, since)
	if err != nil {
		log.Warn("monitor cannot read the kills no router asked for", "err", err)
	}
	h.unasked = unasked
	failed, err := st.LostEnds(ctx, since)
	if err != nil {
		log.Warn("monitor cannot read the lost placements", "err", err)
	}
	h.failed = map[string]int{}
	for name, n := range failed {
		if c.alertPerHour > 0 && n > c.alertPerHour {
			h.failed[name] = n
		}
	}
	return h
}

func (h lossHealth) problems() []string {
	var out []string
	for _, name := range slices.Sorted(maps.Keys(h.unasked)) {
		out = append(out, fmt.Sprintf("provider %s: %d sandboxes killed that no router asked for",
			name, h.unasked[name]))
	}
	for _, name := range slices.Sorted(maps.Keys(h.failed)) {
		out = append(out, fmt.Sprintf("provider %s: %d placements ended as provider_failed in the "+
			"last hour", name, h.failed[name]))
	}
	return out
}
