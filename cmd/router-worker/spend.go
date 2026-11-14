package main

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/pricing"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// spendWindow is how far back /monitor counts placements that ended over their price
const spendWindow = 24 * time.Hour

// costDays is how many UTC days back /monitor shows the daily cost check's mismatches
const costDays = 3

// spendCheck holds the pricing rule and the settings of reconcile.cost_alert_fraction
type spendCheck struct {
	rule              pricing.Rule
	costAlertFraction float64
}

// spendHealth is what the last day showed of what placements cost and what providers bill
type spendHealth struct {
	// under counts the placements that ended costing over their markup, loss those costing over
	// what they paid
	under, loss int
	// foreign counts each provider's sandboxes no known deployment owns
	foreign map[string]int
	// alerts are the daily checks that found a mismatch
	alerts   []store.CostDay
	fraction float64
}

// checkSpend reads the margin counts of the last 24 h, the foreign counts and the cost mismatches
func checkSpend(ctx context.Context, log *slog.Logger, st *store.Store, c spendCheck) spendHealth {
	h := spendHealth{fraction: c.costAlertFraction}
	now := time.Now()
	var err error
	h.under, h.loss, err = st.MarginCounts(ctx, now.Add(-spendWindow), c.rule)
	if err != nil {
		log.Warn("monitor cannot read the margins", "err", err)
	}
	foreign, err := st.ForeignSandboxes(ctx)
	if err != nil {
		log.Warn("monitor cannot read the foreign sandboxes", "err", err)
	}
	h.foreign = map[string]int{}
	for name, n := range foreign {
		if n > 0 {
			h.foreign[name] = n
		}
	}
	since := now.UTC().Truncate(24*time.Hour).AddDate(0, 0, -costDays)
	h.alerts, err = st.CostAlerts(ctx, since, c.costAlertFraction)
	if err != nil {
		log.Warn("monitor cannot read the cost checks", "err", err)
	}
	return h
}

func (h spendHealth) problems() []string {
	var out []string
	if h.loss > 0 {
		out = append(out, fmt.Sprintf(
			"%d placements ended costing more than they paid in the last 24 hours, %d more cost "+
				"over their markup", h.loss, h.under-h.loss))
	}
	for _, name := range slices.Sorted(maps.Keys(h.foreign)) {
		out = append(out, fmt.Sprintf("provider %s: %d sandboxes no known deployment owns",
			name, h.foreign[name]))
	}
	for _, a := range h.alerts {
		day := a.Day.Format("2006-01-02")
		if a.TheirsMicros >= 0 &&
			float64(a.TheirsMicros) > float64(a.OursMicros)*(1+h.fraction) {
			out = append(out, fmt.Sprintf("provider %s billed %d micros on %s against our %d",
				a.Provider, a.TheirsMicros, day, a.OursMicros))
		}
		if a.UnknownIDs > 0 {
			out = append(out, fmt.Sprintf("provider %s billed %d sandboxes that are not ours on %s",
				a.Provider, a.UnknownIDs, day))
		}
	}
	return out
}
