package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/store"
)

const (
	// hedgeLoserAfter is how long a hedge loser may outlive its placement's ready before it is a
	// problem, the worker's sweep having destroyed it within a minute by then
	hedgeLoserAfter = 5 * time.Minute
	// maxHedgeLines bounds how many placements /monitor names
	maxHedgeLines = 10
)

// hedgeLines reports CP 18.18's hedge loser line for each placement whose loser still exists
func hedgeLines(log *slog.Logger, st *store.Store) problemSource {
	return func(ctx context.Context) []string {
		ids, err := st.HedgeLosersLeft(ctx, time.Now().Add(-hedgeLoserAfter), maxHedgeLines)
		if err != nil {
			log.Warn("monitor cannot read the hedge losers", "err", err)
			return nil
		}
		var out []string
		for _, id := range ids {
			out = append(out, fmt.Sprintf(
				"hedge loser of placement %s still exists after 5 minutes", id))
		}
		return out
	}
}
