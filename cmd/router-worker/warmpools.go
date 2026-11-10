package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/pool"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

const (
	// noWarmAfter is how long a pool with a target may have no warm member before /monitor says so
	noWarmAfter = 10 * time.Minute
	// stuckFillAfter is how long a member may sit in filling before /monitor says so
	stuckFillAfter = 10 * time.Minute
)

// poolLines reports CP 18.18's pool lines, each a count or a name
func poolLines(log *slog.Logger, st *store.Store, m *pool.Manager) problemSource {
	return func(ctx context.Context) []string {
		if m == nil {
			return nil
		}
		now := time.Now()
		var out []string
		statuses, err := m.Status(ctx)
		if err != nil {
			log.Warn("monitor cannot read the pools", "err", err)
			return nil
		}
		for _, s := range statuses {
			name := s.Provider + " " + s.Size
			if s.Target > 0 && s.Warm == 0 && s.NoWarmFor > noWarmAfter {
				out = append(out, fmt.Sprintf("pool %s has had no warm member for %d minutes",
					name, int(s.NoWarmFor.Minutes())))
			}
			if !s.PausedUntil.IsZero() {
				out = append(out, fmt.Sprintf("pool %s refill is paused: %s", name, s.PausedReason))
			}
		}
		members, err := st.LiveMembers(ctx)
		if err != nil {
			log.Warn("monitor cannot read the pool members", "err", err)
			return out
		}
		for _, mem := range members {
			if mem.State == store.MemberFilling && now.Sub(mem.CreatedAt) > stuckFillAfter {
				out = append(out, fmt.Sprintf("pool member %d minutes in filling",
					int(now.Sub(mem.CreatedAt).Minutes())))
			}
		}
		capped, cost, limit, err := m.Spend(ctx)
		if err != nil {
			log.Warn("monitor cannot read the pool spend", "err", err)
			return out
		}
		if capped {
			out = append(out, fmt.Sprintf(
				"reservations are off: pool waste is $%.2f of the $%.2f daily cap",
				float64(cost)/1e6, float64(limit)/1e6))
		}
		return out
	}
}
