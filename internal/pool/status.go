package pool

import (
	"context"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// KeptByRouter says the router keeps a pool's members, a provider-kept pool says provider
const (
	KeptByRouter   = "router"
	KeptByProvider = "provider"
)

// Status is what ListPools answers for one pool
type Status struct {
	Key
	KeptBy   string
	Target   int
	Min, Max int
	Warm     int
	Filling  int
	Held     int
	// ClaimsLastHour and MissesLastHour count orders and reservations
	ClaimsLastHour, MissesLastHour int
	// WastedLastDay counts members destroyed unused in the last day
	WastedLastDay int
	// CostMicrosLastDay is the idle cost and waste over the last day
	CostMicrosLastDay int64
	FillP95, WakeP95  time.Duration
	// PausedUntil and PausedReason are zero while the pool refills
	PausedUntil  time.Time
	PausedReason string
	// NoWarmFor is how long the pool has had no warm member, zero while it has one
	NoWarmFor time.Duration
}

// Status reports every configured pool as ListPools answers, read from Postgres alone
func (m *Manager) Status(ctx context.Context) ([]Status, error) {
	return ReadStatus(ctx, m.d.Store, m.cfgs, m.now(), Prices(m.d.Registry),
		Idle(m.d.Registry))
}

// PriceFunc gives the running and parked hourly prices of a provider's size
type PriceFunc func(provider, size string) (hourly, parked int64)

// ReadStatus reads the pools' status from Postgres, so router-api serves it without a worker's
// memory, prices and idle being nil for no costs and no provider-kept pools
func ReadStatus(
	ctx context.Context, st *store.Store, cfgs []Config, now time.Time, prices PriceFunc,
	idle IdleFunc,
) ([]Status, error) {
	stats, err := st.PoolStats(ctx, now)
	if err != nil {
		return nil, err
	}
	targets, err := st.Targets(ctx)
	if err != nil {
		return nil, err
	}
	pauses, err := st.Pauses(ctx)
	if err != nil {
		return nil, err
	}
	members, err := st.MembersSince(ctx, now.Add(-costDay))
	if err != nil {
		return nil, err
	}
	out := make([]Status, 0, len(cfgs))
	for _, c := range cfgs {
		s := Status{Key: c.Key, KeptBy: KeptByRouter, Min: c.Min, Max: c.Max, Target: c.Min}
		for _, p := range stats {
			if p.Provider != c.Provider || p.SizeName != c.Size {
				continue
			}
			s.Warm, s.Filling, s.Held = p.Warm, p.Filling, p.Held
			s.ClaimsLastHour, s.MissesLastHour = p.ClaimsLastHour, p.MissesLastHour
			s.WastedLastDay, s.FillP95, s.WakeP95 = p.WastedLastDay, p.FillP95, p.WakeP95
			if p.Warm == 0 && !p.LastWarmChange.IsZero() {
				s.NoWarmFor = max(now.Sub(p.LastWarmChange), 0)
			}
		}
		for _, t := range targets {
			if t.Provider == c.Provider && t.SizeName == c.Size &&
				now.Sub(t.ComputedAt) <= staleTarget {
				s.Target = min(max(t.Target, c.Min), c.Max)
			}
		}
		if p := pauseOf(pauses, c.Key); p.Until.After(now) {
			s.PausedUntil, s.PausedReason = p.Until, p.Reason
		}
		// A provider-kept pool has no member rows, so its idle cost is its target for the whole day
		if idle != nil {
			if hourly, kept := idle(c.Provider, c.Size); kept {
				s.KeptBy = KeptByProvider
				s.CostMicrosLastDay += int64(s.Target) * hourly * int64(costDay/time.Hour)
			}
		}
		if prices != nil {
			hourly, parked := prices(c.Provider, c.Size)
			for _, mem := range members {
				if mem.Provider == c.Provider && mem.SizeName == c.Size {
					s.CostMicrosLastDay += OverheadMicros(mem, hourly, parked,
						now.Add(-costDay), now)
				}
			}
		}
		out = append(out, s)
	}
	return out, nil
}
