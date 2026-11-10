package pool

import (
	"context"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// costDay is the window the spend cap and the pool costs read
const costDay = 24 * time.Hour

// OverheadMicros is what a member cost the pool inside [from, to]
//
// Running time before the member is warm is billed at the provider's hourly price and parked time
// at the parked price. Once a placement binds a member, its running time is the order's, so only
// a member that is held or ends unbound keeps being billed here.
func OverheadMicros(
	mem store.Member, hourly, parked int64, from, to time.Time,
) int64 {
	end := to
	if mem.EndedAt != nil && mem.EndedAt.Before(end) {
		end = *mem.EndedAt
	}
	warmAt := end
	if mem.WarmAt != nil && mem.WarmAt.Before(warmAt) {
		warmAt = *mem.WarmAt
	}
	idleEnd := end
	if mem.ClaimedAt != nil && mem.ClaimedAt.Before(idleEnd) {
		idleEnd = *mem.ClaimedAt
	}
	total := price(hourly, mem.CreatedAt, warmAt, from, to)
	if mem.WarmAt != nil {
		total += price(parked, *mem.WarmAt, idleEnd, from, to)
	}
	// A claimed member that no placement bound keeps running at the full price until it ends
	if mem.ClaimedAt != nil && mem.State != store.MemberBound {
		total += price(hourly, *mem.ClaimedAt, end, from, to)
	}
	return total
}

// price bills a price an hour over [a, b] cut to [from, to], rounded up to a micro
func price(perHour int64, a, b, from, to time.Time) int64 {
	if a.Before(from) {
		a = from
	}
	if b.After(to) {
		b = to
	}
	if !b.After(a) || perHour <= 0 {
		return 0
	}
	return (perHour*b.Sub(a).Milliseconds() + 3_599_999) / 3_600_000
}

// Prices gives the running and parked hourly prices of the sizes the registry's providers sell
func Prices(reg *provider.Registry) PriceFunc {
	return func(name, size string) (hourly, parked int64) {
		prov, ok := reg.Get(name)
		if !ok {
			return 0, 0
		}
		sz, ok := sizeOf(prov, size)
		if !ok {
			return 0, 0
		}
		hourly = prov.HourlyCostMicros(sz)
		if pk, ok := prov.(provider.Parker); ok {
			parked = pk.ParkedHourlyCostMicros(sz)
		}
		return hourly, parked
	}
}

// spendCapped reports whether the last day's waste and idle cost over all pools reached the cap
func (m *Manager) spendCapped(ctx context.Context, now time.Time) (bool, int64, error) {
	limit := m.d.Settings.MaxDailyMicros
	if limit <= 0 {
		return false, 0, nil
	}
	members, err := m.d.Store.MembersSince(ctx, now.Add(-costDay))
	if err != nil {
		return false, 0, err
	}
	prices := Prices(m.d.Registry)
	var total int64
	for _, mem := range members {
		hourly, parked := prices(mem.Provider, mem.SizeName)
		total += OverheadMicros(mem, hourly, parked, now.Add(-costDay), now)
	}
	return total >= limit, total, nil
}

// Spend reports whether the last day's waste and idle cost reached the cap, with the cost and the
// cap in micros
func (m *Manager) Spend(ctx context.Context) (capped bool, cost, limit int64, err error) {
	capped, cost, err = m.spendCapped(ctx, m.now())
	return capped, cost, m.d.Settings.MaxDailyMicros, err
}
