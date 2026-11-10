package saga

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/budget"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/spend"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// quote prices an attempt of size on prov for its record, the hour and the start fee
func quote(prov provider.Provider, size provider.Size) (hourly, fee int64) {
	if c, ok := provider.As[provider.StartCoster](prov); ok {
		if f, ok := c.StartCostMicros(size); ok {
			fee = f
		}
	}
	return prov.HourlyCostMicros(size), fee
}

// markDestroyed stamps the attempt that made a destroyed sandbox, a failure only logging since
// the stamp prices an attempt and never gates a destroy
func (s *Saga) markDestroyed(ctx context.Context, prov provider.Provider, ref provider.Ref) {
	if ref.ID == "" {
		return
	}
	ctx, cancel := step(context.WithoutCancel(ctx))
	defer cancel()
	if err := s.d.Store.MarkSandboxDestroyed(ctx, prov.Name(), ref.ID, s.now()); err != nil {
		s.d.Log.Error("stamp destroyed sandbox", "provider", prov.Name(), "sandbox", ref.ID,
			"err", err)
	}
}

// spansCost prices attempts with the billing increments of the registry's providers
func (s *Saga) spansCost(spans []store.AttemptSpan, end time.Time) int64 {
	return spend.SpansCost(spans, end, func(name string) time.Duration {
		if prov, ok := s.d.Registry.Get(name); ok {
			return prov.BillingIncrement()
		}
		return 0
	})
}

// usedBytes reads what the edges and the egress proxy counted for a placement, zero for a counter
// that cannot be read since the end must not wait on it
func (s *Saga) usedBytes(ctx context.Context, id uuid.UUID) (edge, egress int64) {
	var err error
	if edge, err = s.meter.Used(ctx, budget.Edge, id); err != nil {
		s.d.Log.Warn("read edge bytes", "placement", id, "err", err)
	}
	if egress, err = s.meter.Used(ctx, budget.Egress, id); err != nil {
		s.d.Log.Warn("read egress bytes", "placement", id, "err", err)
	}
	return edge, egress
}

// judgeMargin logs at error level a placement whose cost is over (1 - margin_floor) of what its
// orders paid (CP 10.7)
func (s *Saga) judgeMargin(p store.Placement) {
	v := spend.Judge(p.CostMicros, p.PaidMicros, s.d.MarginFloor)
	if !v.BelowFloor {
		return
	}
	s.d.Log.Error("placement cost is over the margin floor", "placement", p.ID,
		"provider", p.Provider, "cost_micros", p.CostMicros, "paid_micros", p.PaidMicros,
		"loss", v.Loss)
}
