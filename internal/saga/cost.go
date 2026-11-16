package saga

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/budget"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/spend"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// quote prices an attempt of size on prov for its record, the hour and the start fee, a per-node
// provider's at the size's cap until its rented node's price is known
func quote(prov provider.Provider, size provider.Size) (hourly, fee int64) {
	if np, ok := provider.As[provider.NodePricer](prov); ok {
		hourly = np.CapHourlyMicros(size)
		if f, ok := np.StartCostAt(size, hourly); ok {
			fee = f
		}
		return hourly, fee
	}
	if c, ok := provider.As[provider.StartCoster](prov); ok {
		if f, ok := c.StartCostMicros(size); ok {
			fee = f
		}
	}
	return prov.HourlyCostMicros(size), fee
}

// rentedQuote is the hour and start fee of the node ref runs on, false for a provider that prices
// no node on its own, a ref that names no price or a size no longer sold
func rentedQuote(
	prov provider.Provider, size string, ref provider.Ref,
) (hourly, fee int64, ok bool) {
	np, isNode := provider.As[provider.NodePricer](prov)
	if !isNode {
		return 0, 0, false
	}
	sz, found := sizeNamed(prov, size)
	if !found {
		return 0, 0, false
	}
	if hourly, ok = np.RentedHourlyMicros(ref); !ok {
		return 0, 0, false
	}
	if fee, ok = np.StartCostAt(sz, hourly); !ok {
		return 0, 0, false
	}
	return hourly, fee, true
}

// rentCap is the most c's node may cost an hour for the order, the hourly figure c was priced at
// on a provider that prices each node, 0 for any other
func rentCap(c placement.Candidate) int64 {
	if _, ok := provider.As[provider.NodePricer](c.Provider); ok {
		return c.HourlyMicros
	}
	return 0
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

// judgeMargin logs at warn level a placement whose cost plus markup is over what its orders
// paid, and at error level one whose cost alone is over it (CP 10.7)
func (s *Saga) judgeMargin(p store.Placement) {
	v := spend.Judge(p.CostMicros, p.PaidMicros, s.d.Rule)
	if v.Loss {
		s.d.Log.Error("placement cost is over what it paid", "placement", p.ID,
			"provider", p.Provider, "cost_micros", p.CostMicros, "paid_micros", p.PaidMicros)
		return
	}
	if v.UnderMarkup {
		s.d.Log.Warn("placement cost is over its markup", "placement", p.ID,
			"provider", p.Provider, "cost_micros", p.CostMicros, "paid_micros", p.PaidMicros)
	}
}
