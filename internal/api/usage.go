package api

import (
	"context"
	"net/http"
	"slices"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	routerv1 "github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1"

	"github.com/oynozan/bugnet-router/internal/budget"
	"github.com/oynozan/bugnet-router/internal/store"
)

// budgetKinds names each budget as the edge events do
var budgetKinds = map[budget.Kind]routerv1.BudgetKind{
	budget.Edge:   routerv1.BudgetKind_BUDGET_KIND_EDGE,
	budget.Egress: routerv1.BudgetKind_BUDGET_KIND_EGRESS,
}

// live returns the ready placements among ids, leaving out malformed, unknown and ended ones
func (s *Server) live(ctx context.Context, ids []string) ([]store.Placement, error) {
	parsed := make([]uuid.UUID, 0, len(ids))
	for _, raw := range ids {
		if id, err := uuid.Parse(raw); err == nil {
			parsed = append(parsed, id)
		}
	}
	all, err := s.d.Store.PlacementsByIDs(ctx, parsed)
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(all, func(p store.Placement) bool {
		return p.State != store.PlacementReady || p.Deadline == nil
	}), nil
}

// limit is what the placement may use of kind, at its start package's pace
func (s *Server) limit(p store.Placement, kind budget.Kind) budget.Limit {
	l := budget.Limit{Placement: p.ID, Budget: p.EdgeBudgetBytes, Deadline: *p.Deadline}
	if kind == budget.Egress {
		l.Budget = p.EgressBudgetBytes
	}
	if pkg, ok := s.d.Catalog.Get(p.PackageID); ok {
		l.BytesPerSecond = budget.PaceFor(pkg, s.d.Budget, kind)
	}
	return l
}

// report adds one process's usage and answers the bytes left and pace by placement id
func (s *Server) report(
	ctx context.Context, kind budget.Kind, by budget.Carrier, in []*routerv1.PlacementUsage,
) (left, pace map[string]uint64, err error) {
	ids := make([]string, len(in))
	for i, u := range in {
		ids[i] = u.GetPlacementId()
	}
	ps, err := s.live(ctx, ids)
	if err != nil {
		return nil, nil, s.status(err)
	}
	limits := make(map[uuid.UUID]budget.Limit, len(ps))
	for _, p := range ps {
		limits[p.ID] = s.limit(p, kind)
	}
	usage := make([]budget.Usage, 0, len(in))
	for _, u := range in {
		if id, err := uuid.Parse(u.GetPlacementId()); err == nil {
			usage = append(usage, budget.Usage{Placement: id, In: u.GetBytesIn(),
				Out: u.GetBytesOut()})
		}
	}
	beforeReport()
	answers, spent, err := s.meter.Report(ctx, kind, by, usage, limits)
	if err != nil {
		return nil, nil, s.status(err)
	}
	for _, id := range s.crossed(ctx, kind, spent, limits, answers) {
		s.events.Publish(ctx, &routerv1.EdgeEvent{Event: &routerv1.EdgeEvent_BudgetSpent{
			BudgetSpent: &routerv1.BudgetSpent{PlacementId: id.String(), Kind: budgetKinds[kind]},
		}})
	}
	left = make(map[string]uint64, len(answers))
	pace = make(map[string]uint64, len(answers))
	for id, a := range answers {
		left[id.String()], pace[id.String()] = a.Remaining, a.Pace
	}
	return left, pace, nil
}

// beforeReport runs between a report reading its budgets and counting its bytes, for tests
var beforeReport = func() {}

// crossed keeps the placements whose crossing stands, taking back one an extension outgrew
func (s *Server) crossed(
	ctx context.Context, kind budget.Kind, spent []uuid.UUID, limits map[uuid.UUID]budget.Limit,
	answers map[uuid.UUID]budget.Answer,
) []uuid.UUID {
	if len(spent) == 0 {
		return nil
	}
	// The budgets were read before the bytes were counted, so an extension may have come between
	now, err := s.d.Store.PlacementsByIDs(ctx, spent)
	if err != nil {
		s.d.Log.Warn("api: reread budgets after a crossing, keeping it", "err", err)
		return spent
	}
	grown := map[uuid.UUID]budget.Limit{}
	for _, p := range now {
		if p.Deadline == nil {
			continue
		}
		if l := s.limit(p, kind); l.Budget > limits[p.ID].Budget {
			grown[p.ID] = l
		}
	}
	var out []uuid.UUID
	for _, id := range spent {
		l, ok := grown[id]
		if !ok {
			out = append(out, id)
			continue
		}
		back, err := s.meter.Uncross(ctx, kind, id, l.Budget)
		if err != nil || !back {
			out = append(out, id)
			continue
		}
		if left, err := s.meter.Remaining(ctx, kind, l); err == nil {
			a := answers[id]
			a.Remaining = left
			answers[id] = a
		}
	}
	return out
}

// leases returns the live placements among ids and the bytes each has left of kind
func (s *Server) leases(
	ctx context.Context, kind budget.Kind, ids []string,
) ([]store.Placement, map[uuid.UUID]uint64, error) {
	ps, err := s.live(ctx, ids)
	if err != nil {
		return nil, nil, s.status(err)
	}
	limits := make([]budget.Limit, len(ps))
	for i, p := range ps {
		limits[i] = s.limit(p, kind)
	}
	left, err := s.meter.RemainingAll(ctx, kind, limits)
	if err != nil {
		return nil, nil, s.status(err)
	}
	return ps, left, nil
}

// writerKey carries a request's ResponseWriter in its context
type writerKey struct{}

// withWriter hands h's handlers the ResponseWriter, so a stream can send its headers early
func withWriter(path string, h http.Handler) (string, http.Handler) {
	return path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), writerKey{}, w)))
	})
}

// sendHeaders flushes a stream's headers at once, so the caller's open returns before any event
func sendHeaders(ctx context.Context) {
	if w, ok := ctx.Value(writerKey{}).(http.ResponseWriter); ok {
		_ = http.NewResponseController(w).Flush()
	}
}

// watch sends the edge events after cursor until the caller or the server goes away
func (s *Server) watch(
	ctx context.Context, peer connect.Peer, cursor string,
	send func(string, *routerv1.EdgeEvent) error,
) error {
	// gRPC-Web carries an error before the first message in the headers, so they must wait
	if peer.Protocol != connect.ProtocolGRPCWeb {
		sendHeaders(ctx)
	}
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(s.ctx, cancel)()
	err := s.events.Watch(wctx, cursor, send)
	switch {
	case err == nil:
		return nil
	case ctx.Err() != nil:
		return s.status(ctx.Err())
	case s.ctx.Err() != nil:
		return s.status(errShuttingDown)
	}
	return s.status(err)
}
