package api

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/bootstrap"
	"github.com/bugnet-sh/bugnet-router/internal/budget"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// FetchSource reasons, named as CP 4.9 and 13.1 name them
const (
	reasonNoSource       = "NO_SOURCE"
	reasonToolMissing    = "TOOL_MISSING"
	reasonSecretRequired = "SECRET_REQUIRED"
	reasonKeyReserved    = "IDEMPOTENCY_KEY_RESERVED"
)

// FetchSource puts the placement's GitHub source in its sandbox, safe to call again
func (s *Server) FetchSource(
	ctx context.Context, req *connect.Request[routerv1.FetchSourceRequest],
) (*connect.Response[routerv1.FetchSourceResponse], error) {
	p, _, err := s.sandbox(ctx, req.Msg.GetHandle())
	if err != nil {
		return nil, s.status(err)
	}
	b, err := s.boot.Fetch(ctx, p)
	if err != nil {
		return nil, s.fetchStatus(err)
	}
	return connect.NewResponse(&routerv1.FetchSourceResponse{Bootstrap: b}), nil
}

// fetchStatus names the refusals of FetchSource, leaving other errors to status
func (s *Server) fetchStatus(err error) error {
	switch {
	case errors.Is(err, bootstrap.ErrNoSource):
		return withInfo(connect.NewError(connect.CodeFailedPrecondition, bootstrap.ErrNoSource),
			reasonNoSource)
	case errors.Is(err, bootstrap.ErrToolMissing):
		return withInfo(connect.NewError(connect.CodeFailedPrecondition,
			bootstrap.ErrToolMissing), reasonToolMissing)
	case errors.Is(err, bootstrap.ErrSecretRequired):
		return withInfo(connect.NewError(connect.CodeFailedPrecondition,
			bootstrap.ErrSecretRequired), reasonSecretRequired)
	case errors.Is(err, bootstrap.ErrNotConfigured):
		return withInfo(connect.NewError(connect.CodeFailedPrecondition,
			bootstrap.ErrNotConfigured), reasonFeatureUnavailable)
	case errors.Is(err, bootstrap.ErrRateLimited):
		return withRetry(withInfo(connect.NewError(connect.CodeResourceExhausted,
			bootstrap.ErrRateLimited), reasonRateLimited), bootstrap.RetryAfter)
	}
	return s.status(err)
}

// egressCounter counts a fetch's streamed bytes in the placement's egress budget
type egressCounter struct{ s *Server }

var _ bootstrap.Counter = egressCounter{}

// Left is what remains of the placement's egress budget, none once it was crossed
func (c egressCounter) Left(ctx context.Context, p store.Placement) (uint64, error) {
	lim := c.s.limit(p, budget.Egress)
	spent, err := c.s.meter.SpentFor(ctx, budget.Egress, lim)
	if err != nil || spent {
		return 0, err
	}
	return c.s.meter.Remaining(ctx, budget.Egress, lim)
}

// Add counts n bytes like the proxy's own reports do, announcing the budget when n crosses it
func (c egressCounter) Add(ctx context.Context, p store.Placement, n uint64) (uint64, error) {
	lim := c.s.limit(p, budget.Egress)
	answers, spent, err := c.s.meter.Report(ctx, budget.Egress, budget.ByEdge,
		[]budget.Usage{{Placement: p.ID, Out: n}}, map[uuid.UUID]budget.Limit{p.ID: lim})
	if err != nil {
		return 0, err
	}
	for _, id := range spent {
		c.s.events.Publish(ctx, &routerv1.EdgeEvent{Event: &routerv1.EdgeEvent_BudgetSpent{
			BudgetSpent: &routerv1.BudgetSpent{
				PlacementId: id.String(), Kind: routerv1.BudgetKind_BUDGET_KIND_EGRESS},
		}})
	}
	c.s.walletSpent(ctx, answers, map[uuid.UUID]budget.Limit{p.ID: lim})
	return answers[p.ID].Remaining, nil
}
