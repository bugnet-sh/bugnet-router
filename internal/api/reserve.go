package api

import (
	"context"
	"net/netip"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bugnet-sh/bugnet-router/internal/saga"
)

// skips maps what Reserve held back to the contract's reasons
var skips = map[saga.Skip]routerv1.ReserveSkip{
	saga.SkipDisabled:    routerv1.ReserveSkip_RESERVE_SKIP_DISABLED,
	saga.SkipNoWarm:      routerv1.ReserveSkip_RESERVE_SKIP_NO_WARM,
	saga.SkipBusy:        routerv1.ReserveSkip_RESERVE_SKIP_BUSY,
	saga.SkipBraked:      routerv1.ReserveSkip_RESERVE_SKIP_BRAKED,
	saga.SkipQuoteSpent:  routerv1.ReserveSkip_RESERVE_SKIP_QUOTE_SPENT,
	saga.SkipNotPoolable: routerv1.ReserveSkip_RESERVE_SKIP_NOT_POOLABLE,
}

// Reserve holds a prepared sandbox for the quote's order, or says why it holds none
//
// The answer names no provider, no sandbox and no reservation id. Capacity, caps and provider
// trouble answer SKIPPED, never an error. Only a bad quote id or an unknown package is refused.
func (s *Server) Reserve(
	ctx context.Context, req *connect.Request[routerv1.ReserveRequest],
) (*connect.Response[routerv1.ReserveResponse], error) {
	m := req.Msg
	// An address that does not parse counts as unknown, never as a refusal
	ip, _ := netip.ParseAddr(m.GetClientIp())
	out, err := s.d.Saga.Reserve(ctx, saga.ReserveInput{
		QuoteID: m.GetQuoteId(), PackageID: m.GetPackageId(),
		Requirements: requirements(m.GetRequirements()), ClientIP: ip,
	})
	if err != nil {
		return nil, s.status(err)
	}
	if out.Skip != "" {
		return connect.NewResponse(&routerv1.ReserveResponse{
			Outcome: routerv1.ReserveOutcome_RESERVE_OUTCOME_SKIPPED, Skip: skips[out.Skip],
		}), nil
	}
	return connect.NewResponse(&routerv1.ReserveResponse{
		Outcome:   routerv1.ReserveOutcome_RESERVE_OUTCOME_HELD,
		ExpiresAt: timestamppb.New(out.ExpiresAt),
	}), nil
}

// ReleaseReservation gives back the quote's open reservation, and answers OK for a quote with none
func (s *Server) ReleaseReservation(
	ctx context.Context, req *connect.Request[routerv1.ReleaseReservationRequest],
) (*connect.Response[routerv1.ReleaseReservationResponse], error) {
	if err := s.d.Saga.ReleaseReservation(ctx, req.Msg.GetQuoteId()); err != nil {
		return nil, s.status(err)
	}
	return connect.NewResponse(&routerv1.ReleaseReservationResponse{}), nil
}
