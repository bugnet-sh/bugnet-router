package api

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	routerv1 "github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"

	"github.com/oynozan/bugnet-router/internal/store"
)

// PaymentsServer implements routerv1connect.PaymentsServiceHandler for Payments
type PaymentsServer struct {
	// Answers Unimplemented for every RPC of the contract without a method here
	routerv1connect.UnimplementedPaymentsServiceHandler
	s *Server
}

var _ routerv1connect.PaymentsServiceHandler = (*PaymentsServer)(nil)

// GetOrder says what became of an order, redeemed false when the router never took it
func (ps *PaymentsServer) GetOrder(
	ctx context.Context, req *connect.Request[routerv1.GetOrderRequest],
) (*connect.Response[routerv1.GetOrderResponse], error) {
	id := req.Msg.GetOrderId()
	if id == "" {
		return nil, invalid("order_id is required")
	}
	o, err := ps.s.d.Store.OrderByID(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return connect.NewResponse(&routerv1.GetOrderResponse{}), nil
	}
	if err != nil {
		return nil, ps.s.status(err)
	}
	p, err := ps.s.d.Store.PlacementByID(ctx, o.PlacementID)
	if err != nil {
		return nil, ps.s.status(err)
	}
	msg := placementMsg(p)
	return connect.NewResponse(&routerv1.GetOrderResponse{
		Redeemed:       true,
		Kind:           o.Kind,
		PlacementState: p.State,
		// An extension is recorded only once applied, so a refund is the start order's
		Refundable:        o.Kind == store.OrderStart && p.Refundable,
		FailureReason:     msg.GetFailureReason(),
		TerminationReason: msg.GetTerminationReason(),
		EndedAt:           msg.GetEndedAt(),
	}), nil
}
