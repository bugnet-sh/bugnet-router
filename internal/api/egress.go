package api

import (
	"context"

	"connectrpc.com/connect"
	routerv1 "github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/oynozan/bugnet-router/internal/budget"
)

// EgressServer implements routerv1connect.EgressServiceHandler for bugnet-egress
type EgressServer struct {
	// Answers Unimplemented for every RPC of the contract without a method here
	routerv1connect.UnimplementedEgressServiceHandler
	s *Server
}

var _ routerv1connect.EgressServiceHandler = (*EgressServer)(nil)

// ReportEgressUsage adds tunnel bytes to the egress budget and answers the bytes left
func (g *EgressServer) ReportEgressUsage(
	ctx context.Context, req *connect.Request[routerv1.ReportEgressUsageRequest],
) (*connect.Response[routerv1.ReportEgressUsageResponse], error) {
	left, _, err := g.s.report(ctx, budget.Egress, budget.ByEdge, req.Msg.GetUsage())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&routerv1.ReportEgressUsageResponse{BytesRemaining: left}), nil
}

// CheckEgressLeases answers each ready placement's deadline, egress bytes left and secrets version
func (g *EgressServer) CheckEgressLeases(
	ctx context.Context, req *connect.Request[routerv1.CheckEgressLeasesRequest],
) (*connect.Response[routerv1.CheckEgressLeasesResponse], error) {
	ps, left, err := g.s.leases(ctx, budget.Egress, req.Msg.GetPlacementIds())
	if err != nil {
		return nil, err
	}
	leases := make(map[string]*routerv1.EgressLease, len(ps))
	for _, p := range ps {
		leases[p.ID.String()] = &routerv1.EgressLease{
			Deadline: timestamppb.New(*p.Deadline), BytesRemaining: left[p.ID],
			SecretsVersion: uint64(max(p.SecretsVersion, 0)),
		}
	}
	return connect.NewResponse(&routerv1.CheckEgressLeasesResponse{Leases: leases}), nil
}

// WatchEgressEvents streams the edge events after the cursor, from now when it is empty
func (g *EgressServer) WatchEgressEvents(
	ctx context.Context, req *connect.Request[routerv1.WatchEgressEventsRequest],
	stream *connect.ServerStream[routerv1.WatchEgressEventsResponse],
) error {
	send := func(c string, ev *routerv1.EdgeEvent) error {
		return stream.Send(&routerv1.WatchEgressEventsResponse{Cursor: c, Event: ev})
	}
	return g.s.watch(ctx, req.Peer(), req.Msg.GetCursor(), send)
}
