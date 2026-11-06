package api

import (
	"context"

	"connectrpc.com/connect"
	routerv1 "github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/oynozan/bugnet-router/internal/budget"
)

// EdgeServer implements routerv1connect.EdgeServiceHandler for bugnet-edge
type EdgeServer struct {
	// Answers Unimplemented for every RPC of the contract without a method here
	routerv1connect.UnimplementedEdgeServiceHandler
	s *Server
}

var _ routerv1connect.EdgeServiceHandler = (*EdgeServer)(nil)

// ReportEdgeUsage adds preview and transfer bytes and answers the bytes left and the pace
func (e *EdgeServer) ReportEdgeUsage(
	ctx context.Context, req *connect.Request[routerv1.ReportEdgeUsageRequest],
) (*connect.Response[routerv1.ReportEdgeUsageResponse], error) {
	left, pace, err := e.s.report(ctx, budget.Edge, budget.ByEdge, req.Msg.GetUsage())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&routerv1.ReportEdgeUsageResponse{
		BytesRemaining: left, BytesPerSecond: pace,
	}), nil
}

// CheckEdgeLeases answers the deadline and edge bytes left of each placement still ready
func (e *EdgeServer) CheckEdgeLeases(
	ctx context.Context, req *connect.Request[routerv1.CheckEdgeLeasesRequest],
) (*connect.Response[routerv1.CheckEdgeLeasesResponse], error) {
	ps, left, err := e.s.leases(ctx, budget.Edge, req.Msg.GetPlacementIds())
	if err != nil {
		return nil, err
	}
	leases := make(map[string]*routerv1.EdgeLease, len(ps))
	for _, p := range ps {
		leases[p.ID.String()] = &routerv1.EdgeLease{
			Deadline: timestamppb.New(*p.Deadline), BytesRemaining: left[p.ID],
		}
	}
	return connect.NewResponse(&routerv1.CheckEdgeLeasesResponse{Leases: leases}), nil
}

// WatchEdgeEvents streams the edge events after the cursor, from now when it is empty
func (e *EdgeServer) WatchEdgeEvents(
	ctx context.Context, req *connect.Request[routerv1.WatchEdgeEventsRequest],
	stream *connect.ServerStream[routerv1.WatchEdgeEventsResponse],
) error {
	send := func(c string, ev *routerv1.EdgeEvent) error {
		return stream.Send(&routerv1.WatchEdgeEventsResponse{Cursor: c, Event: ev})
	}
	return e.s.watch(ctx, req.Peer(), req.Msg.GetCursor(), send)
}
