package api

import (
	"context"

	"connectrpc.com/connect"
	routerv1 "github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"

	"github.com/oynozan/bugnet-router/internal/budget"
)

// SSHGatewayServer implements routerv1connect.SshGatewayServiceHandler for bugnet-ssh
type SSHGatewayServer struct {
	// Answers Unimplemented for every RPC of the contract without a method here
	routerv1connect.UnimplementedSshGatewayServiceHandler
	s *Server
}

var _ routerv1connect.SshGatewayServiceHandler = (*SSHGatewayServer)(nil)

// ReportSshUsage adds SSH bytes to the edge budget and answers the bytes left and the pace
func (g *SSHGatewayServer) ReportSshUsage(
	ctx context.Context, req *connect.Request[routerv1.ReportSshUsageRequest],
) (*connect.Response[routerv1.ReportSshUsageResponse], error) {
	left, pace, err := g.s.report(ctx, budget.Edge, budget.BySSH, req.Msg.GetUsage())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&routerv1.ReportSshUsageResponse{
		BytesRemaining: left, BytesPerSecond: pace,
	}), nil
}

// WatchSshEvents streams the edge events after the cursor, from now when it is empty
func (g *SSHGatewayServer) WatchSshEvents(
	ctx context.Context, req *connect.Request[routerv1.WatchSshEventsRequest],
	stream *connect.ServerStream[routerv1.WatchSshEventsResponse],
) error {
	return g.s.watch(ctx, req.Msg.GetCursor(), func(c string, ev *routerv1.EdgeEvent) error {
		return stream.Send(&routerv1.WatchSshEventsResponse{Cursor: c, Event: ev})
	})
}
