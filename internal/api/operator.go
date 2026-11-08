package api

import "github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"

// OperatorServer implements routerv1connect.OperatorServiceHandler for bugnet-dev's operators
type OperatorServer struct {
	// Answers Unimplemented for every RPC of the contract without a method here
	routerv1connect.UnimplementedOperatorServiceHandler
	s *Server
}

var _ routerv1connect.OperatorServiceHandler = (*OperatorServer)(nil)
