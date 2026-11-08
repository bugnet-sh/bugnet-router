package api

import "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"

// SecretsRouteServer implements routerv1connect.SecretsRouteServiceHandler for bugnet-secrets
type SecretsRouteServer struct {
	// Answers Unimplemented for every RPC of the contract without a method here
	routerv1connect.UnimplementedSecretsRouteServiceHandler
}

var _ routerv1connect.SecretsRouteServiceHandler = (*SecretsRouteServer)(nil)
