package api_test

import (
	"context"
	"slices"
	"testing"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
)

// unbuilt maps each contract RPC the router does not serve yet to a call of it, none being left
func unbuilt(
	ctx context.Context, c routerv1connect.SandboxRouterServiceClient,
) map[string]func() error {
	return map[string]func() error{
		"Reserve": func() error {
			_, err := c.Reserve(ctx, connect.NewRequest(&routerv1.ReserveRequest{QuoteId: "q"}))
			return err
		},
		"ReleaseReservation": func() error {
			_, err := c.ReleaseReservation(ctx,
				connect.NewRequest(&routerv1.ReleaseReservationRequest{QuoteId: "q"}))
			return err
		},
	}
}

// built names the RPCs Server implements
var built = []string{
	"Estimate", "PricePackage", "CreatePlacement", "GetPlacement", "WatchPlacement",
	"ExtendPlacement", "CheckExtendPlacement", "ListExtensions", "StopPlacement", "Exec",
	"WatchJob", "CancelJob", "ReadFile", "WriteFile", "ListFiles", "CreateTransfer",
	"CreatePreview", "CreateSecretLink", "CreateSecretPutGrant", "GetSecretSealingKey",
	"PutSecret", "ListSecrets", "DeleteSecret", "AddSshKey", "RemoveSshKey", "CheckX402",
	"ListX402", "EnableX402", "AppendSessionEvents", "RotateSessionLink", "FetchSource",
}

func TestUnbuiltHoldsEveryOtherRPC(t *testing.T) {
	calls := unbuilt(t.Context(), nil)
	methods := routerv1.File_bugnet_router_v1_router_proto.Services().
		ByName("SandboxRouterService").Methods()
	var missing []string
	for i := range methods.Len() {
		name := string(methods.Get(i).Name())
		if !slices.Contains(built, name) && calls[name] == nil {
			missing = append(missing, name)
		}
	}
	eq(t, missing, []string(nil))
	eq(t, len(built)+len(calls), methods.Len())
}

func TestNewRPCsAnswerUnimplemented(t *testing.T) {
	e := newEnv(t, options{})
	for name, call := range unbuilt(t.Context(), e.client) {
		t.Run(name, func(t *testing.T) {
			mustCode(t, call(), connect.CodeUnimplemented, "")
		})
	}
}

func TestNewRPCsNeedTheInternalToken(t *testing.T) {
	e := newEnv(t, options{})
	stranger := e.clientWith(t, "Bearer wrong-token-0123456789abcdef0123456789", true)
	for name, call := range unbuilt(t.Context(), stranger) {
		t.Run(name, func(t *testing.T) {
			mustCode(t, call(), connect.CodeUnauthenticated, "")
		})
	}
}

func TestPoolRPCsAnswerUnimplemented(t *testing.T) {
	e := newEnv(t, options{})
	op := e.operator(t, operatorToken)
	_, err := op.ListPools(t.Context(), connect.NewRequest(&routerv1.ListPoolsRequest{}))
	mustCode(t, err, connect.CodeUnimplemented, "")
	_, err = op.DrainPool(t.Context(), connect.NewRequest(&routerv1.DrainPoolRequest{}))
	mustCode(t, err, connect.CodeUnimplemented, "")
	stranger := e.operator(t, "Bearer wrong-token-0123456789abcdef0123456789")
	_, err = stranger.ListPools(t.Context(), connect.NewRequest(&routerv1.ListPoolsRequest{}))
	mustCode(t, err, connect.CodeUnauthenticated, "")
}
