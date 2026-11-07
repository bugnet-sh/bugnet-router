package api_test

import (
	"context"
	"slices"
	"testing"

	"connectrpc.com/connect"
	routerv1 "github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
)

// unbuilt maps each contract RPC the router does not serve yet to a call of it
func unbuilt(
	ctx context.Context, c routerv1connect.SandboxRouterServiceClient,
) map[string]func() error {
	const h = "sbx_aaaaaaaaaaaaaaaaaaaaaaaaaa"
	return map[string]func() error{
		"CreatePreview": func() error {
			_, err := c.CreatePreview(ctx, connect.NewRequest(
				&routerv1.CreatePreviewRequest{Handle: h, Port: 3000}))
			return err
		},
		"AddSshKey": func() error {
			_, err := c.AddSshKey(ctx, connect.NewRequest(&routerv1.AddSshKeyRequest{Handle: h}))
			return err
		},
		"RemoveSshKey": func() error {
			_, err := c.RemoveSshKey(ctx, connect.NewRequest(
				&routerv1.RemoveSshKeyRequest{Handle: h}))
			return err
		},
		"CreateSecretLink": func() error {
			_, err := c.CreateSecretLink(ctx, connect.NewRequest(
				&routerv1.CreateSecretLinkRequest{Handle: h}))
			return err
		},
		"CreateSecretPutGrant": func() error {
			_, err := c.CreateSecretPutGrant(ctx, connect.NewRequest(
				&routerv1.CreateSecretPutGrantRequest{Handle: h}))
			return err
		},
		"GetSecretSealingKey": func() error {
			_, err := c.GetSecretSealingKey(ctx, connect.NewRequest(
				&routerv1.GetSecretSealingKeyRequest{Handle: h}))
			return err
		},
		"PutSecret": func() error {
			_, err := c.PutSecret(ctx, connect.NewRequest(&routerv1.PutSecretRequest{Handle: h}))
			return err
		},
		"ListSecrets": func() error {
			_, err := c.ListSecrets(ctx, connect.NewRequest(
				&routerv1.ListSecretsRequest{Handle: h}))
			return err
		},
		"DeleteSecret": func() error {
			_, err := c.DeleteSecret(ctx, connect.NewRequest(
				&routerv1.DeleteSecretRequest{Handle: h}))
			return err
		},
		"FetchSource": func() error {
			_, err := c.FetchSource(ctx, connect.NewRequest(
				&routerv1.FetchSourceRequest{Handle: h}))
			return err
		},
		"CheckX402": func() error {
			_, err := c.CheckX402(ctx, connect.NewRequest(&routerv1.CheckX402Request{}))
			return err
		},
		"EnableX402": func() error {
			_, err := c.EnableX402(ctx, connect.NewRequest(
				&routerv1.EnableX402Request{Handle: h}))
			return err
		},
		"ListX402": func() error {
			_, err := c.ListX402(ctx, connect.NewRequest(&routerv1.ListX402Request{Handle: h}))
			return err
		},
	}
}

// built names the RPCs Server implements
var built = []string{
	"Estimate", "PricePackage", "CreatePlacement", "GetPlacement", "WatchPlacement",
	"ExtendPlacement", "CheckExtendPlacement", "ListExtensions", "StopPlacement", "Exec",
	"WatchJob", "CancelJob", "ReadFile", "WriteFile", "ListFiles", "CreateTransfer",
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
