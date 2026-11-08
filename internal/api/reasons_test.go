package api_test

import (
	"bytes"
	"crypto/rand"
	"errors"
	"slices"
	"strings"
	"testing"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"

	"github.com/bugnet-sh/bugnet-router/internal/handle"
)

// handleCalls maps each built RPC that takes a handle to a call of it on the handle given
func (e *env) handleCalls(t *testing.T) map[string]func(h string) error {
	ctx := t.Context()
	calls := e.sandboxOps(t)
	calls["GetPlacement"] = func(h string) error {
		_, err := e.client.GetPlacement(ctx, connect.NewRequest(
			&routerv1.GetPlacementRequest{Handle: h}))
		return err
	}
	calls["WatchPlacement"] = func(h string) error { return e.watchFirst(t, h) }
	calls["ExtendPlacement"] = func(h string) error {
		// The token must name a well formed handle, and a malformed one fails before the match
		bound := h
		if _, err := handle.Hash(h); err != nil {
			bound, _ = handle.New()
		}
		_, err := e.client.ExtendPlacement(ctx, connect.NewRequest(
			&routerv1.ExtendPlacementRequest{
				Handle: h, OrderToken: e.extendToken(t, bound, morePkg, morePrice),
			}))
		return err
	}
	calls["CheckExtendPlacement"] = func(h string) error {
		_, err := e.checkExtend(t, h, morePkg)
		return err
	}
	calls["ListExtensions"] = func(h string) error {
		_, err := e.listExtensions(t, h)
		return err
	}
	calls["StopPlacement"] = func(h string) error {
		_, err := e.client.StopPlacement(ctx, connect.NewRequest(
			&routerv1.StopPlacementRequest{Handle: h}))
		return err
	}
	for name, call := range e.secretCalls(t) {
		calls[name] = call
	}
	calls["AddSshKey"] = func(h string) error {
		_, err := e.client.AddSshKey(ctx, connect.NewRequest(
			&routerv1.AddSshKeyRequest{Handle: h, PublicKey: "ssh-ed25519 AAAA"}))
		return err
	}
	calls["RemoveSshKey"] = func(h string) error {
		_, err := e.client.RemoveSshKey(ctx, connect.NewRequest(
			&routerv1.RemoveSshKeyRequest{Handle: h, Fingerprint: "SHA256:x"}))
		return err
	}
	calls["CheckX402"] = func(h string) error {
		return e.checkX402(t, h, specsOn("api.example.com"))
	}
	calls["ListX402"] = func(h string) error {
		_, err := e.listX402(t, h)
		return err
	}
	calls["EnableX402"] = func(h string) error {
		_, err := e.enableX402(t, h, "r1", false, specsOn("api.example.com"))
		return err
	}
	return calls
}

func TestHandleCallsHoldEveryBuiltRPCWithAHandle(t *testing.T) {
	calls := (&env{}).handleCalls(t)
	methods := routerv1.File_bugnet_router_v1_router_proto.Services().
		ByName("SandboxRouterService").Methods()
	var want, got []string
	for i := range methods.Len() {
		m := methods.Get(i)
		if slices.Contains(built, string(m.Name())) && m.Input().Fields().ByName("handle") != nil {
			want = append(want, string(m.Name()))
		}
	}
	for name := range calls {
		got = append(got, name)
	}
	slices.Sort(want)
	slices.Sort(got)
	eq(t, got, want)
}

func TestUnknownOrMalformedHandleIsHandleUnknown(t *testing.T) {
	e := newEnv(t, options{})
	unknown, _ := handle.New()
	handles := map[string]string{"unknown": unknown, "malformed": "sbx_not-a-handle"}
	for name, call := range e.handleCalls(t) {
		for kind, h := range handles {
			t.Run(name+"/"+kind, func(t *testing.T) {
				err := call(h)
				mustCode(t, err, connect.CodeNotFound, "HANDLE_UNKNOWN")
				quotesNone(t, err, h)
			})
		}
	}
}

func TestUnknownJobIsJobUnknownAndMissingFileHasNoReason(t *testing.T) {
	e := newEnv(t, options{})
	ctx := t.Context()
	h, _ := e.readyPlacement(t)
	other, _ := e.readyPlacement(t)
	jobs := map[string]string{
		"unknown":             "j_" + strings.ToLower(rand.Text()),
		"malformed":           "j_nope",
		"another placement's": e.exec(t, sh(other, "true")).GetJobId(),
	}
	for kind, job := range jobs {
		t.Run("WatchJob/"+kind, func(t *testing.T) {
			_, err := e.watchJob(t, &routerv1.WatchJobRequest{Handle: h, JobId: job})
			mustCode(t, err, connect.CodeNotFound, "JOB_UNKNOWN")
			quotesNone(t, err, h, job)
		})
		t.Run("CancelJob/"+kind, func(t *testing.T) {
			_, err := e.client.CancelJob(ctx, connect.NewRequest(
				&routerv1.CancelJobRequest{Handle: h, JobId: job}))
			mustCode(t, err, connect.CodeNotFound, "JOB_UNKNOWN")
			quotesNone(t, err, h, job)
		})
	}
	t.Run("ReadFile of a missing path", func(t *testing.T) {
		_, err := e.client.ReadFile(ctx, connect.NewRequest(
			&routerv1.ReadFileRequest{Handle: h, Path: "/root/missing.txt"}))
		mustCode(t, err, connect.CodeNotFound, "FILE_NOT_FOUND")
		quotesNone(t, err, h)
	})
}

// quotesNone fails the test when err's message or any of its details holds one of ids
func quotesNone(t *testing.T, err error, ids ...string) {
	t.Helper()
	var ce *connect.Error
	if !errors.As(err, &ce) {
		t.Fatalf("error %v is not a Connect error", err)
	}
	for _, id := range ids {
		if strings.Contains(ce.Message(), id) {
			t.Errorf("message %q quotes %q", ce.Message(), id)
		}
		for _, d := range ce.Details() {
			if bytes.Contains(d.Bytes(), []byte(id)) {
				t.Errorf("detail %s quotes %q", d.Type(), id)
			}
		}
	}
}

func TestUnknownStartOrderIsOrderUnknown(t *testing.T) {
	e := newEnv(t, options{})
	_, err := e.payments(t).LookupPlacement(t.Context(), connect.NewRequest(
		&routerv1.LookupPlacementRequest{
			Key: &routerv1.LookupPlacementRequest_StartOrderId{StartOrderId: "ord_unknown"},
		}))
	mustCode(t, err, connect.CodeNotFound, "ORDER_UNKNOWN")
	quotesNone(t, err, "ord_unknown")
}
