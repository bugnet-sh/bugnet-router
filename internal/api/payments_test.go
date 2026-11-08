package api_test

import (
	"encoding/hex"
	"strconv"
	"strings"
	"testing"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"google.golang.org/protobuf/proto"

	"github.com/bugnet-sh/bugnet-router/internal/handle"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

func byHash(sha string) *routerv1.LookupPlacementRequest {
	return &routerv1.LookupPlacementRequest{
		Key: &routerv1.LookupPlacementRequest_HandleSha256{HandleSha256: sha},
	}
}

func byStartOrder(id string) *routerv1.LookupPlacementRequest {
	return &routerv1.LookupPlacementRequest{
		Key: &routerv1.LookupPlacementRequest_StartOrderId{StartOrderId: id},
	}
}

func (e *env) lookup(
	t *testing.T, req *routerv1.LookupPlacementRequest,
) (*routerv1.LookupPlacementResponse, error) {
	t.Helper()
	resp, err := e.payments(t).LookupPlacement(t.Context(), connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func (e *env) mustLookup(
	t *testing.T, req *routerv1.LookupPlacementRequest,
) *routerv1.LookupPlacementResponse {
	t.Helper()
	got, err := e.lookup(t, req)
	if err != nil {
		t.Fatalf("LookupPlacement: %v", err)
	}
	return got
}

// hashHex is the lower-case hex SHA-256 of handle h
func hashHex(t *testing.T, h string) string {
	t.Helper()
	sha, err := handle.Hash(h)
	if err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(sha)
}

// readyOrder starts a placement from a new start order, waits for it to be ready and returns both
func (e *env) readyOrder(t *testing.T) (string, string, *routerv1.Placement) {
	t.Helper()
	order, tok := e.startOrder(t, smallPkg, smallPrice)
	resp, err := e.createWith(t, tok)
	if err != nil {
		t.Fatalf("create placement: %v", err)
	}
	h := resp.Msg.GetHandle()
	return order, h, e.waitReady(t, h)
}

func TestLookupPlacementBothKeys(t *testing.T) {
	e := newEnv(t, options{})
	order, h, p := e.readyOrder(t)
	a := e.mustLookup(t, byHash(hashHex(t, h)))
	b := e.mustLookup(t, byStartOrder(order))
	eq(t, proto.Equal(a, b), true)
	eq(t, a.GetPlacementId(), p.GetPlacementId())
	eq(t, a.GetStartOrderId(), order)
	eq(t, a.GetState(), "ready")
	eq(t, a.GetDeadline().AsTime().Equal(p.GetDeadline().AsTime()), true)
	eq(t, a.GetEndedAt() == nil, true)
}

func TestLookupPlacementCapable(t *testing.T) {
	e := newEnv(t, options{secrets: true})
	for _, needs := range []bool{true, false} {
		_, p := e.placingWith(t, needs)
		got := e.mustLookup(t, byStartOrder(p.OrderID))
		eq(t, got.GetState(), "placing")
		eq(t, got.GetX402Capable(), needs)
		eq(t, got.GetDeadline() == nil, true)
	}
	_, h, _ := e.readyOrder(t)
	eq(t, e.mustLookup(t, byHash(hashHex(t, h))).GetX402Capable(), true)

	other := newEnv(t, options{})
	_, h, _ = other.readyOrder(t)
	eq(t, other.mustLookup(t, byHash(hashHex(t, h))).GetX402Capable(), false)
}

func TestLookupPlacementFailedBeforeReady(t *testing.T) {
	e := newEnv(t, options{secrets: true})
	for _, needs := range []bool{true, false} {
		_, sha := handle.New()
		p, _, err := e.store.CreatePlacement(t.Context(), store.NewPlacement{
			HandleSHA256: sha, OrderID: "failed-" + strconv.FormatBool(needs),
			PackageID: smallPkg, FailureReason: "no_candidate", NeedsSecrets: needs,
		})
		if err != nil {
			t.Fatal(err)
		}
		got := e.mustLookup(t, byHash(hex.EncodeToString(sha)))
		eq(t, got.GetState(), "failed")
		eq(t, got.GetX402Capable(), needs)
		eq(t, got.GetDeadline() == nil, true)
		eq(t, got.GetEndedAt().AsTime().Equal(*p.TerminatedAt), true)
	}
}

func TestLookupPlacementErrors(t *testing.T) {
	e := newEnv(t, options{})
	_, h, _ := e.readyOrder(t)
	sha := hashHex(t, h)
	for name, bad := range map[string]string{
		"63 hex":     sha[:63],
		"65 hex":     sha + "0",
		"upper case": strings.ToUpper(sha),
		"not hex":    strings.Repeat("g", 64),
		"empty":      "",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := e.lookup(t, byHash(bad))
			mustCode(t, err, connect.CodeInvalidArgument, "")
		})
	}
	_, err := e.lookup(t, &routerv1.LookupPlacementRequest{})
	mustCode(t, err, connect.CodeInvalidArgument, "")
	_, err = e.lookup(t, byStartOrder(""))
	mustCode(t, err, connect.CodeInvalidArgument, "")

	unknown, _ := handle.New()
	_, err = e.lookup(t, byHash(hashHex(t, unknown)))
	mustCode(t, err, connect.CodeNotFound, "HANDLE_UNKNOWN")

	_, err = e.lookup(t, byStartOrder("ord_unknown"))
	mustCode(t, err, connect.CodeNotFound, "ORDER_UNKNOWN")
	quotesNone(t, err, "ord_unknown")

	if _, err := e.client.ExtendPlacement(t.Context(), connect.NewRequest(
		&routerv1.ExtendPlacementRequest{
			Handle: h, OrderToken: e.extendToken(t, h, morePkg, morePrice),
		})); err != nil {
		t.Fatal(err)
	}
	ext := "extend-" + strconv.FormatInt(e.orders.Load(), 10)
	eq(t, e.getOrder(t, ext).GetKind(), "extend")
	_, err = e.lookup(t, byStartOrder(ext))
	mustCode(t, err, connect.CodeNotFound, "ORDER_UNKNOWN")
}

func TestLookupPlacementEndedFields(t *testing.T) {
	e := newEnv(t, options{})
	order, h, ready := e.readyOrder(t)
	stopped := e.stop(t, h)
	for _, req := range []*routerv1.LookupPlacementRequest{
		byHash(hashHex(t, h)), byStartOrder(order),
	} {
		got := e.mustLookup(t, req)
		eq(t, got.GetState(), "terminated")
		eq(t, got.GetEndedAt() != nil, true)
		eq(t, got.GetEndedAt().AsTime().Equal(stopped.GetEndedAt().AsTime()), true)
		eq(t, got.GetDeadline().AsTime().Equal(ready.GetDeadline().AsTime()), true)
	}
}

func TestLookupPlacementPackageID(t *testing.T) {
	e := newEnv(t, options{})
	order, h, _ := e.readyOrder(t)
	_, placing := e.placingWith(t, false)
	for _, c := range []struct {
		req          *routerv1.LookupPlacementRequest
		order, place string
	}{
		{byHash(hashHex(t, h)), order, ""},
		{byStartOrder(order), order, ""},
		{byHash(hex.EncodeToString(placing.HandleSHA256)), placing.OrderID, placing.ID.String()},
		{byStartOrder(placing.OrderID), placing.OrderID, placing.ID.String()},
	} {
		got := e.mustLookup(t, c.req)
		eq(t, got.GetPackageId(), smallPkg)
		eq(t, got.GetStartOrderId(), c.order)
		if c.place != "" {
			eq(t, got.GetPlacementId(), c.place)
		}
	}
}

func (e *env) resolved(t *testing.T, m *routerv1.AppendX402ResolvedRequest) error {
	t.Helper()
	_, err := e.payments(t).AppendX402Resolved(t.Context(), connect.NewRequest(m))
	return err
}

func TestAppendX402Resolved(t *testing.T) {
	e := newEnv(t, options{})
	order, tok := e.startOrder(t, smallPkg, smallPrice)
	res, err := e.createWith(t, tok)
	if err != nil {
		t.Fatal(err)
	}
	p := e.waitReady(t, res.Msg.GetHandle())
	good := &routerv1.AppendX402ResolvedRequest{StartOrderId: order, Ref: "aaaaaaaaaaaaaaaa",
		Status: "paid", AmountMicros: 10000, Tx: sig, PayTo: usdc, Host: "agent402.tools",
		Method: "POST", Path: "/" + strings.Repeat("p", 120)}
	eq(t, e.resolved(t, good), nil)
	eq(t, e.resolved(t, good), nil)
	rows := e.sessionEvents(t, p)["x402_resolved"]
	eq(t, len(rows), 1)
	eq(t, len([]rune(rows[0]["path"].(string))), 80)
	eq(t, rows[0]["tx"], any(sig))
	rel := &routerv1.AppendX402ResolvedRequest{StartOrderId: order, Ref: "bbbbbbbbbbbbbbbb",
		Status: "released"}
	eq(t, e.resolved(t, rel), nil)
	eq(t, len(e.sessionEvents(t, p)["x402_resolved"]), 2)
	type req = routerv1.AppendX402ResolvedRequest
	for _, c := range []func(*req){
		func(m *req) { m.Ref = "short" },
		func(m *req) { m.Status = "other" },
		func(m *req) { m.Tx = "" },
		func(m *req) { m.Status, m.Ref = "released", "cccccccccccccccc" },
		func(m *req) { m.PayTo = "nope" },
	} {
		bad := &routerv1.AppendX402ResolvedRequest{StartOrderId: order, Ref: "dddddddddddddddd",
			Status: "paid", Tx: sig}
		c(bad)
		mustCode(t, e.resolved(t, bad), connect.CodeInvalidArgument, "")
	}
	good.StartOrderId = "ord_unknown"
	mustCode(t, e.resolved(t, good), connect.CodeNotFound, "ORDER_UNKNOWN")
}
