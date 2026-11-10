package telemetry_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"github.com/google/uuid"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/telemetry"
	"github.com/bugnet-sh/bugnet-router/internal/telemetry/telemetrytest"
)

func placementOf(h string) store.Placement {
	return store.Placement{ID: uuid.New(), HandleSHA256: sha(h), OrderID: "ord-1",
		PackageID: "small-10m"}
}

// routerFake answers Exec as its cwd says, joining the order as a handler that found a placement
type routerFake struct {
	routerv1connect.UnimplementedSandboxRouterServiceHandler
	tel *telemetry.Telemetry
	p   store.Placement
}

func (f *routerFake) Exec(
	ctx context.Context, req *connect.Request[routerv1.ExecRequest],
) (*connect.Response[routerv1.ExecResponse], error) {
	if req.Msg.GetCwd() != "nojoin" {
		f.tel.JoinOrder(ctx, f.p)
	}
	switch req.Msg.GetCwd() {
	case "notfound":
		return nil, withReason(connect.CodeNotFound, "PLACEMENT_UNKNOWN")
	case "internal":
		return nil, withReason(connect.CodeInternal, "SAGA_BROKE")
	case "plain":
		return nil, connect.NewError(connect.CodeUnavailable, nil)
	}
	return connect.NewResponse(&routerv1.ExecResponse{}), nil
}

func withReason(code connect.Code, reason string) error {
	e := connect.NewError(code, context.Canceled)
	if d, err := connect.NewErrorDetail(&errdetails.ErrorInfo{
		Reason: reason, Domain: "bugnet.router",
	}); err == nil {
		e.AddDetail(d)
	}
	return e
}

// edgeFake answers OpenTransfer for one known token
type edgeFake struct {
	routerv1connect.UnimplementedEdgeServiceHandler
	tel *telemetry.Telemetry
	p   store.Placement
}

func (f *edgeFake) OpenTransfer(
	ctx context.Context, req *connect.Request[routerv1.OpenTransferRequest],
) (*connect.Response[routerv1.OpenTransferResponse], error) {
	if req.Msg.GetToken() != "known" {
		return nil, connect.NewError(connect.CodeNotFound, nil)
	}
	f.tel.JoinOrder(ctx, f.p)
	return connect.NewResponse(&routerv1.OpenTransferResponse{}), nil
}

type rig struct {
	tel    *telemetrytest.Telemetry
	srv    *httptest.Server
	router routerv1connect.SandboxRouterServiceClient
	edge   routerv1connect.EdgeServiceClient
	p      store.Placement
}

func newRig(t *testing.T, ratio float64) *rig {
	t.Helper()
	tel := telemetrytest.New(t, "router-api", ratio, nil)
	p := placementOf(handleA)
	mux := http.NewServeMux()
	opt := connect.WithInterceptors(tel.Interceptor())
	mux.Handle(routerv1connect.NewSandboxRouterServiceHandler(
		&routerFake{tel: tel.Telemetry, p: p}, opt))
	mux.Handle(routerv1connect.NewEdgeServiceHandler(&edgeFake{tel: tel.Telemetry, p: p}, opt))
	mux.Handle(routerv1connect.NewSshGatewayServiceHandler(
		routerv1connect.UnimplementedSshGatewayServiceHandler{}, opt))
	mux.Handle(routerv1connect.NewEgressServiceHandler(
		routerv1connect.UnimplementedEgressServiceHandler{}, opt))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &rig{tel: tel, srv: srv, p: p,
		router: routerv1connect.NewSandboxRouterServiceClient(srv.Client(), srv.URL),
		edge:   routerv1connect.NewEdgeServiceClient(srv.Client(), srv.URL)}
}

func (r *rig) exec(t *testing.T, handle, cwd string, hdr map[string]string) error {
	t.Helper()
	req := connect.NewRequest(&routerv1.ExecRequest{Handle: handle, Cwd: cwd, Argv: []string{"x"}})
	for k, v := range hdr {
		req.Header().Set(k, v)
	}
	_, err := r.router.Exec(t.Context(), req)
	return err
}

func (r *rig) spans(t *testing.T) []telemetrytest.Span { return r.tel.Flushed(t) }

func only(t *testing.T, spans []telemetrytest.Span, name string) telemetrytest.Span {
	t.Helper()
	var got []telemetrytest.Span
	for _, s := range spans {
		if s.Name == name {
			got = append(got, s)
		}
	}
	if len(got) != 1 {
		t.Fatalf("want one %s span, got %d of %d", name, len(got), len(spans))
	}
	return got[0]
}

const execName = "bugnet.router.v1.SandboxRouterService/Exec"

func TestInterceptorContinuesTraceparentAndTracestate(t *testing.T) {
	r := newRig(t, 1)
	tid := randomTraceID(t)
	hdr := map[string]string{
		"traceparent": "00-" + tid.String() + "-00f067aa0ba902b7-01", "tracestate": "vendor=a1",
	}
	if err := r.exec(t, handleA, "", hdr); err != nil {
		t.Fatal(err)
	}
	s := only(t, r.spans(t), execName)
	eq(t, s.TraceID, tid.String())
	eq(t, s.ParentID, "00f067aa0ba902b7")
	eq(t, s.TraceState, "vendor=a1")
	eq(t, at(s.Attrs, "rpc.system.name"), "connectrpc")
	eq(t, at(s.Attrs, "rpc.method"), execName)
	eq(t, at(s.Attrs, "rpc.response.status_code"), "OK")
	// The foreign trace is one link away from the order
	eq(t, len(s.Links), 1)
	eq(t, s.Links[0].TraceID, "55a4dee648a7a4d0a70cd0a8b24c672f")
	eq(t, at(s.Links[0].Attrs, "bugnet.link"), "order")
}

func TestInterceptorHandleWithoutTraceparentJoinsOrder(t *testing.T) {
	r := newRig(t, 1)
	if err := r.exec(t, handleA, "", nil); err != nil {
		t.Fatal(err)
	}
	s := only(t, r.spans(t), execName)
	eq(t, s.TraceID, "55a4dee648a7a4d0a70cd0a8b24c672f")
	eq(t, s.ParentID, "69923b8439c47854")
	eq(t, len(s.Links), 0)
	eq(t, at(s.Attrs, "bugnet.placement_id"), r.p.ID.String())
	eq(t, at(s.Attrs, "bugnet.handle_hash"), "9a5bf94fe52b")
}

func TestJoinOrderLinksForeignTrace(t *testing.T) {
	r := newRig(t, 1)
	hdr := map[string]string{"traceparent": "00-" + randomTraceID(t).String() +
		"-00f067aa0ba902b7-01"}
	if err := r.exec(t, handleA, "", hdr); err != nil {
		t.Fatal(err)
	}
	s := only(t, r.spans(t), execName)
	eq(t, len(s.Links), 1)
	eq(t, s.Links[0].SpanID, "69923b8439c47854")
}

func TestInterceptorMalformedHeadersCountAsAbsent(t *testing.T) {
	r := newRig(t, 1)
	// A bad tracestate is dropped, the traceparent still continues
	tid := randomTraceID(t)
	hdr := map[string]string{"traceparent": "00-" + tid.String() + "-00f067aa0ba902b7-01",
		"tracestate": "Bad Key=x"}
	if err := r.exec(t, handleA, "", hdr); err != nil {
		t.Fatal(err)
	}
	// A malformed traceparent is as good as none, so the handle's order root parents the span
	if err := r.exec(t, handleA, "", map[string]string{"traceparent": "garbage"}); err != nil {
		t.Fatal(err)
	}
	spans := r.spans(t)
	eq(t, len(spans), 2)
	for _, s := range spans {
		switch s.TraceID {
		case tid.String():
			eq(t, s.TraceState, "")
		default:
			eq(t, s.ParentID, "69923b8439c47854")
		}
	}
}

func TestInterceptorNoHandleStartsRoot(t *testing.T) {
	r := newRig(t, 1)
	if err := r.exec(t, "", "nojoin", nil); err != nil {
		t.Fatal(err)
	}
	s := only(t, r.spans(t), execName)
	eq(t, s.ParentID, "")
	eq(t, len(s.Links), 0)
}

func TestDeferredSpanOnlyAfterJoin(t *testing.T) {
	r := newRig(t, 1)
	call := func(token string) {
		_, _ = r.edge.OpenTransfer(t.Context(), connect.NewRequest(
			&routerv1.OpenTransferRequest{Token: token}))
	}
	call("bnx_unknown")
	eq(t, len(r.spans(t)), 0)
	call("known")
	s := only(t, r.spans(t), "bugnet.router.v1.EdgeService/OpenTransfer")
	eq(t, s.ParentID, "69923b8439c47854")
	eq(t, s.TraceID, "55a4dee648a7a4d0a70cd0a8b24c672f")
	eq(t, at(s.Attrs, "bugnet.placement_id"), r.p.ID.String())
	// Backdated to the arrival, so it starts before the join and ends after it
	if !s.Start.Before(s.End) {
		t.Fatalf("span %v to %v", s.Start, s.End)
	}
}

func TestDeferredWithTraceparentContinues(t *testing.T) {
	r := newRig(t, 1)
	tid := randomTraceID(t)
	req := connect.NewRequest(&routerv1.OpenTransferRequest{Token: "bnx_unknown"})
	req.Header().Set("traceparent", "00-"+tid.String()+"-00f067aa0ba902b7-01")
	_, _ = r.edge.OpenTransfer(t.Context(), req)
	s := only(t, r.spans(t), "bugnet.router.v1.EdgeService/OpenTransfer")
	eq(t, s.TraceID, tid.String())
	eq(t, at(s.Attrs, "rpc.response.status_code"), "NOT_FOUND")
}

func TestStatusOnlyForServerFaults(t *testing.T) {
	r := newRig(t, 1)
	for _, cwd := range []string{"notfound", "internal", "plain"} {
		_ = r.exec(t, handleA, cwd, nil)
	}
	spans := r.spans(t)
	eq(t, len(spans), 3)
	for _, s := range spans {
		switch at(s.Attrs, "rpc.response.status_code") {
		case "NOT_FOUND":
			eq(t, int(s.StatusCode), 0)
			eq(t, at(s.Attrs, "error.type"), "PLACEMENT_UNKNOWN")
		case "INTERNAL":
			eq(t, int(s.StatusCode), 2)
			eq(t, s.StatusText, "SAGA_BROKE")
			eq(t, at(s.Attrs, "error.type"), "SAGA_BROKE")
		case "UNAVAILABLE":
			eq(t, int(s.StatusCode), 2)
			eq(t, at(s.Attrs, "error.type"), "UNAVAILABLE")
		default:
			t.Fatalf("unexpected status %v", s.Attrs)
		}
		eq(t, s.Events, 0)
	}
}

// post sends one call of a procedure by hand, as a Connect client with an empty message would
func post(t *testing.T, base, proc string, stream bool, hdr map[string]string) {
	t.Helper()
	var body *bytes.Reader
	ctype := "application/json"
	if stream {
		ctype = "application/connect+json"
		env := make([]byte, 5, 7)
		binary.BigEndian.PutUint32(env[1:], 2)
		body = bytes.NewReader(append(env, '{', '}'))
	} else {
		body = bytes.NewReader([]byte("{}"))
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, base+proc, body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", ctype)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
}

func TestInterceptorNeverTraced(t *testing.T) {
	r := newRig(t, 1)
	eq(t, len(telemetry.NeverTraced), 11)
	hdr := map[string]string{"traceparent": "00-" + randomTraceID(t).String() +
		"-00f067aa0ba902b7-01"}
	for _, proc := range telemetry.NeverTraced {
		stream := strings.Contains(proc, "/Watch")
		post(t, r.srv.URL, proc, stream, nil)
		post(t, r.srv.URL, proc, stream, hdr)
	}
	eq(t, len(r.spans(t)), 0)
	// A procedure beside them with the same header is traced
	post(t, r.srv.URL, "/bugnet.router.v1.EdgeService/OpenTransfer", false, hdr)
	eq(t, len(r.spans(t)), 1)
}

func TestInterceptorOffInstallsNothing(t *testing.T) {
	off := telemetry.Off()
	eq(t, off.Enabled(), false)
	eq(t, len(off.Middleware(true)), 0)
	eq(t, off.SandboxHash(provider.Ref{Provider: "boat", ID: "x"}), "")
}
