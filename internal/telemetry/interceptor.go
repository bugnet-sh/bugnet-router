package telemetry

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/bugnet-sh/bugnet-router/internal/handle"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/telemetry/attr"
)

const (
	routerPkg    = "/bugnet.router.v1."
	tracerScope  = "bugnet-router/telemetry"
	linkOrderVal = "order"
)

// NeverTraced lists the procedures that make no span and whose trace header is ignored, DP 20.3.2
var NeverTraced = []string{
	"/bugnet.router.v1.EdgeService/ReportEdgeUsage",
	"/bugnet.router.v1.EdgeService/CheckEdgeLeases",
	"/bugnet.router.v1.EdgeService/WatchEdgeEvents",
	"/bugnet.router.v1.SshGatewayService/RenewSshConnection",
	"/bugnet.router.v1.SshGatewayService/CheckSshGrants",
	"/bugnet.router.v1.SshGatewayService/ReportSshUsage",
	"/bugnet.router.v1.SshGatewayService/WatchSshEvents",
	"/bugnet.router.v1.EgressService/ReportEgressUsage",
	"/bugnet.router.v1.EgressService/CheckEgressLeases",
	"/bugnet.router.v1.EgressService/WatchEgressEvents",
	"/bugnet.router.v1.EgressService/AppendX402Events",
}

// deferredServices make no span for a call without a traceparent until a placement resolves
var deferredServices = []string{
	routerPkg + "EdgeService/", routerPkg + "SecretsRouteService/",
	routerPkg + "SshGatewayService/", routerPkg + "EgressService/",
	routerPkg + "SessionPageService/",
}

// serverFaults are the Connect codes that mark a server span failed
var serverFaults = map[connect.Code]bool{
	connect.CodeUnknown: true, connect.CodeDeadlineExceeded: true, connect.CodeUnimplemented: true,
	connect.CodeInternal: true, connect.CodeUnavailable: true, connect.CodeDataLoss: true,
}

// slot is the call's place in its context, shared between the interceptor and JoinOrder
type slot struct {
	mu      sync.Mutex
	name    string
	arrival time.Time
	span    trace.Span
	joined  bool
}

type slotKey struct{}

// Interceptor starts or defers server spans by procedure, handler side only
func (t *Telemetry) Interceptor() connect.Interceptor { return interceptor{t} }

type interceptor struct{ t *Telemetry }

func (i interceptor) WrapStreamingClient(
	next connect.StreamingClientFunc,
) connect.StreamingClientFunc {
	return next
}

func (i interceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		proc := req.Spec().Procedure
		if neverTraced(proc) {
			return next(ctx, req)
		}
		ctx, finish := i.t.begin(ctx, proc, req.Header(), unaryHandleParent(req.Any()))
		resp, err := next(ctx, req)
		finish(err)
		return resp, err
	}
}

func (i interceptor) WrapStreamingHandler(
	next connect.StreamingHandlerFunc,
) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		proc := conn.Spec().Procedure
		if neverTraced(proc) {
			return next(ctx, conn)
		}
		ctx, finish := i.t.begin(ctx, proc, conn.RequestHeader(), nil)
		err := next(ctx, conn)
		finish(err)
		return err
	}
}

func neverTraced(proc string) bool {
	for _, p := range NeverTraced {
		if p == proc {
			return true
		}
	}
	return false
}

func deferred(proc string) bool {
	for _, p := range deferredServices {
		if strings.HasPrefix(proc, p) {
			return true
		}
	}
	return false
}

// unaryHandleParent is the SHA-256 of a well formed handle the request names, else nil
func unaryHandleParent(msg any) []byte {
	h, ok := msg.(interface{ GetHandle() string })
	if !ok {
		return nil
	}
	sum, err := handle.Hash(h.GetHandle())
	if err != nil {
		return nil
	}
	return sum
}

// begin starts the call's span, or leaves a pending slot, and returns the function that ends it
func (t *Telemetry) begin(ctx context.Context, proc string, h interface{ Get(string) string },
	handleSum []byte) (context.Context, func(error)) {
	sl := &slot{name: strings.TrimPrefix(proc, "/"), arrival: time.Now()}
	ctx = context.WithValue(ctx, slotKey{}, sl)
	finish := func(err error) { sl.finish(err) }
	remote := propagation.TraceContext{}.Extract(context.Background(), headerGetter{h})
	rsc := trace.SpanContextFromContext(remote)
	switch {
	case rsc.IsValid():
		ctx = trace.ContextWithRemoteSpanContext(ctx, rsc)
	case handleSum != nil:
		if order := t.Order(handleSum); order.IsValid() {
			ctx = trace.ContextWithRemoteSpanContext(ctx, order)
		}
	case deferred(proc):
		return ctx, finish
	}
	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindServer), trace.WithTimestamp(sl.arrival),
		trace.WithAttributes(attr.RPCSystem(), attr.RPCMethod(sl.name)),
	}
	if !trace.SpanContextFromContext(ctx).IsValid() {
		opts = append(opts, trace.WithNewRoot())
	}
	ctx, sl.span = t.provider.Tracer(tracerScope).Start(ctx, sl.name, opts...)
	return ctx, finish
}

// headerGetter adapts a Connect header to the propagator
type headerGetter struct {
	h interface{ Get(string) string }
}

func (g headerGetter) Get(key string) string { return g.h.Get(key) }
func (g headerGetter) Set(string, string)    {}
func (g headerGetter) Keys() []string        { return nil }

// finish sets the outcome on the span, when one started, and ends it
func (s *slot) finish(err error) {
	s.mu.Lock()
	span := s.span
	s.mu.Unlock()
	if span == nil {
		return
	}
	code, reason := "OK", ""
	var fault bool
	if err != nil {
		c, r := failure(err)
		code, reason, fault = strings.ToUpper(c.String()), r, serverFaults[c]
		if reason == "" {
			reason = code
		}
	}
	span.SetAttributes(attr.RPCStatus(code))
	if err != nil {
		span.SetAttributes(attr.ErrorType(reason))
		if fault {
			span.SetStatus(codes.Error, reason)
		}
	}
	span.End()
}

// failure reads the Connect code and the ErrorInfo reason of err
func failure(err error) (connect.Code, string) {
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return connect.CodeUnknown, ""
	}
	for _, d := range ce.Details() {
		if v, derr := d.Value(); derr == nil {
			if info, ok := v.(*errdetails.ErrorInfo); ok {
				return ce.Code(), info.GetReason()
			}
		}
	}
	return ce.Code(), ""
}

// JoinOrder starts a deferred server span under the order, or links a foreign trace to it
func (t *Telemetry) JoinOrder(ctx context.Context, p store.Placement) context.Context {
	sl, _ := ctx.Value(slotKey{}).(*slot)
	if !t.Enabled() || sl == nil {
		return ctx
	}
	sl.mu.Lock()
	defer sl.mu.Unlock()
	if sl.joined {
		return ctx
	}
	sl.joined = true
	order := t.Order(p.HandleSHA256)
	switch {
	case sl.span == nil && order.IsValid():
		ctx, sl.span = t.provider.Tracer(tracerScope).Start(
			trace.ContextWithRemoteSpanContext(ctx, order), sl.name,
			trace.WithSpanKind(trace.SpanKindServer), trace.WithTimestamp(sl.arrival),
			trace.WithAttributes(attr.RPCSystem(), attr.RPCMethod(sl.name)))
	case sl.span == nil:
		return ctx
	case order.IsValid() && sl.span.SpanContext().TraceID() != order.TraceID():
		sl.span.AddLink(trace.Link{
			SpanContext: order, Attributes: []attr.KeyValue{attr.Link(linkOrderVal)},
		})
	}
	sl.span.SetAttributes(t.placementAttrs(p)...)
	return ctx
}
