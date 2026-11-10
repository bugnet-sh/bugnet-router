package telemetry

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"

	"go.opentelemetry.io/otel/trace"

	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/telemetry/attr"
)

// orderLabel is the derivation label of DP 20.2.1, which the gateway and the edges pin too
const orderLabel = "bugnet-order-trace-v1|"

// OrderIDs derives the order trace id and root span id from a handle's SHA-256
func OrderIDs(handleSHA256 []byte) (trace.TraceID, trace.SpanID) {
	h := sha256.New()
	h.Write([]byte(orderLabel))
	h.Write(handleSHA256)
	d := h.Sum(nil)
	return trace.TraceID(d[0:16]), trace.SpanID(d[16:24])
}

// orderSampled is the decision of the SDK's TraceIDRatioBased on the derived trace id
func orderSampled(tid trace.TraceID, ratio float64) bool {
	switch {
	case ratio >= 1:
		return true
	case ratio <= 0:
		return false
	}
	x := binary.BigEndian.Uint64(tid[8:16]) >> 1
	return x < uint64(ratio*(1<<63))
}

// Order returns the remote span context of a placement's order root, sampled by the ratio
func (t *Telemetry) Order(handleSHA256 []byte) trace.SpanContext {
	tid, sid := OrderIDs(handleSHA256)
	if !t.Enabled() || len(handleSHA256) != sha256.Size || !tid.IsValid() || !sid.IsValid() {
		return trace.SpanContext{}
	}
	var flags trace.TraceFlags
	if orderSampled(tid, t.ratio) {
		flags = trace.FlagsSampled
	}
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: tid, SpanID: sid, TraceFlags: flags, Remote: true,
	})
}

// forcedKey carries the ids the next root must take
type forcedKey struct{}

type forcedIDs struct {
	trace trace.TraceID
	span  trace.SpanID
}

// idGenerator is the SDK's random generator, except for one forced root
type idGenerator struct{}

func (idGenerator) NewIDs(ctx context.Context) (trace.TraceID, trace.SpanID) {
	if f, ok := ctx.Value(forcedKey{}).(forcedIDs); ok {
		return f.trace, f.span
	}
	var tid trace.TraceID
	for !tid.IsValid() {
		_, _ = rand.Read(tid[:])
	}
	return tid, randomSpanID()
}

func (idGenerator) NewSpanID(context.Context, trace.TraceID) trace.SpanID {
	return randomSpanID()
}

func randomSpanID() trace.SpanID {
	var sid trace.SpanID
	for !sid.IsValid() {
		_, _ = rand.Read(sid[:])
	}
	return sid
}

// StartOrderRoot starts bugnet.order with the derived ids, linked to the span in ctx
func (t *Telemetry) StartOrderRoot(ctx context.Context, p store.Placement) (
	context.Context, trace.Span) {
	tid, sid := OrderIDs(p.HandleSHA256)
	if !t.Enabled() || len(p.HandleSHA256) != sha256.Size || !tid.IsValid() || !sid.IsValid() {
		return ctx, trace.SpanFromContext(context.Background())
	}
	attrs := append(t.placementAttrs(p), attr.PackageID(p.PackageID))
	if h := t.OrderHash(p.OrderID); h != "" {
		attrs = append(attrs, attr.OrderHash(h))
	}
	opts := []trace.SpanStartOption{
		trace.WithNewRoot(), trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attrs...),
	}
	caller := trace.SpanContextFromContext(ctx)
	if caller.IsValid() {
		opts = append(opts, trace.WithLinks(trace.Link{
			SpanContext: caller, Attributes: []attr.KeyValue{attr.Link("request")},
		}))
	}
	forced := context.WithValue(ctx, forcedKey{}, forcedIDs{tid, sid})
	_, root := t.provider.Tracer("bugnet-router/telemetry").Start(forced, "bugnet.order", opts...)
	if caller.IsValid() {
		trace.SpanFromContext(ctx).AddLink(trace.Link{
			SpanContext: root.SpanContext(), Attributes: []attr.KeyValue{attr.Link("order")},
		})
	}
	return trace.ContextWithSpan(ctx, root), root
}

// placementAttrs are the attributes every span about a placement carries
func (t *Telemetry) placementAttrs(p store.Placement) []attr.KeyValue {
	out := []attr.KeyValue{attr.PlacementID(p.ID.String())}
	if len(p.HandleSHA256) >= 6 {
		out = append(out, attr.HandleHash(hex.EncodeToString(p.HandleSHA256)[:12]))
	}
	return out
}

// InOrder starts a span in the placement's order trace for work no request carried
func (t *Telemetry) InOrder(ctx context.Context, p store.Placement, name string,
	attrs ...attr.KeyValue) (context.Context, trace.Span) {
	order := t.Order(p.HandleSHA256)
	if !order.IsValid() {
		return ctx, trace.SpanFromContext(context.Background())
	}
	ctx = trace.ContextWithRemoteSpanContext(ctx, order)
	return t.provider.Tracer("bugnet-router/telemetry").Start(ctx, name,
		trace.WithAttributes(t.placementAttrs(p)...), trace.WithAttributes(attrs...))
}
