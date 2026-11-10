// Package carrier reads the W3C trace context of a span, a leaf the store can import
package carrier

import (
	"context"

	"go.opentelemetry.io/otel/trace"
)

// Carrier is the W3C context of ctx's span, empty strings when it has none
func Carrier(ctx context.Context) (traceparent, tracestate string) {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return "", ""
	}
	return Traceparent(sc), sc.TraceState().String()
}

// Traceparent formats sc as a version 00 traceparent header value
func Traceparent(sc trace.SpanContext) string {
	return "00-" + sc.TraceID().String() + "-" + sc.SpanID().String() + "-" +
		sc.TraceFlags().String()
}
