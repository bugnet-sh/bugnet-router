package telemetry

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// LogHandler adds trace_id and span_id to records logged with a span in their context
func LogHandler(next slog.Handler) slog.Handler { return traceHandler{next} }

type traceHandler struct{ next slog.Handler }

func (h traceHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.next.Enabled(ctx, l)
}

func (h traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()))
	}
	return h.next.Handle(ctx, r)
}

func (h traceHandler) WithAttrs(a []slog.Attr) slog.Handler {
	return traceHandler{h.next.WithAttrs(a)}
}

func (h traceHandler) WithGroup(name string) slog.Handler {
	return traceHandler{h.next.WithGroup(name)}
}
