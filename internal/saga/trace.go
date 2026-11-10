package saga

import (
	"context"
	"errors"
	"strings"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/telemetry"
	"github.com/bugnet-sh/bugnet-router/internal/telemetry/attr"
)

// span starts a child-only span, a no-op without a span in ctx (DP 20.4.1)
func (s *Saga) span(ctx context.Context, name string, attrs ...attr.KeyValue) (
	context.Context, trace.Span) {
	return telemetry.Child(ctx, s.tr, name, attrs...)
}

// orderSpan starts a span under the call or job span, else in the placement's order trace
func (s *Saga) orderSpan(ctx context.Context, p store.Placement, name string,
	attrs ...attr.KeyValue) (context.Context, trace.Span) {
	if trace.SpanContextFromContext(ctx).IsValid() {
		return s.span(ctx, name, attrs...)
	}
	return s.d.Telemetry.InOrder(ctx, p, name, attrs...)
}

// orderSpanByID is orderSpan for a caller holding only the id, reading the row while traces are on
func (s *Saga) orderSpanByID(ctx context.Context, id uuid.UUID, name string,
	attrs ...attr.KeyValue) (context.Context, trace.Span) {
	none := trace.SpanFromContext(context.Background())
	if !s.d.Telemetry.Enabled() {
		return ctx, none
	}
	p, err := s.d.Store.PlacementByID(ctx, id)
	if err != nil {
		return ctx, none
	}
	return s.orderSpan(ctx, p, name, attrs...)
}

// finish ends span, marking it failed with a stable reason when err is not nil
func finish(span trace.Span, err error) {
	if err != nil {
		telemetry.Fail(span, errorReason(err))
	}
	span.End()
}

// errorReason is a stable word for err that quotes no provider or runner text
func errorReason(err error) string {
	var run *RunnerError
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.As(err, &run):
		if run.Value != "" {
			return run.Value
		}
		return "runner_failed"
	}
	if pe, ok := provider.AsError(err); ok {
		return "provider_" + string(pe.Code)
	}
	return "failed"
}

// stateOf names a placement state for the order root's attributes
func stateOf(p store.Placement) string { return strings.ToLower(p.State) }
