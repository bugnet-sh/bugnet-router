package telemetry

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/bugnet-sh/bugnet-router/internal/telemetry/attr"
)

// filterExporter re-checks every span against the allowlist before it leaves the process
type filterExporter struct {
	next    sdktrace.SpanExporter
	res     *resource.Resource
	log     *slog.Logger
	dropped atomic.Int64
	warned  sync.Map
}

func newFilterExporter(
	next sdktrace.SpanExporter, res *resource.Resource, log *slog.Logger,
) *filterExporter {
	return &filterExporter{next: next, res: res, log: log}
}

// ExportSpans passes each span on with only the attributes the allowlist names
func (f *filterExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	out := make([]sdktrace.ReadOnlySpan, len(spans))
	for i, s := range spans {
		out[i] = f.clean(s)
	}
	return f.next.ExportSpans(ctx, out)
}

// Shutdown stops the wrapped exporter
func (f *filterExporter) Shutdown(ctx context.Context) error { return f.next.Shutdown(ctx) }

// Dropped counts the attributes, names and descriptions the filter removed
func (f *filterExporter) Dropped() int64 { return f.dropped.Load() }

// drop counts removed items and warns once per key per process, naming the key, never the value
func (f *filterExporter) drop(keys []string) {
	for _, k := range keys {
		f.dropped.Add(1)
		if _, seen := f.warned.LoadOrStore(k, true); !seen {
			f.log.Warn("trace attribute dropped by the allowlist", "key", k)
		}
	}
}

func (f *filterExporter) clean(s sdktrace.ReadOnlySpan) sdktrace.ReadOnlySpan {
	attrs, dropped := attr.Filter(s.Attributes())
	f.drop(dropped)
	links := s.Links()
	cleanLinks := make([]sdktrace.Link, len(links))
	for i, l := range links {
		kept, dropped := attr.Filter(l.Attributes)
		f.drop(dropped)
		cleanLinks[i] = sdktrace.Link{
			SpanContext: l.SpanContext, Attributes: kept,
			DroppedAttributeCount: l.DroppedAttributeCount + len(dropped),
		}
	}
	name := s.Name()
	if !attr.SpanNameOK(name) {
		f.drop([]string{"span.name"})
		name = "unnamed"
	}
	status := s.Status()
	if status.Description != "" && !attr.ErrorTypeOK(status.Description) {
		f.drop([]string{"status.description"})
		status.Description = ""
	}
	if n := len(s.Events()); n > 0 {
		f.dropped.Add(int64(n))
	}
	return cleanSpan{
		ReadOnlySpan: s, name: name, attrs: attrs, links: cleanLinks, status: status, res: f.res,
	}
}

// cleanSpan is a span with its attributes, links, events and status replaced
type cleanSpan struct {
	sdktrace.ReadOnlySpan
	name   string
	attrs  []attr.KeyValue
	links  []sdktrace.Link
	status sdktrace.Status
	res    *resource.Resource
}

func (c cleanSpan) Name() string                 { return c.name }
func (c cleanSpan) Attributes() []attr.KeyValue  { return c.attrs }
func (c cleanSpan) Links() []sdktrace.Link       { return c.links }
func (c cleanSpan) Events() []sdktrace.Event     { return nil }
func (c cleanSpan) Status() sdktrace.Status      { return c.status }
func (c cleanSpan) Resource() *resource.Resource { return c.res }
