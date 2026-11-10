// Package telemetry builds the router's OpenTelemetry traces, one trace per order (DP 20)
package telemetry

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"regexp"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/bugnet-sh/bugnet-router/internal/dpkeys"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/telemetry/attr"
)

// Settings is the [telemetry] table
type Settings struct {
	Traces        bool
	OTLPEndpoint  string
	SampleRatio   float64
	Environment   string
	ExportTimeout time.Duration
	BatchTimeout  time.Duration
	BatchSize     int
	QueueSize     int
}

// Process names the binary on every span it exports
type Process struct {
	Service    string // router-api or router-worker
	InstanceID string // replica_id
	Version    string // from debug.ReadBuildInfo when empty
}

// Telemetry is one process's tracing, a no-op when traces are off
type Telemetry struct {
	on       bool
	provider *sdktrace.TracerProvider
	ratio    float64
	key      []byte
	log      *slog.Logger
	filter   *filterExporter
	noop     trace.TracerProvider
}

// Off returns a Telemetry that builds no spans and sends no headers
func Off() *Telemetry {
	return &Telemetry{noop: noop.NewTracerProvider(), log: slog.Default()}
}

// New builds the exporter and tracer provider, authorization being BUGNET_OTLP_AUTHORIZATION
func New(s Settings, p Process, authorization string, keys [][]byte, log *slog.Logger) (
	*Telemetry, error) {
	if !s.Traces {
		return Off(), nil
	}
	if log == nil {
		log = slog.Default()
	}
	if err := s.validate(); err != nil {
		return nil, err
	}
	version := p.Version
	if version == "" {
		version = buildVersion()
	}
	if !attr.ResourceAllowed(p.Service, version, p.InstanceID, s.Environment) {
		return nil, errors.New("telemetry: the process name, version or instance id is not allowed")
	}
	res := resource.NewSchemaless(attr.Resource(p.Service, version, p.InstanceID, s.Environment)...)
	exp, err := otlptracehttp.New(context.Background(), exporterOptions(s, authorization)...)
	if err != nil {
		return nil, fmt.Errorf("telemetry: %w", err)
	}
	t := &Telemetry{on: true, ratio: s.SampleRatio, log: log, noop: noop.NewTracerProvider()}
	if len(keys) > 0 {
		t.key = dpkeys.Derive(keys[0], "telemetry")
	}
	t.filter = newFilterExporter(exp, res, log)
	t.provider = sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(s.SampleRatio))),
		sdktrace.WithIDGenerator(idGenerator{}),
		sdktrace.WithRawSpanLimits(spanLimits),
		sdktrace.WithBatcher(t.filter,
			sdktrace.WithMaxQueueSize(s.QueueSize),
			sdktrace.WithMaxExportBatchSize(s.BatchSize),
			sdktrace.WithBatchTimeout(s.BatchTimeout),
			sdktrace.WithExportTimeout(s.ExportTimeout)),
	)
	watchEnvironment(log)
	setErrorHandler(log)
	return t, nil
}

// spanLimits are DP 20.6.4's, no events and four links
var spanLimits = sdktrace.SpanLimits{
	AttributeValueLengthLimit:   256,
	AttributeCountLimit:         32,
	EventCountLimit:             0,
	LinkCountLimit:              4,
	AttributePerEventCountLimit: 0,
	AttributePerLinkCountLimit:  4,
}

// validate refuses settings the config decoder would have refused, for callers that skip it
func (s Settings) validate() error {
	u, err := url.Parse(s.OTLPEndpoint)
	switch {
	case s.OTLPEndpoint == "":
		return errors.New("telemetry: otlp_endpoint is required while traces is true")
	case err != nil || u.Host == "" || u.User != nil:
		return errors.New("telemetry: otlp_endpoint is not a URL without credentials")
	case u.Scheme != "https" && (u.Scheme != "http" || !loopbackHost(u.Hostname())):
		return errors.New("telemetry: otlp_endpoint must be https, or http to a loopback host")
	case !(s.SampleRatio >= 0 && s.SampleRatio <= 1):
		return errors.New("telemetry: sample_ratio must be 0 to 1")
	case s.QueueSize < 1 || s.BatchSize < 1 || s.BatchSize > s.QueueSize:
		return errors.New("telemetry: batch_size must be 1 to queue_size")
	case s.ExportTimeout <= 0 || s.BatchTimeout <= 0:
		return errors.New("telemetry: export_timeout and batch_timeout must be positive")
	}
	return nil
}

func loopbackHost(h string) bool {
	if h == "localhost" {
		return true
	}
	return strings.HasPrefix(h, "127.") || h == "::1"
}

// exporterOptions passes every exporter option explicitly, so no OTEL_ variable applies
func exporterOptions(s Settings, authorization string) []otlptracehttp.Option {
	headers := map[string]string{}
	if authorization != "" {
		headers["Authorization"] = authorization
	}
	opts := []otlptracehttp.Option{
		otlptracehttp.WithEndpointURL(s.OTLPEndpoint),
		otlptracehttp.WithHeaders(headers),
		otlptracehttp.WithTimeout(s.ExportTimeout),
		otlptracehttp.WithCompression(otlptracehttp.GzipCompression),
		otlptracehttp.WithRetry(otlptracehttp.RetryConfig{
			Enabled: true, InitialInterval: time.Second, MaxInterval: 5 * time.Second,
			MaxElapsedTime: s.ExportTimeout,
		}),
	}
	if strings.HasPrefix(s.OTLPEndpoint, "https://") {
		// The system roots, whatever OTEL_EXPORTER_OTLP_CERTIFICATE says
		opts = append(opts, otlptracehttp.WithTLSClientConfig(&tls.Config{
			MinVersion: tls.VersionTLS12,
		}))
	}
	return opts
}

// buildVersion is 12 hex characters of vcs.revision, with +dirty when modified
func buildVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	rev, dirty := "", false
	for _, kv := range info.Settings {
		switch kv.Key {
		case "vcs.revision":
			rev = kv.Value
		case "vcs.modified":
			dirty = kv.Value == "true"
		}
	}
	if len(rev) < 12 {
		return "unknown"
	}
	if dirty {
		return rev[:12] + "+dirty"
	}
	return rev[:12]
}

var envOnce sync.Once

// watchEnvironment logs the names of OTEL_ variables once, since none of them applies
func watchEnvironment(log *slog.Logger) {
	envOnce.Do(func() {
		var names []string
		for _, kv := range os.Environ() {
			if name, _, _ := strings.Cut(kv, "="); strings.HasPrefix(name, "OTEL_") {
				names = append(names, name)
			}
		}
		if len(names) > 0 {
			log.Warn("OTEL_ variables are ignored, settings live in the telemetry table",
				"names", names)
		}
	})
}

var (
	errMu   sync.Mutex
	errLog  *slog.Logger
	errLast time.Time
	errN    int
	errOnce sync.Once
	reHTTP  = regexp.MustCompile(`\b([1-5][0-9]{2})\b`)
)

// setErrorHandler logs export errors at most once a minute, with a count and the HTTP status
func setErrorHandler(log *slog.Logger) {
	errMu.Lock()
	errLog = log
	errMu.Unlock()
	errOnce.Do(func() {
		otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
			errMu.Lock()
			defer errMu.Unlock()
			errN++
			if time.Since(errLast) < time.Minute {
				return
			}
			errLast = time.Now()
			args := []any{"count", errN}
			if m := reHTTP.FindString(err.Error()); m != "" {
				args = append(args, "status", m)
			}
			errN = 0
			errLog.Warn("trace export failed", args...)
		}))
	})
}

// Flush exports every finished span now, within ctx
func (t *Telemetry) Flush(ctx context.Context) error {
	if !t.Enabled() {
		return nil
	}
	return t.provider.ForceFlush(ctx)
}

// Enabled reports whether spans are exported
func (t *Telemetry) Enabled() bool { return t != nil && t.on }

// Tracer returns the tracer of one package, scoped by its import path
func (t *Telemetry) Tracer(pkg string) trace.Tracer {
	if !t.Enabled() {
		return noop.NewTracerProvider().Tracer(pkg)
	}
	return t.provider.Tracer(pkg)
}

// Shutdown flushes the queue within ctx and stops the exporter
func (t *Telemetry) Shutdown(ctx context.Context) error {
	if !t.Enabled() {
		return nil
	}
	return t.provider.Shutdown(ctx)
}

// SandboxHash is the keyed hash of a provider sandbox id, empty without data-plane keys
func (t *Telemetry) SandboxHash(ref provider.Ref) string {
	return t.keyed(ref.Provider + "\x00" + ref.ID)
}

// OrderHash is the keyed hash of a Payments order id, empty without data-plane keys
func (t *Telemetry) OrderHash(orderID string) string {
	return t.keyed("order\x00" + orderID)
}

func (t *Telemetry) keyed(msg string) string {
	if !t.Enabled() || len(t.key) == 0 {
		return ""
	}
	m := hmac.New(sha256.New, t.key)
	m.Write([]byte(msg))
	return hex.EncodeToString(m.Sum(nil))[:12]
}

// Child starts a span only under a valid span context, else returns ctx and a no-op span
func Child(ctx context.Context, tr trace.Tracer, name string, attrs ...attr.KeyValue) (
	context.Context, trace.Span) {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return ctx, trace.SpanFromContext(context.Background())
	}
	return tr.Start(ctx, name, trace.WithAttributes(attrs...))
}

// Fail marks span failed with a stable reason, never an error's text
func Fail(span trace.Span, reason string) {
	span.SetAttributes(attr.Reason(reason))
	span.SetStatus(codes.Error, strings.ToUpper(reason))
}

var reTraceparent = regexp.MustCompile(`^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$`)

// Continue returns ctx holding the remote span context of a stored traceparent and tracestate,
// and false when traces are off or the traceparent is not valid
func (t *Telemetry) Continue(ctx context.Context, traceparent, tracestate string) (
	context.Context, bool) {
	if !t.Enabled() || !reTraceparent.MatchString(traceparent) {
		return ctx, false
	}
	h := propagation.MapCarrier{"traceparent": traceparent, "tracestate": tracestate}
	out := propagation.TraceContext{}.Extract(ctx, h)
	if !trace.SpanContextFromContext(out).IsValid() {
		return ctx, false
	}
	return out, true
}
