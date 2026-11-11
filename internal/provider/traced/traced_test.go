package traced_test

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/traced"
	"github.com/bugnet-sh/bugnet-router/internal/telemetry/telemetrytest"
)

func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

// stream is a command that exits with code
type stream struct {
	code int
	read io.Reader
}

func (s *stream) Read(p []byte) (int, error) { return s.read.Read(p) }
func (s *stream) Wait() (int, error)         { return s.code, nil }
func (s *stream) Close() error               { return nil }

// base is a provider doing nothing but what the tests ask
type base struct {
	provider.Provider
	name string
	err  error
}

func (b *base) Name() string { return b.name }
func (b *base) Create(context.Context, provider.CreateRequest) (provider.Ref, error) {
	return provider.Ref{Provider: b.name, ID: "sbx-1"}, b.err
}
func (b *base) WaitReady(context.Context, provider.Ref) error { return nil }
func (b *base) Destroy(context.Context, provider.Ref) error   { return nil }
func (b *base) Exec(context.Context, provider.Ref, provider.ExecSpec) (provider.ExecStream, error) {
	return &stream{code: 3, read: strings.NewReader("out")}, nil
}
func (b *base) ReadFile(context.Context, provider.Ref, string, int64, int) ([]byte, bool, error) {
	return []byte("12345"), true, nil
}
func (b *base) WriteFile(context.Context, provider.Ref, string, []byte, fs.FileMode) error {
	return nil
}

// costly is a provider with a start fee, as Boat is
type costly struct{ *base }

func (costly) StartCostMicros(provider.Size) (int64, bool) { return 5, true }

func TestProviderAsThroughWrapper(t *testing.T) {
	tel := telemetrytest.New(t, "router-api", 1, nil)
	tr := tel.Tracer("test")
	boat := traced.Wrap(costly{&base{name: "boat"}}, tr, nil)
	sc, ok := provider.As[provider.StartCoster](boat)
	eq(t, ok, true)
	micros, _ := sc.StartCostMicros(provider.Size{})
	eq(t, micros, int64(5))
	e2b := traced.Wrap(&base{name: "e2b"}, tr, nil)
	_, ok = provider.As[provider.StartCoster](e2b)
	eq(t, ok, false)
	// A wrapper's own methods are the port alone, so a direct assertion never finds a fee
	_, direct := boat.(provider.StartCoster)
	eq(t, direct, false)
	// Wrapped twice still reaches the adapter
	_, ok = provider.As[provider.StartCoster](traced.Wrap(boat, tr, nil))
	eq(t, ok, true)
}

// ready is a provider that learns Ref.Meta entries while it waits, as Lium is
type ready struct{ *base }

func (ready) ReadyMeta(context.Context, provider.Ref) (map[string]string, error) {
	return map[string]string{"ssh_host": "203.0.113.7"}, nil
}

func TestReadyReporterThroughWrapper(t *testing.T) {
	tel := telemetrytest.New(t, "router-api", 1, nil)
	tr := tel.Tracer("test")
	lium := traced.Wrap(ready{&base{name: "lium"}}, tr, nil)
	rr, ok := provider.As[provider.ReadyReporter](lium)
	eq(t, ok, true)
	meta, err := rr.ReadyMeta(t.Context(), provider.Ref{Provider: "lium", ID: "sbx-1"})
	eq(t, err, nil)
	eq(t, meta, map[string]string{"ssh_host": "203.0.113.7"})
	_, direct := lium.(provider.ReadyReporter)
	eq(t, direct, false)
	_, ok = provider.As[provider.ReadyReporter](traced.Wrap(&base{name: "e2b"}, tr, nil))
	eq(t, ok, false)
}

// ending is a provider whose sandboxes each end at a moment of their own, as HF Jobs do
type ending struct{ *base }

// end is the last moment ending gives every sandbox
var end = time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)

func (ending) MaxUntil(provider.Ref) (time.Time, bool) { return end, true }

func TestMaxUntilerThroughWrapper(t *testing.T) {
	tel := telemetrytest.New(t, "router-api", 1, nil)
	tr := tel.Tracer("test")
	hf := traced.Wrap(ending{&base{name: "hf"}}, tr, nil)
	mu, ok := provider.As[provider.MaxUntiler](hf)
	eq(t, ok, true)
	until, ok := mu.MaxUntil(provider.Ref{Provider: "hf", ID: "sbx-1"})
	eq(t, ok, true)
	eq(t, until, end)
	_, direct := hf.(provider.MaxUntiler)
	eq(t, direct, false)
	_, ok = provider.As[provider.MaxUntiler](traced.Wrap(hf, tr, nil))
	eq(t, ok, true)
	_, ok = provider.As[provider.MaxUntiler](traced.Wrap(&base{name: "e2b"}, tr, nil))
	eq(t, ok, false)
}

func TestExecSpanEndsAtWait(t *testing.T) {
	tel := telemetrytest.New(t, "router-api", 1, testKey)
	p := traced.Wrap(&base{name: "local"}, tel.Tracer("test"), tel.SandboxHash)
	ctx, parent := tel.Tracer("test").Start(t.Context(), "provider_exec_parent")
	ref := provider.Ref{Provider: "local", ID: "sbx-1"}
	st, err := p.Exec(ctx, ref, provider.ExecSpec{Argv: []string{"secret-command"}})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(st)
	eq(t, named(tel.Flushed(t), "provider.exec"), 0)
	code, _ := st.Wait()
	eq(t, code, 3)
	// A second end changes nothing
	_ = st.Close()
	parent.End()
	spans := tel.Flushed(t)
	eq(t, named(spans, "provider.exec"), 1)
	for _, s := range spans {
		if s.Name != "provider.exec" {
			continue
		}
		eq(t, s.Attrs["bugnet.exit_code"], any(int64(3)))
		eq(t, s.Attrs["bugnet.provider"], any("local"))
		eq(t, len(s.Attrs["bugnet.sandbox_hash"].(string)), 12)
		eq(t, s.ParentID != "", true)
		for k, v := range s.Attrs {
			if strings.Contains(k, "command") || v == "secret-command" || v == "sbx-1" {
				t.Fatalf("exported %s=%v", k, v)
			}
		}
	}
}

func TestProviderSpansAndErrors(t *testing.T) {
	tel := telemetrytest.New(t, "router-api", 1, testKey)
	failing := &base{name: "boat", err: &provider.Error{
		Code: provider.CodeCapacity, CanFailOver: true, Err: errors.New("Bearer secret-text"),
	}}
	p := traced.Wrap(failing, tel.Tracer("test"), tel.SandboxHash)
	ctx, parent := tel.Tracer("test").Start(t.Context(), "saga.attempt")
	_, err := p.Create(ctx, provider.CreateRequest{Size: provider.Size{Name: "small"}})
	if err == nil {
		t.Fatal("create succeeded")
	}
	ref := provider.Ref{Provider: "boat", ID: "sbx-1"}
	data, _, _ := p.ReadFile(ctx, ref, "/x", 0, 10)
	eq(t, len(data), 5)
	_ = p.WriteFile(ctx, ref, "/x", []byte("abc"), 0o644)
	_ = p.Destroy(ctx, ref)
	parent.End()
	byName := map[string]telemetrytest.Span{}
	for _, s := range tel.Flushed(t) {
		byName[s.Name] = s
	}
	c := byName["provider.create"]
	eq(t, c.Attrs["bugnet.size"], any("small"))
	eq(t, c.Attrs["bugnet.provider_code"], any("capacity"))
	eq(t, c.Attrs["bugnet.can_fail_over"], any(true))
	eq(t, c.Attrs["error.type"], any("CAPACITY"))
	eq(t, c.StatusText, "CAPACITY")
	eq(t, byName["provider.read_file"].Attrs["bugnet.size_bytes"], any(int64(5)))
	eq(t, byName["provider.write_file"].Attrs["bugnet.size_bytes"], any(int64(3)))
	eq(t, byName["provider.destroy"].Name, "provider.destroy")
}

func TestChildOnlyProviderCallsExportNothing(t *testing.T) {
	tel := telemetrytest.New(t, "router-api", 1, testKey)
	p := traced.Wrap(&base{name: "local"}, tel.Tracer("test"), tel.SandboxHash)
	ref := provider.Ref{Provider: "local", ID: "sbx-1"}
	_, _ = p.Create(t.Context(), provider.CreateRequest{})
	_ = p.Destroy(t.Context(), ref)
	st, _ := p.Exec(t.Context(), ref, provider.ExecSpec{})
	_, _ = st.Wait()
	eq(t, len(tel.Flushed(t)), 0)
}

var testKey = []byte("0123456789abcdef0123456789abcdef")

func named(spans []telemetrytest.Span, name string) int {
	n := 0
	for _, s := range spans {
		if s.Name == name {
			n++
		}
	}
	return n
}
