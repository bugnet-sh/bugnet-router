// Package traced wraps a provider adapter so each port call is a child-only span (DP 20.4.4)
package traced

import (
	"context"
	"io/fs"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/telemetry"
	"github.com/bugnet-sh/bugnet-router/internal/telemetry/attr"
)

// Wrap returns p with each port call a child-only span, its optional interfaces behind As
func Wrap(p provider.Provider, tr trace.Tracer, hash func(provider.Ref) string) provider.Provider {
	if hash == nil {
		hash = func(provider.Ref) string { return "" }
	}
	return &wrapped{Provider: p, tr: tr, hash: hash}
}

type wrapped struct {
	provider.Provider
	tr   trace.Tracer
	hash func(provider.Ref) string
}

// Unwrap returns the adapter, so provider.As reaches its optional interfaces
func (w *wrapped) Unwrap() provider.Provider { return w.Provider }

// start opens a provider span, a no-op under a context without a span
func (w *wrapped) start(ctx context.Context, method string, ref *provider.Ref,
	extra ...attr.KeyValue) (context.Context, trace.Span) {
	ctx, span := telemetry.Child(ctx, w.tr, "provider."+method,
		append([]attr.KeyValue{attr.Provider(w.Name())}, extra...)...)
	if ref != nil {
		w.tagRef(span, *ref)
	}
	return ctx, span
}

func (w *wrapped) tagRef(span trace.Span, ref provider.Ref) {
	if h := w.hash(ref); h != "" {
		span.SetAttributes(attr.SandboxHash(h))
	}
}

// end finishes span with the failure's stable words, never its text
func end(span trace.Span, err error) {
	if err != nil {
		fail(span, err)
	}
	span.End()
}

func fail(span trace.Span, err error) {
	typ := "ERROR"
	if pe, ok := provider.AsError(err); ok {
		span.SetAttributes(attr.ProviderCode(string(pe.Code)), attr.CanFailOver(pe.CanFailOver))
		typ = strings.ToUpper(string(pe.Code))
	}
	span.SetAttributes(attr.ErrorType(typ))
	span.SetStatus(codes.Error, typ)
}

func (w *wrapped) Create(ctx context.Context, req provider.CreateRequest) (provider.Ref, error) {
	ctx, span := w.start(ctx, "create", nil, attr.Size(req.Size.Name))
	ref, err := w.Provider.Create(ctx, req)
	if err == nil {
		w.tagRef(span, ref)
	}
	end(span, err)
	return ref, err
}

func (w *wrapped) WaitReady(ctx context.Context, ref provider.Ref) error {
	ctx, span := w.start(ctx, "wait_ready", &ref)
	err := w.Provider.WaitReady(ctx, ref)
	end(span, err)
	return err
}

func (w *wrapped) SetLifetime(ctx context.Context, ref provider.Ref, until time.Time) error {
	ctx, span := w.start(ctx, "set_lifetime", &ref)
	err := w.Provider.SetLifetime(ctx, ref, until)
	end(span, err)
	return err
}

func (w *wrapped) Destroy(ctx context.Context, ref provider.Ref) error {
	ctx, span := w.start(ctx, "destroy", &ref)
	err := w.Provider.Destroy(ctx, ref)
	end(span, err)
	return err
}

func (w *wrapped) Exec(ctx context.Context, ref provider.Ref, spec provider.ExecSpec) (
	provider.ExecStream, error) {
	ctx, span := w.start(ctx, "exec", &ref)
	st, err := w.Provider.Exec(ctx, ref, spec)
	if err != nil {
		end(span, err)
		return nil, err
	}
	return &execStream{ExecStream: st, span: span}, nil
}

func (w *wrapped) ReadFile(ctx context.Context, ref provider.Ref, path string, offset int64,
	length int) ([]byte, bool, error) {
	ctx, span := w.start(ctx, "read_file", &ref)
	data, eof, err := w.Provider.ReadFile(ctx, ref, path, offset, length)
	span.SetAttributes(attr.SizeBytes(int64(len(data))))
	end(span, err)
	return data, eof, err
}

func (w *wrapped) WriteFile(ctx context.Context, ref provider.Ref, path string, data []byte,
	mode fs.FileMode) error {
	ctx, span := w.start(ctx, "write_file", &ref, attr.SizeBytes(int64(len(data))))
	err := w.Provider.WriteFile(ctx, ref, path, data, mode)
	end(span, err)
	return err
}

func (w *wrapped) ListFiles(ctx context.Context, ref provider.Ref, path string, depth int,
	into *provider.Listing) error {
	ctx, span := w.start(ctx, "list_files", &ref)
	err := w.Provider.ListFiles(ctx, ref, path, depth, into)
	span.SetAttributes(attr.Count(len(into.Entries)))
	end(span, err)
	return err
}

func (w *wrapped) ListOwned(ctx context.Context, labels map[string]string) (
	[]provider.Summary, error) {
	ctx, span := w.start(ctx, "list_owned", nil)
	out, err := w.Provider.ListOwned(ctx, labels)
	span.SetAttributes(attr.Count(len(out)))
	end(span, err)
	return out, err
}

func (w *wrapped) StatFile(ctx context.Context, ref provider.Ref, path string) (
	provider.FileEntry, error) {
	ctx, span := w.start(ctx, "stat_file", &ref)
	e, err := w.Provider.StatFile(ctx, ref, path)
	end(span, err)
	return e, err
}

func (w *wrapped) OpenReader(ctx context.Context, ref provider.Ref, path string, offset int64) (
	provider.FileReader, error) {
	ctx, span := w.start(ctx, "open_reader", &ref)
	r, err := w.Provider.OpenReader(ctx, ref, path, offset)
	if err != nil {
		end(span, err)
		return nil, err
	}
	return &reader{FileReader: r, span: span}, nil
}

func (w *wrapped) OpenWriter(ctx context.Context, ref provider.Ref, path string,
	spec provider.WriteSpec) (provider.FileWriter, error) {
	ctx, span := w.start(ctx, "open_writer", &ref)
	fw, err := w.Provider.OpenWriter(ctx, ref, path, spec)
	if err != nil {
		end(span, err)
		return nil, err
	}
	return &writer{FileWriter: fw, span: span}, nil
}

// execStream ends its span at Wait or Close, whichever comes first
type execStream struct {
	provider.ExecStream
	span trace.Span
	once sync.Once
}

func (s *execStream) Wait() (int, error) {
	code, err := s.ExecStream.Wait()
	s.once.Do(func() {
		if err == nil {
			s.span.SetAttributes(attr.ExitCode(code))
		}
		end(s.span, err)
	})
	return code, err
}

func (s *execStream) Close() error {
	err := s.ExecStream.Close()
	s.once.Do(func() { end(s.span, nil) })
	return err
}

// reader ends its span at Close, or at the first failure, with the bytes read
type reader struct {
	provider.FileReader
	span trace.Span
	n    atomic.Int64
	once sync.Once
}

func (r *reader) Read(p []byte) (int, error) {
	n, err := r.FileReader.Read(p)
	r.n.Add(int64(n))
	return n, err
}

func (r *reader) Close() error {
	err := r.FileReader.Close()
	r.once.Do(func() {
		r.span.SetAttributes(attr.SizeBytes(r.n.Load()))
		end(r.span, nil)
	})
	return err
}

// writer ends its span at Commit or Abort, with the bytes written
type writer struct {
	provider.FileWriter
	span trace.Span
	n    atomic.Int64
	once sync.Once
}

func (w *writer) Write(p []byte) (int, error) {
	n, err := w.FileWriter.Write(p)
	w.n.Add(int64(n))
	return n, err
}

func (w *writer) Commit(ctx context.Context, sum []byte) (provider.FileEntry, error) {
	e, err := w.FileWriter.Commit(ctx, sum)
	w.finish(err)
	return e, err
}

func (w *writer) Abort() error {
	err := w.FileWriter.Abort()
	w.finish(nil)
	return err
}

func (w *writer) finish(err error) {
	w.once.Do(func() {
		w.span.SetAttributes(attr.SizeBytes(w.n.Load()))
		end(w.span, err)
	})
}
