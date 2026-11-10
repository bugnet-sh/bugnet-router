package imagebuild

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/image"
	"github.com/bugnet-sh/bugnet-router/internal/leakscan"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
)

const testSecret = "zz-needle-value-1234567890"

var (
	small = provider.Size{Name: "2x2", Resources: provider.Resources{VCPU: 2, MemoryMiB: 2048}}
	large = provider.Size{Name: "4x8", Resources: provider.Resources{VCPU: 4, MemoryMiB: 8192}}
)

// fakeBox is one sandbox of the fake provider
type fakeBox struct {
	id      string
	size    string
	builder bool
	files   map[string][]byte
	random  string
}

// fakeProvider is a provider that runs the build's commands from a table, with hooks to fail them
type fakeProvider struct {
	provider.Provider
	mu        sync.Mutex
	name      string
	user      string
	boxes     map[string]*fakeBox
	creates   []provider.CreateRequest
	destroyed []string
	// destroyCtxLive records whether each destroy still had a live context
	destroyCtxLive []bool
	saved          []string
	deleted        []string
	next, sold     int
	tools          map[string]string
	// hooks, each returning an error or a value for the fake to act on
	failBuild func(size string) bool
	failSave  func(name string) bool
	leakFor   func(size string) bool
	// preset gives each sold sandbox its random bytes in order
	preset     []string
	probeTools func(box *fakeBox) map[string]string
	hostKeyFor func(n int) string
	// onExec runs at the start of every exec, to cancel a context mid-build
	onExec func(argv []string)
}

func newFakeProvider(name string) *fakeProvider {
	return &fakeProvider{
		name: name, user: "root", boxes: map[string]*fakeBox{},
		tools: map[string]string{"git": "2.43", "python3": "3.12", "node": "24.2"},
	}
}

func (f *fakeProvider) Name() string { return f.name }

func (f *fakeProvider) Capabilities() provider.Capabilities {
	return provider.Capabilities{ExecUser: f.user}
}

func (f *fakeProvider) Create(
	_ context.Context, req provider.CreateRequest,
) (provider.Ref, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.creates = append(f.creates, req)
	f.next++
	b := &fakeBox{
		id: fmt.Sprintf("box-%d", f.next), size: req.Size.Name, builder: req.Builder,
		files: map[string][]byte{},
	}
	if !req.Builder {
		b.files[image.ManifestPath] = f.manifestJSON("2026.11.1", "dev", f.name)
		if len(f.preset) > 0 {
			b.random = f.preset[(f.sold)%len(f.preset)]
			f.sold++
		}
	}
	f.boxes[b.id] = b
	return provider.Ref{Provider: f.name, ID: b.id}, nil
}

func (f *fakeProvider) manifestJSON(version, channel, target string) []byte {
	m := image.Manifest{
		V: 1, Version: version, Channel: channel, Target: target, OS: "ubuntu-24.04",
		BuiltAt: time.Unix(0, 0).UTC(), Tools: f.tools,
		RunnerSHA256: map[string]string{}, CAs: []image.CA{},
	}
	raw, _ := json.Marshal(m)
	return raw
}

func (f *fakeProvider) WaitReady(context.Context, provider.Ref) error { return nil }

func (f *fakeProvider) Destroy(ctx context.Context, ref provider.Ref) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.destroyed = append(f.destroyed, ref.ID)
	f.destroyCtxLive = append(f.destroyCtxLive, ctx.Err() == nil)
	return nil
}

func (f *fakeProvider) box(ref provider.Ref) *fakeBox { return f.boxes[ref.ID] }

func (f *fakeProvider) WriteFile(
	_ context.Context, ref provider.Ref, path string, data []byte, _ fs.FileMode,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.box(ref).files[path] = slices.Clone(data)
	return nil
}

func (f *fakeProvider) ReadFile(
	_ context.Context, ref provider.Ref, path string, _ int64, _ int,
) ([]byte, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.box(ref).files[path]
	if !ok {
		return nil, false, &provider.Error{Code: provider.CodeNotFound}
	}
	return b, true, nil
}

type fakeReader struct {
	*bytes.Reader
	size int64
}

func (r fakeReader) Info() provider.FileEntry { return provider.FileEntry{Size: r.size} }
func (r fakeReader) Close() error             { return nil }

func (f *fakeProvider) OpenReader(
	_ context.Context, ref provider.Ref, path string, _ int64,
) (provider.FileReader, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.box(ref).files[path]
	if !ok {
		return nil, &provider.Error{Code: provider.CodeNotFound}
	}
	return fakeReader{bytes.NewReader(b), int64(len(b))}, nil
}

type fakeStream struct {
	io.Reader
	code int
}

func (s fakeStream) Wait() (int, error) { return s.code, nil }
func (s fakeStream) Close() error       { return nil }

func (f *fakeProvider) Exec(
	_ context.Context, ref provider.Ref, spec provider.ExecSpec,
) (provider.ExecStream, error) {
	argv := slices.Clone(spec.Argv)
	if f.onExec != nil {
		f.onExec(argv)
	}
	out, code := f.run(ref, argv)
	return fakeStream{Reader: strings.NewReader(out), code: code}, nil
}

func (f *fakeProvider) run(ref provider.Ref, argv []string) (string, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b := f.box(ref)
	line := strings.Join(argv, " ")
	switch {
	case strings.Contains(line, "setup-image.sh build"):
		if f.failBuild != nil && f.failBuild(b.size) {
			return "setup-image: step4 failed\n", 1
		}
		b.files[image.ManifestPath] = f.manifestJSON("2026.11.1", "dev", f.name)
	case strings.Contains(line, "setup-image.sh finish"):
		b.files[image.ManifestPath] = f.manifestJSON("2026.11.1", "dev", f.name)
	case strings.Contains(line, "setup-image.sh probe"):
		tools := f.tools
		if f.probeTools != nil {
			tools = f.probeTools(b)
		}
		raw, _ := json.Marshal(tools)
		return string(raw), 0
	case strings.HasSuffix(line, "sh "+collectPath):
		b.files[leakscan.ArchivePath] = f.archive(b)
	case strings.HasPrefix(line, "cat "+image.ManifestPath):
		if m, ok := b.files[image.ManifestPath]; ok {
			return string(m), 0
		}
		return "", 1
	case strings.Contains(" "+line, " df "):
		return "     Avail\n 123456789\n", 0
	case strings.Contains(line, "od -An"):
		return b.random, 0
	}
	return "", 0
}

// archive is the tar the collector would leave, with the test secret when leakFor says so
func (f *fakeProvider) archive(b *fakeBox) []byte {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	add := func(name, body string) {
		_ = tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(body))})
		_, _ = tw.Write([]byte(body))
	}
	add("etc/hostname", "box\n")
	if f.leakFor != nil && f.leakFor(b.size) {
		add("etc/leaked.conf", "k="+testSecret)
	}
	_ = tw.Close()
	return buf.Bytes()
}

func (f *fakeProvider) SaveImage(_ context.Context, _ provider.Ref, name string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failSave != nil && f.failSave(name) {
		return "", errors.New("save refused")
	}
	f.saved = append(f.saved, name)
	return name, nil
}

func (f *fakeProvider) DeleteImage(_ context.Context, ref string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, ref)
	return nil
}

// builders counts the builder sandboxes made and how many of them were destroyed
func (f *fakeProvider) builders() (made, gone int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for id, b := range f.boxes {
		if b.builder {
			made++
			if slices.Contains(f.destroyed, id) {
				gone++
			}
		}
	}
	return made, gone
}

// needles is the one needle every test scans for
func needles() leakscan.Needles {
	return leakscan.Needles{{Name: "TEST_NEEDLE", Value: []byte(testSecret)}}
}

func baseSpec() Spec {
	return Spec{
		Version: "2026.11.1", Channel: "dev", Inputs: strings.Repeat("a", 64),
		Payload: []byte("payload"), Sizes: []provider.Size{small, large}, Needles: needles(),
	}
}

func eq[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

var _ = saga.MetaSSHHostKey
