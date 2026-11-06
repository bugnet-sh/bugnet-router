package e2b

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/provider/contract"
)

func TestE2BStreamsContract(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	contract.Streams(t, p, f.add(nil), t.TempDir())
}

// streamUp streams data into name through OpenWriter and commits it
func streamUp(t *testing.T, p *Provider, ref provider.Ref, name string, data []byte,
	mode os.FileMode,
) provider.FileEntry {
	t.Helper()
	w, err := p.OpenWriter(t.Context(), ref, name,
		provider.WriteSpec{Size: int64(len(data)), Mode: mode})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	entry, err := w.Commit(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

func TestE2BStreamsAskForIdentity(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := f.add(nil)
	name := filepath.Join(t.TempDir(), "f")
	streamUp(t, p, ref, name, []byte("0123456789"), 0o644)
	r, err := p.OpenReader(t.Context(), ref, name, 3)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	_ = r.Close()
	eq(t, string(got), "3456789")
	eq(t, err, nil)

	calls := f.filesSeen()
	var methods []string
	for _, c := range calls {
		methods = append(methods, c.method)
		if c.encoding != "identity" || c.protoMajor != 1 {
			t.Errorf("%s /files asked for encoding %q over HTTP/%d, want identity over HTTP/1",
				c.method, c.encoding, c.protoMajor)
		}
	}
	eq(t, methods, []string{http.MethodPost, http.MethodGet})
	eq(t, f.rangesAsked(), []string{"bytes=3-9"})

	tr := p.streams.Transport.(proxyTransport).base.(*http.Transport)
	eq(t, tr.DisableCompression, true)
	eq(t, [3]bool{tr.Protocols.HTTP1(), tr.Protocols.HTTP2(), tr.Protocols.UnencryptedHTTP2()},
		[3]bool{true, false, false})
	eq(t, p.streams.CheckRedirect != nil, true)
}

func TestE2BNoSpace(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := f.add(nil)
	dir := t.TempDir()
	name := filepath.Join(dir, "f")
	if err := os.WriteFile(name, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	f.noSpace.Store(true)
	w, err := p.OpenWriter(t.Context(), ref, name, provider.WriteSpec{Size: 10, Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	_, err = w.Write([]byte("0123456789"))
	if err == nil {
		_, err = w.Commit(t.Context(), nil)
	}
	if !isCode(err, provider.CodeNoSpace) {
		t.Fatalf("an upload envd answered 507 gave %v, want no_space", err)
	}
	got, _ := os.ReadFile(name)
	eq(t, string(got), "old")
	entries, _ := os.ReadDir(dir)
	eq(t, len(entries), 1)

	err = p.WriteFile(t.Context(), ref, filepath.Join(dir, "g"), []byte("x"), 0o644)
	if !isCode(err, provider.CodeNoSpace) {
		t.Fatalf("WriteFile envd answered 507 gave %v, want no_space", err)
	}
}

func TestE2BStreamWritesBesideTheTarget(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := f.add(nil)
	dir := t.TempDir()
	name := filepath.Join(dir, "f")
	entry := streamUp(t, p, ref, name, []byte("0123456789"), 0o600)
	if entry.Path != name || entry.Size != 10 || !entry.Regular {
		t.Errorf("Commit described %+v, want a regular file of 10 bytes at %s", entry, name)
	}

	temp := regexp.MustCompile(`^` + regexp.QuoteMeta(dir) + `/\.f\.bugnet-[0-9a-f]{8}\.part$`)
	calls := f.filesSeen()
	if len(calls) != 1 || !temp.MatchString(calls[0].path) {
		t.Fatalf("uploads went to %+v, want one to a temp file beside %s", calls, name)
	}
	eq(t, f.movesSeen(), [][2]string{{calls[0].path, name}})
	chmod := f.lastStart().msg.GetProcess()
	eq(t, chmod.GetCmd(), "chmod")
	eq(t, chmod.GetArgs(), []string{"--", "600", calls[0].path})
	info, err := os.Stat(name)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("committed file %v, %v, want mode 0600", info, err)
	}
}

func TestE2BReaderFromAnOffset(t *testing.T) {
	modes := []struct {
		name                    string
		noRange, noContentRange bool
	}{
		{name: "range"},
		{name: "range without content range", noContentRange: true},
		{name: "whole file", noRange: true},
	}
	data := make([]byte, 100<<10+7)
	_, _ = rand.Read(data)
	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			f := newFake(t)
			f.noRange, f.noContentRange = mode.noRange, mode.noContentRange
			p := f.provider(t, fakeKey)
			ref := f.add(nil)
			name := filepath.Join(t.TempDir(), "blob")
			streamUp(t, p, ref, name, data, 0o644)
			for _, offset := range []int64{0, 37, int64(len(data)) - 1, int64(len(data))} {
				r, err := p.OpenReader(t.Context(), ref, name, offset)
				if err != nil {
					t.Fatal(err)
				}
				got, err := io.ReadAll(r)
				_ = r.Close()
				if err != nil || !bytes.Equal(got, data[offset:]) {
					t.Errorf("read from %d gave %d bytes, %v, want %d", offset, len(got), err,
						len(data)-int(offset))
				}
			}
		})
	}
}

func TestE2BReaderThatEndsEarlyFails(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := f.add(nil)
	name := filepath.Join(t.TempDir(), "f")
	streamUp(t, p, ref, name, bytes.Repeat([]byte("x"), 64<<10), 0o644)
	f.shortBody.Store(true)
	r, err := p.OpenReader(t.Context(), ref, name, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	got, err := io.ReadAll(r)
	if len(got) != 32<<10 || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("a body cut in half read %d bytes, %v, want 32 KiB and an unexpected EOF",
			len(got), err)
	}
}

func TestE2BReaderRefusesAnEncodedBody(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := f.add(nil)
	name := filepath.Join(t.TempDir(), "f")
	streamUp(t, p, ref, name, []byte("0123456789"), 0o644)
	f.gzipBody.Store(true)
	r, err := p.OpenReader(t.Context(), ref, name, 0)
	if err == nil {
		_ = r.Close()
		t.Fatal("OpenReader took a gzip encoded body")
	}
}

func TestE2BStreamsRefuseForeignRefsAndBadPaths(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := f.add(nil)
	foreign := ref
	foreign.Provider = "boxd"
	spec := provider.WriteSpec{Size: 1, Mode: 0o644}
	_, err := p.StatFile(t.Context(), foreign, "/tmp")
	eq(t, isCode(err, provider.CodeNotFound), true)
	_, err = p.OpenReader(t.Context(), foreign, "/tmp/f", 0)
	eq(t, isCode(err, provider.CodeNotFound), true)
	_, err = p.OpenWriter(t.Context(), foreign, "/tmp/f", spec)
	eq(t, isCode(err, provider.CodeNotFound), true)
	for _, bad := range []string{"", "/", ".."} {
		_, err = p.OpenWriter(t.Context(), ref, bad, spec)
		eq(t, isCode(err, provider.CodeInvalid), true)
	}
	_, err = p.OpenWriter(t.Context(), ref, "/tmp/f", provider.WriteSpec{Size: -1})
	eq(t, isCode(err, provider.CodeInvalid), true)
	eq(t, len(f.filesSeen()), 0)
}

func TestE2BStreamsRefuseAgentRedirects(t *testing.T) {
	agent, reached := contract.Redirector(t)
	f := newFake(t)
	p, err := New(Options{
		APIKey: fakeKey, Templates: fakeTemplates, BaseURL: f.rest.URL, EnvdURL: agent,
	})
	if err != nil {
		t.Fatal(err)
	}
	ref := f.add(nil)
	w, err := p.OpenWriter(t.Context(), ref, "/tmp/f", provider.WriteSpec{Size: 1, Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = w.Write([]byte("x"))
	if _, err := w.Commit(t.Context(), nil); err == nil {
		t.Error("a stream through a redirecting envd committed")
	}
	if r, err := p.OpenReader(t.Context(), ref, "/etc/hostname", 0); err == nil {
		_ = r.Close()
		t.Error("a read through a redirecting envd opened")
	}
	if reached.Load() {
		t.Fatal("a stream followed an envd redirect")
	}
}
