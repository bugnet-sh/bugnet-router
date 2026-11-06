package koyeb

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/provider/contract"
)

// streamProvider is a fake's adapter with small parts, so tests reach the session path cheaply
func streamProvider(t *testing.T, f *fakeKoyeb, part, total int64) *Provider {
	t.Helper()
	p, err := New(Options{
		APIToken: fakeToken, BaseURL: f.api.URL, PartBytes: part, BufferTotal: total,
	})
	if err != nil {
		t.Fatal(err)
	}
	f.trust(p)
	p.pollInterval = 10 * time.Millisecond
	p.reissueWait = time.Millisecond
	p.exitDir = filepath.Join(t.TempDir(), "exec")
	return p
}

func TestKoyebStreamsContract(t *testing.T) {
	f := newFake(t)
	// 64 KiB parts send the contract's 256 KiB file through a session and its small files whole
	p := streamProvider(t, f, 64<<10, 1<<20)
	contract.Streams(t, p, ready(t, p, "key-streams"), t.TempDir())
	eq(t, f.openSessions(), 0)
}

// streamUp streams data into name in writes of at most step bytes and commits it
func streamUp(
	t *testing.T, p *Provider, ref provider.Ref, name string, data []byte, step int,
) (provider.FileEntry, error) {
	t.Helper()
	sum := sha256.Sum256(data)
	w, err := p.OpenWriter(t.Context(), ref, name,
		provider.WriteSpec{Size: int64(len(data)), Mode: 0o640})
	if err != nil {
		return provider.FileEntry{}, err
	}
	for rest := data; len(rest) > 0; {
		n := min(step, len(rest))
		if _, err := w.Write(rest[:n]); err != nil {
			_ = w.Abort()
			return provider.FileEntry{}, err
		}
		rest = rest[n:]
	}
	return w.Commit(t.Context(), sum[:])
}

// routeCount counts the executor calls of one method whose route starts with prefix
func routeCount(f *fakeKoyeb, method, prefix string) int {
	n := 0
	for _, r := range f.routesCalled() {
		m, route, _ := strings.Cut(r, " ")
		if m == method && strings.HasPrefix(route, prefix) {
			n++
		}
	}
	return n
}

func TestKoyebOnePartIsOnePutAtCommit(t *testing.T) {
	f := newFake(t)
	p := streamProvider(t, f, 1<<20, 4<<20)
	ref := ready(t, p, "key-one-part")
	name := filepath.Join(t.TempDir(), "sub", "f")
	data := make([]byte, 1<<20)
	_, _ = rand.Read(data)
	w, err := p.OpenWriter(t.Context(), ref, name, provider.WriteSpec{Size: 1 << 20, Mode: 0o600})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	eq(t, routeCount(f, http.MethodPut, "/files"), 0)
	sum := sha256.Sum256(data)
	entry, err := w.Commit(t.Context(), sum[:])
	if err != nil {
		t.Fatal(err)
	}
	if entry.Path != name || entry.Size != 1<<20 || !entry.Regular {
		t.Errorf("Commit described %+v, want a regular file of 1 MiB at %s", entry, name)
	}
	up := f.lastUpload()
	q := up.URL.Query()
	eq(t, [4]string{q.Get("path"), q.Get("mode"), q.Get("parents"),
		up.Header.Get("X-Content-SHA256")},
		[4]string{name, "0600", "1", hex.EncodeToString(sum[:])})
	eq(t, routeCount(f, http.MethodPost, "/files/uploads"), 0)
	got, _ := os.ReadFile(name)
	eq(t, bytes.Equal(got, data), true)
}

func TestKoyebMultiPartCompletesOnlyOnCommit(t *testing.T) {
	f := newFake(t)
	p := streamProvider(t, f, 64<<10, 1<<20)
	ref := ready(t, p, "key-multi")
	dir := t.TempDir()
	name := filepath.Join(dir, "deep", "f")
	data := make([]byte, 3*64<<10+5)
	_, _ = rand.Read(data)
	w, err := p.OpenWriter(t.Context(), ref, name,
		provider.WriteSpec{Size: int64(len(data)), Mode: 0o640})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	eq(t, routeCount(f, http.MethodPut, "/files/uploads/"), 3)
	eq(t, routeCount(f, http.MethodPost, "/files/uploads/"), 0)
	if _, err := os.Stat(name); !os.IsNotExist(err) {
		t.Fatalf("the target exists before Commit, %v", err)
	}
	sum := sha256.Sum256(data)
	entry, err := w.Commit(t.Context(), sum[:])
	if err != nil {
		t.Fatal(err)
	}
	if entry.Size != int64(len(data)) || !entry.Regular || entry.Mode.Perm() != 0o640 {
		t.Errorf("Commit described %+v", entry)
	}
	eq(t, routeCount(f, http.MethodPut, "/files/uploads/"), 4)
	eq(t, routeCount(f, http.MethodPost, "/files/uploads/"), 1)
	got, _ := os.ReadFile(name)
	eq(t, bytes.Equal(got, data), true)
	eq(t, f.openSessions(), 0)
}

func TestKoyebPartRetries(t *testing.T) {
	data := make([]byte, 2*64<<10)
	_, _ = rand.Read(data)
	t.Run("FailsTwiceThenSucceeds", func(t *testing.T) {
		f := newFake(t)
		p := streamProvider(t, f, 64<<10, 1<<20)
		ref := ready(t, p, "key-retry")
		f.failFiles("PUT /files/uploads/{id}", fileFault{
			status: http.StatusGatewayTimeout, body: "error code: 524", times: 2,
		})
		name := filepath.Join(t.TempDir(), "f")
		if _, err := streamUp(t, p, ref, name, data, len(data)); err != nil {
			t.Fatal(err)
		}
		eq(t, routeCount(f, http.MethodPut, "/files/uploads/"), 4)
		got, _ := os.ReadFile(name)
		eq(t, bytes.Equal(got, data), true)
	})
	t.Run("FourFailuresAbort", func(t *testing.T) {
		f := newFake(t)
		p := streamProvider(t, f, 64<<10, 1<<20)
		ref := ready(t, p, "key-retry-fail")
		f.failFiles("PUT /files/uploads/{id}", fileFault{
			status: http.StatusServiceUnavailable, body: `{"error":"upstream"}`, times: 4,
		})
		name := filepath.Join(t.TempDir(), "f")
		if _, err := streamUp(t, p, ref, name, data, len(data)); err == nil {
			t.Fatal("an upload whose part failed four times committed")
		}
		eq(t, routeCount(f, http.MethodPut, "/files/uploads/"), 4)
		eq(t, routeCount(f, http.MethodPost, "/files/uploads/"), 0)
		eq(t, routeCount(f, http.MethodDelete, "/files/uploads/"), 1)
		eq(t, f.openSessions(), 0)
		if _, err := os.Stat(name); !os.IsNotExist(err) {
			t.Fatalf("a failed upload left the target, %v", err)
		}
	})
	t.Run("RefusalIsNotRetried", func(t *testing.T) {
		f := newFake(t)
		p := streamProvider(t, f, 64<<10, 1<<20)
		ref := ready(t, p, "key-retry-refused")
		f.failFiles("PUT /files", fileFault{
			status: http.StatusForbidden, body: `{"error":"permission denied"}`,
		})
		_, err := streamUp(t, p, ref, filepath.Join(t.TempDir(), "f"), data[:10], 10)
		eq(t, isCode(err, provider.CodeInvalid), true)
		eq(t, routeCount(f, http.MethodPut, "/files"), 1)
	})
}

func TestKoyebBufferTotalBlocks(t *testing.T) {
	f := newFake(t)
	p := streamProvider(t, f, 64<<10, 64<<10)
	ref := ready(t, p, "key-buffers")
	dir := t.TempDir()
	spec := provider.WriteSpec{Size: 2 * 64 << 10, Mode: 0o644}
	first, err := p.OpenWriter(t.Context(), ref, filepath.Join(dir, "a"), spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	second, err := p.OpenWriter(t.Context(), ref, filepath.Join(dir, "b"), spec)
	if err != nil {
		t.Fatal(err)
	}
	wrote := make(chan error, 1)
	go func() {
		_, err := second.Write([]byte("y"))
		wrote <- err
	}()
	select {
	case err := <-wrote:
		t.Fatalf("a second writer wrote while the first held every buffer, %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	eq(t, first.Abort(), nil)
	select {
	case err := <-wrote:
		eq(t, err, nil)
	case <-time.After(5 * time.Second):
		t.Fatal("the second writer still waits after the first one aborted")
	}
	eq(t, second.Abort(), nil)

	// a writer whose context ends stops waiting for a buffer
	third, err := p.OpenWriter(t.Context(), ref, filepath.Join(dir, "c"), spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := third.Write([]byte("z")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	fourth, err := p.OpenWriter(ctx, ref, filepath.Join(dir, "d"), spec)
	if err != nil {
		t.Fatal(err)
	}
	time.AfterFunc(100*time.Millisecond, cancel)
	if _, err := fourth.Write([]byte("w")); err == nil {
		t.Fatal("a writer whose context ended got a buffer the pool did not have")
	}
	eq(t, third.Abort(), nil)
}

func TestKoyebReissuesShortGet(t *testing.T) {
	f := newFake(t)
	p := streamProvider(t, f, 1<<20, 4<<20)
	ref := ready(t, p, "key-reissue")
	name := filepath.Join(t.TempDir(), "f")
	data := make([]byte, 3<<20+7)
	_, _ = rand.Read(data)
	if err := os.WriteFile(name, data, 0o644); err != nil {
		t.Fatal(err)
	}
	f.failFiles("GET /files", fileFault{cut: 1 << 20, times: 2})
	r, err := p.OpenReader(t.Context(), ref, name, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("a read cut twice at 1 MiB gave %d bytes, %v, want the whole file", len(got),
			err)
	}
	var ranges, ifRanges []string
	for _, h := range f.downloadHeaders() {
		ranges = append(ranges, h.Get("Range"))
		ifRanges = append(ifRanges, h.Get("If-Range"))
		eq(t, h.Get("Accept-Encoding"), "identity")
	}
	size := len(data) - 1
	eq(t, ranges, []string{"bytes=0-" + strconv.Itoa(size), "bytes=1048576-" + strconv.Itoa(size),
		"bytes=2097152-" + strconv.Itoa(size)})
	if ifRanges[0] != "" || ifRanges[1] == "" || ifRanges[1] != ifRanges[2] {
		t.Errorf("reissues sent If-Range %q, want the first answer's ETag on each reissue",
			ifRanges)
	}
}

func TestKoyebGivesUpAfterThreeStalls(t *testing.T) {
	f := newFake(t)
	p := streamProvider(t, f, 1<<20, 4<<20)
	ref := ready(t, p, "key-stalls")
	name := filepath.Join(t.TempDir(), "f")
	data := bytes.Repeat([]byte("x"), 2<<20)
	if err := os.WriteFile(name, data, 0o644); err != nil {
		t.Fatal(err)
	}
	f.failFiles("GET /files", fileFault{cut: 1 << 20, times: 1})
	r, err := p.OpenReader(t.Context(), ref, name, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.failFiles("GET /files", fileFault{cut: 0})
	got, err := io.ReadAll(r)
	_ = r.Close()
	if err == nil || len(got) != 1<<20 {
		t.Fatalf("a read that stalled three times gave %d bytes, %v, want 1 MiB and an error",
			len(got), err)
	}
	eq(t, len(f.downloadHeaders()), 4)
}

func TestKoyebStallsCountOnlyInARow(t *testing.T) {
	f := newFake(t)
	p := streamProvider(t, f, 1<<20, 4<<20)
	ref := ready(t, p, "key-stalls-in-a-row")
	name := filepath.Join(t.TempDir(), "f")
	data := make([]byte, 3<<20)
	_, _ = rand.Read(data)
	if err := os.WriteFile(name, data, 0o644); err != nil {
		t.Fatal(err)
	}
	// four answers cut before any byte, never three in a row, each pair parted by a megabyte
	for _, cut := range []int64{0, 1 << 20, 0, 0, 1 << 20, 0} {
		f.failFiles("GET /files", fileFault{cut: cut, times: 1})
	}
	r, err := p.OpenReader(t.Context(), ref, name, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("a read with stalls parted by progress gave %d bytes, %v", len(got), err)
	}
	eq(t, len(f.downloadHeaders()), 7)
}

func TestKoyebReaderSeesAChangedFile(t *testing.T) {
	f := newFake(t)
	p := streamProvider(t, f, 1<<20, 4<<20)
	ref := ready(t, p, "key-changed")
	name := filepath.Join(t.TempDir(), "f")
	data := bytes.Repeat([]byte("x"), 2<<20)
	if err := os.WriteFile(name, data, 0o644); err != nil {
		t.Fatal(err)
	}
	f.failFiles("GET /files", fileFault{cut: 1 << 20, times: 1})
	r, err := p.OpenReader(t.Context(), ref, name, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	buf := make([]byte, 1<<20)
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatal(err)
	}
	later := time.Now().Add(time.Hour)
	if err := os.Chtimes(name, later, later); err != nil {
		t.Fatal(err)
	}
	if n, err := io.Copy(io.Discard, r); err == nil {
		t.Fatalf("a read across a change of the file went on for %d more bytes", n)
	}
}

func TestKoyebStreamsAskForIdentity(t *testing.T) {
	f := newFake(t)
	p := streamProvider(t, f, 1<<20, 4<<20)
	ref := ready(t, p, "key-identity")
	name := filepath.Join(t.TempDir(), "f")
	if _, err := streamUp(t, p, ref, name, []byte("0123456789"), 10); err != nil {
		t.Fatal(err)
	}
	r, err := p.OpenReader(t.Context(), ref, name, 3)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	_ = r.Close()
	eq(t, string(got), "3456789")
	eq(t, err, nil)
	tr := p.streams.Transport.(*http.Transport)
	eq(t, [2]bool{tr.DisableCompression, tr.ForceAttemptHTTP2}, [2]bool{true, false})
	eq(t, tr.TLSNextProto != nil && len(tr.TLSNextProto) == 0, true)
	eq(t, p.streams.CheckRedirect != nil, true)
}

func TestKoyebNoSpace(t *testing.T) {
	noSpace := `{"error":"write /root/.f.koyeb-tmp-1: no space left on device"}`
	for _, c := range []struct {
		name, route string
		size        int
	}{
		{"OnePart", "PUT /files", 10},
		{"Part", "PUT /files/uploads/{id}", 2 * 64 << 10},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newFake(t)
			p := streamProvider(t, f, 64<<10, 1<<20)
			ref := ready(t, p, "key-no-space")
			f.failFiles(c.route, fileFault{status: http.StatusInternalServerError, body: noSpace})
			_, err := streamUp(t, p, ref, filepath.Join(t.TempDir(), "f"),
				make([]byte, c.size), c.size)
			eq(t, isCode(err, provider.CodeNoSpace), true)
		})
	}
}

func TestKoyebStatDoesNotFollowWhereStatWould(t *testing.T) {
	f := newFake(t)
	p := streamProvider(t, f, 1<<20, 4<<20)
	ref := ready(t, p, "key-stat")
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "target"), []byte("abc"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("target", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(dir, "fifo"), 0o644); err != nil {
		t.Fatal(err)
	}
	link, err := p.StatFile(t.Context(), ref, filepath.Join(dir, "link"))
	if err != nil || link.Regular || link.Mode&fs.ModeSymlink == 0 {
		t.Errorf("StatFile of a link gave %+v, %v, want the link", link, err)
	}
	fifo, err := p.StatFile(t.Context(), ref, filepath.Join(dir, "fifo"))
	if err != nil || fifo.Regular || fifo.Mode&fs.ModeIrregular == 0 {
		t.Errorf("StatFile of a FIFO gave %+v, %v, want an irregular entry", fifo, err)
	}
	file, err := p.StatFile(t.Context(), ref, filepath.Join(dir, "target"))
	if err != nil || !file.Regular || file.Size != 3 || file.Path != filepath.Join(dir, "target") {
		t.Errorf("StatFile of a file gave %+v, %v", file, err)
	}
	_, err = p.StatFile(t.Context(), ref, "relative/path")
	eq(t, isCode(err, provider.CodeInvalid), true)
	if slices.ContainsFunc(f.routesCalled(), func(r string) bool {
		return strings.HasSuffix(r, "/files/stat")
	}) {
		t.Error("StatFile asked GET /files/stat, which follows a final symlink")
	}
}

func TestKoyebOptionsForStreams(t *testing.T) {
	for _, o := range []Options{
		{APIToken: "x", PartBytes: 8 << 20, BufferTotal: 4 << 20},
		{APIToken: "x", PartBytes: -1},
		{APIToken: "x", PartBytes: 200 << 20, BufferTotal: 1 << 30},
	} {
		if _, err := New(o); err == nil {
			t.Errorf("New took parts of %d bytes within %d", o.PartBytes, o.BufferTotal)
		}
	}
	p, err := New(Options{APIToken: "x"})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, p.partBytes, int64(8<<20))
}
