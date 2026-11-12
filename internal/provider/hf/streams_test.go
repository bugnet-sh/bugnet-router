package hf

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/contract"
)

func TestHFStreamsContract(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	contract.Streams(t, p, readyJob(t, f, p), t.TempDir())
}

// TestHFStreamsContractInSmallParts runs the contract with many parts to every stream
func TestHFStreamsContractInSmallParts(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	p.partBytes = 4 << 10
	contract.Streams(t, p, readyJob(t, f, p), t.TempDir())
	eq(t, f.peakOf(sbxReadRoute) <= partsInFlight, true)
}

// streamUp streams data into name through OpenWriter and commits it with its sum
func streamUp(t *testing.T, p *Provider, ref provider.Ref, name string, data []byte) error {
	t.Helper()
	ctx := t.Context()
	w, err := p.OpenWriter(ctx, ref, name, provider.WriteSpec{Size: int64(len(data)),
		Mode: 0o600})
	if err != nil {
		return err
	}
	if _, err := io.Copy(w, bytes.NewReader(data)); err != nil {
		_ = w.Abort()
		return err
	}
	sum := sha256.Sum256(data)
	entry, err := w.Commit(ctx, sum[:])
	if err == nil && (entry.Path != name || entry.Size != int64(len(data)) || !entry.Regular) {
		t.Fatalf("Commit described %+v", entry)
	}
	return err
}

// streamDown reads name from offset through OpenReader
func streamDown(t *testing.T, p *Provider, ref provider.Ref, name string, offset int64) ([]byte,
	error) {
	t.Helper()
	r, err := p.OpenReader(t.Context(), ref, name, offset)
	if err != nil {
		return nil, err
	}
	defer func() { _ = r.Close() }()
	return io.ReadAll(r)
}

func TestHFStreamsLargeFile(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	p.partBytes = 256 << 10
	f.mu.Lock()
	f.sbx.slow = 20 * time.Millisecond
	f.mu.Unlock()
	ref := readyJob(t, f, p)
	data := make([]byte, 3<<20+17)
	_, _ = rand.Read(data)
	name := t.TempDir() + "/big.bin"
	if err := streamUp(t, p, ref, name, data); err != nil {
		t.Fatal(err)
	}
	got, err := streamDown(t, p, ref, name, 5)
	if err != nil || !bytes.Equal(got, data[5:]) {
		t.Fatalf("read back %d bytes, %v", len(got), err)
	}
	// Four parts go side by side, design 4.4, never more of one stream
	eq(t, f.peakOf(sbxReadRoute), 4)
	eq(t, f.peakOf(sbxWriteRoute), 4)
	// The reads cover the file from the offset in parts of partBytes, none past the size
	var offsets []int64
	for _, r := range f.requestsTo("GET", "/v1/files/read") {
		off, _ := strconv.ParseInt(r.query.Get("offset"), 10, 64)
		n, _ := strconv.ParseInt(r.query.Get("length"), 10, 64)
		if n > p.partBytes || off+n > int64(len(data)) {
			t.Fatalf("a read of %d at %d", n, off)
		}
		offsets = append(offsets, off)
	}
	slices.Sort(offsets)
	eq(t, offsets[0], int64(5))
	eq(t, len(offsets), 13)
	// The writes are the empty one at open, the parts, and the last with the size and mode
	w := writes(f)
	eq(t, len(w), 1+13)
	eq(t, len(w[0].body), 0)
	last := w[len(w)-1]
	eq(t, last.query.Get("truncate_to"), strconv.Itoa(len(data)))
	eq(t, last.query.Get("mode"), "600")
	eq(t, last.query.Get("offset"), strconv.Itoa(12*256<<10))
	for _, r := range w {
		if len(r.body) > maxBodyBytes {
			t.Fatalf("a body of %d bytes", len(r.body))
		}
	}
}

func TestHFOpenReaderReissuesAShortRead(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	ref := readyJob(t, f, p)
	data := make([]byte, 100<<10)
	_, _ = rand.Read(data)
	name := t.TempDir() + "/f"
	mustWrite(t, name, string(data), 0o644)
	f.inject(sbxReadRoute, fault{cut: 1000, times: 1})
	got, err := streamDown(t, p, ref, name, 10)
	if err != nil || !bytes.Equal(got, data[10:]) {
		t.Fatalf("read %d bytes, %v", len(got), err)
	}
	reads := f.requestsTo("GET", "/v1/files/read")
	eq(t, len(reads), 2)
	eq(t, reads[1].query.Get("offset"), "1010")
	eq(t, reads[1].query.Get("length"), strconv.Itoa(len(data)-1010))

	// A read that never moves fails after readStalls tries
	f.inject(sbxReadRoute, fault{drop: true})
	before := len(f.requestsTo("GET", "/v1/files/read"))
	_, err = streamDown(t, p, ref, name, 0)
	if c, _ := codeOf(err); c != provider.CodeInternal {
		t.Fatalf("an endless drop gave %v", err)
	}
	// Go's transport sends a GET again once when a reused connection closes under it
	if n := len(f.requestsTo("GET", "/v1/files/read")) - before; n < readStalls ||
		n > 2*readStalls {
		t.Fatalf("%d reads for %d stalls", n, readStalls)
	}
	f.clear(sbxReadRoute)

	// A chunked answer one byte longer than its part is a file that changed, never the part
	f.inject(sbxReadRoute, fault{times: 1, serve: func(w http.ResponseWriter, r *http.Request) {
		n, _ := strconv.Atoi(r.URL.Query().Get("length"))
		w.WriteHeader(http.StatusOK)
		_ = http.NewResponseController(w).Flush()
		_, _ = w.Write(make([]byte, n+1))
	}})
	if _, err := streamDown(t, p, ref, name, 0); err == nil ||
		!strings.Contains(err.Error(), "changed size") {
		t.Fatalf("an answer a byte past its part gave %v", err)
	}

	// A file that shrinks while read is an error, never a short file
	r, err := p.OpenReader(t.Context(), ref, name, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(name, 50); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(r); err == nil || !strings.Contains(err.Error(), "changed size") {
		t.Fatalf("a shrunk file gave %v", err)
	}
	_ = r.Close()
}

func TestHFOpenReaderKeepsAPartsError(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	p.partBytes = 4 << 10
	// One part's room, so the parts go one at a time and the second read is the second part
	p.readBuffers = semaphore.NewWeighted(p.partBytes)
	ref := readyJob(t, f, p)
	data := make([]byte, 4*p.partBytes)
	_, _ = rand.Read(data)
	name := t.TempDir() + "/f"
	mustWrite(t, name, string(data), 0o644)
	f.inject(sbxReadRoute, fault{status: 400, message: "bad", after: 1, times: 1})
	r, err := p.OpenReader(t.Context(), ref, name, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	buf := make([]byte, len(data))
	n, err := r.Read(buf)
	if err != nil || !bytes.Equal(buf[:n], data[:p.partBytes]) {
		t.Fatalf("the first part read %d bytes, %v", n, err)
	}
	_, failed := r.Read(buf)
	if c, _ := codeOf(failed); c != provider.CodeInvalid {
		t.Fatalf("the refused part gave %v", failed)
	}
	// The stream never moves on past the part it lost
	for range 3 {
		if n, err := r.Read(buf); n != 0 || err != failed {
			t.Fatalf("a Read after a failed part gave %d bytes, %v", n, err)
		}
	}
}

func TestHFOpenWriterCommit(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	p.partBytes = 1000
	ref := readyJob(t, f, p)
	dir := t.TempDir()
	ctx := t.Context()
	data := make([]byte, 4500)
	_, _ = rand.Read(data)
	if err := streamUp(t, p, ref, dir+"/f", data); err != nil {
		t.Fatal(err)
	}
	// The sum is checked by sha256sum in the Job, then mv moves the temp over the target
	lines := f.execLines()
	eq(t, slices.Contains(lines[len(lines)-2], digestScript), true)
	eq(t, slices.Contains(lines[len(lines)-1], "-fT"), true)

	// A wrong sum never replaces the target and leaves no temporary file
	w, err := p.OpenWriter(ctx, ref, dir+"/f", provider.WriteSpec{Size: 3, Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	wrong := sha256.Sum256([]byte("abd"))
	if _, err := w.Commit(ctx, wrong[:]); err == nil {
		t.Fatal("a wrong sum committed")
	}
	got, _ := os.ReadFile(dir + "/f")
	eq(t, bytes.Equal(got, data), true)
	left, _ := filepath.Glob(filepath.Join(dir, ".f.bugnet-*"))
	eq(t, len(left), 0)

	// A sum given at open is checked when Commit gives none
	right := sha256.Sum256([]byte("xyz"))
	w, err = p.OpenWriter(ctx, ref, dir+"/g", provider.WriteSpec{Size: 3, Mode: 0o644,
		SHA256: wrong[:]})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = w.Write([]byte("xyz"))
	if _, err := w.Commit(ctx, nil); err == nil {
		t.Fatal("a wrong sum given at open committed")
	}
	w, err = p.OpenWriter(ctx, ref, dir+"/g", provider.WriteSpec{Size: 3, Mode: 0o644,
		SHA256: right[:]})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = w.Write([]byte("xyz"))
	if _, err := w.Commit(ctx, nil); err != nil {
		t.Fatal(err)
	}

	// A part that fails fails the next Write or the Commit, and the abort removes the temp
	f.inject(sbxWriteRoute, fault{status: 400, message: "bad", after: 1, times: 1})
	w, err = p.OpenWriter(ctx, ref, dir+"/h", provider.WriteSpec{Size: 4500, Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	_, werr := w.Write(data)
	if werr == nil {
		_, werr = w.Commit(ctx, nil)
	}
	if c, _ := codeOf(werr); c != provider.CodeInvalid {
		t.Fatalf("a refused part gave %v", werr)
	}
	_ = w.Abort()
	left, _ = filepath.Glob(filepath.Join(dir, ".h.bugnet-*"))
	eq(t, len(left), 0)

	// A target the Job refuses fails at open
	if err := os.WriteFile(dir+"/file", nil, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = p.OpenWriter(ctx, ref, dir+"/file/below", provider.WriteSpec{Size: 1, Mode: 0o644})
	if c, _ := codeOf(err); c != provider.CodeInvalid && c != provider.CodeInternal {
		t.Fatalf("a target under a file gave %v", err)
	}
	if err == nil {
		t.Fatal("a target under a file opened")
	}
}

func TestHFStreamsShareTheBuffers(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	p.partBytes = 1 << 10
	// One part's room in all, so every stream waits its turn and none deadlocks
	p.readBuffers = semaphore.NewWeighted(p.partBytes)
	p.writeBuffers = semaphore.NewWeighted(p.partBytes)
	ref := readyJob(t, f, p)
	dir := t.TempDir()
	data := make([]byte, 10<<10+5)
	_, _ = rand.Read(data)
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for i := range 2 {
		wg.Go(func() {
			name := dir + "/f" + strconv.Itoa(i)
			if err := streamUp(t, p, ref, name, data); err != nil {
				errs <- err
				return
			}
			got, err := streamDown(t, p, ref, name, 0)
			if err == nil && !bytes.Equal(got, data) {
				err = io.ErrUnexpectedEOF
			}
			if err != nil {
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	// Every buffer came back
	if !p.readBuffers.TryAcquire(p.partBytes) || !p.writeBuffers.TryAcquire(p.partBytes) {
		t.Fatal("a buffer was never given back")
	}
}

func TestHFStreamsCloseGivesTheBuffersBack(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	p.partBytes = 1 << 10
	ref := readyJob(t, f, p)
	name := t.TempDir() + "/f"
	mustWrite(t, name, strings.Repeat("x", 20<<10), 0o644)
	ctx, cancel := context.WithCancel(t.Context())
	r, err := p.OpenReader(ctx, ref, name, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Read(make([]byte, 10)); err != nil {
		t.Fatal(err)
	}
	cancel()
	_ = r.Close()
	w, err := p.OpenWriter(t.Context(), ref, name+".w", provider.WriteSpec{Size: 5 << 10,
		Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = w.Write(make([]byte, 3<<10))
	_ = w.Abort()
	if !p.readBuffers.TryAcquire(bufferTotal) || !p.writeBuffers.TryAcquire(bufferTotal) {
		t.Fatal("a buffer was never given back")
	}
	// The aborted writer's temporary file is gone
	left, _ := filepath.Glob(path.Dir(name) + "/.f.w.bugnet-*")
	eq(t, len(left), 0)
}
