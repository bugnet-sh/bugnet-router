package provider_test

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/oynozan/bugnet-router/internal/provider"
)

func TestTempNameSitsBesideTheTargetAndIsHidden(t *testing.T) {
	shape := regexp.MustCompile(`^\.out\.csv\.bugnet-[0-9a-f]{8}\.part$`)
	a, b := provider.TempName("out.csv"), provider.TempName("out.csv")
	if !shape.MatchString(a) || !shape.MatchString(b) {
		t.Fatalf("temp names %q and %q, want .out.csv.bugnet-<8 hex>.part", a, b)
	}
	if a == b {
		t.Fatalf("two temp names for one target are both %q", a)
	}
}

func TestTempNameFitsOneDirectoryEntry(t *testing.T) {
	long := strings.Repeat("é", 127) + "x"
	name := provider.TempName(long)
	if len(name) > 255 || !utf8.ValidString(name) {
		t.Fatalf("temp name of a 255-byte name is %d bytes, valid UTF-8 %v", len(name),
			utf8.ValidString(name))
	}
	if !regexp.MustCompile(`^\.(é)+\.bugnet-[0-9a-f]{8}\.part$`).MatchString(name) {
		t.Fatalf("temp name %q, want the name cut at a whole character", name)
	}
}

// body is a reader body that records closes and can fail or end early
type body struct {
	r      io.Reader
	err    error
	mu     sync.Mutex
	closes int
}

func (b *body) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	if errors.Is(err, io.EOF) && b.err != nil {
		return n, b.err
	}
	return n, err
}

func (b *body) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closes++
	return nil
}

func (b *body) closed() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closes
}

func readEach(r io.Reader, size int) (string, []error) {
	var out []byte
	var errs []error
	buf := make([]byte, size)
	for range 100 {
		n, err := r.Read(buf)
		out = append(out, buf[:n]...)
		if err != nil {
			errs = append(errs, err)
			if len(errs) == 2 {
				break
			}
		}
	}
	return string(out), errs
}

func TestFileReaderStopsAtTheSizeItOpenedWith(t *testing.T) {
	b := &body{r: strings.NewReader("3456789 grown")}
	info := provider.FileEntry{Path: "/f", Size: 10, Regular: true}
	r := provider.NewFileReader(t.Context(), b, info, 3, nil)
	eq(t, r.Info(), info)
	got, errs := readEach(r, 4)
	eq(t, got, "3456789")
	eq(t, errs, []error{io.EOF, io.EOF})
	eq(t, r.Close(), nil)
	eq(t, r.Close(), nil)
	eq(t, b.closed(), 1)
}

func TestFileReaderAtTheEndReadsNothing(t *testing.T) {
	b := &body{r: strings.NewReader("more")}
	r := provider.NewFileReader(t.Context(), b, provider.FileEntry{Size: 4}, 4, nil)
	got, errs := readEach(r, 8)
	eq(t, got, "")
	eq(t, errs, []error{io.EOF, io.EOF})
}

func TestFileReaderThatEndsEarlyFails(t *testing.T) {
	b := &body{r: strings.NewReader("0123")}
	r := provider.NewFileReader(t.Context(), b, provider.FileEntry{Size: 10}, 0, nil)
	got, errs := readEach(r, 16)
	eq(t, got, "0123")
	if len(errs) == 0 || errors.Is(errs[0], io.EOF) || !errors.Is(errs[0], io.ErrUnexpectedEOF) {
		t.Fatalf("a body 6 bytes short gave %v, want an unexpected EOF", errs)
	}
	if e, ok := provider.AsError(errs[0]); !ok || e.Code != provider.CodeInternal {
		t.Fatalf("a short body gave %v, want an internal provider error", errs[0])
	}
}

func TestFileReaderClassifiesBodyErrors(t *testing.T) {
	cause := errors.New("connection reset")
	b := &body{r: strings.NewReader("01"), err: cause}
	classified := &provider.Error{Code: provider.CodeRateLimited, Err: cause}
	r := provider.NewFileReader(t.Context(), b, provider.FileEntry{Size: 10}, 0,
		func(err error) error {
			if errors.Is(err, cause) {
				return classified
			}
			return err
		})
	_, errs := readEach(r, 16)
	if len(errs) == 0 || errs[0] != classified {
		t.Fatalf("a failing body gave %v, want the classified error", errs)
	}
}

func TestFileReaderCancelIsClose(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	b := &body{r: strings.NewReader("0123456789")}
	r := provider.NewFileReader(ctx, b, provider.FileEntry{Size: 10}, 0, nil)
	cancel()
	deadline := time.Now().Add(5 * time.Second)
	for b.closed() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	eq(t, b.closed(), 1)
	if _, err := r.Read(make([]byte, 4)); !errors.Is(err, context.Canceled) {
		t.Fatalf("read after the context ended gave %v, want context.Canceled", err)
	}
	eq(t, r.Close(), nil)
	eq(t, b.closed(), 1)
}

// target is a WriteTarget that records what the writer asked of it
type target struct {
	mu        sync.Mutex
	written   []byte
	finished  [][]byte
	discards  []discardCall
	finishErr error
}

// discardCall is the state of the context one Discard was given
type discardCall struct {
	err      error
	deadline bool
}

func (g *target) Write(p []byte) (int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.written = append(g.written, p...)
	return len(p), nil
}

func (g *target) Finish(_ context.Context, sum []byte) (provider.FileEntry, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.finished = append(g.finished, sum)
	if g.finishErr != nil {
		return provider.FileEntry{}, g.finishErr
	}
	return provider.FileEntry{Path: "/f", Size: int64(len(g.written)), Regular: true}, nil
}

func (g *target) Discard(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	_, deadline := ctx.Deadline()
	g.discards = append(g.discards, discardCall{err: ctx.Err(), deadline: deadline})
	return nil
}

func (g *target) counts() (finishes, discards int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.finished), len(g.discards)
}

func TestFileWriterCommitsExactlySize(t *testing.T) {
	g := &target{}
	w := provider.NewFileWriter(t.Context(), provider.WriteSpec{Size: 10, Mode: 0o644}, g)
	for _, part := range []string{"0123", "", "456789"} {
		if n, err := w.Write([]byte(part)); n != len(part) || err != nil {
			t.Fatalf("Write(%q) gave %d, %v", part, n, err)
		}
	}
	entry, err := w.Commit(t.Context(), []byte("sum"))
	eq(t, err, nil)
	eq(t, entry, provider.FileEntry{Path: "/f", Size: 10, Regular: true})
	eq(t, string(g.written), "0123456789")
	eq(t, g.finished, [][]byte{[]byte("sum")})
	eq(t, w.Abort(), nil)
	finishes, discards := g.counts()
	eq(t, [2]int{finishes, discards}, [2]int{1, 0})
	if _, err := w.Write([]byte("x")); err == nil {
		t.Fatal("a Write after Commit succeeded")
	}
	if _, err := w.Commit(t.Context(), nil); err == nil {
		t.Fatal("a second Commit succeeded")
	}
}

func TestFileWriterOfNothingCommits(t *testing.T) {
	g := &target{}
	w := provider.NewFileWriter(t.Context(), provider.WriteSpec{Mode: 0o644}, g)
	entry, err := w.Commit(t.Context(), nil)
	eq(t, err, nil)
	eq(t, entry.Size, int64(0))
}

func TestFileWriterRefusesBytesPastSize(t *testing.T) {
	g := &target{}
	w := provider.NewFileWriter(t.Context(), provider.WriteSpec{Size: 3, Mode: 0o644}, g)
	n, err := w.Write([]byte("abcd"))
	if e, ok := provider.AsError(err); n != 0 || !ok || e.Code != provider.CodeInvalid {
		t.Fatalf("4 bytes into 3 gave %d, %v, want 0 and invalid", n, err)
	}
	eq(t, len(g.written), 0)
}

func TestFileWriterWrongCountNeverFinishes(t *testing.T) {
	g := &target{}
	w := provider.NewFileWriter(t.Context(), provider.WriteSpec{Size: 10, Mode: 0o644}, g)
	if _, err := w.Write([]byte("012345678")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Commit(t.Context(), nil); err == nil {
		t.Fatal("a Commit after 9 of 10 bytes succeeded")
	}
	finishes, discards := g.counts()
	eq(t, [2]int{finishes, discards}, [2]int{0, 1})
	eq(t, w.Abort(), nil)
	_, discards = g.counts()
	eq(t, discards, 1)
}

func TestFileWriterFailedFinishDiscards(t *testing.T) {
	g := &target{finishErr: &provider.Error{Code: provider.CodeNoSpace}}
	w := provider.NewFileWriter(t.Context(), provider.WriteSpec{Mode: 0o644}, g)
	_, err := w.Commit(t.Context(), nil)
	if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeNoSpace {
		t.Fatalf("Commit gave %v, want the target's no_space", err)
	}
	finishes, discards := g.counts()
	eq(t, [2]int{finishes, discards}, [2]int{1, 1})
}

func TestFileWriterAbortDiscardsOnceWithADetachedContext(t *testing.T) {
	g := &target{}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	w := provider.NewFileWriter(ctx, provider.WriteSpec{Size: 10, Mode: 0o644}, g)
	if _, err := w.Write([]byte("01234")); err != nil {
		t.Fatal(err)
	}
	eq(t, w.Abort(), nil)
	eq(t, w.Abort(), nil)
	cancel()
	finishes, discards := g.counts()
	eq(t, [2]int{finishes, discards}, [2]int{0, 1})
	eq(t, g.discards[0], discardCall{deadline: true})
	if _, err := w.Commit(t.Context(), nil); err == nil {
		t.Fatal("a Commit after Abort succeeded")
	}
}

func TestFileWriterCancelIsAbort(t *testing.T) {
	g := &target{}
	ctx, cancel := context.WithCancel(t.Context())
	w := provider.NewFileWriter(ctx, provider.WriteSpec{Size: 10, Mode: 0o644}, g)
	if _, err := w.Write([]byte("01234")); err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err := w.Write([]byte("56789")); !errors.Is(err, context.Canceled) {
		t.Fatalf("a Write after the context ended gave %v, want context.Canceled", err)
	}
	if _, err := w.Commit(t.Context(), nil); err == nil {
		t.Fatal("a Commit after the context ended succeeded")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		finishes, discards := g.counts()
		if discards == 1 {
			eq(t, finishes, 0)
			g.mu.Lock()
			call := g.discards[0]
			g.mu.Unlock()
			eq(t, call, discardCall{deadline: true})
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the writer did not discard after its context ended")
		}
		time.Sleep(time.Millisecond)
	}
	eq(t, string(g.written), "01234")
}

func TestWriteSpecCheck(t *testing.T) {
	for _, c := range []struct {
		spec provider.WriteSpec
		ok   bool
	}{
		{provider.WriteSpec{Size: 0, Mode: 0o644}, true},
		{provider.WriteSpec{Size: 1 << 30, Mode: 0o777, SHA256: make([]byte, 32)}, true},
		{provider.WriteSpec{Size: -1, Mode: 0o644}, false},
		{provider.WriteSpec{Mode: 0o644 | fs.ModeSetuid}, false},
		{provider.WriteSpec{Mode: fs.ModeDir | 0o755}, false},
		{provider.WriteSpec{Mode: 0o644, SHA256: make([]byte, 31)}, false},
	} {
		err := c.spec.Check()
		if e, isErr := provider.AsError(err); c.ok != (err == nil) ||
			(!c.ok && (!isErr || e.Code != provider.CodeInvalid)) {
			t.Errorf("Check of %+v gave %v, want ok %v or invalid", c.spec, err, c.ok)
		}
	}
}
