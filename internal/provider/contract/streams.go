package contract

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"io/fs"
	"path"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/oynozan/bugnet-router/internal/provider"
)

const (
	// tempWait bounds the wait for a temporary file to show, some providers never list one
	tempWait  = 5 * time.Second
	tempPoll  = 50 * time.Millisecond
	readChunk = 32 << 10
)

// Streams checks the file stream contract inside dir, which the sandbox's exec user can fill
func Streams(t *testing.T, p provider.Provider, ref provider.Ref, dir string) {
	s := streams{p: p, ref: ref, dir: dir}
	for _, c := range []struct {
		name string
		run  func(t *testing.T)
	}{
		{"ReadStopsAtSize", s.readStopsAtSize},
		{"ReadCutAtOpenSize", s.readCutAtOpenSize},
		{"WriteInvisibleUntilCommit", s.writeInvisibleUntilCommit},
		{"ZeroSize", s.zeroSize},
		{"WrongCountNeverCommits", s.wrongCountNeverCommits},
		{"AbortLeavesNoTemp", s.abortLeavesNoTemp},
		{"CancelIsAbort", s.cancelIsAbort},
		{"StatDoesNotFollow", s.statDoesNotFollow},
		{"DirectoryNotRegular", s.directoryNotRegular},
	} {
		t.Run(c.name, c.run)
	}
}

type streams struct {
	p   provider.Provider
	ref provider.Ref
	dir string
}

func (s streams) readStopsAtSize(t *testing.T) {
	data := randomBytes(t, 256<<10+3)
	name := s.dir + "/read.bin"
	s.upload(t, name, data, 0o644)
	size := int64(len(data))
	for _, offset := range []int64{0, 1, 65537, size - 1, size} {
		r := s.open(t, name, offset)
		info := r.Info()
		if info.Size != size || !info.Regular || info.IsDir {
			t.Errorf("reader at %d describes %+v, want a regular file of %d bytes", offset, info,
				size)
		}
		if got := drain(t, r, size-offset); !bytes.Equal(got, data[offset:]) {
			t.Errorf("reader at %d gave %d bytes that differ from the file", offset, len(got))
		}
		eq(t, r.Close(), nil)
	}
	if r, err := s.p.OpenReader(withTimeout(t, opTimeout), s.ref, name, size+1); err == nil {
		_ = r.Close()
		t.Errorf("a reader opened past the end of a %d byte file", size)
	}
}

func (s streams) readCutAtOpenSize(t *testing.T) {
	name := s.dir + "/grow.txt"
	s.upload(t, name, []byte("0123456789"), 0o644)
	r := s.open(t, name, 2)
	s.shell(t, "printf more >> grow.txt")
	eq(t, string(drain(t, r, 8)), "23456789")
	eq(t, r.Close(), nil)
	eq(t, s.readAll(t, name), "0123456789more")
}

func (s streams) writeInvisibleUntilCommit(t *testing.T) {
	ctx := withTimeout(t, opTimeout)
	name := s.dir + "/f"
	s.upload(t, name, []byte("old"), 0o644)
	w := s.openWriter(ctx, t, name, provider.WriteSpec{Size: 10, Mode: 0o640})
	write(t, w, "0123456789")
	eq(t, s.readAll(t, name), "old")
	eq(t, s.stat(t, name).Size, int64(3))
	sum := sha256.Sum256([]byte("0123456789"))
	entry, err := w.Commit(ctx, sum[:])
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if entry.Size != 10 || !entry.Regular {
		t.Errorf("Commit described %+v, want a regular file of 10 bytes", entry)
	}
	eq(t, s.readAll(t, name), "0123456789")
	got := s.stat(t, name)
	if got.Size != 10 || !got.Regular || got.Mode.Perm() != 0o640 {
		t.Errorf("committed file %+v, want a regular file of 10 bytes and mode 0640", got)
	}
	eq(t, w.Abort(), nil)
	eq(t, s.readAll(t, name), "0123456789")
	eq(t, s.temps(t), []string(nil))
}

func (s streams) zeroSize(t *testing.T) {
	ctx := withTimeout(t, opTimeout)
	name := s.dir + "/zero/empty"
	w := s.openWriter(ctx, t, name, provider.WriteSpec{Mode: 0o644})
	sum := sha256.Sum256(nil)
	if _, err := w.Commit(ctx, sum[:]); err != nil {
		t.Fatalf("Commit with no Write: %v", err)
	}
	got := s.stat(t, name)
	if got.Size != 0 || !got.Regular {
		t.Errorf("empty file %+v, want a regular file of 0 bytes", got)
	}
	r := s.open(t, name, 0)
	eq(t, r.Info().Size, int64(0))
	eq(t, len(drain(t, r, 0)), 0)
	eq(t, r.Close(), nil)
}

func (s streams) wrongCountNeverCommits(t *testing.T) {
	ctx := withTimeout(t, opTimeout)
	name := s.dir + "/count"
	s.upload(t, name, []byte("old"), 0o644)
	w := s.openWriter(ctx, t, name, provider.WriteSpec{Size: 10, Mode: 0o644})
	write(t, w, "012345678")
	s.waitTemp(t, "count")
	if _, err := w.Commit(ctx, nil); err == nil {
		t.Fatal("Commit after 9 of 10 bytes succeeded")
	}
	eq(t, s.readAll(t, name), "old")

	over := s.openWriter(ctx, t, name, provider.WriteSpec{Size: 3, Mode: 0o644})
	if _, err := over.Write([]byte("abcd")); err == nil {
		t.Error("a Write of 4 bytes into a 3 byte file succeeded")
	}
	s.waitTemp(t, "count")
	eq(t, over.Abort(), nil)
	eq(t, s.readAll(t, name), "old")
	s.waitNoTemp(t)
}

func (s streams) abortLeavesNoTemp(t *testing.T) {
	ctx := withTimeout(t, opTimeout)
	name := s.dir + "/abort"
	s.upload(t, name, []byte("old"), 0o644)

	idle := s.openWriter(ctx, t, name, provider.WriteSpec{Size: 10, Mode: 0o644})
	s.waitTemp(t, "abort")
	eq(t, idle.Abort(), nil)
	eq(t, idle.Abort(), nil)

	w := s.openWriter(ctx, t, name, provider.WriteSpec{Size: 10, Mode: 0o644})
	write(t, w, "01234")
	s.waitTemp(t, "abort")
	eq(t, w.Abort(), nil)
	eq(t, w.Abort(), nil)
	if _, err := w.Write([]byte("5")); err == nil {
		t.Error("a Write after Abort succeeded")
	}
	if _, err := w.Commit(ctx, nil); err == nil {
		t.Error("a Commit after Abort succeeded")
	}
	eq(t, s.readAll(t, name), "old")
	s.waitNoTemp(t)
}

func (s streams) cancelIsAbort(t *testing.T) {
	name := s.dir + "/cancel"
	s.upload(t, name, []byte("old"), 0o644)
	ctx, cancel := context.WithCancel(withTimeout(t, opTimeout))
	w := s.openWriter(ctx, t, name, provider.WriteSpec{Size: 10, Mode: 0o644})
	write(t, w, "01234")
	s.waitTemp(t, "cancel")
	cancel()
	if _, err := w.Write([]byte("56789")); err == nil {
		t.Error("a Write after the writer's context ended succeeded")
	}
	if _, err := w.Commit(withTimeout(t, opTimeout), nil); err == nil {
		t.Error("a Commit after the writer's context ended succeeded")
	}
	eq(t, s.readAll(t, name), "old")
	s.waitNoTemp(t)

	rctx, rcancel := context.WithCancel(withTimeout(t, opTimeout))
	r, err := s.p.OpenReader(rctx, s.ref, name, 0)
	if err != nil {
		rcancel()
		t.Fatalf("OpenReader: %v", err)
	}
	rcancel()
	if n, err := r.Read(make([]byte, 8)); err == nil || errors.Is(err, io.EOF) {
		t.Errorf("a Read after the reader's context ended gave %d bytes, %v", n, err)
	}
	_ = r.Close()
}

func (s streams) statDoesNotFollow(t *testing.T) {
	ctx := withTimeout(t, opTimeout)
	target := s.dir + "/target"
	s.upload(t, target, []byte("abc"), 0o644)
	s.shell(t, "ln -s target link")

	link := s.stat(t, s.dir+"/link")
	if link.Regular || link.IsDir || link.Mode&fs.ModeSymlink == 0 {
		t.Errorf("StatFile of a symlink gave %+v, want the link itself, not regular", link)
	}
	file := s.stat(t, target)
	if !file.Regular || file.IsDir || file.Size != 3 || !file.Mode.IsRegular() {
		t.Errorf("StatFile of a 3 byte file gave %+v", file)
	}
	if r, err := s.p.OpenReader(ctx, s.ref, s.dir+"/link", 0); err == nil {
		_ = r.Close()
		t.Error("OpenReader opened a symlink")
	}
	_, err := s.p.StatFile(ctx, s.ref, s.dir+"/missing")
	requireCode(t, "StatFile of a missing path", err, provider.CodeNotFound)
	_, err = s.p.OpenReader(ctx, s.ref, s.dir+"/missing", 0)
	requireCode(t, "OpenReader of a missing path", err, provider.CodeNotFound)
}

func (s streams) directoryNotRegular(t *testing.T) {
	s.upload(t, s.dir+"/sub/deeper/x", []byte("x"), 0o644)
	got := s.stat(t, s.dir+"/sub")
	if !got.IsDir || got.Regular || !got.Mode.IsDir() {
		t.Errorf("StatFile of a directory gave %+v", got)
	}
	r, err := s.p.OpenReader(withTimeout(t, opTimeout), s.ref, s.dir+"/sub", 0)
	if err == nil {
		_ = r.Close()
		t.Error("OpenReader opened a directory")
	}
}

/* Stream helpers */

func (s streams) upload(t *testing.T, name string, data []byte, mode fs.FileMode) {
	t.Helper()
	ctx := withTimeout(t, opTimeout)
	spec := provider.WriteSpec{Size: int64(len(data)), Mode: mode}
	w := s.openWriter(ctx, t, name, spec)
	write(t, w, string(data))
	sum := sha256.Sum256(data)
	if _, err := w.Commit(ctx, sum[:]); err != nil {
		t.Fatalf("Commit of %s: %v", name, err)
	}
}

func (s streams) openWriter(
	ctx context.Context, t *testing.T, name string, spec provider.WriteSpec,
) provider.FileWriter {
	t.Helper()
	w, err := s.p.OpenWriter(ctx, s.ref, name, spec)
	if err != nil {
		t.Fatalf("OpenWriter of %s: %v", name, err)
	}
	t.Cleanup(func() { _ = w.Abort() })
	return w
}

func (s streams) open(t *testing.T, name string, offset int64) provider.FileReader {
	t.Helper()
	r, err := s.p.OpenReader(withTimeout(t, opTimeout), s.ref, name, offset)
	if err != nil {
		t.Fatalf("OpenReader of %s at %d: %v", name, offset, err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func (s streams) readAll(t *testing.T, name string) string {
	t.Helper()
	r := s.open(t, name, 0)
	defer func() { _ = r.Close() }()
	return string(drain(t, r, r.Info().Size))
}

func (s streams) stat(t *testing.T, name string) provider.FileEntry {
	t.Helper()
	e, err := s.p.StatFile(withTimeout(t, opTimeout), s.ref, name)
	if err != nil {
		t.Fatalf("StatFile of %s: %v", name, err)
	}
	return e
}

// shell runs script with sh inside dir and fails the test unless it exits 0
func (s streams) shell(t *testing.T, script string) {
	t.Helper()
	spec := provider.ExecSpec{Argv: []string{"sh", "-c", script}, Cwd: s.dir}
	out, code, err := run(withTimeout(t, opTimeout), s.p, s.ref, spec)
	if err != nil || code != 0 {
		t.Fatalf("sh -c %q gave exit %d, %v, output %q", script, code, err, out)
	}
}

// temps lists the temporary files a FileWriter left in dir
func (s streams) temps(t *testing.T) []string {
	t.Helper()
	listing := provider.NewListing(1000)
	if err := s.p.ListFiles(withTimeout(t, opTimeout), s.ref, s.dir, 1, listing); err != nil {
		t.Fatalf("ListFiles of %s: %v", s.dir, err)
	}
	var names []string
	for _, e := range listing.Entries {
		base := path.Base(e.Path)
		if strings.Contains(base, ".bugnet-") && strings.HasSuffix(base, ".part") {
			names = append(names, base)
		}
	}
	return names
}

// waitTemp waits a while for a temporary file of target to show, so a later check sees it go
func (s streams) waitTemp(t *testing.T, target string) {
	t.Helper()
	deadline := time.Now().Add(tempWait)
	for time.Now().Before(deadline) {
		for _, name := range s.temps(t) {
			if strings.HasPrefix(name, "."+target+".bugnet-") {
				return
			}
		}
		time.Sleep(tempPoll)
	}
}

// waitNoTemp fails the test when a temporary file stays after an abort's cleanup had its time
func (s streams) waitNoTemp(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(tempWait)
	for {
		left := s.temps(t)
		if len(left) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("temporary files left in %s: %v", s.dir, left)
		}
		time.Sleep(tempPoll)
	}
}

// drain reads r to its end, failing on more than want bytes or an end before them
func drain(t *testing.T, r provider.FileReader, want int64) []byte {
	t.Helper()
	var out []byte
	buf := make([]byte, readChunk)
	for {
		n, err := r.Read(buf)
		out = append(out, buf[:n]...)
		if int64(len(out)) > want {
			t.Fatalf("reader gave %d bytes, more than the %d it holds", len(out), want)
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("read after %d of %d bytes: %v", len(out), want, err)
		}
	}
	if int64(len(out)) != want {
		t.Fatalf("reader ended at %d of %d bytes", len(out), want)
	}
	if n, err := r.Read(buf); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("a read past the end gave %d bytes, %v, want io.EOF", n, err)
	}
	return out
}

func write(t *testing.T, w provider.FileWriter, data string) {
	t.Helper()
	if n, err := w.Write([]byte(data)); err != nil || n != len(data) {
		t.Fatalf("Write of %d bytes gave %d, %v", len(data), n, err)
	}
}

func randomBytes(t *testing.T, n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("random: %v", err)
	}
	return b
}

func requireCode(t *testing.T, what string, err error, code provider.Code) {
	t.Helper()
	if e, ok := provider.AsError(err); !ok || e.Code != code {
		t.Errorf("%s gave %v, want an *Error with code %s", what, err, code)
	}
}

// eq fails the test when got and want differ
func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}
