package boxd

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"syscall"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/provider/contract"
)

func TestBoxdStreamsContract(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	contract.Streams(t, p, readyMachine(t, f), t.TempDir())
}

// streamUp streams data into name through OpenWriter and commits it
func streamUp(
	t *testing.T, p *Provider, ref provider.Ref, name string, data []byte,
) provider.FileEntry {
	t.Helper()
	w, err := p.OpenWriter(t.Context(), ref, name,
		provider.WriteSpec{Size: int64(len(data)), Mode: 0o644})
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

func TestBoxdNeverWritesTheTarget(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readyMachine(t, f)
	dir := t.TempDir()
	name := filepath.Join(dir, "f")
	entry := streamUp(t, p, ref, name, []byte("0123456789"))
	if entry.Path != name || entry.Size != 10 || !entry.Regular {
		t.Errorf("Commit described %+v, want a regular file of 10 bytes at %s", entry, name)
	}
	paths := f.fsWritePaths()
	// an empty list would pass the check below
	eq(t, len(paths) > 0, true)
	temp := regexp.MustCompile(`/\.f\.bugnet-[0-9a-f]{8}\.part$`)
	for _, p := range paths {
		if !temp.MatchString(p) {
			t.Errorf("FsWrite went to %s, want only a temp file beside the target", p)
		}
	}
	eq(t, f.renames(), [][2]string{{paths[0], name}})
	got, err := os.ReadFile(name)
	eq(t, string(got), "0123456789")
	eq(t, err, nil)
}

func TestBoxdStreamWritesInMiBChunks(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readyMachine(t, f)
	name := filepath.Join(t.TempDir(), "blob")
	data := make([]byte, 3<<20+5)
	_, _ = rand.Read(data)
	w, err := p.OpenWriter(t.Context(), ref, name,
		provider.WriteSpec{Size: int64(len(data)), Mode: 0o600})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Commit(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	eq(t, f.lastWriteChunks(), []int{0, 1 << 20, 1 << 20, 1 << 20, 5})
	head := f.lastWriteHead()
	eq(t, [2]uint64{uint64(head.GetMode()), head.GetExpectedSize()},
		[2]uint64{0o600, uint64(len(data))})
	eq(t, head.Mode != nil && head.ExpectedSize != nil, true)
	got, err := os.ReadFile(name)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("committed file holds %d bytes, %v, want the %d sent", len(got), err, len(data))
	}
	info, _ := os.Stat(name)
	eq(t, info.Mode().Perm(), fs.FileMode(0o600))
}

func TestBoxdStreamsUseASecondConnection(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readyMachine(t, f)
	name := filepath.Join(t.TempDir(), "f")
	streamUp(t, p, ref, name, []byte("abc"))
	r, err := p.OpenReader(t.Context(), ref, name, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	_ = r.Close()
	eq(t, string(got), "abc")
	eq(t, err, nil)
	spec := provider.ExecSpec{Argv: []string{"true"}}
	if _, code, err := runExec(t.Context(), p, ref, spec); err != nil || code != 0 {
		t.Fatalf("exec true gave %d, %v", code, err)
	}

	streams := slices.Concat(f.peersOf("FsRead"), f.peersOf("FsWrite"))
	control := slices.Concat(f.peersOf("Exec"), f.peersOf("FsStat"), f.peersOf("FsRename"))
	if len(streams) == 0 || len(control) == 0 {
		t.Fatalf("the fake saw streams from %v and other calls from %v", streams, control)
	}
	for _, s := range streams {
		if slices.Contains(control, s) {
			t.Errorf("a file stream shared the connection %s with Exec and unary calls", s)
		}
	}
	eq(t, len(slices.Compact(slices.Sorted(slices.Values(streams)))), 1)
}

func TestBoxdNoSpace(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readyMachine(t, f)
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
		t.Fatalf("an upload boxd refused with no_space gave %v, want no_space", err)
	}
	got, _ := os.ReadFile(name)
	eq(t, string(got), "old")
	entries, _ := os.ReadDir(dir)
	eq(t, len(entries), 1)

	err = p.WriteFile(t.Context(), ref, filepath.Join(dir, "g"), []byte("x"), 0o644)
	if !isCode(err, provider.CodeNoSpace) {
		t.Fatalf("WriteFile boxd refused with no_space gave %v, want no_space", err)
	}
}

func TestBoxdNoSpaceMidUploadFailsTheWrite(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readyMachine(t, f)
	dir := t.TempDir()
	name := filepath.Join(dir, "f")
	if err := os.WriteFile(name, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	f.noSpaceAfter.Store(2 << 20)
	// More than gRPC's 16 MiB windows hold, so a Send meets the stream boxd already ended
	data := make([]byte, 24<<20)
	w, err := p.OpenWriter(t.Context(), ref, name,
		provider.WriteSpec{Size: int64(len(data)), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	n, err := w.Write(data)
	if !isCode(err, provider.CodeNoSpace) || n >= len(data) {
		t.Fatalf("a Write boxd ended after 2 MiB sent %d bytes and gave %v, want no_space", n, err)
	}
	eq(t, w.Abort(), nil)
	got, _ := os.ReadFile(name)
	eq(t, string(got), "old")
	entries, _ := os.ReadDir(dir)
	eq(t, len(entries), 1)
}

func TestBoxdFileErrorReasons(t *testing.T) {
	for _, c := range []struct {
		code   codes.Code
		reason string
		want   provider.Code
	}{
		{codes.ResourceExhausted, "no_space", provider.CodeNoSpace},
		{codes.PermissionDenied, "permission_denied", provider.CodeInvalid},
		{codes.PermissionDenied, "read_only", provider.CodeInvalid},
		{codes.NotFound, "not_found", provider.CodeNotFound},
		{codes.PermissionDenied, "", provider.CodeAuth},
	} {
		err := fsError(t.Context(), status.Error(c.code, "boxd said no"),
			metadata.Pairs(reasonTrailer, c.reason))
		if !isCode(err, c.want) {
			t.Errorf("%v with reason %q gave %v, want %s", c.code, c.reason, err, c.want)
		}
	}
}

func TestBoxdReaderRefusesAFileSwappedForALink(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readyMachine(t, f)
	dir := t.TempDir()
	name := filepath.Join(dir, "f")
	streamUp(t, p, ref, name, []byte("abc"))
	if err := os.WriteFile(filepath.Join(dir, "other"), []byte("xyz"), 0o644); err != nil {
		t.Fatal(err)
	}
	f.beforeRead(func(name string) {
		_ = os.Remove(name)
		_ = os.Symlink("other", name)
	})
	r, err := p.OpenReader(t.Context(), ref, name, 0)
	if err == nil {
		got, _ := io.ReadAll(r)
		_ = r.Close()
		t.Fatalf("a file swapped for a link after its stat read %q", got)
	}
	eq(t, isCode(err, provider.CodeInvalid), true)
}

func TestBoxdFilePermissionIsNotAuth(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads a directory of mode 0 anyway")
	}
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readyMachine(t, f)
	locked := filepath.Join(t.TempDir(), "locked")
	if err := os.Mkdir(locked, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(locked, "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(locked, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o700) })
	_, err := p.StatFile(t.Context(), ref, filepath.Join(locked, "f"))
	eq(t, isCode(err, provider.CodeInvalid), true)
	_, _, err = p.ReadFile(t.Context(), ref, filepath.Join(locked, "f"), 0, 1)
	eq(t, isCode(err, provider.CodeInvalid), true)
	_, err = listAll(t.Context(), p, ref, locked, 1)
	eq(t, isCode(err, provider.CodeInvalid), true)
}

func TestBoxdReaderChecksTheByteCount(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readyMachine(t, f)
	name := filepath.Join(t.TempDir(), "f")
	data := bytes.Repeat([]byte("x"), 200<<10)
	streamUp(t, p, ref, name, data)
	f.badBytesRead.Store(true)
	r, err := p.OpenReader(t.Context(), ref, name, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	got, err := io.ReadAll(r)
	if err == nil || !isCode(err, provider.CodeInternal) {
		t.Fatalf("a read whose eof chunk counts another size gave %d bytes, %v, want internal",
			len(got), err)
	}
}

func TestBoxdReaderAsksForTheRestOfTheFile(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readyMachine(t, f)
	name := filepath.Join(t.TempDir(), "f")
	data := make([]byte, 300<<10)
	_, _ = rand.Read(data)
	streamUp(t, p, ref, name, data)
	for _, offset := range []int64{0, 7, int64(len(data))} {
		r, err := p.OpenReader(t.Context(), ref, name, offset)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(r)
		_ = r.Close()
		if err != nil || !bytes.Equal(got, data[offset:]) {
			t.Errorf("read from %d gave %d bytes, %v", offset, len(got), err)
		}
	}
	// a read at the end needs no FsRead
	eq(t, f.readLengths(), []uint64{uint64(len(data)), uint64(len(data) - 7)})
}

func TestBoxdReaderThatEndsEarlyFails(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readyMachine(t, f)
	name := filepath.Join(t.TempDir(), "f")
	streamUp(t, p, ref, name, bytes.Repeat([]byte("x"), 200<<10))
	f.shortRead.Store(true)
	r, err := p.OpenReader(t.Context(), ref, name, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	got, err := io.ReadAll(r)
	if len(got) != 100<<10 || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("a stream that ended halfway read %d bytes, %v, want 100 KiB and an "+
			"unexpected EOF", len(got), err)
	}
}

func TestBoxdReaderWithoutTheEofChunk(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readyMachine(t, f)
	name := filepath.Join(t.TempDir(), "f")
	data := make([]byte, 200<<10)
	_, _ = rand.Read(data)
	streamUp(t, p, ref, name, data)
	f.noEOFChunk.Store(true)
	r, err := p.OpenReader(t.Context(), ref, name, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	got, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("a stream that ended without its eof chunk after every byte gave %d bytes, %v",
			len(got), err)
	}
}

func TestBoxdReaderOfAHostThatSentMore(t *testing.T) {
	for _, c := range []struct {
		name  string
		extra int
	}{
		// 200 KiB and 56 KiB fill four 64 KiB chunks, so the eof chunk comes next
		{"BeforeTheEofChunk", 56 << 10},
		{"InAnotherChunk", 100 << 10},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newFake(t)
			p := f.provider(t, fakeKey)
			ref := readyMachine(t, f)
			name := filepath.Join(t.TempDir(), "f")
			data := make([]byte, 200<<10)
			_, _ = rand.Read(data)
			streamUp(t, p, ref, name, data)
			f.ignoreLength = true
			f.beforeRead(func(name string) {
				file, err := os.OpenFile(name, os.O_WRONLY|os.O_APPEND, 0)
				if err == nil {
					_, err = file.Write(bytes.Repeat([]byte("y"), c.extra))
					err = errors.Join(err, file.Close())
				}
				if err != nil {
					t.Error(err)
				}
			})
			r, err := p.OpenReader(t.Context(), ref, name, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = r.Close() }()
			got, err := io.ReadAll(r)
			if err != nil || !bytes.Equal(got, data) {
				t.Fatalf("a host that sent %d bytes past the stat gave %d bytes, %v, want the "+
					"%d the stat saw", c.extra, len(got), err, len(data))
			}
		})
	}
}

func TestBoxdFifoIsNotRegular(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readyMachine(t, f)
	name := filepath.Join(t.TempDir(), "fifo")
	if err := syscall.Mkfifo(name, 0o644); err != nil {
		t.Fatal(err)
	}
	entry, err := p.StatFile(t.Context(), ref, name)
	if err != nil || entry.Regular || entry.IsDir || entry.Mode&fs.ModeIrregular == 0 {
		t.Fatalf("StatFile of a FIFO gave %+v, %v, want an irregular entry", entry, err)
	}
	_, err = p.OpenReader(t.Context(), ref, name, 0)
	eq(t, isCode(err, provider.CodeInvalid), true)
}

func TestBoxdStreamsRefuseForeignRefsAndBadPaths(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := readyMachine(t, f)
	foreign := ref
	foreign.Provider = "e2b"
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
	eq(t, f.callCount("FsWrite"), 0)
}
