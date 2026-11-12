package hf

import (
	"bytes"
	"crypto/rand"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/contract"
)

// writes are the write calls the fake got
func writes(f *fakeHF) []recorded { return f.requestsTo("PUT", "/v1/files/write") }

// mustWrite puts a file on the host, standing in for one a command left in the Job
func mustWrite(t *testing.T, name, data string, mode fs.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, []byte(data), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(name, mode); err != nil {
		t.Fatal(err)
	}
}

func TestHFStatFile(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	ref := readyJob(t, f, p)
	dir := t.TempDir()
	mustWrite(t, dir+"/file", "abc", 0o640)
	if err := os.Symlink("file", dir+"/link"); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(dir+"/fifo", 0o600); err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()

	file, err := p.StatFile(ctx, ref, dir+"/file")
	if err != nil {
		t.Fatal(err)
	}
	info, _ := os.Stat(dir + "/file")
	eq(t, file, provider.FileEntry{Path: dir + "/file", Size: 3, Mode: 0o640, Regular: true,
		Modified: info.ModTime().Truncate(time.Millisecond).UTC()})
	link, err := p.StatFile(ctx, ref, dir+"/link")
	if err != nil || link.Regular || link.Mode&fs.ModeSymlink == 0 {
		t.Fatalf("a symlink gave %+v, %v", link, err)
	}
	sub, err := p.StatFile(ctx, ref, dir+"/")
	if err != nil || !sub.IsDir || !sub.Mode.IsDir() || sub.Regular || sub.Path != dir {
		t.Fatalf("a directory gave %+v, %v", sub, err)
	}
	// sbx-server names a FIFO a file, so it reads as regular and its read refuses it, design 4.4
	fifo, err := p.StatFile(ctx, ref, dir+"/fifo")
	if err != nil || !fifo.Regular {
		t.Fatalf("a FIFO gave %+v, %v", fifo, err)
	}
	_, err = p.OpenReader(ctx, ref, dir+"/fifo", 0)
	if c, _ := codeOf(err); c != provider.CodeInvalid {
		t.Fatalf("a FIFO opened with %v", err)
	}
	_, err = p.StatFile(ctx, ref, dir+"/missing")
	if c, _ := codeOf(err); c != provider.CodeNotFound {
		t.Fatalf("a missing path gave %v", err)
	}
	// Paths sbx-server would read as other text, and relative ones, never leave the router
	calls := len(f.all())
	for _, bad := range []string{"relative", "", "/tmp/\xff", "/tmp/a\x00b"} {
		_, err := p.StatFile(ctx, ref, bad)
		if c, _ := codeOf(err); c != provider.CodeInvalid {
			t.Errorf("a stat of %q gave %v", bad, err)
		}
	}
	eq(t, len(f.all()), calls)
	// A hostile answer's own path, size and mode are never taken as they come
	f.inject(sbxStatRoute, fault{status: 200, times: 1, raw: `{"name":"x","path":"/etc/shadow",` +
		`"type":"file","size":-5,"mode":"104755","mtime_ms":0}`})
	odd, err := p.StatFile(ctx, ref, dir+"/file")
	if err != nil {
		t.Fatal(err)
	}
	eq(t, odd, provider.FileEntry{Path: dir + "/file", Mode: 0o755, Regular: true})
	f.inject(sbxStatRoute, fault{status: 200, times: 1, raw: `{"name":"x","type":"socket"}`})
	odd, err = p.StatFile(ctx, ref, dir+"/file")
	if err != nil || odd.Regular || odd.IsDir || odd.Mode&fs.ModeIrregular == 0 {
		t.Fatalf("an unknown type gave %+v, %v", odd, err)
	}
	f.inject(sbxStatRoute, fault{status: 200, times: 1,
		raw: `{"name":"` + strings.Repeat("x", maxStatBytes) + `"}`})
	if _, err := p.StatFile(ctx, ref, dir+"/file"); err == nil {
		t.Fatal("a stat answer past its bound was read")
	}
}

func TestHFReadFile(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	ref := readyJob(t, f, p)
	dir := t.TempDir()
	mustWrite(t, dir+"/data", "hello, contract\n", 0o644)
	if err := os.Symlink("data", dir+"/link"); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, dir+"/empty", "", 0o644)
	ctx := t.Context()
	for _, c := range []struct {
		name   string
		offset int64
		length int
		want   string
		eof    bool
	}{
		{"data", 0, 1024, "hello, contract\n", true},
		{"data", 7, 8, "contract", false},
		{"data", 7, 9, "contract\n", true},
		{"data", 16, 4, "", true},
		{"data", 40, 4, "", true},
		{"data", 3, 0, "", false},
		{"empty", 0, 10, "", true},
		// A final symlink is followed, as every adapter's ReadFile does
		{"link", 0, 5, "hello", false},
	} {
		got, eof, err := p.ReadFile(ctx, ref, dir+"/"+c.name, c.offset, c.length)
		if err != nil || string(got) != c.want || eof != c.eof {
			t.Errorf("%s at %d for %d gave %q eof %v, %v, want %q eof %v", c.name, c.offset,
				c.length, got, eof, err, c.want, c.eof)
		}
	}
	// Each read asks one byte past the slice and never more
	for _, r := range f.requestsTo("GET", "/v1/files/read") {
		if r.query.Get("length") == "" || r.query.Get("offset") == "" {
			t.Fatalf("a read asked %v", r.query)
		}
	}
	for _, c := range []struct {
		name string
		code provider.Code
	}{{"missing", provider.CodeNotFound}, {"", provider.CodeInvalid}} {
		_, _, err := p.ReadFile(ctx, ref, dir+"/"+c.name, 0, 10)
		if got, _ := codeOf(err); got != c.code {
			t.Errorf("a read of %q gave %v, want %s", c.name, err, c.code)
		}
	}
	_, _, err := p.ReadFile(ctx, ref, dir+"/data", -1, 10)
	if c, _ := codeOf(err); c != provider.CodeInvalid {
		t.Fatalf("a negative offset gave %v", err)
	}
	// An answer longer than asked, or one cut short, is never taken as the file
	f.inject(sbxReadRoute, fault{status: 200, raw: strings.Repeat("x", 100), times: 1})
	if got, _, err := p.ReadFile(ctx, ref, dir+"/data", 0, 4); err == nil {
		t.Fatalf("an answer of 100 bytes for 5 gave %q", got)
	}
	f.inject(sbxReadRoute, fault{cut: 3, times: 1})
	if got, _, err := p.ReadFile(ctx, ref, dir+"/data", 0, 10); err == nil {
		t.Fatalf("an answer cut at 3 bytes gave %q", got)
	}
}

func TestHFWriteFile(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	ref := readyJob(t, f, p)
	dir := t.TempDir()
	ctx := t.Context()
	name := dir + "/sub/deeper/run.sh"
	if err := p.WriteFile(ctx, ref, name, []byte("#!/bin/sh\necho ran\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(name)
	eq(t, string(got), "#!/bin/sh\necho ran\n")
	info, _ := os.Stat(name)
	eq(t, info.Mode().Perm(), fs.FileMode(0o755))
	// One write to a temporary name beside the target, then mv over it, design 4.2
	w := writes(f)
	eq(t, len(w), 1)
	temp := w[0].query.Get("path")
	if path.Dir(temp) != path.Dir(name) || !strings.HasPrefix(path.Base(temp), ".run.sh.bugnet-") ||
		!strings.HasSuffix(temp, ".part") {
		t.Fatalf("wrote to %q", temp)
	}
	eq(t, w[0].query.Get("offset"), "0")
	eq(t, w[0].query.Get("truncate_to"), "19")
	eq(t, w[0].query.Get("mode"), "755")
	eq(t, w[0].header.Get("Content-Length"), "19")
	lines := f.execLines()
	mv := lines[len(lines)-1]
	eq(t, mv[len(mv)-5:], []string{p.mv, "-fT", "--", temp, name})

	// A rewrite replaces the file whole and leaves no temporary file
	if err := p.WriteFile(ctx, ref, name, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, _ = os.ReadFile(name)
	eq(t, string(got), "x")
	left, _ := filepath.Glob(filepath.Join(path.Dir(name), ".*"))
	eq(t, len(left), 0)

	// An empty file is one write with truncate_to 0
	if err := p.WriteFile(ctx, ref, dir+"/empty", nil, 0o644); err != nil {
		t.Fatal(err)
	}
	w = writes(f)
	eq(t, w[len(w)-1].query.Get("truncate_to"), "0")
	eq(t, w[len(w)-1].header.Get("Content-Length"), "0")
	if info, err := os.Stat(dir + "/empty"); err != nil || info.Size() != 0 {
		t.Fatalf("empty file %v, %v", info, err)
	}

	// A directory in the target's place stays, and the temporary file goes
	if err := os.Mkdir(dir+"/taken", 0o755); err != nil {
		t.Fatal(err)
	}
	err := p.WriteFile(ctx, ref, dir+"/taken", []byte("x"), 0o644)
	if c, _ := codeOf(err); c != provider.CodeInvalid {
		t.Fatalf("a write over a directory gave %v", err)
	}
	if info, err := os.Stat(dir + "/taken"); err != nil || !info.IsDir() {
		t.Fatalf("the directory became %v, %v", info, err)
	}
	left, _ = filepath.Glob(filepath.Join(dir, ".taken*"))
	eq(t, len(left), 0)

	calls := len(f.all())
	for _, bad := range []string{"relative", "/", dir + "/..", "/tmp/\xff"} {
		err := p.WriteFile(ctx, ref, bad, []byte("x"), 0o644)
		if c, _ := codeOf(err); c != provider.CodeInvalid {
			t.Errorf("a write to %q gave %v", bad, err)
		}
	}
	err = p.WriteFile(ctx, ref, dir+"/f", []byte("x"), fs.ModeDir|0o644)
	if c, _ := codeOf(err); c != provider.CodeInvalid {
		t.Errorf("a mode with a type gave %v", err)
	}
	eq(t, len(f.all()), calls)
}

func TestHFWriteFileInParts(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	p.partBytes = 1000
	ref := readyJob(t, f, p)
	data := make([]byte, 4500)
	_, _ = rand.Read(data)
	name := t.TempDir() + "/big.bin"
	if err := p.WriteFile(t.Context(), ref, name, data, 0o600); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(name)
	if !bytes.Equal(got, data) {
		t.Fatal("the file differs from what was written")
	}
	// Parts of at most partBytes at their offsets, the last alone with truncate_to and the mode
	w := writes(f)
	eq(t, len(w), 5)
	var offsets []string
	for i, r := range w {
		offsets = append(offsets, r.query.Get("offset"))
		last := i == len(w)-1
		if last != (r.query.Get("truncate_to") == "4500") ||
			last != (r.query.Get("mode") == "600") {
			t.Fatalf("part %d asked %v", i, r.query)
		}
		if len(r.body) > 1000 {
			t.Fatalf("part %d holds %d bytes", i, len(r.body))
		}
	}
	slices.Sort(offsets)
	eq(t, offsets, []string{"0", "1000", "2000", "3000", "4000"})
	eq(t, w[len(w)-1].query.Get("offset"), "4000")
}

func TestHFWriteFileFailures(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	ref := readyJob(t, f, p)
	dir := t.TempDir()
	ctx := t.Context()
	// A part the proxy dropped is sent again
	f.inject(sbxWriteRoute, fault{drop: true, times: 1})
	if err := p.WriteFile(ctx, ref, dir+"/again", []byte("abc"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(dir + "/again")
	eq(t, string(got), "abc")
	// A refused part is not, and its temporary file goes
	f.inject(sbxWriteRoute, fault{status: 500, message: "No space left on device (os error 28)",
		served: true, times: 1})
	err := p.WriteFile(ctx, ref, dir+"/full", []byte("abc"), 0o644)
	if c, _ := codeOf(err); c != provider.CodeNoSpace {
		t.Fatalf("a full disk gave %v", err)
	}
	left, _ := filepath.Glob(filepath.Join(dir, ".full*"))
	eq(t, len(left), 0)
	if _, err := os.Stat(dir + "/full"); !os.IsNotExist(err) {
		t.Fatalf("the target exists: %v", err)
	}
}

func TestHFListFiles(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	ref := readyJob(t, f, p)
	dir := t.TempDir()
	for _, name := range []string{"a", "b", "c", "d/e", "d/f/g", "h"} {
		mustWrite(t, dir+"/"+name, "x", 0o644)
	}
	if err := os.Symlink("d", dir+"/link"); err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	list := func(depth, room int) *provider.Listing {
		t.Helper()
		l := provider.NewListing(room)
		if err := p.ListFiles(ctx, ref, dir, depth, l); err != nil {
			t.Fatal(err)
		}
		return l
	}
	paths := func(l *provider.Listing) []string {
		var out []string
		for _, e := range l.Entries {
			out = append(out, strings.TrimPrefix(e.Path, dir+"/"))
		}
		return out
	}
	// Paged two at a time with after, and depth by recursion into directories, never links
	f.mu.Lock()
	f.sbx.listMax = 2
	f.mu.Unlock()
	l := list(3, 100)
	eq(t, paths(l), []string{"a", "b", "c", "d", "h", "link", "d/e", "d/f", "d/f/g"})
	eq(t, l.Truncated, false)
	afters := []string{}
	for _, r := range f.requestsTo("GET", "/v1/files/list") {
		if r.query.Get("path") == dir {
			afters = append(afters, r.query.Get("after"))
		}
	}
	eq(t, afters, []string{"", "b", "d"})
	eq(t, paths(list(1, 100)), []string{"a", "b", "c", "d", "h", "link"})
	for _, e := range list(1, 100).Entries {
		want := !slices.Contains([]string{"d", "link"}, path.Base(e.Path))
		if e.Regular != want || e.IsDir != (path.Base(e.Path) == "d") || e.Size < 0 {
			t.Fatalf("entry %+v", e)
		}
	}
	// Room for three of six stops there and says so, asking for no more than it has room for
	short := list(1, 3)
	eq(t, paths(short), []string{"a", "b", "c"})
	eq(t, short.Truncated, true)
	reqs := f.requestsTo("GET", "/v1/files/list")
	eq(t, reqs[len(reqs)-1].query.Get("limit"), "1")

	// A hostile page's odd names are skipped and a page that never moves on ends the listing
	f.mu.Lock()
	f.sbx.listMax = 0
	f.mu.Unlock()
	f.inject(sbxListRoute, fault{status: 200, times: 2, raw: `{"entries":[` +
		`{"name":"../../etc","type":"dir"},{"name":"a/b","type":"file"},` +
		`{"name":"..","type":"dir"},` +
		`{"name":"","type":"file"},{"name":"ok","type":"file","size":1}],` +
		`"truncated":true,"next":"ok"}`})
	odd := list(2, 100)
	eq(t, paths(odd), []string{"ok"})
	eq(t, odd.Truncated, true)
	// An answer past its bound is a truncated listing
	f.inject(sbxListRoute, fault{status: 200, times: 1,
		raw: `{"entries":[{"name":"` + strings.Repeat("x", maxListBytes) + `"}]}`})
	eq(t, list(1, 100).Truncated, true)

	for _, c := range []struct {
		name  string
		depth int
		code  provider.Code
	}{
		{dir + "/missing", 1, provider.CodeNotFound}, {dir + "/a", 1, provider.CodeInvalid},
		{dir, 0, provider.CodeInvalid}, {"relative", 1, provider.CodeInvalid},
	} {
		err := p.ListFiles(ctx, ref, c.name, c.depth, provider.NewListing(10))
		if got, _ := codeOf(err); got != c.code {
			t.Errorf("a listing of %s to %d gave %v, want %s", c.name, c.depth, err, c.code)
		}
	}
}

func TestHFListLimit(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	contract.ListLimit(t, p, readyJob(t, f, p), t.TempDir())
}

func TestHFFilesOnAGoneJob(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	ref := readyJob(t, f, p)
	if err := p.Destroy(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	dir := t.TempDir()
	_, err := p.StatFile(ctx, ref, dir)
	eq(t, isCode(err, provider.CodeNotFound), true)
	_, _, err = p.ReadFile(ctx, ref, dir+"/x", 0, 1)
	eq(t, isCode(err, provider.CodeNotFound), true)
	eq(t, isCode(p.WriteFile(ctx, ref, dir+"/x", []byte("x"), 0o644), provider.CodeNotFound),
		true)
	eq(t, isCode(p.ListFiles(ctx, ref, dir, 1, provider.NewListing(1)), provider.CodeNotFound),
		true)
	_, err = p.OpenReader(ctx, ref, dir+"/x", 0)
	eq(t, isCode(err, provider.CodeNotFound), true)
	_, err = p.OpenWriter(ctx, ref, dir+"/x", provider.WriteSpec{Size: 1, Mode: 0o644})
	eq(t, isCode(err, provider.CodeNotFound), true)
	// The gone Job is read once a call fails, never retried as a busy proxy would be
	eq(t, len(writes(f)) <= 2, true)
}
