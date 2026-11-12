package lium

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

func TestLiumFiles(t *testing.T) {
	f, p := liveFake(t)
	ref, _ := readyRef(t, f, p)
	ctx := context.Background()
	dir := t.TempDir()
	name := dir + "/a/b/data.txt"
	data := []byte("hello, lium\n")

	// The parents are made and the mode is exact, whatever the umask
	before := time.Now().Add(-time.Second)
	if err := p.WriteFile(ctx, ref, name, data, 0o640); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(name)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, info.Mode().Perm(), fs.FileMode(0o640))

	reads := []struct {
		offset int64
		length int
		want   string
		eof    bool
	}{
		{0, 1024, "hello, lium\n", true},
		{7, 4, "lium", false},
		{7, 5, "lium\n", true},
		{12, 4, "", true},
		{100, 4, "", true},
		{0, 0, "", false},
	}
	for _, r := range reads {
		got, eof, err := p.ReadFile(ctx, ref, name, r.offset, r.length)
		if err != nil || string(got) != r.want || eof != r.eof {
			t.Errorf("ReadFile at %d for %d gave %q eof %v, %v, want %q eof %v", r.offset,
				r.length, got, eof, err, r.want, r.eof)
		}
	}

	e, err := p.StatFile(ctx, ref, name)
	if err != nil {
		t.Fatal(err)
	}
	if e.Path != name || e.Size != 12 || !e.Regular || e.IsDir || e.Mode != 0o640 ||
		e.Modified.Before(before.Truncate(time.Second)) {
		t.Fatalf("StatFile gave %+v", e)
	}
	d, err := p.StatFile(ctx, ref, dir+"/a/")
	if err != nil || d.Path != dir+"/a" || !d.IsDir || d.Regular || !d.Mode.IsDir() {
		t.Fatalf("StatFile of a directory gave %+v, %v", d, err)
	}

	// A second write replaces the file whole and leaves no temporary file beside it
	if err := p.WriteFile(ctx, ref, name, []byte("new"), 0o755); err != nil {
		t.Fatal(err)
	}
	got, eof, err := p.ReadFile(ctx, ref, name, 0, 64)
	if err != nil || string(got) != "new" || !eof {
		t.Fatalf("ReadFile after a rewrite gave %q eof %v, %v", got, eof, err)
	}
	left, _ := os.ReadDir(filepath.Dir(name))
	if len(left) != 1 {
		t.Fatalf("the directory holds %d entries after two writes", len(left))
	}
	if info, _ := os.Stat(name); info.Mode().Perm() != 0o755 {
		t.Fatalf("the rewrite gave mode %v", info.Mode())
	}

	// Each call used its own session and gave its slot back
	pl, err := p.poolFor(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		pl.mu.Lock()
		defer pl.mu.Unlock()
		for _, c := range pl.conns {
			if c.sessions != 0 {
				return false
			}
		}
		return true
	})
}

func TestLiumFileErrors(t *testing.T) {
	f, p := liveFake(t)
	ref, _ := readyRef(t, f, p)
	ctx := context.Background()
	dir := t.TempDir()
	if err := os.WriteFile(dir+"/file", []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir+"/sub", 0o755); err != nil {
		t.Fatal(err)
	}

	_, _, err := p.ReadFile(ctx, ref, dir+"/missing", 0, 8)
	wantCode(t, err, provider.CodeNotFound, false)
	_, err = p.StatFile(ctx, ref, dir+"/missing")
	wantCode(t, err, provider.CodeNotFound, false)
	// EISDIR is invalid, a read of a directory and a write over one
	_, _, err = p.ReadFile(ctx, ref, dir+"/sub", 0, 8)
	wantCode(t, err, provider.CodeInvalid, false)
	wantCode(t, p.WriteFile(ctx, ref, dir+"/sub", []byte("x"), 0o644), provider.CodeInvalid,
		false)
	// A parent that is a file
	wantCode(t, p.WriteFile(ctx, ref, dir+"/file/x", []byte("x"), 0o644),
		provider.CodeInvalid, false)
	for _, bad := range []struct {
		name   string
		offset int64
		length int
	}{
		{dir + "/file", -1, 8}, {dir + "/file", 0, -1}, {"/x\x00y", 0, 8},
		{"/" + strings.Repeat("x", 4096), 0, 8},
	} {
		_, _, err = p.ReadFile(ctx, ref, bad.name, bad.offset, bad.length)
		wantCode(t, err, provider.CodeInvalid, false)
	}
	wantCode(t, p.WriteFile(ctx, ref, dir+"/f", nil, fs.ModeSetuid|0o755),
		provider.CodeInvalid, false)
	_, _, err = p.ReadFile(ctx, provider.Ref{Provider: "e2b", ID: "x"}, dir+"/file", 0, 8)
	wantCode(t, err, provider.CodeNotFound, false)

	// EACCES is invalid, which only a user other than root meets
	if os.Geteuid() != 0 {
		if err := os.Chmod(dir+"/sub", 0o555); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(dir+"/sub", 0o755) })
		wantCode(t, p.WriteFile(ctx, ref, dir+"/sub/x", []byte("x"), 0o644),
			provider.CodeInvalid, false)
	}
}
