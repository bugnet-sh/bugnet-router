package local_test

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
)

func TestLocalHomeIsTheSandboxRoot(t *testing.T) {
	p := newLocal(t, local.Options{})
	ref := createReady(t, p, nil)
	var pather provider.Pather = p
	eq(t, pather.Home(ref), "/root")
	if err := p.WriteFile(t.Context(), ref, "probe", []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	entry, err := p.StatFile(t.Context(), ref, pather.Home(ref)+"/probe")
	if err != nil || !entry.Regular {
		t.Fatalf("a relative write landed outside /root: %+v, %v", entry, err)
	}
}

func TestLocalRealPathReadsLinksAsSandboxPaths(t *testing.T) {
	root := t.TempDir()
	p := newLocal(t, local.Options{Root: root})
	ref := createReady(t, p, nil)
	fsDir := filepath.Join(root, ref.ID, "fs")
	for _, dir := range []string{"root/project", "opt/app"} {
		if err := os.MkdirAll(filepath.Join(fsDir, dir), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	links := map[string]string{
		"root/abs": "/opt/app", "root/rel": "project", "root/loop": "loop",
		"root/up": "../opt",
	}
	for name, target := range links {
		if err := os.Symlink(target, filepath.Join(fsDir, name)); err != nil {
			t.Fatal(err)
		}
	}
	for in, want := range map[string]string{
		"/root/abs/f":          "/opt/app/f",
		"/root/rel/./x/../y":   "/root/project/y",
		"/root/up/app":         "/opt/app",
		"/root/missing/../out": "/root/out",
		"/root//project/":      "/root/project",
		"/":                    "/",
		"/../..":               "/",
	} {
		got, err := p.RealPath(t.Context(), ref, in)
		if err != nil || got != want {
			t.Errorf("RealPath(%q) = %q, %v, want %q", in, got, err, want)
		}
	}
	_, err := p.RealPath(t.Context(), ref, "/root/loop/x")
	requireCode(t, "RealPath through a link loop", err, provider.CodeInvalid)
	_, err = p.RealPath(t.Context(), ref, "root/rel")
	requireCode(t, "RealPath of a relative path", err, provider.CodeInvalid)
}

func TestLocalListingMarksRegularFiles(t *testing.T) {
	root := t.TempDir()
	p := newLocal(t, local.Options{Root: root})
	ref := createReady(t, p, nil)
	if err := p.WriteFile(t.Context(), ref, "/tmp/file", []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(root, ref.ID, "fs", "tmp", "fifo"), 0o644); err != nil {
		t.Fatal(err)
	}
	l := provider.NewListing(10)
	if err := p.ListFiles(t.Context(), ref, "/tmp", 1, l); err != nil {
		t.Fatal(err)
	}
	regular := map[string]bool{}
	for _, e := range l.Entries {
		regular[e.Path] = e.Regular
	}
	eq(t, regular, map[string]bool{"/tmp/file": true, "/tmp/fifo": false})
}
