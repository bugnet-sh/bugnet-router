package transfer_test

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/oynozan/bugnet-router/internal/transfer"
)

// access runs the access test with sh on the host as the test's user and answers its exit code
func access(t *testing.T, mode, p string) int {
	t.Helper()
	err := exec.Command("sh", "-c", transfer.AccessScript, "sh", mode, p).Run()
	var exit *exec.ExitError
	switch {
	case err == nil:
		return 0
	case errors.As(err, &exit):
		return exit.ExitCode()
	}
	t.Fatalf("run the access test on %s: %v", p, err)
	return 0
}

func TestAccessScriptOnTheHost(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root passes every permission test")
	}
	root := t.TempDir()
	at := func(p string) string { return filepath.Join(root, p) }
	chmod := func(p string, mode os.FileMode) {
		t.Helper()
		if err := os.Chmod(at(p), mode); err != nil {
			t.Fatal(err)
		}
	}
	mkdir := func(p string) {
		t.Helper()
		if err := os.Mkdir(at(p), 0o755); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(at(p), 0o755) })
	}
	file := func(p string, mode os.FileMode) {
		t.Helper()
		if err := os.WriteFile(at(p), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		chmod(p, mode)
	}
	mkdir("open")
	file("open/mine", 0o644)
	file("open/readonly", 0o400)
	file("open/writeonly", 0o200)
	mkdir("locked")
	file("locked/f", 0o644)
	chmod("locked", 0o555)
	mkdir("sticky")
	chmod("sticky", os.ModeSticky|0o777)
	file("sticky/mine", 0o644)
	// A world-writable file another user owns, as root's /dev/null is
	if err := os.Symlink("/dev/null", at("sticky/theirs")); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		mode, path string
		code       int
	}{
		{"w", "open/mine", 0},
		{"w", "open/readonly", 1},
		{"w", "locked/f", 1},
		{"w", "open/new/deep/f", 0},
		{"w", "locked/new", 1},
		{"w", "sticky/mine", 0},
		{"w", "sticky/theirs", 1},
		{"w", "open/mine/below", 1},
		{"r", "open/mine", 0},
		{"r", "open/writeonly", 1},
		{"r", "open/none", 1},
	} {
		if got := access(t, c.mode, at(c.path)); got != c.code {
			t.Errorf("%s %s exited %d, want %d", c.mode, c.path, got, c.code)
		}
	}
}
