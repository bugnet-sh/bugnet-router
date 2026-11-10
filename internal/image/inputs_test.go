package image_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/image"
)

// TestImageInputsNeedANewVersion fails when an input moved under a version that was built
func TestImageInputsNeedANewVersion(t *testing.T) {
	_, err := image.SyncInputs(filepath.Join("..", "..", "deploy"), false)
	if err != nil {
		t.Fatal(err)
	}
}

func deployTree(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, f := range image.InputFiles {
		put(t, dir, f, "x")
	}
	put(t, dir, "image/VERSION", "2026.11.1\n")
	return dir
}

func put(t *testing.T, dir, name, content string) {
	t.Helper()
	p := filepath.Join(dir, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestInputsRewriteRule(t *testing.T) {
	dir := deployTree(t)
	changed, err := image.SyncInputs(dir, true)
	eq(t, err, nil)
	eq(t, changed, true)
	changed, err = image.SyncInputs(dir, true)
	eq(t, err, nil)
	eq(t, changed, false)
	// No build record yet, so an input may move and the line follows
	put(t, dir, "image/Dockerfile", "y")
	changed, err = image.SyncInputs(dir, true)
	eq(t, err, nil)
	eq(t, changed, true)
	// Once the version was built, a moved input fails and the line stays
	put(t, dir, "image/builds/dev-2026.11.1.json", "{}")
	put(t, dir, "image/Dockerfile", "z")
	before, _ := os.ReadFile(filepath.Join(dir, "image/inputs.sha256"))
	_, err = image.SyncInputs(dir, true)
	eq(t, err, image.ErrInputsMoved)
	after, _ := os.ReadFile(filepath.Join(dir, "image/inputs.sha256"))
	eq(t, string(after), string(before))
	// A new version rewrites it
	put(t, dir, "image/VERSION", "2026.12.1\n")
	changed, err = image.SyncInputs(dir, true)
	eq(t, err, nil)
	eq(t, changed, true)
}

func TestInputsHashCoversEveryInput(t *testing.T) {
	dir := deployTree(t)
	base, err := image.InputsSHA256(dir)
	eq(t, err, nil)
	for _, f := range image.InputFiles {
		put(t, dir, f, "changed")
		got, err := image.InputsSHA256(dir)
		eq(t, err, nil)
		eq(t, got != base, true)
		put(t, dir, f, "x")
	}
}
