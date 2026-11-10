package image

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// InputFiles are the image inputs by path under the deploy directory
var InputFiles = []string{
	"image/Dockerfile", "image/setup-image.sh", "image/pins.conf",
	"e2b/template.ts", "e2b/build.ts",
}

const (
	versionFile = "image/VERSION"
	inputsFile  = "image/inputs.sha256"
	buildsDir   = "image/builds"
)

// InputsSHA256 hashes the image inputs under dir, the deploy directory of the repository
func InputsSHA256(dir string) (string, error) {
	files := slices.Clone(InputFiles)
	slices.Sort(files)
	var list bytes.Buffer
	for _, f := range files {
		b, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(f)))
		if err != nil {
			return "", fmt.Errorf("image: input %s: %w", f, err)
		}
		fmt.Fprintf(&list, "%s %x\n", f, sha256.Sum256(b))
	}
	return fmt.Sprintf("%x", sha256.Sum256(list.Bytes())), nil
}

// ErrInputsMoved means an input changed under a version that was already built
var ErrInputsMoved = errors.New(
	"image: deploy/image/inputs.sha256 is stale, run make image-inputs, which needs a new " +
		"deploy/image/VERSION once that version was built")

// SyncInputs applies the rewrite rule of CP 19.6.3 to the inputs line under dir
//
// It rewrites the line once VERSION moved or while no build record of VERSION exists, and reports
// whether it wrote. Otherwise it leaves the line and fails with ErrInputsMoved when the hash moved.
func SyncInputs(dir string, write bool) (changed bool, err error) {
	version, err := readVersion(dir)
	if err != nil {
		return false, err
	}
	sum, err := InputsSHA256(dir)
	if err != nil {
		return false, err
	}
	line := sum + " " + version + "\n"
	have, err := os.ReadFile(filepath.Join(dir, inputsFile))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if string(have) == line {
		return false, nil
	}
	oldSum, oldVersion, _ := strings.Cut(strings.TrimSpace(string(have)), " ")
	if oldVersion == version && builtOnce(dir, version) && oldSum != sum {
		return false, ErrInputsMoved
	}
	if !write {
		return false, ErrInputsMoved
	}
	return true, os.WriteFile(filepath.Join(dir, inputsFile), []byte(line), 0o644)
}

func readVersion(dir string) (string, error) {
	b, err := os.ReadFile(filepath.Join(dir, versionFile))
	if err != nil {
		return "", fmt.Errorf("image: %w", err)
	}
	v := strings.TrimSpace(string(b))
	if !VersionPattern.MatchString(v) {
		return "", fmt.Errorf("image: VERSION %q is malformed", v)
	}
	return v, nil
}

// builtOnce reports whether a build record of version exists for either channel
func builtOnce(dir, version string) bool {
	for _, ch := range []string{"dev", "prod"} {
		if _, err := os.Stat(filepath.Join(dir, buildsDir, ch+"-"+version+".json")); err == nil {
			return true
		}
	}
	return false
}
