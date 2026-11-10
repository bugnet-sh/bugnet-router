package main

import (
	"archive/tar"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/bugnet-sh/bugnet-router/internal/leakscan"
)

// image runs the base image commands, scan only until the builds land
func (c *cli) image(args []string) error {
	if len(args) == 0 || args[0] != "scan" {
		return fmt.Errorf("%w: image takes scan", errUsage)
	}
	return c.imageScan(args[1:])
}

// imageScan scans a directory or a local OCI image for every needle of the environment
func (c *cli) imageScan(args []string) error {
	f := newFlags("image scan")
	dir := f.String("dir", "", "scan the files under DIR")
	oci := f.String("oci", "", "scan the local OCI image TAG")
	if err := parse(f, args, false); err != nil {
		return err
	}
	if (*dir == "") == (*oci == "") {
		return fmt.Errorf("%w: give exactly one of --dir and --oci", errUsage)
	}
	needles, err := leakscan.FromEnv(os.Environ())
	if err != nil {
		return err
	}
	var archive io.Reader
	if *dir != "" {
		archive, err = dirArchive(*dir)
	} else {
		archive, err = c.ociArchive(*oci)
	}
	if err != nil {
		return err
	}
	found, err := leakscan.ScanImage(archive, needles, "")
	if err != nil {
		return err
	}
	if len(found) > 0 {
		_, _ = io.WriteString(c.stderr, leakscan.FormatFindings(found))
		return exitCode(1)
	}
	_, _ = fmt.Fprintln(c.stdout, "image scan: no findings")
	return nil
}

// dirArchive tars the regular files under dir, with their paths relative to it
func dirArchive(dir string) (io.Reader, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || !d.Type().IsRegular() {
			return err
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		if err := tw.WriteHeader(&tar.Header{Name: filepath.ToSlash(rel), Mode: 0o644,
			Size: int64(len(b)), Typeflag: tar.TypeReg}); err != nil {
			return err
		}
		_, err = tw.Write(b)
		return err
	})
	if err != nil {
		return nil, err
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return &buf, nil
}

// ociArchive runs the leak collector in the image with no network and returns its archive
func (c *cli) ociArchive(tag string) (io.Reader, error) {
	cmd := exec.CommandContext(c.ctx, "docker", "run", "--rm", "--network", "none",
		"--entrypoint", "sh", tag, "-c",
		`eval "$1" >&2; cat `+leakscan.ArchivePath, "sh", leakscan.Collect)
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return nil, fmt.Errorf("the collector in the image exited %d", ee.ExitCode())
		}
		return nil, err
	}
	return &out, nil
}
