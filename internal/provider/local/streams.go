package local

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"

	"github.com/oynozan/bugnet-router/internal/provider"
)

// tempTries bounds the names tried for one temporary file, as os.CreateTemp does
const tempTries = 100

var _ provider.FileStreamer = (*Provider)(nil)

// beforeOpen runs between a reader's Lstat and its open, so a test can swap the file there
var beforeOpen func(host string)

// entry describes host as the sandbox sees it, from info that did not follow a final symlink
func (s sandbox) entry(host string, info fs.FileInfo) provider.FileEntry {
	return provider.FileEntry{
		Path:     s.sandboxPath(host),
		Size:     info.Size(),
		IsDir:    info.IsDir(),
		Mode:     info.Mode(),
		Modified: info.ModTime().UTC(),
		Regular:  info.Mode().IsRegular(),
	}
}

// StatFile describes a sandbox path with Lstat, so a final symlink is the link itself
func (p *Provider) StatFile(
	_ context.Context, ref provider.Ref, name string,
) (provider.FileEntry, error) {
	s, err := p.open(ref)
	if err != nil {
		return provider.FileEntry{}, err
	}
	host, err := s.hostPath(name)
	if err != nil {
		return provider.FileEntry{}, err
	}
	info, err := os.Lstat(host)
	if err != nil {
		return provider.FileEntry{}, fileErr(err)
	}
	return s.entry(host, info), nil
}

// OpenReader reads a regular file from offset, refusing a final symlink, FIFO or device
func (p *Provider) OpenReader(
	ctx context.Context, ref provider.Ref, name string, offset int64,
) (provider.FileReader, error) {
	s, err := p.open(ref)
	if err != nil {
		return nil, err
	}
	host, err := s.hostPath(name)
	if err != nil {
		return nil, err
	}
	// Lstat first, since opening a FIFO blocks until a writer comes
	info, err := os.Lstat(host)
	if err != nil {
		return nil, fileErr(err)
	}
	if !info.Mode().IsRegular() {
		return nil, invalid(fmt.Errorf("%s is not a regular file", name))
	}
	if beforeOpen != nil {
		beforeOpen(host)
	}
	f, err := os.OpenFile(host, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fileErr(err)
	}
	opened, err := f.Stat()
	switch {
	case err != nil:
	case !os.SameFile(info, opened):
		err = invalid(fmt.Errorf("%s changed while it was opened", name))
	case offset < 0 || offset > opened.Size():
		err = invalid(fmt.Errorf("offset %d is outside a file of %d bytes", offset,
			opened.Size()))
	default:
		_, err = f.Seek(offset, io.SeekStart)
	}
	if err != nil {
		_ = f.Close()
		return nil, fileErr(err)
	}
	return provider.NewFileReader(ctx, f, s.entry(host, opened), offset, fileErr), nil
}

// OpenWriter fills a hidden file beside the target, which Commit syncs, chmods and renames over it
func (p *Provider) OpenWriter(
	ctx context.Context, ref provider.Ref, name string, spec provider.WriteSpec,
) (provider.FileWriter, error) {
	s, err := p.open(ref)
	if err != nil {
		return nil, err
	}
	if err := spec.Check(); err != nil {
		return nil, err
	}
	host, err := s.hostPath(name)
	if err != nil {
		return nil, err
	}
	if host == s.fsDir() {
		return nil, invalid(fmt.Errorf("path %q names no file", name))
	}
	if err := os.MkdirAll(filepath.Dir(host), 0o755); err != nil {
		return nil, fileErr(err)
	}
	f, temp, err := createTemp(host)
	if err != nil {
		return nil, fileErr(err)
	}
	u := &upload{s: s, f: f, temp: temp, target: host, mode: spec.Mode.Perm()}
	return provider.NewFileWriter(ctx, spec, u), nil
}

// createTemp makes the temporary file for target with O_EXCL, so no two writers share one
func createTemp(target string) (*os.File, string, error) {
	for range tempTries {
		temp := filepath.Join(filepath.Dir(target), provider.TempName(filepath.Base(target)))
		f, err := os.OpenFile(temp, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if errors.Is(err, fs.ErrExist) {
			continue
		}
		return f, temp, err
	}
	return nil, "", fmt.Errorf("no free temporary name beside %s", target)
}

// upload is the host side of a local FileWriter
type upload struct {
	s      sandbox
	f      *os.File
	temp   string
	target string
	mode   fs.FileMode
}

func (u *upload) Write(p []byte) (int, error) {
	n, err := u.f.Write(p)
	return n, fileErr(err)
}

func (u *upload) Finish(context.Context, []byte) (provider.FileEntry, error) {
	err := u.f.Sync()
	if err == nil {
		err = u.f.Chmod(u.mode)
	}
	var info fs.FileInfo
	if err == nil {
		info, err = u.f.Stat()
	}
	if cerr := u.f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(u.temp, u.target)
	}
	if err != nil {
		return provider.FileEntry{}, fileErr(err)
	}
	return u.s.entry(u.target, info), nil
}

func (u *upload) Discard(context.Context) error {
	_ = u.f.Close()
	if err := os.Remove(u.temp); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fileErr(err)
	}
	return nil
}
