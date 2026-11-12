package lium

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"

	"github.com/pkg/sftp"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// ReadFile reads at most length bytes at offset over SFTP, eof once they reach the file's size
func (p *Provider) ReadFile(
	ctx context.Context, ref provider.Ref, name string, offset int64, length int,
) ([]byte, bool, error) {
	if offset < 0 || length < 0 {
		return nil, false, invalid(fmt.Errorf("read of %d bytes at offset %d", length, offset),
			false)
	}
	clean, err := podPath(name)
	if err != nil {
		return nil, false, err
	}
	var data []byte
	var eof bool
	err = p.withSFTP(ctx, ref, func(cl *sftp.Client) error {
		// A stat first, since opening a FIFO waits for a writer
		info, err := cl.Stat(clean)
		if err != nil {
			return fmt.Errorf("stat %s: %w", clean, err)
		}
		if err := regularFile(clean, info); err != nil {
			return err
		}
		f, err := cl.Open(clean)
		if err != nil {
			return fmt.Errorf("open %s: %w", clean, err)
		}
		size, err := openSize(clean, f)
		if err != nil {
			return err
		}
		if offset >= size {
			eof = true
			return nil
		}
		buf := make([]byte, min(int64(length), size-offset))
		n, err := f.ReadAt(buf, offset)
		if err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("read %s: %w", clean, err)
		}
		data, eof = buf[:n], errors.Is(err, io.EOF) || offset+int64(n) >= size
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return data, eof, nil
}

// openSize is the size of an open regular file, from Stat on its handle
func openSize(name string, f *sftp.File) (int64, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat %s: %w", name, err)
	}
	if err := regularFile(name, info); err != nil {
		return 0, err
	}
	e, err := fileEntry(name, info)
	return e.Size, err
}

// WriteFile writes a temporary file beside the target with the exact mode, then renames it over
func (p *Provider) WriteFile(
	ctx context.Context, ref provider.Ref, name string, data []byte, mode fs.FileMode,
) error {
	if mode&^fs.ModePerm != 0 {
		return invalid(fmt.Errorf("file mode %v is not permission bits", mode), false)
	}
	clean, err := podPath(name)
	if err != nil {
		return err
	}
	return p.withSFTP(ctx, ref, func(cl *sftp.Client) error {
		t, err := createTemp(cl, clean, mode)
		if err != nil {
			return err
		}
		if _, err := t.f.Write(data); err != nil {
			return t.fail(fmt.Errorf("write %s: %w", t.temp, err))
		}
		if err := t.close(int64(len(data))); err != nil {
			return err
		}
		_, err = t.rename()
		return err
	})
}

// tempFile is the hidden file beside a target that a write fills before the rename over it
type tempFile struct {
	cl           *sftp.Client
	f            *sftp.File
	target, temp string
}

// createTemp makes the target's parents and opens a new temporary file beside it with mode
func createTemp(cl *sftp.Client, target string, mode fs.FileMode) (*tempFile, error) {
	dir := path.Dir(target)
	if err := cl.MkdirAll(dir); err != nil {
		return nil, spaceError(cl, dir, fmt.Errorf("make %s: %w", dir, err))
	}
	temp := path.Join(dir, provider.TempName(path.Base(target)))
	f, err := cl.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL)
	if err != nil {
		return nil, spaceError(cl, dir, fmt.Errorf("create %s: %w", temp, err))
	}
	t := &tempFile{cl: cl, f: f, target: target, temp: temp}
	if err := f.Chmod(mode.Perm()); err != nil {
		return nil, t.fail(fmt.Errorf("chmod %s: %w", temp, err))
	}
	return t, nil
}

// close closes the temporary file and checks the pod holds size bytes in it
func (t *tempFile) close(size int64) error {
	if err := t.f.Close(); err != nil {
		return t.fail(fmt.Errorf("close %s: %w", t.temp, err))
	}
	info, err := t.cl.Lstat(t.temp)
	if err != nil {
		return t.fail(fmt.Errorf("stat %s: %w", t.temp, err))
	}
	if info.Size() != size {
		return t.fail(&provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("%s holds %d of %d bytes", t.temp, info.Size(), size)})
	}
	return nil
}

// rename moves the closed temporary file over the target and describes what is there now
func (t *tempFile) rename() (provider.FileEntry, error) {
	if err := t.cl.PosixRename(t.temp, t.target); err != nil {
		err = fmt.Errorf("rename %s over %s: %w", t.temp, t.target, err)
		// OpenSSH's sftp-server answers EISDIR as a bare failure
		if d, serr := t.cl.Lstat(t.target); serr == nil && d.IsDir() {
			err = invalid(fmt.Errorf("%s is a directory: %w", t.target, err), false)
		}
		return provider.FileEntry{}, t.fail(err)
	}
	info, err := t.cl.Lstat(t.target)
	if err != nil {
		return provider.FileEntry{}, fmt.Errorf("stat %s: %w", t.target, err)
	}
	return fileEntry(t.target, info)
}

// fail removes the temporary file after err, reading a bare failure on a full disk as no_space
func (t *tempFile) fail(err error) error {
	err = spaceError(t.cl, t.dir(), err)
	_ = t.f.Close()
	_ = t.cl.Remove(t.temp)
	return err
}

func (t *tempFile) dir() string { return path.Dir(t.temp) }

// StatFile describes a path with Lstat, so a final symlink is the link itself
func (p *Provider) StatFile(
	ctx context.Context, ref provider.Ref, name string,
) (provider.FileEntry, error) {
	clean, err := podPath(name)
	if err != nil {
		return provider.FileEntry{}, err
	}
	var entry provider.FileEntry
	err = p.withSFTP(ctx, ref, func(cl *sftp.Client) error {
		info, err := cl.Lstat(clean)
		if err != nil {
			return fmt.Errorf("stat %s: %w", clean, err)
		}
		entry, err = fileEntry(clean, info)
		return err
	})
	return entry, err
}
