package lium

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pkg/sftp"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const (
	// streamChunk is the most one stream read asks the pod for, its requests running side by side
	streamChunk = 1 << 20
	// digestScript reads the file on stdin, since sha256sum escapes a name holding \ or a newline
	digestScript = `sha256sum < "$1"`
	// maxDigestOut bounds what the digest's output is read to
	maxDigestOut = 256
)

var _ provider.Provider = (*Provider)(nil)

// OpenReader streams a regular file from offset up to the size Stat gave on its open handle
func (p *Provider) OpenReader(
	ctx context.Context, ref provider.Ref, name string, offset int64,
) (provider.FileReader, error) {
	id, ok := podID(ref)
	if !ok {
		return nil, notLium(ref)
	}
	clean, err := podPath(name)
	if err != nil {
		return nil, err
	}
	if offset < 0 {
		return nil, invalid(fmt.Errorf("read at offset %d", offset), false)
	}
	sctx, cancel := p.opContext(ctx)
	defer cancel()
	s, err := p.openSFTP(sctx, ref, true)
	if err != nil {
		return nil, err
	}
	stop := s.guard(sctx)
	info, f, err := openRegular(s.cl, clean)
	if err == nil && offset > info.Size {
		err = invalid(fmt.Errorf("offset %d is past a file of %d bytes", offset, info.Size),
			false)
	}
	if stopped := stop(); err != nil || !stopped {
		s.end()
		if !stopped {
			return nil, timedOut(sctx, id)
		}
		return nil, s.classify(ctx, err)
	}
	body := &sftpBody{s: s, f: f, name: clean, off: offset, left: info.Size - offset,
		timed: !hasDeadline(ctx)}
	return provider.NewFileReader(ctx, body, info, offset, func(err error) error {
		return s.classify(ctx, err)
	}), nil
}

// openRegular opens a path that Lstat shows is a regular file, and describes the open handle
func openRegular(cl *sftp.Client, name string) (provider.FileEntry, *sftp.File, error) {
	// Lstat first, since opening follows a final symlink and a FIFO waits for a writer
	info, err := cl.Lstat(name)
	if err != nil {
		return provider.FileEntry{}, nil, fmt.Errorf("stat %s: %w", name, err)
	}
	if err := regularFile(name, info); err != nil {
		return provider.FileEntry{}, nil, err
	}
	f, err := cl.Open(name)
	if err != nil {
		return provider.FileEntry{}, nil, fmt.Errorf("open %s: %w", name, err)
	}
	if info, err = f.Stat(); err != nil {
		return provider.FileEntry{}, nil, fmt.Errorf("stat %s: %w", name, err)
	}
	if err := regularFile(name, info); err != nil {
		return provider.FileEntry{}, nil, err
	}
	e, err := fileEntry(name, info)
	return e, f, err
}

// hasDeadline reports a caller that bounds its own call
func hasDeadline(ctx context.Context) bool {
	_, ok := ctx.Deadline()
	return ok
}

// sftpBody reads an open file ahead in chunks, never past the size its handle gave
type sftpBody struct {
	s    *sftpSession
	f    *sftp.File
	name string
	// off is where the next chunk starts, left the bytes to the size
	off, left int64
	chunk     []byte
	// buf is the part of the last chunk not read yet
	buf []byte
	// timed bounds each chunk by the operation limit, the caller having set no deadline
	timed bool
}

func (b *sftpBody) Read(p []byte) (int, error) {
	if len(b.buf) == 0 {
		if b.left == 0 {
			return 0, io.EOF
		}
		if err := b.fill(); err != nil {
			return 0, err
		}
	}
	n := copy(p, b.buf)
	b.buf = b.buf[n:]
	return n, nil
}

// fill reads the next chunk, whose requests pkg/sftp sends side by side
func (b *sftpBody) fill() error {
	if b.chunk == nil {
		b.chunk = make([]byte, min(streamChunk, b.left))
	}
	want := b.chunk[:min(int64(len(b.chunk)), b.left)]
	stop := b.s.limit(b.timed)
	n, err := b.f.ReadAt(want, b.off)
	if stop() {
		return &provider.Error{Code: provider.CodeInternal, Err: fmt.Errorf(
			"a read of %s on lium pod %s took past %v", b.name, b.s.pod, b.s.p.ssh.op)}
	}
	b.off, b.left, b.buf = b.off+int64(n), b.left-int64(n), want[:n]
	if n > 0 {
		return nil
	}
	if errors.Is(err, io.EOF) {
		return io.EOF
	}
	return fmt.Errorf("read %s: %w", b.name, err)
}

// Close ends the session, whose server closes the handle with it
func (b *sftpBody) Close() error {
	b.s.end()
	return nil
}

// limit aborts the session once the operation limit passes, when on, and stop reports it did
func (s *sftpSession) limit(on bool) (stop func() bool) {
	if !on {
		return func() bool { return false }
	}
	var fired atomic.Bool
	t := time.AfterFunc(s.p.ssh.op, func() {
		fired.Store(true)
		s.abort()
	})
	return func() bool {
		t.Stop()
		return fired.Load()
	}
}

// OpenWriter opens a new temporary file beside the target with the mode, which Commit renames
func (p *Provider) OpenWriter(
	ctx context.Context, ref provider.Ref, name string, spec provider.WriteSpec,
) (provider.FileWriter, error) {
	id, ok := podID(ref)
	if !ok {
		return nil, notLium(ref)
	}
	if err := spec.Check(); err != nil {
		return nil, err
	}
	clean, err := podPath(name)
	if err != nil {
		return nil, err
	}
	sctx, cancel := p.opContext(ctx)
	defer cancel()
	s, err := p.openSFTP(sctx, ref, true)
	if err != nil {
		return nil, err
	}
	stop := s.guard(sctx)
	t, err := createTemp(s.cl, clean, spec.Mode)
	if stopped := stop(); err != nil || !stopped {
		s.end()
		if !stopped {
			return nil, timedOut(sctx, id)
		}
		return nil, s.classify(ctx, err)
	}
	u := &upload{p: p, ref: ref, s: s, t: t, size: spec.Size, timed: !hasDeadline(ctx)}
	return provider.NewFileWriter(ctx, spec, u), nil
}

// upload is the provider half of one OpenWriter, its session held until Finish or Discard
type upload struct {
	p    *Provider
	ref  provider.Ref
	s    *sftpSession
	t    *tempFile
	size int64
	// timed bounds each write by the operation limit, the caller having set no deadline
	timed bool
}

func (u *upload) Write(b []byte) (int, error) {
	stop := u.s.limit(u.timed)
	n, err := u.t.f.Write(b)
	if stop() {
		return n, &provider.Error{Code: provider.CodeInternal, Err: fmt.Errorf(
			"a write of %s on lium pod %s took past %v", u.t.temp, u.s.pod, u.p.ssh.op)}
	}
	if err != nil {
		err = spaceError(u.t.cl, u.t.dir(), fmt.Errorf("write %s: %w", u.t.temp, err))
		return n, u.s.classify(context.Background(), err)
	}
	return n, nil
}

// Finish closes the temporary file, checks its bytes and sum, and renames it over the target
func (u *upload) Finish(ctx context.Context, sum []byte) (provider.FileEntry, error) {
	fctx, cancel := u.p.opContext(ctx)
	defer cancel()
	stop := u.s.guard(fctx)
	entry, err := u.finish(fctx, sum)
	if stopped := stop(); err != nil && !stopped {
		return provider.FileEntry{}, timedOut(fctx, u.s.pod)
	}
	if err != nil {
		return provider.FileEntry{}, u.s.classify(ctx, err)
	}
	u.s.end()
	return entry, nil
}

func (u *upload) finish(ctx context.Context, sum []byte) (provider.FileEntry, error) {
	if err := u.t.close(u.size); err != nil {
		return provider.FileEntry{}, err
	}
	if sum != nil {
		got, err := u.p.digest(ctx, u.ref, u.t.temp)
		if err != nil {
			return provider.FileEntry{}, err
		}
		if !bytes.Equal(got, sum) {
			return provider.FileEntry{}, &provider.Error{Code: provider.CodeInternal,
				Err: fmt.Errorf("lium pod %s holds %s with SHA-256 %x, not %x", u.s.pod,
					u.t.temp, got, sum)}
		}
	}
	return u.t.rename()
}

// Discard ends the stream's session, which stops a write in progress, and removes the temp
func (u *upload) Discard(ctx context.Context) error {
	u.s.end()
	return u.p.withSFTP(ctx, u.ref, func(cl *sftp.Client) error {
		if err := cl.Remove(u.t.temp); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove %s: %w", u.t.temp, err)
		}
		return nil
	})
}

// digest is a file's SHA-256 as sha256sum computes it in the pod
func (p *Provider) digest(ctx context.Context, ref provider.Ref, name string) ([]byte, error) {
	s, err := p.Exec(ctx, ref, provider.ExecSpec{
		Argv: []string{"sh", "-c", digestScript, "sh", name},
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = s.Close() }()
	out, err := io.ReadAll(io.LimitReader(s, maxDigestOut))
	if err != nil {
		return nil, err
	}
	// What a digest prints past the cap is never read, so it must not hold its exit
	_, _ = io.Copy(io.Discard, s)
	code, err := s.Wait()
	if err != nil {
		return nil, err
	}
	field, _, _ := strings.Cut(string(out), " ")
	sum, herr := hex.DecodeString(field)
	if code != 0 || herr != nil || len(sum) != sha256.Size {
		return nil, &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("sha256sum of %s on lium pod %s exited %d", name, ref.ID, code)}
	}
	return sum, nil
}
