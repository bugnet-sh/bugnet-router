package lium

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const (
	// podHome is root's home in our image, where a relative path starts as sftp-server's do
	podHome = "/root"
	// maxPathBytes is Linux's PATH_MAX, the longest path a file call takes
	maxPathBytes = 4096
	// maxFileBytes bounds a size the pod's sftp-server states, far past any disk a size rents
	maxFileBytes = 1 << 44
	// lowFreeBytes is the free space under which a write's bare failure reads as a full disk
	lowFreeBytes = 1 << 20
)

// sftpSession is one SFTP session on a pooled connection, holding its slot until end
type sftpSession struct {
	p    *Provider
	pl   *sshPool
	c    *sshConn
	sess *ssh.Session
	pod  string
	// ended closes once the subsystem's stdout ended, the server having closed the session
	ended chan struct{}
	in    io.WriteCloser
	out   *endReader
	// cl speaks SFTP on in and frames, nil for a session that reads its packets itself
	cl     *sftp.Client
	frames *sftpFrames
	once   sync.Once
}

// endReader closes ended once its reader fails
type endReader struct {
	r     io.Reader
	once  sync.Once
	ended chan struct{}
}

func (e *endReader) Read(b []byte) (int, error) {
	n, err := e.r.Read(b)
	if err != nil {
		e.once.Do(func() { close(e.ended) })
	}
	return n, err
}

// withSFTP runs fn on a session of its own under the caller's deadline, else the operation limit
func (p *Provider) withSFTP(
	ctx context.Context, ref provider.Ref, fn func(cl *sftp.Client) error,
) error {
	id, ok := podID(ref)
	if !ok {
		return notLium(ref)
	}
	sctx, cancel := p.opContext(ctx)
	defer cancel()
	s, err := p.openSFTP(sctx, ref, true)
	if err != nil {
		return err
	}
	defer s.end()
	stop := s.guard(sctx)
	err = fn(s.cl)
	if !stop() && err != nil {
		return timedOut(sctx, id)
	}
	return s.classify(ctx, err)
}

// classify is sftpError, but names the answer that broke the session when one did
func (s *sftpSession) classify(ctx context.Context, err error) error {
	if bad := s.frames.err(); bad != nil && err != nil {
		return bad
	}
	return s.p.sftpError(ctx, s.pod, err)
}

// timedOut is the error of a file call the deadline cut
func timedOut(ctx context.Context, id string) error {
	return &provider.Error{Code: provider.CodeInternal,
		Err: fmt.Errorf("sftp on lium pod %s: %w", id, context.Cause(ctx))}
}

// openSFTP starts sftp-server on a session of the pod's pool, with a client unless raw
func (p *Provider) openSFTP(
	ctx context.Context, ref provider.Ref, client bool,
) (*sftpSession, error) {
	id := ref.ID
	pl, err := p.poolFor(ctx, ref)
	if err != nil {
		return nil, p.sshError(ctx, id, err)
	}
	sess, c, err := pl.session(ctx)
	if err != nil {
		return nil, p.sshError(ctx, id, err)
	}
	s := &sftpSession{p: p, pl: pl, c: c, sess: sess, pod: id, ended: make(chan struct{})}
	stop := s.guard(ctx)
	err = s.start(client)
	if !stop() {
		err = context.Cause(ctx)
	}
	if err != nil {
		s.end()
		return nil, p.sshError(ctx, id, fmt.Errorf("start sftp: %w", err))
	}
	return s, nil
}

// start asks for the sftp subsystem and, for a client, runs SFTP's version exchange
func (s *sftpSession) start(client bool) error {
	in, err := s.sess.StdinPipe()
	if err != nil {
		return err
	}
	out, err := s.sess.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := s.sess.StderrPipe()
	if err != nil {
		return err
	}
	s.in, s.out = in, &endReader{r: out, ended: s.ended}
	// Nothing reads sftp-server's stderr, which would otherwise hold the session's window
	go func() { _, _ = io.Copy(io.Discard, stderr) }()
	if err := s.sess.RequestSubsystem("sftp"); err != nil {
		return err
	}
	if client {
		s.frames = &sftpFrames{r: s.out}
		// Writes go to a temporary file, so a write cut short leaves no hole in the target
		s.cl, err = sftp.NewClientPipe(s.frames, s.in, sftp.UseConcurrentWrites(true))
	}
	if bad := s.frames.err(); bad != nil {
		return bad
	}
	return err
}

// sftpFrames hands pkg/sftp's client only packets whose fields fit, since it reads them unchecked
type sftpFrames struct {
	r io.Reader
	// pkt is what is left to hand on of the packet read last
	pkt []byte
	mu  sync.Mutex
	bad error
}

func (f *sftpFrames) Read(b []byte) (int, error) {
	if len(f.pkt) == 0 {
		if err := f.next(); err != nil {
			return 0, err
		}
	}
	n := copy(b, f.pkt)
	f.pkt = f.pkt[n:]
	return n, nil
}

// next reads one packet, never one longer than pkg/sftp's own bound, and checks its fields
func (f *sftpFrames) next() error {
	var head [4]byte
	if _, err := io.ReadFull(f.r, head[:]); err != nil {
		return err
	}
	n := binary.BigEndian.Uint32(head[:])
	if n == 0 || n > maxListPacket {
		return f.fail(malformed(fmt.Sprintf("a packet of %d bytes", n)))
	}
	pkt := make([]byte, 4+n)
	copy(pkt, head[:])
	if _, err := io.ReadFull(f.r, pkt[4:]); err != nil {
		return err
	}
	if err := checkFrame(pkt[4], pkt[5:]); err != nil {
		return f.fail(err)
	}
	f.pkt = pkt
	return nil
}

func (f *sftpFrames) fail(err error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bad = err
	return err
}

// err is why the frames refused a packet, nil while they refused none or for a raw session
func (f *sftpFrames) err() error {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.bad
}

// checkFrame refuses an answer whose fixed fields pkg/sftp reads past the packet's end
func checkFrame(typ byte, body []byte) error {
	id, rest, _ := readUint32(body)
	switch typ {
	case fxpStatus:
		if _, _, ok := readUint32(rest); !ok {
			return malformed("a status cut short")
		}
	case fxpHandle, fxpData:
		if _, _, ok := readString(rest); !ok {
			return malformed(fmt.Sprintf("an answer of type %d to request %d cut short", typ,
				id))
		}
	}
	return nil
}

// guard aborts the session when ctx ends before stop
func (s *sftpSession) guard(ctx context.Context) (stop func() bool) {
	return context.AfterFunc(ctx, s.abort)
}

// abort closes the session under a call whose time ran out, dropping a connection that stalls
func (s *sftpSession) abort() {
	_ = s.sess.Close()
	s.settle()
}

// settle waits a stall period for the server to close the session, then drops the connection
func (s *sftpSession) settle() {
	t := time.NewTimer(s.p.ssh.stall)
	defer t.Stop()
	select {
	case <-s.ended:
	case <-t.C:
		s.pl.drop(s.c)
	}
}

// end closes the session out of the caller's way and frees its slot once the server closed it
func (s *sftpSession) end() {
	s.once.Do(func() {
		_ = s.sess.Close()
		if s.out != nil && s.cl == nil {
			// A client's own reader sees the end, a raw session's stdout needs one
			go func() { _, _ = io.Copy(io.Discard, s.out) }()
		}
		go func() {
			defer s.pl.release(s.c)
			if s.out != nil {
				s.settle()
			}
		}()
	})
}

// podPath cleans a path for the pod's sftp-server, a relative one starting at root's home
func podPath(name string) (string, error) {
	switch {
	case len(name) > maxPathBytes:
		return "", invalid(fmt.Errorf("a path of %d bytes is past PATH_MAX", len(name)), false)
	case strings.IndexByte(name, 0) >= 0:
		return "", invalid(errors.New("the path holds a NUL byte"), false)
	case !path.IsAbs(name):
		return path.Join(podHome, name), nil
	}
	return path.Clean(name), nil
}

// fileEntry describes name from what the pod's sftp-server stated, refusing a size past reason
func fileEntry(name string, info fs.FileInfo) (provider.FileEntry, error) {
	if size := info.Size(); size < 0 || size > maxFileBytes {
		return provider.FileEntry{}, &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("the pod's sftp-server gave %s a size of %d", name, uint64(size))}
	}
	mode := info.Mode()
	return provider.FileEntry{
		Path: name, Size: info.Size(), IsDir: mode.IsDir(), Mode: mode,
		Modified: info.ModTime().UTC(), Regular: mode.IsRegular(),
	}, nil
}

// regularFile refuses what is not a regular file, a directory being EISDIR
func regularFile(name string, info fs.FileInfo) error {
	switch m := info.Mode(); {
	case m.IsDir():
		return &os.PathError{Op: "read", Path: name, Err: syscall.EISDIR}
	case !m.IsRegular():
		return invalid(fmt.Errorf("%s is not a regular file", name), false)
	}
	return nil
}

// sftpError classifies an SFTP failure, reading the pod through the API when the session broke
func (p *Provider) sftpError(ctx context.Context, id string, err error) error {
	if err == nil {
		return nil
	}
	if e, ok := provider.AsError(err); ok {
		return e
	}
	var st *sftp.StatusError
	switch {
	case errors.Is(err, os.ErrNotExist):
		return &provider.Error{Code: provider.CodeNotFound,
			Err: fmt.Errorf("lium pod %s: %w", id, err)}
	case errors.Is(err, os.ErrPermission), errors.Is(err, syscall.EISDIR),
		errors.Is(err, syscall.ENOTDIR):
		return invalid(fmt.Errorf("lium pod %s: %w", id, err), false)
	case errors.As(err, &st):
		return &provider.Error{Code: statusCode(err, st),
			Err: fmt.Errorf("lium pod %s: %w", id, err)}
	}
	return p.sshError(ctx, id, err)
}

// statusCode reads the status in err by its words, where they name ENOSPC or EISDIR
func statusCode(err error, st *sftp.StatusError) provider.Code {
	text := strings.ToLower(err.Error())
	switch {
	case strings.Contains(text, "no space left"), strings.Contains(text, "quota exceeded"):
		return provider.CodeNoSpace
	case strings.Contains(text, "is a directory"), strings.Contains(text, "not a directory"),
		st.FxCode() == sftp.ErrSSHFxBadMessage:
		return provider.CodeInvalid
	}
	return provider.CodeInternal
}

// spaceError reads a write's bare failure as no_space when the pod's disk is about full
func spaceError(cl *sftp.Client, dir string, err error) error {
	var st *sftp.StatusError
	if !errors.As(err, &st) || st.FxCode() != sftp.ErrSSHFxFailure ||
		statusCode(err, st) != provider.CodeInternal {
		return err
	}
	vfs, verr := cl.StatVFS(dir)
	if verr != nil || vfs.FreeSpace() >= lowFreeBytes && (vfs.Files == 0 || vfs.Ffree > 0) {
		return err
	}
	return &provider.Error{Code: provider.CodeNoSpace,
		Err: fmt.Errorf("%w, with %d bytes free", err, vfs.FreeSpace())}
}
