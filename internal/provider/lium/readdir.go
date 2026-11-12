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
	"time"

	"github.com/pkg/sftp"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// SFTP version 3 packets, status codes and attribute flags a listing reads, filexfer-02
const (
	fxpInit    = 1
	fxpVersion = 2
	fxpClose   = 4
	fxpOpendir = 11
	fxpReaddir = 12
	fxpStatus  = 101
	fxpHandle  = 102
	fxpData    = 103
	fxpName    = 104

	fxEOF              = 1
	fxNoSuchFile       = 2
	fxPermissionDenied = 3

	attrSize     = 0x1
	attrUIDGID   = 0x2
	attrPerms    = 0x4
	attrTimes    = 0x8
	attrExtended = 0x80000000

	sftpVersion = 3
	// maxListPacket is the longest packet a listing reads, the bound pkg/sftp's client keeps
	maxListPacket = 256 << 10
	// maxHandleBytes is the longest handle the protocol allows
	maxHandleBytes = 256
	// maxNameBytes is Linux's NAME_MAX, the longest entry name a listing takes
	maxNameBytes = 255
	// maxStatusText bounds the server's words an error keeps
	maxStatusText = 200
	// listBaseBytes and listEntryBytes make a listing's byte cap, 64 KiB and 512 bytes an entry
	listBaseBytes, listEntryBytes = 64 << 10, 512
)

// errListCap ends a listing whose session gave all its byte cap allows
var errListCap = errors.New("the listing reached its byte cap")

// ListFiles walks a directory down to depth on a raw SFTP session of its own under a byte cap
func (p *Provider) ListFiles(
	ctx context.Context, ref provider.Ref, name string, depth int, into *provider.Listing,
) error {
	id, ok := podID(ref)
	if !ok {
		return notLium(ref)
	}
	if depth < 1 {
		return invalid(fmt.Errorf("depth %d is below 1", depth), false)
	}
	clean, err := podPath(name)
	if err != nil {
		return err
	}
	sctx, cancel := p.opContext(ctx)
	defer cancel()
	s, err := p.openSFTP(sctx, ref, false)
	if err != nil {
		return err
	}
	defer s.end()
	stop := s.guard(sctx)
	l := &lister{in: s.in, out: &capReader{r: s.out, left: listCap(into.Room())}}
	err = l.walk(clean, depth, into)
	stopped := stop()
	switch {
	case errors.Is(err, errListCap):
		into.Truncated = true
		return nil
	case err != nil && !stopped:
		return timedOut(sctx, id)
	}
	return p.sftpError(ctx, id, err)
}

// listCap is a listing's byte cap, 64 KiB and 512 bytes for each entry it has room for
func listCap(room int) int64 {
	return listBaseBytes + listEntryBytes*int64(room)
}

// capReader fails once its reader gave left bytes
type capReader struct {
	r    io.Reader
	left int64
}

func (c *capReader) Read(b []byte) (int, error) {
	if c.left <= 0 {
		return 0, errListCap
	}
	if int64(len(b)) > c.left {
		b = b[:c.left]
	}
	n, err := c.r.Read(b)
	c.left -= int64(n)
	return n, err
}

// lister reads directories over a raw SFTP session, never pkg/sftp's ReadDir, which reads to EOF
type lister struct {
	in  io.Writer
	out *capReader
	id  uint32
}

// walk lists root and the directories under it down to depth, skipping ones gone meanwhile
func (l *lister) walk(root string, depth int, into *provider.Listing) error {
	if err := l.init(); err != nil {
		return err
	}
	dirs := []string{root}
	for level := 1; level <= depth && len(dirs) > 0; level++ {
		var next []string
		for _, dir := range dirs {
			if !into.Visit() {
				return nil
			}
			full, err := l.list(dir, func(e provider.FileEntry) bool {
				if !into.Add(e) {
					return false
				}
				if e.IsDir {
					next = append(next, e.Path)
				}
				return true
			})
			switch {
			case full:
				return nil
			case err != nil && level > 1 && (errors.Is(err, os.ErrNotExist) ||
				errors.Is(err, os.ErrPermission)):
				// Commands in the pod may remove or close a directory while the listing runs
			case err != nil:
				return err
			}
		}
		dirs = next
	}
	return nil
}

// init sends SFTP's version and reads the server's
func (l *lister) init() error {
	pkt := binary.BigEndian.AppendUint32([]byte{fxpInit}, sftpVersion)
	if err := l.send(pkt); err != nil {
		return err
	}
	typ, body, err := l.recv()
	if err != nil {
		return err
	}
	if v, _, ok := readUint32(body); typ != fxpVersion || !ok || v != sftpVersion {
		return malformed("an answer to INIT that is not version 3")
	}
	return nil
}

// list reads one directory into add, full once add refused an entry, and closes its handle
func (l *lister) list(dir string, add func(provider.FileEntry) bool) (full bool, err error) {
	typ, body, err := l.call(fxpOpendir, dir)
	if err != nil {
		return false, err
	}
	handle, _, ok := readString(body)
	switch {
	case typ == fxpStatus:
		return false, statusErr(body)
	case typ != fxpHandle || !ok || len(handle) > maxHandleBytes:
		return false, malformed("an answer to OPENDIR that is no handle")
	}
	for {
		typ, body, err := l.call(fxpReaddir, string(handle))
		if err != nil {
			return false, err
		}
		if typ == fxpStatus {
			if err := statusErr(body); !errors.Is(err, io.EOF) {
				return false, err
			}
			break
		}
		if typ != fxpName {
			return false, malformed("an answer to READDIR that is no name list")
		}
		if full, err := readNames(dir, body, add); full || err != nil {
			return full, err
		}
	}
	// The status only frees the handle, which the session's end frees too
	_, _, err = l.call(fxpClose, string(handle))
	return false, err
}

// readNames hands each entry of a NAME answer but the dot entries to add, until add refuses one
func readNames(dir string, body []byte, add func(provider.FileEntry) bool) (bool, error) {
	count, body, ok := readUint32(body)
	if !ok {
		return false, malformed("a name list with no count")
	}
	for range count {
		var name []byte
		if name, body, ok = readString(body); ok {
			_, body, ok = readString(body)
		}
		if !ok {
			return false, malformed("a name list shorter than its count")
		}
		var e provider.FileEntry
		var err error
		if e, body, err = readAttrs(body); err != nil {
			return false, err
		}
		n := string(name)
		switch {
		case n == "." || n == "..":
			continue
		case n == "" || len(n) > maxNameBytes || strings.ContainsAny(n, "/\x00"):
			return false, &provider.Error{Code: provider.CodeInternal, Err: fmt.Errorf(
				"the pod's sftp-server listed a name of %d bytes in %s", len(n), dir)}
		}
		e.Path = path.Join(dir, n)
		if !add(e) {
			return true, nil
		}
	}
	return false, nil
}

// readAttrs reads one ATTRS into an entry without its path, refusing a size past reason
func readAttrs(b []byte) (provider.FileEntry, []byte, error) {
	flags, b, ok := readUint32(b)
	var e provider.FileEntry
	var size uint64
	if ok && flags&attrSize != 0 {
		if ok = len(b) >= 8; ok {
			size, b = binary.BigEndian.Uint64(b), b[8:]
		}
	}
	if ok && flags&attrUIDGID != 0 {
		if ok = len(b) >= 8; ok {
			b = b[8:]
		}
	}
	// With no permissions the entry's type is unknown, which no caller takes as a regular file
	e.Mode = fs.ModeIrregular
	if ok && flags&attrPerms != 0 {
		var perms uint32
		if perms, b, ok = readUint32(b); ok {
			e.Mode = (&sftp.FileStat{Mode: perms}).FileMode()
		}
	}
	if ok && flags&attrTimes != 0 {
		var mtime uint32
		if _, b, ok = readUint32(b); ok {
			if mtime, b, ok = readUint32(b); ok {
				e.Modified = time.Unix(int64(mtime), 0).UTC()
			}
		}
	}
	if ok && flags&attrExtended != 0 {
		var n uint32
		n, b, ok = readUint32(b)
		for i := uint32(0); ok && i < n; i++ {
			if _, b, ok = readString(b); ok {
				_, b, ok = readString(b)
			}
		}
	}
	if !ok {
		return e, nil, malformed("attributes cut short")
	}
	if size > maxFileBytes {
		return e, nil, &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("the pod's sftp-server listed a size of %d", size)}
	}
	e.Size, e.IsDir, e.Regular = int64(size), e.Mode.IsDir(), e.Mode.IsRegular()
	return e, b, nil
}

// call sends a request of typ with one string field and reads its answer past the request id
func (l *lister) call(typ byte, field string) (byte, []byte, error) {
	l.id++
	pkt := binary.BigEndian.AppendUint32([]byte{typ}, l.id)
	pkt = binary.BigEndian.AppendUint32(pkt, uint32(len(field)))
	pkt = append(pkt, field...)
	if err := l.send(pkt); err != nil {
		return 0, nil, err
	}
	rtyp, body, err := l.recv()
	if err != nil {
		return 0, nil, err
	}
	id, body, ok := readUint32(body)
	if !ok || id != l.id {
		return 0, nil, malformed("an answer to another request")
	}
	return rtyp, body, nil
}

// send writes one packet, its length first
func (l *lister) send(pkt []byte) error {
	_, err := l.in.Write(append(binary.BigEndian.AppendUint32(nil, uint32(len(pkt))), pkt...))
	return err
}

// recv reads one packet, never one longer than the bound or than the cap has left
func (l *lister) recv() (byte, []byte, error) {
	var head [4]byte
	if _, err := io.ReadFull(l.out, head[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(head[:])
	switch {
	case n == 0 || n > maxListPacket:
		return 0, nil, malformed(fmt.Sprintf("a packet of %d bytes", n))
	case int64(n) > l.out.left:
		return 0, nil, errListCap
	}
	pkt := make([]byte, n)
	if _, err := io.ReadFull(l.out, pkt); err != nil {
		return 0, nil, err
	}
	return pkt[0], pkt[1:], nil
}

// statusErr reads a STATUS answer as the error pkg/sftp's client would give
func statusErr(body []byte) error {
	code, body, ok := readUint32(body)
	if !ok {
		return malformed("a status with no code")
	}
	msg, _, _ := readString(body)
	switch code {
	case fxEOF:
		return io.EOF
	case fxNoSuchFile:
		return os.ErrNotExist
	case fxPermissionDenied:
		return os.ErrPermission
	}
	text := string(msg[:min(len(msg), maxStatusText)])
	return fmt.Errorf("%w: %q", &sftp.StatusError{Code: code}, text)
}

// malformed is the error of an answer the protocol does not allow
func malformed(what string) error {
	return &provider.Error{Code: provider.CodeInternal,
		Err: errors.New("the pod's sftp-server sent " + what)}
}

func readUint32(b []byte) (uint32, []byte, bool) {
	if len(b) < 4 {
		return 0, b, false
	}
	return binary.BigEndian.Uint32(b), b[4:], true
}

func readString(b []byte) ([]byte, []byte, bool) {
	n, rest, ok := readUint32(b)
	if !ok || uint64(n) > uint64(len(rest)) {
		return nil, b, false
	}
	return rest[:n], rest[n:], true
}
