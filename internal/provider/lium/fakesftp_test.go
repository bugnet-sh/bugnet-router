package lium

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// SFTP packet types the fake reads back from what the adapter sent
const (
	fakeFxpWrite = 6
	fakeFxpMkdir = 14
)

// sftpLog is every byte the adapter sent one SFTP session
type sftpLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *sftpLog) bytes() []byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	return bytes.Clone(l.buf.Bytes())
}

// sftpWire is one SFTP session's channel, logging what the adapter sends and counting the answers
type sftpWire struct {
	ch  ssh.Channel
	in  *sftpLog
	out *atomic.Int64
}

func (w *sftpWire) Read(b []byte) (int, error) {
	n, err := w.ch.Read(b)
	w.in.mu.Lock()
	w.in.buf.Write(b[:n])
	w.in.mu.Unlock()
	return n, err
}

func (w *sftpWire) Write(b []byte) (int, error) {
	n, err := w.ch.Write(b)
	w.out.Add(int64(n))
	return n, err
}

func (w *sftpWire) Close() error { return w.ch.Close() }

// serveSFTP serves one session's SFTP over the host's files, or as the hostile pod, then exits 0
func (s *fakeSSHD) serveSFTP(ch ssh.Channel) {
	in := &sftpLog{}
	s.f.mu.Lock()
	s.f.sftpIn = append(s.f.sftpIn, in)
	s.f.mu.Unlock()
	wire := &sftpWire{ch: ch, in: in, out: &s.sftpOut}
	s.mu.Lock()
	hostile, broken := s.hostile, s.broken
	s.mu.Unlock()
	if broken != "" {
		serveBroken(wire, broken)
	} else if hostile != nil {
		_ = sftp.NewRequestServer(wire, hostile.handlers()).Serve()
	} else if srv, err := sftp.NewServer(wire,
		sftp.WithServerWorkingDirectory(s.f.sh.home)); err == nil {
		_ = srv.Serve()
	}
	_, _ = ch.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{0}))
	_ = ch.Close()
}

// setHostile makes the pod's sftp-server the hostile one, whose stalled calls end at cleanup
func (s *fakeSSHD) setHostile(t *testing.T) {
	h := &hostileFS{release: make(chan struct{})}
	t.Cleanup(func() { close(h.release) })
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hostile = h
}

// setBroken makes the pod's sftp-server cut the answer that kind names short
func (s *fakeSSHD) setBroken(kind string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.broken = kind
}

// Answers a broken sftp-server cuts short, each a field pkg/sftp's client reads unchecked
const (
	// brokenStatus answers every request with a status that ends after its id
	brokenStatus = "status"
	// brokenHandle answers OPEN with a handle longer than its packet
	brokenHandle = "handle"
	// brokenData answers READ with data longer than its packet
	brokenData = "data"
)

// serveBroken speaks SFTP for a regular file of 1 MiB but cuts short the answer kind names
func serveBroken(rw io.ReadWriter, kind string) {
	answer := func(typ byte, fields ...[]byte) bool {
		pkt := []byte{typ}
		for _, f := range fields {
			pkt = append(pkt, f...)
		}
		_, err := rw.Write(append(binary.BigEndian.AppendUint32(nil, uint32(len(pkt))), pkt...))
		return err == nil
	}
	u32 := func(v uint32) []byte { return binary.BigEndian.AppendUint32(nil, v) }
	for {
		var head [4]byte
		if _, err := io.ReadFull(rw, head[:]); err != nil {
			return
		}
		pkt := make([]byte, binary.BigEndian.Uint32(head[:]))
		if _, err := io.ReadFull(rw, pkt); err != nil || len(pkt) < 5 {
			return
		}
		typ, id := pkt[0], pkt[1:5]
		ok := true
		switch {
		case typ == 1:
			ok = answer(2, u32(3))
		case kind == brokenStatus:
			ok = answer(101, id)
		case typ == 7 || typ == 8 || typ == 17:
			// LSTAT, FSTAT and STAT state a regular file of 1 MiB
			ok = answer(105, id, u32(0x5), binary.BigEndian.AppendUint64(nil, 1<<20),
				u32(0o100644))
		case typ == 3 && kind == brokenHandle:
			ok = answer(102, id, u32(1000), []byte("h"))
		case typ == 3:
			ok = answer(102, id, u32(1), []byte("h"))
		case typ == 5:
			ok = answer(103, id, u32(1000), []byte("abc"))
		default:
			ok = answer(101, id, u32(4), u32(0), u32(0))
		}
		if !ok {
			return
		}
	}
}

// sftpSent is every SFTP session's input and each file the adapter wrote, reassembled
func (f *fakeLium) sftpSent() (sessions, files [][]byte) {
	f.mu.Lock()
	logs := slices.Clone(f.sftpIn)
	f.mu.Unlock()
	for _, l := range logs {
		in := l.bytes()
		sessions = append(sessions, in)
		files = append(files, sftpWrites(in)...)
	}
	return sessions, files
}

// sftpMkdirs are the directories the adapter made over SFTP
func (f *fakeLium) sftpMkdirs() []string {
	sessions, _ := f.sftpSent()
	var dirs []string
	for _, in := range sessions {
		sftpPackets(in, func(typ byte, body []byte) {
			if name, _, ok := sshString(body); ok && typ == fakeFxpMkdir {
				dirs = append(dirs, string(name))
			}
		})
	}
	return dirs
}

// sftpWrites puts each handle's SSH_FXP_WRITE data at its offsets, one file a handle
func sftpWrites(in []byte) [][]byte {
	files := map[string][]byte{}
	var order []string
	sftpPackets(in, func(typ byte, body []byte) {
		handle, rest, ok := sshString(body)
		if typ != fakeFxpWrite || !ok || len(rest) < 8 {
			return
		}
		off := binary.BigEndian.Uint64(rest)
		data, _, ok := sshString(rest[8:])
		if !ok {
			return
		}
		h := string(handle)
		if _, seen := files[h]; !seen {
			order = append(order, h)
		}
		buf := files[h]
		if end := int(off) + len(data); end > len(buf) {
			buf = append(buf, make([]byte, end-len(buf))...)
		}
		copy(buf[off:], data)
		files[h] = buf
	})
	out := make([][]byte, 0, len(order))
	for _, h := range order {
		out = append(out, files[h])
	}
	return out
}

// sftpPackets calls each with the type and the body past the request id of every whole packet
func sftpPackets(in []byte, each func(typ byte, body []byte)) {
	for len(in) >= 4 {
		n := int(binary.BigEndian.Uint32(in))
		if n < 1 || n > len(in)-4 {
			return
		}
		pkt := in[4 : 4+n]
		in = in[4+n:]
		if len(pkt) >= 5 {
			each(pkt[0], pkt[5:])
		}
	}
}

// sshString reads one SSH string and returns the bytes after it
func sshString(b []byte) ([]byte, []byte, bool) {
	if len(b) < 4 {
		return nil, nil, false
	}
	n := int(binary.BigEndian.Uint32(b))
	if n > len(b)-4 {
		return nil, nil, false
	}
	return b[4 : 4+n], b[4+n:], true
}

// hostileFS answers as a hostile pod's sftp-server may, each path showing one behaviour
type hostileFS struct {
	// release ends the calls it stalls
	release chan struct{}
}

// Paths the hostile pod answers
const (
	// hostileEndless is a directory of entries without end
	hostileEndless = "/endless"
	// hostileDots is a directory of dot entries without end, which a listing skips
	hostileDots = "/dots"
	// hostileLongName is a directory holding a name past NAME_MAX
	hostileLongName = "/longname"
	// hostileStallDir is a directory whose reads never answer
	hostileStallDir = "/stalldir"
	// hostileHuge is a file of 2^62 bytes
	hostileHuge = "/huge"
	// hostileWrapped is a file whose size is 2^64-1 on the wire
	hostileWrapped = "/wrapped"
	// hostileStall is a file of 10 bytes whose reads never answer
	hostileStall = "/stall"
	// hostileNoSpace is a directory where writes fail as ENOSPC
	hostileNoSpace = "/nospace"
	// hostileFull is a directory on a full disk where writes fail with OpenSSH's bare Failure
	hostileFull = "/full"
	// hostileStallWrite is a directory where writes never answer
	hostileStallWrite = "/stallwrite"
)

func (h *hostileFS) handlers() sftp.Handlers {
	return sftp.Handlers{FileGet: h, FilePut: h, FileCmd: h, FileList: h}
}

// fakeInfo is one entry the hostile pod states
type fakeInfo struct {
	name string
	size int64
	mode fs.FileMode
}

func (i fakeInfo) Name() string       { return i.name }
func (i fakeInfo) Size() int64        { return i.size }
func (i fakeInfo) Mode() fs.FileMode  { return i.mode }
func (i fakeInfo) ModTime() time.Time { return time.Unix(1_700_000_000, 0) }
func (i fakeInfo) IsDir() bool        { return i.mode.IsDir() }
func (i fakeInfo) Sys() any           { return nil }

// info states a hostile path, false for one that does not exist
func (h *hostileFS) info(name string) (fakeInfo, bool) {
	dir := fakeInfo{name: path.Base(name), mode: fs.ModeDir | 0o755}
	switch name {
	case "/", hostileEndless, hostileDots, hostileLongName, hostileStallDir, hostileNoSpace,
		hostileFull, hostileStallWrite:
		return dir, true
	case hostileHuge:
		return fakeInfo{name: path.Base(name), size: 1 << 62, mode: 0o644}, true
	case hostileWrapped:
		return fakeInfo{name: path.Base(name), size: -1, mode: 0o644}, true
	case hostileStall:
		return fakeInfo{name: path.Base(name), size: 10, mode: 0o644}, true
	}
	return fakeInfo{}, false
}

// Filelist states a hostile path, or lists a hostile directory as its path says
func (h *hostileFS) Filelist(r *sftp.Request) (sftp.ListerAt, error) {
	if r.Method != "List" {
		i, ok := h.info(r.Filepath)
		if !ok {
			return nil, os.ErrNotExist
		}
		return listerAt{i}, nil
	}
	switch r.Filepath {
	case hostileEndless:
		return endlessLister(func(n int64) fakeInfo {
			return fakeInfo{name: fmt.Sprintf("f%0199d", n), size: 1, mode: 0o644}
		}), nil
	case hostileDots:
		return endlessLister(func(n int64) fakeInfo {
			return fakeInfo{name: [...]string{".", ".."}[n%2], mode: fs.ModeDir | 0o755}
		}), nil
	case hostileLongName:
		return listerAt{fakeInfo{name: strings.Repeat("x", 300), size: 1, mode: 0o644}}, nil
	case hostileStallDir:
		return stalledLister(h.release), nil
	}
	return nil, os.ErrNotExist
}

// Fileread serves the stalled file, which never answers, and the huge ones as zeros
func (h *hostileFS) Fileread(r *sftp.Request) (io.ReaderAt, error) {
	switch r.Filepath {
	case hostileStall:
		return stalledReader(h.release), nil
	case hostileHuge, hostileWrapped:
		return zeros{}, nil
	}
	return nil, os.ErrNotExist
}

// Filewrite takes files only on the two full disks, where every write fails, and the stalled one
func (h *hostileFS) Filewrite(r *sftp.Request) (io.WriterAt, error) {
	switch path.Dir(r.Filepath) {
	case hostileNoSpace:
		return failWriter{&os.PathError{Op: "write", Path: r.Filepath, Err: syscall.ENOSPC}}, nil
	case hostileFull:
		return failWriter{errors.New("failure")}, nil
	case hostileStallWrite:
		return stalledWriter(h.release), nil
	}
	return nil, os.ErrPermission
}

// Filecmd takes every command, so a write gets as far as its data
func (h *hostileFS) Filecmd(*sftp.Request) error { return nil }

// StatVFS states a full disk under hostileFull and a roomy one elsewhere
func (h *hostileFS) StatVFS(r *sftp.Request) (*sftp.StatVFS, error) {
	free := uint64(1 << 20)
	if r.Filepath == hostileFull || path.Dir(r.Filepath) == hostileFull {
		free = 0
	}
	return &sftp.StatVFS{
		Bsize: 4096, Frsize: 4096, Blocks: 1 << 20, Bfree: free, Bavail: free, Files: 1 << 16,
		Ffree: 1 << 10, Favail: 1 << 10, Namemax: 255,
	}, nil
}

// listerAt lists its entries and ends
type listerAt []fs.FileInfo

func (l listerAt) ListAt(ls []fs.FileInfo, off int64) (int, error) {
	if off >= int64(len(l)) {
		return 0, io.EOF
	}
	n := copy(ls, l[off:])
	if off+int64(n) >= int64(len(l)) {
		return n, io.EOF
	}
	return n, nil
}

// endlessLister fills every listing call and never ends
type endlessLister func(n int64) fakeInfo

func (e endlessLister) ListAt(ls []fs.FileInfo, off int64) (int, error) {
	for i := range ls {
		ls[i] = e(off + int64(i))
	}
	return len(ls), nil
}

// stalledLister answers no listing call until the channel closes
type stalledLister chan struct{}

func (s stalledLister) ListAt([]fs.FileInfo, int64) (int, error) {
	<-s
	return 0, io.EOF
}

// stalledReader answers no read until the channel closes
type stalledReader chan struct{}

func (s stalledReader) ReadAt([]byte, int64) (int, error) {
	<-s
	return 0, io.EOF
}

// zeros reads as an endless run of zero bytes
type zeros struct{}

func (zeros) ReadAt(b []byte, _ int64) (int, error) {
	clear(b)
	return len(b), nil
}

// stalledWriter answers no write until the channel closes
type stalledWriter chan struct{}

func (s stalledWriter) WriteAt(b []byte, _ int64) (int, error) {
	<-s
	return len(b), nil
}

// failWriter fails every write with err
type failWriter struct{ err error }

func (w failWriter) WriteAt([]byte, int64) (int, error) { return 0, w.err }
