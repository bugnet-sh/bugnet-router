package provider

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

const (
	// maxNameBytes is the longest directory entry Linux filesystems take
	maxNameBytes = 255
	tempPrefix   = "."
	tempMarker   = ".bugnet-"
	tempSuffix   = ".part"
	tempRandom   = 4
	// abortTimeout bounds the detached cleanup an Abort or a failed Commit runs
	abortTimeout = 15 * time.Second
)

// TempName names the hidden file beside a target called base that a FileWriter fills
func TempName(base string) string {
	room := maxNameBytes - len(tempPrefix) - len(tempMarker) - 2*tempRandom - len(tempSuffix)
	if len(base) > room {
		cut := room
		for cut > 0 && !utf8.RuneStart(base[cut]) {
			cut--
		}
		base = base[:cut]
	}
	b := make([]byte, tempRandom)
	_, _ = rand.Read(b)
	return tempPrefix + base + tempMarker + hex.EncodeToString(b) + tempSuffix
}

// Check refuses a negative size, a mode beyond permission bits and a digest of the wrong length
func (s WriteSpec) Check() error {
	switch {
	case s.Size < 0:
		return &Error{Code: CodeInvalid, Err: fmt.Errorf("file size %d is negative", s.Size)}
	case s.Mode&^fs.ModePerm != 0:
		return &Error{Code: CodeInvalid, Err: fmt.Errorf("file mode %v is not permission bits",
			s.Mode)}
	case s.SHA256 != nil && len(s.SHA256) != 32:
		return &Error{Code: CodeInvalid, Err: fmt.Errorf("a SHA-256 of %d bytes", len(s.SHA256))}
	}
	return nil
}

/* Readers */

type fileReader struct {
	ctx      context.Context
	body     io.ReadCloser
	info     FileEntry
	left     int64
	classify func(error) error
	stop     func() bool
	close    func() error
}

// NewFileReader serves body from offset as a FileReader of info, closing body once ctx ends
func NewFileReader(
	ctx context.Context, body io.ReadCloser, info FileEntry, offset int64,
	classify func(error) error,
) FileReader {
	r := &fileReader{
		ctx: ctx, body: body, info: info, left: max(info.Size-offset, 0), classify: classify,
		close: sync.OnceValue(body.Close),
	}
	r.stop = context.AfterFunc(ctx, func() { _ = r.close() })
	return r
}

func (r *fileReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if r.left == 0 {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	n, err := r.body.Read(p[:min(int64(len(p)), r.left)])
	r.left -= int64(n)
	switch {
	case r.left == 0:
		return n, nil
	case r.ctx.Err() != nil:
		return n, r.ctx.Err()
	// Only a bare io.EOF ends the body, a wrapped one is a failure such as a dropped connection
	case err == io.EOF:
		return n, &Error{Code: CodeInternal, Err: fmt.Errorf(
			"file ended %d bytes before its size of %d: %w", r.left, r.info.Size,
			io.ErrUnexpectedEOF)}
	case err != nil && r.classify != nil:
		return n, r.classify(err)
	}
	return n, err
}

func (r *fileReader) Info() FileEntry { return r.info }

func (r *fileReader) Close() error {
	r.stop()
	return r.close()
}

/* Writers */

// WriteTarget is the provider half of a FileWriter, which NewFileWriter drives
type WriteTarget interface {
	// Write passes p on toward the temporary file, and Discard may run while it blocks
	Write(p []byte) (int, error)
	// Finish completes the temporary file with every byte and moves it over the path
	Finish(ctx context.Context, sum []byte) (FileEntry, error)
	// Discard stops a Write in progress and removes the temporary file
	Discard(ctx context.Context) error
}

const (
	writerOpen int32 = iota
	writerCommitting
	writerDone
)

var errWriterDone = errors.New("the file writer was already committed or aborted")

type fileWriter struct {
	ctx     context.Context
	spec    WriteSpec
	target  WriteTarget
	written int64
	state   atomic.Int32
	stop    func() bool
}

// NewFileWriter holds target to the FileWriter contract, an end of ctx aborting it
func NewFileWriter(ctx context.Context, spec WriteSpec, target WriteTarget) FileWriter {
	w := &fileWriter{ctx: ctx, spec: spec, target: target}
	// The callback never reads w.stop, which a context already ended may run before it is set
	w.stop = context.AfterFunc(ctx, func() { _ = w.end() })
	return w
}

func (w *fileWriter) Write(p []byte) (int, error) {
	if err := w.closed(); err != nil {
		return 0, err
	}
	if int64(len(p)) > w.spec.Size-w.written {
		return 0, &Error{Code: CodeInvalid, Err: fmt.Errorf(
			"%d more bytes after %d pass the size of %d", len(p), w.written, w.spec.Size)}
	}
	if len(p) == 0 {
		return 0, nil
	}
	n, err := w.target.Write(p)
	w.written += int64(n)
	if err != nil {
		if cerr := w.closed(); cerr != nil {
			return n, cerr
		}
		return n, err
	}
	return n, nil
}

// closed reports why the writer takes no more bytes, the context's end before an abort
func (w *fileWriter) closed() error {
	if err := w.ctx.Err(); err != nil {
		return err
	}
	if w.state.Load() != writerOpen {
		return errWriterDone
	}
	return nil
}

func (w *fileWriter) Commit(ctx context.Context, sum []byte) (FileEntry, error) {
	if !w.state.CompareAndSwap(writerOpen, writerCommitting) {
		return FileEntry{}, w.closed()
	}
	w.stop()
	defer w.state.Store(writerDone)
	if err := w.ctx.Err(); err != nil {
		_ = w.discard()
		return FileEntry{}, err
	}
	if w.written != w.spec.Size {
		_ = w.discard()
		return FileEntry{}, &Error{Code: CodeInvalid, Err: fmt.Errorf(
			"wrote %d of %d bytes", w.written, w.spec.Size)}
	}
	entry, err := w.target.Finish(ctx, sum)
	if err != nil {
		_ = w.discard()
		return FileEntry{}, err
	}
	return entry, nil
}

func (w *fileWriter) Abort() error {
	w.stop()
	return w.end()
}

// end discards what was written unless the writer already committed or ended
func (w *fileWriter) end() error {
	if !w.state.CompareAndSwap(writerOpen, writerDone) {
		return nil
	}
	return w.discard()
}

func (w *fileWriter) discard() error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(w.ctx), abortTimeout)
	defer cancel()
	return w.target.Discard(ctx)
}
