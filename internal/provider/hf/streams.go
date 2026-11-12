package hf

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const (
	// defaultPartBytes is each ranged part of a stream, STARTING VALUE, design 4.4
	defaultPartBytes = 8 << 20
	// partsInFlight is how many parts of one stream go side by side, STARTING VALUE
	partsInFlight = 4
	// bufferTotal bounds the parts reads hold, and writes apart, so no write waits on a read
	bufferTotal = 128 << 20
	// partRetries is how often a part is sent again after a failure worth a retry
	partRetries = 3
	// readStalls is how many reads of a part in a row may move no bytes before it fails
	readStalls       = 3
	defaultRetryWait = time.Second
	// discardTimeout bounds the removal of a temporary file a failed write left
	discardTimeout = 15 * time.Second
)

var _ provider.Provider = (*Provider)(nil)

// retryStatus reports an answer that a part sent again may meet with another outcome
func retryStatus(code int) bool {
	return code == http.StatusRequestTimeout || code == http.StatusServiceUnavailable ||
		code >= http.StatusBadGateway && code <= http.StatusGatewayTimeout
}

// resend sends an idempotent call, again after a failure worth it unless HF shows the Job gone
func (p *Provider) resend(
	ctx context.Context, sb sandbox, build func() (*http.Request, error),
) (*http.Response, error) {
	for attempt := 0; ; attempt++ {
		req, err := build()
		if err != nil {
			return nil, err
		}
		resp, err := p.sbx.client.Do(req)
		if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return resp, nil
		}
		again := err != nil || retryStatus(resp.StatusCode)
		if err != nil {
			err = p.lost(ctx, sb, &provider.Error{Code: provider.CodeInternal,
				Err: fmt.Errorf("hf Job %s sbx-server %s: %w", sb.id, routeName(req), err)})
		} else {
			err = p.answerError(ctx, sb, req, resp)
			drainClose(resp)
		}
		if !again || isCode(err, provider.CodeNotFound) || attempt == partRetries ||
			ctx.Err() != nil {
			return nil, err
		}
		if serr := sleep(ctx, p.retryWait<<attempt); serr != nil {
			return nil, err
		}
	}
}

/* Writes */

// OpenWriter makes the temporary file beside the target, which Commit moves over it
func (p *Provider) OpenWriter(
	ctx context.Context, ref provider.Ref, name string, spec provider.WriteSpec,
) (provider.FileWriter, error) {
	if _, ok := jobOf(ref); !ok {
		return nil, notHF(ref)
	}
	if err := spec.Check(); err != nil {
		return nil, err
	}
	target, err := targetPath(name)
	if err != nil {
		return nil, err
	}
	sb, err := p.sandbox(ctx, ref)
	if err != nil {
		return nil, err
	}
	u := p.newUpload(ctx, sb, target, spec)
	// The temp is made here with its parents, so a target the Job refuses fails at open
	if err := u.put(ctx, []byte{}, 0, false); err != nil {
		u.cancel()
		return nil, err
	}
	return provider.NewFileWriter(ctx, spec, u), nil
}

// upload fills a temporary file in parts side by side, the last sent once the others landed
type upload struct {
	p            *Provider
	sb           sandbox
	ctx          context.Context
	cancel       context.CancelFunc
	target, temp string
	spec         provider.WriteSpec
	// mu is held by Write, finish and Discard, so Discard waits for a Write it ended
	mu sync.Mutex
	// buf is the part being filled, held the pool bytes it took, sent the bytes before it
	buf  []byte
	held int64
	sent int64
	// slots holds one token for each part in flight
	slots chan struct{}
	wg    sync.WaitGroup
	emu   sync.Mutex
	err   error
}

func (p *Provider) newUpload(
	ctx context.Context, sb sandbox, target string, spec provider.WriteSpec,
) *upload {
	uctx, cancel := context.WithCancel(ctx)
	return &upload{
		p: p, sb: sb, ctx: uctx, cancel: cancel, target: target, spec: spec,
		temp:  path.Join(path.Dir(target), provider.TempName(path.Base(target))),
		slots: make(chan struct{}, partsInFlight),
	}
}

// Write fills the part buffer, sending each full part but the last as it fills
func (u *upload) Write(b []byte) (int, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	n := 0
	for len(b) > 0 {
		if err := u.failed(); err != nil {
			return n, err
		}
		if u.buf == nil {
			size := min(u.p.partBytes, u.spec.Size-u.sent)
			if size <= 0 {
				return n, invalid(fmt.Errorf("%d bytes past the size of %s", len(b),
					u.target), false)
			}
			// Waiting here for a free buffer is the back-pressure every Write may apply
			if err := u.p.writeBuffers.Acquire(u.ctx, size); err != nil {
				return n, err
			}
			u.held, u.buf = size, make([]byte, 0, size)
		}
		c := copy(u.buf[len(u.buf):cap(u.buf)], b)
		u.buf, b, n = u.buf[:len(u.buf)+c], b[c:], n+c
		if len(u.buf) == cap(u.buf) && u.sent+int64(len(u.buf)) < u.spec.Size {
			if err := u.dispatch(); err != nil {
				return n, err
			}
		}
	}
	return n, nil
}

// dispatch sends the full buffer as a part of its own once a slot is free
func (u *upload) dispatch() error {
	select {
	case u.slots <- struct{}{}:
	case <-u.ctx.Done():
		return u.ctx.Err()
	}
	buf, held, offset := u.buf, u.held, u.sent
	u.buf, u.held, u.sent = nil, 0, u.sent+int64(len(buf))
	u.wg.Add(1)
	go func() {
		defer u.wg.Done()
		defer func() { <-u.slots }()
		defer u.p.writeBuffers.Release(held)
		if err := u.put(u.ctx, buf, offset, false); err != nil {
			u.fail(err)
		}
	}()
	return nil
}

// put writes data at offset, the last part with the final size and the mode
func (u *upload) put(ctx context.Context, data []byte, offset int64, last bool) error {
	q := url.Values{"path": {u.temp}, "offset": {strconv.FormatInt(offset, 10)}}
	if last {
		q.Set("truncate_to", strconv.FormatInt(u.spec.Size, 10))
		q.Set("mode", strconv.FormatUint(uint64(u.spec.Mode.Perm()), 8))
	}
	resp, err := u.p.resend(ctx, u.sb, func() (*http.Request, error) {
		req, err := u.p.sbx.request(ctx, u.sb, http.MethodPut, "/v1/files/write", q, data)
		if err == nil {
			req.Header.Set("Content-Type", "application/octet-stream")
		}
		return req, err
	})
	if err != nil {
		return err
	}
	drainClose(resp)
	return nil
}

func (u *upload) fail(err error) {
	u.emu.Lock()
	defer u.emu.Unlock()
	if u.err == nil {
		u.err = err
	}
}

func (u *upload) failed() error {
	u.emu.Lock()
	defer u.emu.Unlock()
	return u.err
}

// finish sends the last part once the others landed, checks the sum and moves the temp over
func (u *upload) finish(ctx context.Context, sum []byte) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.wg.Wait()
	if err := u.failed(); err != nil {
		return err
	}
	last := u.buf
	if last == nil {
		last = []byte{}
	}
	err := u.put(ctx, last, u.sent, true)
	u.release()
	if err != nil {
		return err
	}
	if sum != nil {
		got, err := u.p.digest(ctx, u.sb, u.temp)
		if err != nil {
			return err
		}
		if !bytes.Equal(got, sum) {
			return &provider.Error{Code: provider.CodeInternal, Err: fmt.Errorf(
				"hf Job %s holds %s with SHA-256 %x, not %x", u.sb.id, u.temp, got, sum)}
		}
	}
	return u.p.replace(ctx, u.sb, u.temp, u.target)
}

// Finish completes the temporary file, moves it over the target and describes the target
func (u *upload) Finish(ctx context.Context, sum []byte) (provider.FileEntry, error) {
	if sum == nil {
		sum = u.spec.SHA256
	}
	if sum != nil && len(sum) != sha256.Size {
		return provider.FileEntry{}, invalid(fmt.Errorf("a SHA-256 of %d bytes", len(sum)),
			false)
	}
	if err := u.finish(ctx, sum); err != nil {
		return provider.FileEntry{}, err
	}
	return u.p.stat(ctx, u.sb, u.target)
}

// Discard ends the parts in flight and a wait for a buffer, then removes the temporary file
func (u *upload) Discard(ctx context.Context) error {
	u.cancel()
	u.mu.Lock()
	defer u.mu.Unlock()
	u.wg.Wait()
	u.release()
	return u.p.remove(ctx, u.sb, u.temp)
}

// release gives the part buffer back to the pool
func (u *upload) release() {
	if u.held > 0 {
		u.p.writeBuffers.Release(u.held)
	}
	u.held, u.buf = 0, nil
}

/* Reads */

// errReaderClosed ends a Read after Close
var errReaderClosed = errors.New("the file reader was closed")

// OpenReader stats the file, then reads it in ranged parts side by side
func (p *Provider) OpenReader(
	ctx context.Context, ref provider.Ref, name string, offset int64,
) (provider.FileReader, error) {
	if _, ok := jobOf(ref); !ok {
		return nil, notHF(ref)
	}
	clean, err := filePath(name)
	if err != nil {
		return nil, err
	}
	sb, err := p.sandbox(ctx, ref)
	if err != nil {
		return nil, err
	}
	info, err := p.stat(ctx, sb, clean)
	switch {
	case err != nil:
		return nil, err
	case !info.Regular:
		return nil, invalid(fmt.Errorf("%s is not a regular file", clean), false)
	case offset < 0 || offset > info.Size:
		return nil, invalid(fmt.Errorf("offset %d is outside a file of %d bytes", offset,
			info.Size), false)
	case offset == info.Size:
		// A read at the end still meets the read route's own check, which refuses a FIFO
		if _, err := p.readRange(ctx, sb, clean, offset, 1); err != nil {
			return nil, err
		}
		return provider.NewFileReader(ctx, http.NoBody, info, offset, nil), nil
	}
	rctx, cancel := context.WithCancel(ctx)
	r := &rangeReader{
		p: p, sb: sb, name: clean, ctx: rctx, cancel: cancel, size: info.Size, next: offset,
	}
	return provider.NewFileReader(ctx, r, info, offset, nil), nil
}

// readPart is one range of a file, read into data while done is open
type readPart struct {
	offset int64
	data   []byte
	done   chan struct{}
	err    error
}

// rangeReader reads a file from next to size through ranged parts, at most partsInFlight at once
type rangeReader struct {
	p      *Provider
	sb     sandbox
	name   string
	ctx    context.Context
	cancel context.CancelFunc
	size   int64
	// mu is held by Read and by Close's release, so a Close waits for the Read it ended
	mu     sync.Mutex
	next   int64
	parts  []*readPart
	cur    []byte
	held   int64
	wg     sync.WaitGroup
	closed bool
	// err is the first failed part's error, which every later Read returns
	err error
}

func (r *rangeReader) Read(b []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for len(r.cur) == 0 {
		r.releaseCur()
		if r.closed {
			return 0, errReaderClosed
		}
		if r.err != nil {
			return 0, r.err
		}
		if len(r.parts) == 0 && r.next >= r.size {
			return 0, io.EOF
		}
		if err := r.schedule(); err != nil {
			return 0, err
		}
		pt := r.parts[0]
		select {
		case <-pt.done:
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		}
		r.parts = r.parts[1:]
		if pt.err != nil {
			r.p.readBuffers.Release(int64(len(pt.data)))
			r.err = pt.err
			return 0, r.err
		}
		r.cur, r.held = pt.data, int64(len(pt.data))
	}
	n := copy(b, r.cur)
	r.cur = r.cur[n:]
	return n, nil
}

// schedule starts parts up to partsInFlight, waiting for a buffer only when none is in flight
func (r *rangeReader) schedule() error {
	for len(r.parts) < partsInFlight && r.next < r.size {
		n := min(r.p.partBytes, r.size-r.next)
		if len(r.parts) == 0 {
			if err := r.p.readBuffers.Acquire(r.ctx, n); err != nil {
				return err
			}
		} else if !r.p.readBuffers.TryAcquire(n) {
			return nil
		}
		pt := &readPart{offset: r.next, data: make([]byte, n), done: make(chan struct{})}
		r.next += n
		r.parts = append(r.parts, pt)
		r.wg.Add(1)
		go r.fetch(pt)
	}
	return nil
}

// fetch fills a part, reissuing a read that ends early from where it stopped
func (r *rangeReader) fetch(pt *readPart) {
	defer r.wg.Done()
	defer close(pt.done)
	got, stalls := 0, 0
	for got < len(pt.data) {
		n, again, err := r.get(pt.offset+int64(got), pt.data[got:])
		got += n
		if err == nil {
			continue
		}
		if !again || r.ctx.Err() != nil {
			pt.err = err
			return
		}
		if n > 0 {
			stalls = 0
			continue
		}
		if stalls++; stalls >= readStalls {
			pt.err = &provider.Error{Code: provider.CodeInternal, Err: fmt.Errorf(
				"hf read of %s moved no bytes %d times in a row: %w", r.name, readStalls, err)}
			return
		}
		if serr := sleep(r.ctx, r.p.retryWait); serr != nil {
			pt.err = serr
			return
		}
	}
}

// get reads buf's length at offset, again reporting a failure a later read may not meet
func (r *rangeReader) get(offset int64, buf []byte) (int, bool, error) {
	n := int64(len(buf))
	req, err := r.p.sbx.request(r.ctx, r.sb, http.MethodGet, "/v1/files/read",
		readQuery(r.name, offset, n), nil)
	if err != nil {
		return 0, false, err
	}
	resp, err := r.p.sbx.client.Do(req)
	if err != nil {
		lost := r.p.lost(r.ctx, r.sb, &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("hf Job %s read of %s: %w", r.sb.id, r.name, err)})
		return 0, !isCode(lost, provider.CodeNotFound), lost
	}
	defer drainClose(resp)
	if resp.StatusCode != http.StatusOK {
		e := r.p.answerError(r.ctx, r.sb, req, resp)
		return 0, retryStatus(resp.StatusCode) && !isCode(e, provider.CodeNotFound), e
	}
	if resp.ContentLength >= 0 && resp.ContentLength != n {
		// sbx-server clamps a read to the size, so a shorter answer is a file that shrank
		return 0, false, r.changed()
	}
	got, err := io.ReadFull(resp.Body, buf)
	if err != nil {
		return got, true, &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("hf Job %s read of %s ended early: %w", r.sb.id, r.name, err)}
	}
	if resp.ContentLength < 0 {
		if extra, _ := resp.Body.Read(make([]byte, 1)); extra > 0 {
			return got, false, r.changed()
		}
	}
	return got, false, nil
}

func (r *rangeReader) changed() error {
	return &provider.Error{Code: provider.CodeInternal,
		Err: fmt.Errorf("hf file %s changed size while it was read", r.name)}
}

// releaseCur gives back the buffer of the part read through
func (r *rangeReader) releaseCur() {
	if r.held > 0 {
		r.p.readBuffers.Release(r.held)
	}
	r.cur, r.held = nil, 0
}

// Close ends the parts in flight and gives their buffers back
func (r *rangeReader) Close() error {
	r.cancel()
	r.wg.Wait()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	for _, pt := range r.parts {
		r.p.readBuffers.Release(int64(len(pt.data)))
	}
	r.parts = nil
	r.releaseCur()
	return nil
}
