package koyeb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oynozan/bugnet-router/internal/provider"
)

const (
	defaultPartBytes   = 8 << 20
	defaultBufferTotal = 256 << 20
	// maxPartBytes is the most one request carries through Koyeb's front, 100 MiB
	maxPartBytes = 100 << 20
	// partRetries is how often a buffered part is sent again after a failure worth a retry
	partRetries = 3
	// readStalls is how many reads in a row may move no bytes before a download fails
	readStalls         = 3
	defaultReissueWait = time.Second
	// streamHeaderTimeout bounds the executor's answer once a request went out whole
	streamHeaderTimeout = 2 * time.Minute
)

var _ provider.Provider = (*Provider)(nil)

// newStreamClient talks HTTP/1.1 only and never asks for compression, so lengths stay exact
func newStreamClient() *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.ForceAttemptHTTP2 = false
	t.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
	t.DisableCompression = true
	t.ResponseHeaderTimeout = streamHeaderTimeout
	return &http.Client{Transport: t, CheckRedirect: provider.NoRedirect}
}

// filePath cleans an absolute sandbox path, refusing a relative one
func filePath(name string) (string, error) {
	clean := path.Clean(name)
	if !path.IsAbs(clean) {
		return "", invalid(fmt.Errorf("path %q is not absolute", name))
	}
	return clean, nil
}

/* Stat */

// listEntry is one line of GET /files/list, an lstat of the entry
type listEntry struct {
	Path  string `json:"path"`
	Type  string `json:"type"`
	Size  int64  `json:"size"`
	Mode  string `json:"mode"`
	MTime string `json:"mtime"`
	Error string `json:"error"`
}

// StatFile finds the path in its directory's listing, since GET /files/stat follows a symlink
func (p *Provider) StatFile(
	ctx context.Context, ref provider.Ref, name string,
) (provider.FileEntry, error) {
	if _, err := appID(ref); err != nil {
		return provider.FileEntry{}, err
	}
	t, err := p.target(ctx, ref)
	if err != nil {
		return provider.FileEntry{}, err
	}
	return p.stat(ctx, ref, t, name)
}

func (p *Provider) stat(
	ctx context.Context, ref provider.Ref, t target, name string,
) (provider.FileEntry, error) {
	clean, err := filePath(name)
	if err != nil {
		return provider.FileEntry{}, err
	}
	if clean == "/" {
		return p.statFollow(ctx, ref, t, clean)
	}
	dir, base := path.Dir(clean), path.Base(clean)
	req, err := t.request(ctx, http.MethodGet, "/files/list", url.Values{"path": {dir}}, nil)
	if err != nil {
		return provider.FileEntry{}, err
	}
	resp, err := p.do(ctx, ref, req)
	if err != nil {
		return provider.FileEntry{}, err
	}
	defer drainClose(resp)
	body := &cappedReader{r: resp.Body, left: maxListBytes}
	dec := json.NewDecoder(body)
	for {
		var e listEntry
		err := dec.Decode(&e)
		switch {
		case err != nil && body.cut:
			// A directory too large to list is stated through the call that follows links
			return p.statFollow(ctx, ref, t, clean)
		case errors.Is(err, io.EOF):
			return provider.FileEntry{}, notFound(fmt.Errorf("koyeb has no %s", clean))
		case err != nil:
			return provider.FileEntry{}, internal(fmt.Errorf("koyeb file list of %s: %w", dir,
				err))
		case e.Path != base:
			continue
		case e.Error != "":
			return provider.FileEntry{}, notFound(fmt.Errorf("koyeb could not read %s: %s",
				clean, e.Error))
		}
		return fileEntry(dir, e.Path, e.Type, e.Size, e.Mode, e.MTime), nil
	}
}

// statFollow asks GET /files/stat, which describes what a final symlink points to
func (p *Provider) statFollow(
	ctx context.Context, ref provider.Ref, t target, name string,
) (provider.FileEntry, error) {
	req, err := t.request(ctx, http.MethodGet, "/files/stat", url.Values{"path": {name}}, nil)
	if err != nil {
		return provider.FileEntry{}, err
	}
	resp, err := p.do(ctx, ref, req)
	if err != nil {
		return provider.FileEntry{}, err
	}
	defer drainClose(resp)
	var e listEntry
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxErrorBytes)).Decode(&e); err != nil {
		return provider.FileEntry{}, internal(fmt.Errorf("koyeb stat of %s: %w", name, err))
	}
	entry := fileEntry(path.Dir(name), path.Base(name), e.Type, e.Size, e.Mode, e.MTime)
	entry.Path = name
	return entry, nil
}

/* Reads */

// OpenReader stats the file, then reads it with ranged GETs reissued when one ends early
func (p *Provider) OpenReader(
	ctx context.Context, ref provider.Ref, name string, offset int64,
) (provider.FileReader, error) {
	if _, err := appID(ref); err != nil {
		return nil, err
	}
	t, err := p.target(ctx, ref)
	if err != nil {
		return nil, err
	}
	info, err := p.stat(ctx, ref, t, name)
	if err != nil {
		return nil, err
	}
	if !info.Regular {
		return nil, invalid(fmt.Errorf("%s is not a regular file", name))
	}
	if offset < 0 || offset > info.Size {
		return nil, invalid(fmt.Errorf("offset %d is outside a file of %d bytes", offset,
			info.Size))
	}
	if offset == info.Size {
		return provider.NewFileReader(ctx, http.NoBody, info, offset, nil), nil
	}
	rctx, cancel := context.WithCancel(ctx)
	b := &rangeBody{
		p: p, ctx: rctx, cancel: cancel, ref: ref, t: t, name: info.Path, size: info.Size,
		pos: offset,
	}
	// The first GET runs here, so a file gone since the stat fails at open
	if err := b.attempt(); err != nil {
		cancel()
		return nil, err
	}
	return provider.NewFileReader(ctx, b, info, offset, nil), nil
}

// rangeBody reads one file through as many ranged GETs as Koyeb's cuts take
type rangeBody struct {
	p      *Provider
	ctx    context.Context
	cancel context.CancelFunc
	ref    provider.Ref
	t      target
	name   string
	size   int64
	pos    int64
	etag   string
	// stalls counts the GETs in a row that moved no bytes, moved whether the current one did
	stalls int
	moved  bool
	mu     sync.Mutex
	resp   *http.Response
}

// attempt issues GETs from pos until one answers, each failure counting as a stall
func (b *rangeBody) attempt() error {
	for {
		if b.stalls > 0 && !sleep(b.ctx, b.p.reissueWait) {
			return b.ctx.Err()
		}
		again, err := b.get()
		if err == nil {
			return nil
		}
		if !again || b.ctx.Err() != nil {
			return err
		}
		if b.stalls++; b.stalls >= readStalls {
			return stalled(b.name, err)
		}
	}
}

func stalled(name string, err error) error {
	return &provider.Error{Code: provider.CodeInternal, Err: fmt.Errorf(
		"koyeb read of %s moved no bytes %d times in a row: %w", name, readStalls, err)}
}

// get sends one ranged GET from pos and reports whether a failure is worth another try
func (b *rangeBody) get() (bool, error) {
	req, err := b.t.request(b.ctx, http.MethodGet, "/files", url.Values{"path": {b.name}}, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("Range", "bytes="+strconv.FormatInt(b.pos, 10)+"-"+
		strconv.FormatInt(b.size-1, 10))
	if b.etag != "" {
		req.Header.Set("If-Range", b.etag)
	}
	resp, err := b.p.streams.Do(req)
	if err != nil {
		return true, internal(fmt.Errorf("koyeb download: %w", err))
	}
	if enc := resp.Header.Get("Content-Encoding"); enc != "" && enc != "identity" {
		drainClose(resp)
		return false, internal(fmt.Errorf("koyeb sent a file in %s encoding", enc))
	}
	switch resp.StatusCode {
	case http.StatusPartialContent:
		start, total, ok := contentRange(resp.Header.Get("Content-Range"))
		if !ok || start != b.pos || total != b.size {
			drainClose(resp)
			return false, b.changed()
		}
	case http.StatusOK:
		// A whole file after If-Range means it changed, before any validator Koyeb skipped Range
		if b.etag != "" || resp.ContentLength != b.size {
			drainClose(resp)
			return false, b.changed()
		}
		if _, err := io.CopyN(io.Discard, resp.Body, b.pos); err != nil {
			_ = resp.Body.Close()
			return true, internal(fmt.Errorf("koyeb download: %w", err))
		}
	case http.StatusRequestedRangeNotSatisfiable:
		drainClose(resp)
		return false, b.changed()
	default:
		err := executorError(resp)
		drainClose(resp)
		return retryStatus(resp.StatusCode) && !isCode(err, provider.CodeNoSpace), err
	}
	if etag := resp.Header.Get("ETag"); b.etag == "" && strings.HasPrefix(etag, `"`) {
		b.etag = etag
	}
	b.mu.Lock()
	b.resp, b.moved = resp, false
	b.mu.Unlock()
	return false, nil
}

func (b *rangeBody) changed() error {
	return internal(fmt.Errorf("koyeb file %s changed while it was read", b.name))
}

// contentRange reads a Content-Range such as bytes 5-9/10 into its start and total
func contentRange(header string) (int64, int64, bool) {
	spec, ok := strings.CutPrefix(header, "bytes ")
	if !ok {
		return 0, 0, false
	}
	span, size, ok := strings.Cut(spec, "/")
	first, _, ok2 := strings.Cut(span, "-")
	start, err := strconv.ParseInt(first, 10, 64)
	total, err2 := strconv.ParseInt(size, 10, 64)
	return start, total, ok && ok2 && err == nil && err2 == nil
}

func (b *rangeBody) Read(p []byte) (int, error) {
	for {
		if err := b.ctx.Err(); err != nil {
			return 0, err
		}
		if b.pos >= b.size {
			return 0, io.EOF
		}
		b.mu.Lock()
		resp := b.resp
		b.mu.Unlock()
		if resp == nil {
			if err := b.attempt(); err != nil {
				return 0, err
			}
			continue
		}
		n, err := resp.Body.Read(p[:min(int64(len(p)), b.size-b.pos)])
		b.pos += int64(n)
		b.moved = b.moved || n > 0
		if err != nil || b.pos >= b.size {
			b.drop(resp)
		}
		if err != nil && b.pos < b.size && b.ctx.Err() == nil {
			// The answer ended early, so the next read reissues from pos
			if b.moved {
				b.stalls = 0
			} else if b.stalls++; b.stalls >= readStalls {
				return n, stalled(b.name, err)
			}
		}
		if n > 0 {
			return n, nil
		}
	}
}

// drop closes resp and forgets it when it is still the current answer
func (b *rangeBody) drop(resp *http.Response) {
	_ = resp.Body.Close()
	b.mu.Lock()
	if b.resp == resp {
		b.resp = nil
	}
	b.mu.Unlock()
}

func (b *rangeBody) Close() error {
	b.cancel()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.resp != nil {
		_ = b.resp.Body.Close()
		b.resp = nil
	}
	return nil
}

// retryStatus reports an answer a request may meet again with another outcome
func retryStatus(code int) bool {
	return code == http.StatusRequestTimeout || code == http.StatusTooManyRequests ||
		code >= http.StatusInternalServerError
}

/* Writes */

// OpenWriter buffers the file in parts, one PUT at Commit when it fits one, a session otherwise
func (p *Provider) OpenWriter(
	ctx context.Context, ref provider.Ref, name string, spec provider.WriteSpec,
) (provider.FileWriter, error) {
	if _, err := appID(ref); err != nil {
		return nil, err
	}
	if err := spec.Check(); err != nil {
		return nil, err
	}
	target, err := filePath(name)
	if err != nil {
		return nil, err
	}
	if base := path.Base(target); base == "/" || base == "." || base == ".." {
		return nil, invalid(fmt.Errorf("path %q names no file", name))
	}
	t, err := p.target(ctx, ref)
	if err != nil {
		return nil, err
	}
	wctx, cancel := context.WithCancel(ctx)
	u := &partUpload{
		p: p, ctx: wctx, cancel: cancel, ref: ref, t: t, path: target, spec: spec,
		part: min(p.partBytes, spec.Size),
	}
	if spec.Size > p.partBytes {
		// The session opens before any byte, so a target Koyeb refuses fails here
		if err := u.open(ctx); err != nil {
			cancel()
			return nil, err
		}
	}
	return provider.NewFileWriter(ctx, spec, u), nil
}

// partUpload holds one part in memory at a time, from a buffer pool every upload shares
type partUpload struct {
	p      *Provider
	ctx    context.Context
	cancel context.CancelFunc
	ref    provider.Ref
	t      target
	path   string
	spec   provider.WriteSpec
	part   int64
	// mu is held by Write, Finish and Discard, so Discard waits for a Write it ended
	mu   sync.Mutex
	id   string
	buf  []byte
	held int64
	sent int64
}

func (u *partUpload) mode() string { return fmt.Sprintf("%04o", u.spec.Mode.Perm()) }

// open makes the parents, then a session whose temp file the executor keeps invisible
func (u *partUpload) open(ctx context.Context) error {
	if err := u.makeDir(ctx, path.Dir(u.path)); err != nil {
		return err
	}
	body := map[string]any{"path": u.path, "size": u.spec.Size, "mode": u.mode()}
	if u.spec.SHA256 != nil {
		body["sha256"] = hex.EncodeToString(u.spec.SHA256)
	}
	var reply struct {
		UploadID    string `json:"upload_id"`
		MaxPartSize int64  `json:"max_part_size"`
	}
	if err := u.call(ctx, http.MethodPost, "/files/uploads", body, &reply); err != nil {
		return err
	}
	if reply.UploadID == "" {
		return internal(fmt.Errorf("koyeb upload session for %s has no id", u.path))
	}
	u.id = reply.UploadID
	if reply.MaxPartSize > 0 {
		u.part = min(u.part, reply.MaxPartSize)
	}
	return nil
}

// makeDir creates dir and its parents through the legacy route, which answers 200 either way
func (u *partUpload) makeDir(ctx context.Context, dir string) error {
	var reply struct {
		Success bool   `json:"success"`
		Error   string `json:"error"`
	}
	err := u.call(ctx, http.MethodPost, "/make_dir", map[string]string{"path": dir}, &reply)
	if err != nil {
		return err
	}
	if reply.Success {
		return nil
	}
	e := &provider.Error{
		Code: provider.CodeInvalid,
		Err:  fmt.Errorf("koyeb could not make %s: %s", dir, reply.Error),
	}
	if strings.Contains(reply.Error, "no space left on device") {
		e.Code = provider.CodeNoSpace
	}
	return e
}

// call sends one JSON executor call and decodes its 2xx answer into reply
func (u *partUpload) call(ctx context.Context, method, route string, body, reply any) error {
	var data io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return invalid(err)
		}
		data = bytes.NewReader(raw)
	}
	req, err := u.t.request(ctx, method, route, nil, data)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := u.p.do(ctx, u.ref, req)
	if err != nil {
		return err
	}
	defer drainClose(resp)
	if reply == nil {
		return nil
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxErrorBytes)).Decode(reply); err != nil {
		return internal(fmt.Errorf("koyeb %s %s reply: %w", method, route, err))
	}
	return nil
}

// Write fills the part buffer, sending each full part of a session before it takes more
func (u *partUpload) Write(p []byte) (int, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.buf == nil {
		// Waiting here for a free buffer is the back-pressure every Write may apply
		if err := u.p.buffers.Acquire(u.ctx, u.part); err != nil {
			return 0, err
		}
		u.held, u.buf = u.part, make([]byte, 0, u.part)
	}
	n := 0
	for len(p) > 0 {
		c := copy(u.buf[len(u.buf):cap(u.buf)], p)
		if c == 0 {
			return n, invalid(fmt.Errorf("%d bytes past the size of %s", len(p), u.path))
		}
		u.buf, p, n = u.buf[:len(u.buf)+c], p[c:], n+c
		if len(u.buf) == cap(u.buf) && u.id != "" {
			if err := u.flush(u.ctx); err != nil {
				return n, err
			}
		}
	}
	return n, nil
}

// flush sends the buffer as the session's part at the bytes sent so far
func (u *partUpload) flush(ctx context.Context) error {
	sum := sha256.Sum256(u.buf)
	q := url.Values{"offset": {strconv.FormatInt(u.sent, 10)}}
	resp, err := u.put(ctx, "/files/uploads/"+url.PathEscape(u.id), q, sum[:])
	if err != nil {
		return err
	}
	drainClose(resp)
	u.sent += int64(len(u.buf))
	u.buf = u.buf[:0]
	return nil
}

// put sends the buffer with its digest, again up to partRetries times after a failure worth it
func (u *partUpload) put(
	ctx context.Context, route string, q url.Values, sum []byte,
) (*http.Response, error) {
	for attempt := 0; ; attempt++ {
		req, err := u.t.request(ctx, http.MethodPut, route, q, bytes.NewReader(u.buf))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("X-Content-SHA256", hex.EncodeToString(sum))
		resp, err := u.p.streams.Do(req)
		if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return resp, nil
		}
		again, status := true, 0
		if err != nil {
			err = &provider.Error{
				Code: provider.CodeInternal, CanFailOver: true,
				Err: fmt.Errorf("koyeb upload: %w", err),
			}
		} else {
			status = resp.StatusCode
			err = executorError(resp)
			drainClose(resp)
			again = retryStatus(status) && !isCode(err, provider.CodeNoSpace)
		}
		if !again || attempt == partRetries || ctx.Err() != nil {
			if status >= http.StatusBadGateway {
				err = u.p.executorDown(ctx, u.ref, err)
			}
			return nil, err
		}
	}
}

// Finish sends a one-part file with the router's digest, or the last part and then complete
func (u *partUpload) Finish(ctx context.Context, sum []byte) (provider.FileEntry, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	defer u.release()
	var reply struct {
		Size int64 `json:"size"`
	}
	if u.id == "" {
		if len(sum) != sha256.Size {
			own := sha256.Sum256(u.buf)
			sum = own[:]
		}
		q := url.Values{"path": {u.path}, "mode": {u.mode()}, "parents": {"1"}}
		resp, err := u.put(ctx, "/files", q, sum)
		if err != nil {
			return provider.FileEntry{}, err
		}
		err = json.NewDecoder(io.LimitReader(resp.Body, maxErrorBytes)).Decode(&reply)
		drainClose(resp)
		if err != nil {
			return provider.FileEntry{}, internal(fmt.Errorf("koyeb upload reply: %w", err))
		}
	} else {
		if len(u.buf) > 0 {
			if err := u.flush(ctx); err != nil {
				return provider.FileEntry{}, err
			}
		}
		// complete is never sent twice, since a lost answer may hide a session already retired
		route := "/files/uploads/" + url.PathEscape(u.id) + "/complete"
		if err := u.call(ctx, http.MethodPost, route, nil, &reply); err != nil {
			return provider.FileEntry{}, err
		}
		u.id = ""
	}
	if reply.Size != u.spec.Size {
		return provider.FileEntry{}, internal(fmt.Errorf("koyeb wrote %d of %d bytes to %s",
			reply.Size, u.spec.Size, u.path))
	}
	return u.p.statFollow(ctx, u.ref, u.t, u.path)
}

// Discard ends a part in flight or a wait for a buffer, then deletes the session
func (u *partUpload) Discard(ctx context.Context) error {
	u.cancel()
	u.mu.Lock()
	defer u.mu.Unlock()
	defer u.release()
	if u.id == "" {
		return nil
	}
	id := u.id
	u.id = ""
	err := u.call(ctx, http.MethodDelete, "/files/uploads/"+url.PathEscape(id), nil, nil)
	if isCode(err, provider.CodeNotFound) {
		return nil
	}
	return err
}

// release gives the part buffer back to the pool
func (u *partUpload) release() {
	if u.held > 0 {
		u.p.buffers.Release(u.held)
	}
	u.held, u.buf = 0, nil
}
