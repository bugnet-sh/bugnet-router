package boat

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/provider/boat/api"
)

const (
	// streamTTLSeconds keeps a stream's signed URL valid past any transfer, Boat's maximum
	streamTTLSeconds = 3600
	// streamHeaderTimeout bounds the sandbox's answer once a request went out whole
	streamHeaderTimeout = time.Minute
	streamDialTimeout   = 30 * time.Second
	streamTLSTimeout    = 10 * time.Second
	streamIdleTimeout   = 90 * time.Second
	// kindScript prints what its argument is without following a final symlink
	kindScript = `if [ -L "$1" ]; then echo link; elif [ -f "$1" ]; then echo file; ` +
		`elif [ -d "$1" ]; then echo dir; elif [ -e "$1" ]; then echo other; ` +
		`else echo missing; fi`
	maxKindBytes = 64
)

var _ provider.Provider = (*Provider)(nil)

var (
	errAborted     = errors.New("boat upload aborted")
	errUploadEnded = internal(errors.New("boat answered the upload before its end"))
)

// newStreamClient talks HTTP/1.1 only and never asks for compression, so lengths stay exact
func newStreamClient() *http.Client {
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	return &http.Client{
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           (&net.Dialer{Timeout: streamDialTimeout}).DialContext,
			TLSHandshakeTimeout:   streamTLSTimeout,
			ResponseHeaderTimeout: streamHeaderTimeout,
			IdleConnTimeout:       streamIdleTimeout,
			DisableCompression:    true,
			Protocols:             protocols,
		},
		CheckRedirect: provider.NoRedirect,
	}
}

// streamPath cleans a path under /home/user or /tmp, the two themselves only when roots is set
func streamPath(name string, roots bool) (string, error) {
	clean := path.Clean(name)
	under := strings.HasPrefix(clean, "/home/user/") || strings.HasPrefix(clean, "/tmp/")
	if under || (roots && (clean == "/home/user" || clean == "/tmp")) {
		return clean, nil
	}
	return "", invalid(fmt.Errorf(
		"boat's file API reaches only paths under /home/user and /tmp, not %q", name))
}

/* Stat */

// StatFile reads Boat's file info, then asks the shell for the type info does not carry
func (p *Provider) StatFile(
	ctx context.Context, ref provider.Ref, name string,
) (provider.FileEntry, error) {
	id, err := sandboxID(ref)
	if err != nil {
		return provider.FileEntry{}, err
	}
	clean, err := streamPath(name, true)
	if err != nil {
		return provider.FileEntry{}, err
	}
	entry, err := p.statInfo(ctx, id, clean)
	if err != nil || entry.IsDir || entry.Mode&fs.ModeSymlink != 0 {
		return entry, err
	}
	// Boat's info has no file type, so a FIFO or a device would read as a file without this
	kind, err := p.kind(ctx, ref, clean)
	if err != nil {
		return provider.FileEntry{}, err
	}
	switch kind {
	case "file":
		entry.Regular = true
	case "link":
		entry.Mode |= fs.ModeSymlink
	case "dir":
		entry.Mode, entry.IsDir = entry.Mode|fs.ModeDir, true
	case "other":
		entry.Mode |= fs.ModeIrregular
	case "missing":
		return provider.FileEntry{}, notFound(fmt.Errorf("boat has no %s", clean))
	default:
		return provider.FileEntry{}, internal(fmt.Errorf("boat typed %s as %q", clean, kind))
	}
	return entry, nil
}

// statInfo reads GET /files/info into an entry that is not yet known to be regular
func (p *Provider) statInfo(ctx context.Context, id, name string) (provider.FileEntry, error) {
	params := &api.GetFileInfoParams{Path: name}
	raw, err := p.send(ctx, noEffect, func(ctx context.Context) (*http.Response, error) {
		return p.api.GetFileInfo(ctx, id, params)
	})
	if err != nil {
		return provider.FileEntry{}, err
	}
	var item fileItem
	if err := decode(raw, &item); err != nil {
		return provider.FileEntry{}, internal(fmt.Errorf("boat file info of %s: %w", name, err))
	}
	entry := fileEntry(path.Dir(name), item)
	entry.Path = name
	return entry, nil
}

// kind runs a fixed test in the sandbox that names the path's type
func (p *Provider) kind(ctx context.Context, ref provider.Ref, name string) (string, error) {
	stream, err := p.Exec(ctx, ref, provider.ExecSpec{
		Argv: []string{"sh", "-c", kindScript, "sh", name},
	})
	if err != nil {
		return "", err
	}
	defer func() { _ = stream.Close() }()
	out, err := io.ReadAll(io.LimitReader(stream, maxKindBytes))
	if err != nil {
		return "", err
	}
	code, err := stream.Wait()
	if err != nil {
		return "", err
	}
	if code != 0 {
		return "", internal(fmt.Errorf("boat type check of %s exited %d", name, code))
	}
	return strings.TrimSpace(string(out)), nil
}

/* Signed URLs */

// signedURL asks for a URL the sandbox serves for one path, which must never reach a log
func (p *Provider) signedURL(ctx context.Context, id, name string, upload bool) (string, error) {
	req := api.FileUrlRequest{Path: name, TtlSeconds: new(streamTTLSeconds)}
	raw, err := p.send(ctx, noEffect, func(ctx context.Context) (*http.Response, error) {
		if upload {
			return p.api.CreateUploadUrl(ctx, id, req)
		}
		return p.api.CreateDownloadUrl(ctx, id, req)
	})
	if err != nil {
		return "", err
	}
	var grant uploadReply
	if err := decode(raw, &grant); err != nil || grant.URL == "" {
		return "", internal(fmt.Errorf("boat signed URL reply for %s holds no URL", name))
	}
	if u, err := url.Parse(grant.URL); err != nil || (u.Scheme != "https" && u.Scheme != "http") {
		return "", internal(fmt.Errorf("boat signed URL for %s does not parse", name))
	}
	return grant.URL, nil
}

/* Reads */

// OpenReader stats the file, then streams one ranged GET of a signed download URL
func (p *Provider) OpenReader(
	ctx context.Context, ref provider.Ref, name string, offset int64,
) (provider.FileReader, error) {
	info, err := p.StatFile(ctx, ref, name)
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
	link, err := p.signedURL(ctx, ref.ID, info.Path, false)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, link, nil)
	if err != nil {
		return nil, internal(fmt.Errorf("boat download URL for %s does not parse", name))
	}
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("Range", "bytes="+strconv.FormatInt(offset, 10)+"-"+
		strconv.FormatInt(info.Size-1, 10))
	resp, err := p.streams.Do(req)
	if err != nil {
		return nil, internal(fmt.Errorf("boat download of %s: %w", name, redact(err)))
	}
	if err := fromOffset(resp, offset, info.Size); err != nil {
		_ = resp.Body.Close()
		return nil, err
	}
	return provider.NewFileReader(ctx, resp.Body, info, offset, bodyError), nil
}

// bodyError classifies a failed read of a signed URL's body without the URL
func bodyError(err error) error {
	return internal(fmt.Errorf("boat download: %w", redact(err)))
}

// fromOffset checks the answer to a ranged GET and leaves its body at offset
func fromOffset(resp *http.Response, offset, size int64) error {
	if enc := resp.Header.Get("Content-Encoding"); enc != "" && enc != "identity" {
		return internal(fmt.Errorf("boat sent a file in %s encoding", enc))
	}
	switch resp.StatusCode {
	case http.StatusPartialContent:
		start, total, ok := contentRange(resp.Header.Get("Content-Range"))
		if !ok || start != offset || (total >= 0 && total != size) {
			return internal(fmt.Errorf("boat sent %q for a read of %d bytes from %d",
				resp.Header.Get("Content-Range"), size, offset))
		}
		return nil
	case http.StatusOK:
		// Boat's signed URLs need not take Range, so the offset is skipped here
		_, err := io.CopyN(io.Discard, resp.Body, offset)
		if errors.Is(err, io.EOF) {
			return internal(fmt.Errorf("boat sent a file shorter than the offset %d: %w",
				offset, io.ErrUnexpectedEOF))
		}
		if err != nil {
			return bodyError(err)
		}
		return nil
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBytes))
	return apiError(resp.StatusCode, resp.Header, raw, noEffect)
}

// contentRange reads a Content-Range such as bytes 5-9/10, a total of * as -1
func contentRange(header string) (int64, int64, bool) {
	spec, ok := strings.CutPrefix(header, "bytes ")
	if !ok {
		return 0, 0, false
	}
	span, size, ok := strings.Cut(spec, "/")
	first, _, ok2 := strings.Cut(span, "-")
	start, err := strconv.ParseInt(first, 10, 64)
	if !ok || !ok2 || err != nil {
		return 0, 0, false
	}
	if size == "*" {
		return start, -1, true
	}
	total, err := strconv.ParseInt(size, 10, 64)
	return start, total, err == nil
}

/* Writes */

// OpenWriter PUTs a hidden file beside the target through a signed URL, which Commit moves over it
func (p *Provider) OpenWriter(
	ctx context.Context, ref provider.Ref, name string, spec provider.WriteSpec,
) (provider.FileWriter, error) {
	id, err := sandboxID(ref)
	if err != nil {
		return nil, err
	}
	if err := spec.Check(); err != nil {
		return nil, err
	}
	target, err := streamPath(name, false)
	if err != nil {
		return nil, err
	}
	temp := path.Join(path.Dir(target), provider.TempName(path.Base(target)))
	link, err := p.signedURL(ctx, id, temp, true)
	if err != nil {
		return nil, err
	}
	uctx, cancel := context.WithCancel(ctx)
	pr, pw := io.Pipe()
	var body io.Reader = pr
	if spec.Size == 0 {
		body = http.NoBody
	}
	req, err := http.NewRequestWithContext(uctx, http.MethodPut, link, body)
	if err != nil {
		cancel()
		return nil, internal(fmt.Errorf("boat upload URL for %s does not parse", name))
	}
	req.ContentLength = spec.Size
	req.Header.Set("Content-Type", "application/octet-stream")
	u := &upload{
		p: p, id: id, target: target, temp: temp, mode: spec.Mode.Perm(), pw: pw,
		cancel: cancel, done: make(chan struct{}),
	}
	go u.send(req, pr)
	return provider.NewFileWriter(ctx, spec, u), nil
}

// upload is one streamed PUT, its body fed through a pipe so Write meets back-pressure
type upload struct {
	p      *Provider
	id     string
	target string
	temp   string
	mode   fs.FileMode
	pw     *io.PipeWriter
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

// send runs the PUT and closes the pipe with its outcome, so a blocked Write returns
func (u *upload) send(req *http.Request, pr *io.PipeReader) {
	resp, err := u.p.streams.Do(req)
	if err != nil {
		err = internal(fmt.Errorf("boat upload of %s: %w", u.target, redact(err)))
	} else {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBytes))
		_ = resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			err = apiError(resp.StatusCode, resp.Header, raw, noEffect)
		}
	}
	u.err = err
	_ = pr.CloseWithError(cmp.Or(err, errUploadEnded))
	close(u.done)
}

func (u *upload) Write(p []byte) (int, error) { return u.pw.Write(p) }

// Finish ends the body, sets the temp file's mode and moves it over the target
func (u *upload) Finish(ctx context.Context, _ []byte) (provider.FileEntry, error) {
	_ = u.pw.Close()
	select {
	case <-u.done:
	case <-ctx.Done():
		return provider.FileEntry{}, ctx.Err()
	}
	u.cancel()
	if u.err != nil {
		return provider.FileEntry{}, u.err
	}
	perm := api.FilePermissionsRequest{Path: u.temp, Mode: fmt.Sprintf("%03o", u.mode)}
	_, err := u.p.send(ctx, noEffect, func(ctx context.Context) (*http.Response, error) {
		return u.p.api.SetFilePermissions(ctx, u.id, perm)
	})
	if err != nil {
		return provider.FileEntry{}, err
	}
	move := api.FileMoveRequest{Source: u.temp, Destination: u.target, Overwrite: new(true)}
	_, err = u.p.send(ctx, noEffect, func(ctx context.Context) (*http.Response, error) {
		return u.p.api.MoveFile(ctx, u.id, move)
	})
	if err != nil {
		return provider.FileEntry{}, err
	}
	entry, err := u.p.statInfo(ctx, u.id, u.target)
	if err != nil {
		return provider.FileEntry{}, err
	}
	// The move put the uploaded file there, so only a link or directory in its place is odd
	entry.Regular = !entry.IsDir && entry.Mode&fs.ModeSymlink == 0
	return entry, nil
}

// Discard ends the PUT, waits for it and removes the temp file Boat may have made
func (u *upload) Discard(ctx context.Context) error {
	u.cancel()
	_ = u.pw.CloseWithError(errAborted)
	select {
	case <-u.done:
	case <-ctx.Done():
	}
	params := &api.DeleteFileParams{Path: u.temp}
	_, err := u.p.send(ctx, noEffect, func(ctx context.Context) (*http.Response, error) {
		return u.p.api.DeleteFile(ctx, u.id, params)
	})
	if err == nil || isCode(err, provider.CodeNotFound) {
		return nil
	}
	return err
}
