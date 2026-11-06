package e2b

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"

	"github.com/oynozan/bugnet-router/internal/provider"
	filesystempb "github.com/oynozan/bugnet-router/internal/provider/e2b/envdpb/filesystem"
	"github.com/oynozan/bugnet-router/internal/provider/e2b/envdpb/filesystem/filesystemconnect"
)

const (
	// streamHeaderTimeout bounds envd's answer once a request went out whole
	streamHeaderTimeout = time.Minute
	dialTimeout         = 30 * time.Second
	tlsTimeout          = 10 * time.Second
	idleTimeout         = 90 * time.Second
)

var _ provider.Provider = (*Provider)(nil)

var (
	errAborted     = errors.New("e2b upload aborted")
	errUploadEnded = &provider.Error{
		Code: provider.CodeInternal, Err: errors.New("e2b answered the upload before its end"),
	}
)

// newStreamClient talks HTTP/1.1 only and never asks for compression, so lengths stay exact
func newStreamClient() *http.Client {
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: dialTimeout}).DialContext,
		TLSHandshakeTimeout:   tlsTimeout,
		ResponseHeaderTimeout: streamHeaderTimeout,
		IdleConnTimeout:       idleTimeout,
		DisableCompression:    true,
		Protocols:             protocols,
	}
	return &http.Client{
		Transport: proxyTransport{base: transport}, CheckRedirect: provider.NoRedirect,
	}
}

func (p *Provider) filesystem(ref provider.Ref) filesystemconnect.FilesystemClient {
	return filesystemconnect.NewFilesystemClient(p.envd, p.envdBase(ref), p.codec...)
}

// asRoot routes a Connect call to the sandbox's envd and runs it as root
func asRoot(h http.Header, ref provider.Ref) {
	envdHeaders(h, ref)
	h.Set("Authorization", rootAuth)
}

// StatFile asks envd's Stat, which flags a final symlink instead of describing its target
func (p *Provider) StatFile(
	ctx context.Context, ref provider.Ref, name string,
) (provider.FileEntry, error) {
	if _, err := sandboxID(ref); err != nil {
		return provider.FileEntry{}, err
	}
	req := connect.NewRequest(&filesystempb.StatRequest{Path: name})
	asRoot(req.Header(), ref)
	resp, err := p.filesystem(ref).Stat(ctx, req)
	if err != nil {
		return provider.FileEntry{}, envdError(err)
	}
	if resp.Msg.GetEntry() == nil {
		return provider.FileEntry{}, &provider.Error{
			Code: provider.CodeInternal, Err: fmt.Errorf("e2b stat of %s named no entry", name),
		}
	}
	return fileEntry(path.Dir(name), resp.Msg.GetEntry()), nil
}

// OpenReader stats the file, then streams it with one ranged GET held to the size it saw
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
		return provider.NewFileReader(ctx, http.NoBody, info, offset, envdError), nil
	}
	req, err := p.filesRequest(ctx, http.MethodGet, ref, name, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("Range", "bytes="+strconv.FormatInt(offset, 10)+"-"+
		strconv.FormatInt(info.Size-1, 10))
	resp, err := p.streams.Do(req)
	if err != nil {
		return nil, envdError(err)
	}
	if err := fromOffset(resp, offset); err != nil {
		drainClose(resp)
		return nil, err
	}
	return provider.NewFileReader(ctx, resp.Body, info, offset, envdError), nil
}

// fromOffset checks the answer to a ranged GET and leaves its body at offset
func fromOffset(resp *http.Response, offset int64) error {
	if enc := resp.Header.Get("Content-Encoding"); enc != "" && enc != "identity" {
		return &provider.Error{
			Code: provider.CodeInternal, Err: fmt.Errorf("e2b sent a file in %s encoding", enc),
		}
	}
	switch resp.StatusCode {
	case http.StatusPartialContent:
		if start, ok := rangeStart(resp.Header.Get("Content-Range")); ok && start != offset {
			return &provider.Error{
				Code: provider.CodeInternal,
				Err:  fmt.Errorf("e2b sent bytes from %d for a read from %d", start, offset),
			}
		}
		return nil
	case http.StatusOK:
		// envd ignored Range, so the whole file comes and the offset is skipped here
		_, err := io.CopyN(io.Discard, resp.Body, offset)
		if errors.Is(err, io.EOF) {
			return &provider.Error{Code: provider.CodeInternal, Err: fmt.Errorf(
				"e2b sent a file shorter than the offset %d: %w", offset, io.ErrUnexpectedEOF)}
		}
		if err != nil {
			return envdError(err)
		}
		return nil
	}
	return filesError(resp)
}

// rangeStart reads the first byte of a Content-Range such as bytes 5-9/10
func rangeStart(header string) (int64, bool) {
	spec, ok := strings.CutPrefix(header, "bytes ")
	if !ok {
		return 0, false
	}
	first, _, ok := strings.Cut(spec, "-")
	if !ok {
		return 0, false
	}
	start, err := strconv.ParseInt(first, 10, 64)
	return start, err == nil
}

// OpenWriter streams one POST into a hidden file beside the target, then Commit moves it there
func (p *Provider) OpenWriter(
	ctx context.Context, ref provider.Ref, name string, spec provider.WriteSpec,
) (provider.FileWriter, error) {
	if _, err := sandboxID(ref); err != nil {
		return nil, err
	}
	if err := spec.Check(); err != nil {
		return nil, err
	}
	target := path.Clean(name)
	base := path.Base(target)
	if base == "/" || base == "." || base == ".." {
		return nil, invalid(fmt.Errorf("path %q names no file", name))
	}
	temp := path.Join(path.Dir(target), provider.TempName(base))

	uctx, cancel := context.WithCancel(ctx)
	pr, pw := io.Pipe()
	var body io.Reader = pr
	if spec.Size == 0 {
		body = http.NoBody
	}
	req, err := p.filesRequest(uctx, http.MethodPost, ref, temp, body)
	if err != nil {
		cancel()
		return nil, err
	}
	req.ContentLength = spec.Size
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Accept-Encoding", "identity")
	u := &upload{
		p: p, ref: ref, target: target, temp: temp, mode: spec.Mode.Perm(), pw: pw,
		cancel: cancel, done: make(chan struct{}),
	}
	go u.send(req, pr)
	return provider.NewFileWriter(ctx, spec, u), nil
}

// upload is one streamed POST to envd, its body fed through a pipe so Write meets back-pressure
type upload struct {
	p      *Provider
	ref    provider.Ref
	target string
	temp   string
	mode   fs.FileMode
	pw     *io.PipeWriter
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

// send runs the POST and closes the pipe with its outcome, so a blocked Write returns
func (u *upload) send(req *http.Request, pr *io.PipeReader) {
	resp, err := u.p.streams.Do(req)
	if err != nil {
		err = envdError(err)
	} else {
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			err = filesError(resp)
		}
		drainClose(resp)
	}
	u.err = err
	_ = pr.CloseWithError(cmp.Or(err, error(errUploadEnded)))
	close(u.done)
}

func (u *upload) Write(p []byte) (int, error) { return u.pw.Write(p) }

// Finish ends the body, then sets the mode on the temp file and moves it over the target
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
	if err := u.p.chmod(ctx, u.ref, u.temp, u.mode); err != nil {
		return provider.FileEntry{}, err
	}
	req := connect.NewRequest(&filesystempb.MoveRequest{Source: u.temp, Destination: u.target})
	asRoot(req.Header(), u.ref)
	resp, err := u.p.filesystem(u.ref).Move(ctx, req)
	if err != nil {
		return provider.FileEntry{}, envdError(err)
	}
	if e := resp.Msg.GetEntry(); e != nil {
		return fileEntry(path.Dir(u.target), e), nil
	}
	return u.p.StatFile(ctx, u.ref, u.target)
}

// Discard ends the POST, waits for it and removes the temp file envd may have made
func (u *upload) Discard(ctx context.Context) error {
	u.cancel()
	_ = u.pw.CloseWithError(errAborted)
	select {
	case <-u.done:
	case <-ctx.Done():
	}
	req := connect.NewRequest(&filesystempb.RemoveRequest{Path: u.temp})
	asRoot(req.Header(), u.ref)
	_, err := u.p.filesystem(u.ref).Remove(ctx, req)
	if err == nil {
		return nil
	}
	err = envdError(err)
	if e, ok := provider.AsError(err); ok && e.Code == provider.CodeNotFound {
		return nil
	}
	return err
}
