package koyeb

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oynozan/bugnet-router/internal/provider"
)

const (
	// defaultExitDir holds the exit status files of the commands Exec wraps
	defaultExitDir = "/tmp/bugnet/exec"
	exitNameBytes  = 8
	maxExitBytes   = 64
	maxErrorBytes  = 64 << 10
	// maxEventLine fits one 1 MiB log line after JSON escaping, and bounds a whole event too
	maxEventLine = 8 << 20
	// maxListBytes bounds one directory listing reply
	maxListBytes = 16 << 20
	// healthTimeout bounds one health probe as the SDK does, so a hung probe is retried
	healthTimeout = 5 * time.Second
	killTimeout   = 5 * time.Second
)

// safeWord matches the words a shell reads as they are, which need no quotes
var safeWord = regexp.MustCompile(`^[A-Za-z0-9@%+=:,./_-]+$`)

// newExecutorClient speaks HTTP/1.1 only, since HTTP/2 requests cannot wake a sleeping service
func newExecutorClient() *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.ForceAttemptHTTP2 = false
	t.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
	return &http.Client{Transport: t, CheckRedirect: provider.NoRedirect}
}

// target is how one sandbox's executor is reached
type target struct {
	base, secret, routing string
}

// target takes the executor from the ref, asking Koyeb for whatever the ref lacks
func (p *Provider) target(ctx context.Context, ref provider.Ref) (target, error) {
	t := target{
		base: ref.Meta[metaExecutor], secret: ref.Meta[metaSecret], routing: ref.Meta[metaRouting],
	}
	if t.base != "" && t.secret != "" {
		return t, nil
	}
	svc, err := p.service(ctx, ref)
	if err != nil {
		return target{}, err
	}
	dep, err := p.deployment(ctx, svc.LatestDeploymentID)
	if err != nil {
		return target{}, err
	}
	return p.targetOf(ctx, ref, dep)
}

// targetOf builds the executor of a deployment, falling back to the app's domain like the SDK
func (p *Provider) targetOf(
	ctx context.Context, ref provider.Ref, dep deploymentRecord,
) (target, error) {
	t := target{secret: cmp.Or(ref.Meta[metaSecret], envOf(dep)[secretEnv])}
	t.base, t.routing = sandboxURL(dep)
	if t.base == "" {
		app, err := p.api.getApp(ctx, ref.ID)
		if err != nil {
			return target{}, err
		}
		if len(app.Domains) == 0 || app.Domains[0].Name == "" {
			return target{}, internal(fmt.Errorf("koyeb sandbox %s has no public URL yet",
				ref.ID))
		}
		t.base = "https://" + app.Domains[0].Name + executorPath
	}
	return t, nil
}

// request builds an executor call carrying the secret and the routing key
func (t target) request(
	ctx context.Context, method, route string, q url.Values, body io.Reader,
) (*http.Request, error) {
	u := t.base + route
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, internal(err)
	}
	req.Header.Set("Authorization", "Bearer "+t.secret)
	if t.routing != "" {
		req.Header.Set("X-Routing-Key", t.routing)
	}
	return req, nil
}

// do sends an executor call and returns its response when the status is 2xx or among ok
func (p *Provider) do(
	ctx context.Context, ref provider.Ref, req *http.Request, ok ...int,
) (*http.Response, error) {
	resp, err := p.exec.Do(req)
	if err != nil {
		return nil, p.executorDown(ctx, ref, &provider.Error{
			Code: provider.CodeInternal, CanFailOver: true,
			Err: fmt.Errorf("koyeb executor: %w", err),
		})
	}
	if (resp.StatusCode >= 200 && resp.StatusCode < 300) || slices.Contains(ok, resp.StatusCode) {
		return resp, nil
	}
	defer drainClose(resp)
	err = executorError(resp)
	if resp.StatusCode >= http.StatusBadGateway {
		return nil, p.executorDown(ctx, ref, err)
	}
	return nil, err
}

// executorDown reports not_found when Koyeb shows the sandbox of a failed executor is gone
func (p *Provider) executorDown(ctx context.Context, ref provider.Ref, err error) error {
	if _, serr := p.service(ctx, ref); isCode(serr, provider.CodeNotFound) {
		return serr
	}
	return err
}

// executorError classifies an error answer of the executor, whose body is {"error"} or text
func executorError(resp *http.Response) error {
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBytes))
	var body struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(raw, &body)
	msg := cmp.Or(body.Error, strings.TrimSpace(string(raw)), resp.Status)
	e := &provider.Error{
		Code: provider.CodeInternal, CanFailOver: true,
		Err: fmt.Errorf("koyeb executor answered %d: %s", resp.StatusCode, msg),
	}
	switch resp.StatusCode {
	case http.StatusInternalServerError:
		// The executor answers a full disk as a plain 500 holding the OS error's text
		if strings.Contains(msg, "no space left on device") {
			e.Code, e.CanFailOver = provider.CodeNoSpace, false
		}
	case http.StatusBadRequest, http.StatusForbidden, http.StatusConflict:
		// 403 is a file the executor's user may not touch, a wrong secret is 401
		e.Code, e.CanFailOver = provider.CodeInvalid, false
	case http.StatusUnauthorized:
		e.Code = provider.CodeAuth
	case http.StatusNotFound:
		e.Code, e.CanFailOver = provider.CodeNotFound, false
	case http.StatusTooManyRequests:
		e.Code = provider.CodeRateLimited
	}
	return e
}

func drainClose(resp *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxErrorBytes))
	_ = resp.Body.Close()
}

// health asks the executor's health check, which reports status ok once the executor is up
func (p *Provider) health(ctx context.Context, t target) error {
	ctx, cancel := context.WithTimeout(ctx, healthTimeout)
	defer cancel()
	req, err := t.request(ctx, http.MethodGet, "/health", nil, nil)
	if err != nil {
		return err
	}
	resp, err := p.exec.Do(req)
	if err != nil {
		return internal(fmt.Errorf("koyeb executor health: %w", err))
	}
	defer drainClose(resp)
	if resp.StatusCode != http.StatusOK {
		return executorError(resp)
	}
	var body struct {
		Status string `json:"status"`
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBytes))
	if json.Unmarshal(raw, &body) != nil || body.Status != "ok" {
		return internal(fmt.Errorf("koyeb executor health answered %.200s", raw))
	}
	return nil
}

/* Exec */

// Exec runs argv through start_process, as /run and /run_streaming kill it on a disconnect
func (p *Provider) Exec(
	ctx context.Context, ref provider.Ref, spec provider.ExecSpec,
) (provider.ExecStream, error) {
	if _, err := appID(ref); err != nil {
		return nil, err
	}
	cmd, exitPath, err := p.wrap(spec.Argv)
	if err != nil {
		return nil, err
	}
	t, err := p.target(ctx, ref)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(struct {
		Cmd string            `json:"cmd"`
		Cwd string            `json:"cwd,omitempty"`
		Env map[string]string `json:"env,omitempty"`
	}{cmd, spec.Cwd, spec.Env})
	if err != nil {
		return nil, invalid(err)
	}
	req, err := t.request(ctx, http.MethodPost, "/start_process", nil, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.do(ctx, ref, req)
	if err != nil {
		return nil, err
	}
	var started struct {
		ID string `json:"id"`
	}
	err = json.NewDecoder(io.LimitReader(resp.Body, maxErrorBytes)).Decode(&started)
	drainClose(resp)
	if err != nil || started.ID == "" {
		return nil, internal(fmt.Errorf("koyeb start_process reply names no process: %v", err))
	}

	sctx, cancel := context.WithCancel(ctx)
	req, err = t.request(sctx, http.MethodGet, "/process_logs_streaming",
		url.Values{"id": {started.ID}}, nil)
	if err == nil {
		resp, err = p.do(sctx, ref, req)
	}
	if err != nil {
		cancel()
		// The process runs on without anyone reading it, so it is stopped best effort
		go p.kill(ctx, t, started.ID)
		return nil, err
	}
	lines := bufio.NewScanner(resp.Body)
	lines.Buffer(make([]byte, 0, 64<<10), maxEventLine)
	return &execStream{
		p: p, ctx: ctx, ref: ref, t: t, process: started.ID, exitPath: exitPath,
		body: resp.Body, lines: lines, cancel: cancel,
	}, nil
}

// wrap execs argv through a fixed script, the shell around it then saves the exit status
func (p *Provider) wrap(argv []string) (string, string, error) {
	if len(argv) == 0 {
		return "", "", invalid(errors.New("exec needs a command"))
	}
	words := make([]string, len(argv))
	for i, w := range argv {
		if strings.ContainsRune(w, 0) {
			return "", "", invalid(fmt.Errorf("argument %d holds a NUL byte", i))
		}
		words[i] = quote(w)
	}
	name := make([]byte, exitNameBytes)
	_, _ = rand.Read(name)
	exitPath := path.Join(p.exitDir, hex.EncodeToString(name)+".exit")
	// The command replaces the inner shell, so it is the started shell's own child
	cmd := "mkdir -p " + quote(p.exitDir) + ` && { sh -c 'exec "$@"' sh ` +
		strings.Join(words, " ") + "; echo $? > " + quote(exitPath) + "; }"
	return cmd, exitPath, nil
}

// quote makes a word one shell word, leaving plain words as they are
func quote(s string) string {
	if safeWord.MatchString(s) {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// execStream reads one command's stdout from its log stream, one line per log event
type execStream struct {
	p        *Provider
	ctx      context.Context
	ref      provider.Ref
	t        target
	process  string
	exitPath string
	body     io.ReadCloser
	lines    *bufio.Scanner
	cancel   context.CancelFunc
	pending  []byte
	complete atomic.Bool
	done     bool
	err      error
	closed   sync.Once
}

func (s *execStream) Read(p []byte) (int, error) {
	for len(s.pending) == 0 {
		if s.done {
			if s.err != nil {
				return 0, s.err
			}
			return 0, io.EOF
		}
		s.next()
	}
	n := copy(p, s.pending)
	s.pending = s.pending[n:]
	return n, nil
}

// next takes one event, keeping stdout lines and the end and dropping stderr and keepalives
func (s *execStream) next() {
	name, data, err := s.event()
	if err != nil {
		s.finish(&provider.Error{
			Code: provider.CodeInternal, CanFailOver: true,
			Err: fmt.Errorf("koyeb log stream: %w", err),
		})
		return
	}
	switch name {
	case "log":
		var entry struct {
			Stream string `json:"stream"`
			Data   string `json:"data"`
		}
		if err := json.Unmarshal([]byte(data), &entry); err != nil {
			s.finish(internal(fmt.Errorf("koyeb log event %.200q: %w", data, err)))
			return
		}
		if entry.Stream == "stdout" {
			// The executor strips each line's newline, so every line gets one back
			s.pending = append(append(s.pending, entry.Data...), '\n')
		}
	case "complete":
		s.complete.Store(true)
		s.finish(nil)
	case "error":
		var body struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal([]byte(data), &body)
		s.finish(internal(fmt.Errorf("koyeb log stream: %s", cmp.Or(body.Error, data))))
	}
}

// event reads one server-sent event, its data lines joined by newlines
func (s *execStream) event() (string, string, error) {
	var name string
	var data []string
	seen := false
	size := 0
	for s.lines.Scan() {
		line := s.lines.Text()
		// The stream comes from inside the sandbox, so an event of endless lines is refused
		if size += len(line) + 1; size > maxEventLine {
			return "", "", fmt.Errorf("koyeb log event is over %d bytes", maxEventLine)
		}
		if line == "" {
			if seen {
				return name, strings.Join(data, "\n"), nil
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		key, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch key {
		case "event":
			name, seen = value, true
		case "data":
			data, seen = append(data, value), true
		}
	}
	if err := s.lines.Err(); err != nil {
		return "", "", err
	}
	return "", "", io.ErrUnexpectedEOF
}

func (s *execStream) finish(err error) {
	s.done, s.err = true, err
	s.cancel()
	_ = s.body.Close()
}

// Wait drains the stream and reads the exit status the wrapper left
func (s *execStream) Wait() (int, error) {
	for !s.done {
		s.next()
	}
	s.pending = nil
	if s.err != nil {
		return -1, s.err
	}
	data, _, err := s.p.read(s.ctx, s.ref, s.t, s.exitPath, 0, maxExitBytes)
	if isCode(err, provider.CodeNotFound) {
		return -1, unknown(errors.New("koyeb command ended without leaving its exit status"))
	}
	if err != nil {
		return -1, err
	}
	code, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return -1, unknown(fmt.Errorf("koyeb command left exit status %q", data))
	}
	return code, nil
}

// Close stops reading and kills a command that has not finished without waiting for the kill
func (s *execStream) Close() error {
	s.closed.Do(func() {
		s.cancel()
		_ = s.body.Close()
		if !s.complete.Load() {
			go s.p.kill(s.ctx, s.t, s.process)
		}
	})
	return nil
}

// kill ends a started process, best effort since the sandbox may already be gone
func (p *Provider) kill(ctx context.Context, t target, id string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), killTimeout)
	defer cancel()
	body, _ := json.Marshal(map[string]string{"id": id})
	err := func() error {
		req, err := t.request(ctx, http.MethodPost, "/kill_process", nil, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := p.exec.Do(req)
		if err != nil {
			return err
		}
		defer drainClose(resp)
		// 400 is a process that already ended
		if resp.StatusCode >= 300 && resp.StatusCode != http.StatusBadRequest {
			return executorError(resp)
		}
		return nil
	}()
	if err != nil {
		p.log.Warn("koyeb could not kill a command whose stream closed early", "process", id,
			"err", err)
	}
}

/* Files */

// ReadFile asks the executor for the byte range, cutting it from a whole file sent instead
func (p *Provider) ReadFile(
	ctx context.Context, ref provider.Ref, name string, offset int64, length int,
) ([]byte, bool, error) {
	if _, err := appID(ref); err != nil {
		return nil, false, err
	}
	if offset < 0 || length < 0 {
		return nil, false, invalid(fmt.Errorf("read of %d bytes at offset %d", length, offset))
	}
	t, err := p.target(ctx, ref)
	if err != nil {
		return nil, false, err
	}
	return p.read(ctx, ref, t, name, offset, length)
}

func (p *Provider) read(
	ctx context.Context, ref provider.Ref, t target, name string, offset int64, length int,
) ([]byte, bool, error) {
	req, err := t.request(ctx, http.MethodGet, "/files", url.Values{"path": {name}}, nil)
	if err != nil {
		return nil, false, err
	}
	// One byte past the slice tells whether it reaches the end of the file
	last := offset + int64(length)
	req.Header.Set("Range", "bytes="+strconv.FormatInt(offset, 10)+"-"+strconv.FormatInt(last, 10))
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := p.do(ctx, ref, req, http.StatusRequestedRangeNotSatisfiable)
	if err != nil {
		return nil, false, err
	}
	defer drainClose(resp)
	switch resp.StatusCode {
	case http.StatusPartialContent:
		data, err := io.ReadAll(io.LimitReader(resp.Body, int64(length)+1))
		if err != nil {
			return nil, false, internal(err)
		}
		if len(data) > length {
			return data[:length], false, nil
		}
		return data, true, nil
	case http.StatusRequestedRangeNotSatisfiable:
		return nil, true, nil
	}
	return readSlice(resp.Body, offset, length)
}

// readSlice cuts the range from a whole file, as sent for an empty file, reading one byte past it
func readSlice(r io.Reader, offset int64, length int) ([]byte, bool, error) {
	if _, err := io.CopyN(io.Discard, r, offset); errors.Is(err, io.EOF) {
		return nil, true, nil
	} else if err != nil {
		return nil, false, internal(err)
	}
	data, err := io.ReadAll(io.LimitReader(r, int64(length)))
	if err != nil {
		return nil, false, internal(err)
	}
	if len(data) < length {
		return data, true, nil
	}
	_, err = io.ReadFull(r, make([]byte, 1))
	if errors.Is(err, io.EOF) {
		return data, true, nil
	}
	if err != nil {
		return nil, false, internal(err)
	}
	return data, false, nil
}

// WriteFile uploads data with its mode and checksum, the executor makes missing parents
func (p *Provider) WriteFile(
	ctx context.Context, ref provider.Ref, name string, data []byte, mode fs.FileMode,
) error {
	if _, err := appID(ref); err != nil {
		return err
	}
	t, err := p.target(ctx, ref)
	if err != nil {
		return err
	}
	q := url.Values{
		"path": {name}, "mode": {fmt.Sprintf("%04o", mode.Perm())}, "parents": {"1"},
	}
	req, err := t.request(ctx, http.MethodPut, "/files", q, bytes.NewReader(data))
	if err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Content-SHA256", hex.EncodeToString(sum[:]))
	resp, err := p.do(ctx, ref, req)
	if err != nil {
		return err
	}
	drainClose(resp)
	return nil
}

// ListFiles lists the directory one level per call, descending into subdirectories to depth
func (p *Provider) ListFiles(
	ctx context.Context, ref provider.Ref, name string, depth int, into *provider.Listing,
) error {
	if _, err := appID(ref); err != nil {
		return err
	}
	if depth < 1 {
		return invalid(fmt.Errorf("depth %d is below 1", depth))
	}
	t, err := p.target(ctx, ref)
	if err != nil {
		return err
	}
	dirs := []string{name}
	for level := 1; level <= depth && len(dirs) > 0; level++ {
		var next []string
		for _, dir := range dirs {
			if !into.Visit() {
				return nil
			}
			entries, err := p.list(ctx, ref, t, dir, into)
			if err != nil {
				// A subdirectory may vanish or refuse listing while the walk goes on
				if level > 1 && (isCode(err, provider.CodeNotFound) ||
					isCode(err, provider.CodeInvalid)) {
					continue
				}
				return err
			}
			for _, e := range entries {
				if !into.Add(e) {
					return nil
				}
				if e.IsDir {
					next = append(next, e.Path)
				}
			}
		}
		dirs = next
	}
	return nil
}

// list reads one level of a directory, skipping entries the executor could not read
func (p *Provider) list(
	ctx context.Context, ref provider.Ref, t target, dir string, into *provider.Listing,
) ([]provider.FileEntry, error) {
	req, err := t.request(ctx, http.MethodGet, "/files/list", url.Values{"path": {dir}}, nil)
	if err != nil {
		return nil, err
	}
	resp, err := p.do(ctx, ref, req)
	if err != nil {
		return nil, err
	}
	defer drainClose(resp)
	var out []provider.FileEntry
	// The executor runs in the sandbox, so a reply is cut at a size and at the room left
	body := &cappedReader{r: resp.Body, left: maxListBytes}
	dec := json.NewDecoder(body)
	for {
		var e struct {
			Path  string `json:"path"`
			Type  string `json:"type"`
			Size  int64  `json:"size"`
			Mode  string `json:"mode"`
			MTime string `json:"mtime"`
			Error string `json:"error"`
		}
		if err := dec.Decode(&e); err != nil {
			if body.cut {
				into.Truncated = true
				return out, nil
			}
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return nil, internal(fmt.Errorf("koyeb file list of %s: %w", dir, err))
		}
		if e.Error != "" || e.Path == "" {
			continue
		}
		if len(out) >= into.Room() {
			into.Truncated = true
			return out, nil
		}
		out = append(out, fileEntry(dir, e.Path, e.Type, e.Size, e.Mode, e.MTime))
	}
}

// cappedReader ends a body after left bytes and records that it cut the body short
type cappedReader struct {
	r    io.Reader
	left int64
	cut  bool
}

func (c *cappedReader) Read(b []byte) (int, error) {
	if c.left <= 0 {
		c.cut = true
		return 0, io.EOF
	}
	n, err := c.r.Read(b[:min(int64(len(b)), c.left)])
	c.left -= int64(n)
	return n, err
}

func fileEntry(dir, rel, typ string, size int64, mode, mtime string) provider.FileEntry {
	perm, _ := strconv.ParseUint(mode, 8, 32)
	m := fs.FileMode(perm).Perm()
	switch typ {
	case "dir":
		m |= fs.ModeDir
	case "symlink":
		m |= fs.ModeSymlink
	case "other":
		m |= fs.ModeIrregular
	}
	entry := provider.FileEntry{
		Path: path.Join(dir, rel), Size: size, IsDir: m.IsDir(), Mode: m, Regular: typ == "file",
	}
	if t, err := time.Parse(time.RFC3339Nano, mtime); err == nil {
		entry.Modified = t.UTC()
	}
	return entry
}
