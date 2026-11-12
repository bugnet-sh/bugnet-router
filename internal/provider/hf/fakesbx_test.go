package hf

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider/shell"
)

const (
	// fakeToolEnv makes the test binary act as util-linux setsid -w or GNU mv -fT, which macOS lacks
	fakeToolEnv = "HF_FAKE_TOOL"
	// fakeChunk is the most one stdout or stderr event carries, as sbx-server reads
	fakeChunk = 32 << 10
	// fakeListDefault and fakeListMax are sbx-server's list limits
	fakeListDefault = 10000
	fakeListMax     = 50000
)

// sbx-server's routes in dedicated mode, each reached on a Job's own host
const (
	sbxExecRoute   = "POST /v1/exec"
	sbxReadRoute   = "GET /v1/files/read"
	sbxWriteRoute  = "PUT /v1/files/write"
	sbxListRoute   = "GET /v1/files/list"
	sbxStatRoute   = "GET /v1/files/stat"
	sbxDeleteRoute = "DELETE /v1/files/delete"
)

func TestMain(m *testing.M) {
	switch os.Getenv(fakeToolEnv) {
	case "setsid":
		os.Exit(fakeSetsid(os.Args[1:]))
	case "mv":
		os.Exit(fakeMv(os.Args[1:]))
	}
	os.Exit(m.Run())
}

// toolEnv is the environment a fake tool passes on, without the switch that made it one
func toolEnv() []string {
	return slices.DeleteFunc(os.Environ(), func(kv string) bool {
		return strings.HasPrefix(kv, fakeToolEnv+"=")
	})
}

// fakeSetsid starts a session and runs argv in it, as util-linux setsid -w does
func fakeSetsid(args []string) int {
	if len(args) > 0 && args[0] == "-w" {
		args = args[1:]
	}
	if len(args) == 0 {
		return 1
	}
	path, err := exec.LookPath(args[0])
	if err != nil {
		return 127
	}
	if _, err := syscall.Setsid(); err == nil {
		_ = syscall.Exec(path, args, toolEnv())
		return 126
	}
	// A group leader cannot start a session, so setsid forks and waits as -w asks
	cmd := exec.Command(path)
	cmd.Args, cmd.Env = args, toolEnv()
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	err = cmd.Run()
	var ee *exec.ExitError
	switch {
	case err == nil:
		return 0
	case errors.As(err, &ee) && ee.ExitCode() >= 0:
		return ee.ExitCode()
	}
	return 1
}

// fakeMv renames as GNU mv -fT does, refusing any other flags so the adapter's own are checked
func fakeMv(args []string) int {
	if len(args) != 4 || args[0] != "-fT" || args[1] != "--" {
		fmt.Fprintf(os.Stderr, "fake mv takes -fT -- src dst, got %q\n", args)
		return 2
	}
	if err := os.Rename(args[2], args[3]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

// fakeSBX is where every Job's sbx-server runs, the host's own shell and files standing in
type fakeSBX struct {
	// home is the commands' home, execDir the adapter's pid files, bin the tool helpers
	home, execDir, bin string
	// pingEvery is the silence after which an exec stream gets a ping
	pingEvery time.Duration
	// listMax caps a list page below what the adapter asks, sbx-server's maximum when 0
	listMax int
	// groups are the process groups of the commands still running
	groups map[int]bool
	// wrote are the paths every write named
	wrote []string
}

// newFakeSBX makes the tree the fake's commands run in, with the tool helpers
func newFakeSBX(t *testing.T) fakeSBX {
	t.Helper()
	dir := t.TempDir()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	s := fakeSBX{
		home: filepath.Join(dir, "root"), execDir: filepath.Join(dir, "exec"),
		bin: filepath.Join(dir, "bin"), pingEvery: 15 * time.Second, groups: map[int]bool{},
	}
	for _, d := range []string{s.home, s.bin} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, tool := range []string{"setsid", "mv"} {
		script := "#!/bin/sh\n" + fakeToolEnv + "=" + tool + " exec " + shell.Quote(exe) +
			" \"$@\"\n"
		if err := os.WriteFile(filepath.Join(s.bin, tool), []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

// env is what sbx-server's commands start with before the request's own
func (s fakeSBX) env() []string {
	return []string{"PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin", "HOME=" + s.home,
		"LANG=C"}
}

// stopCommands kills every group the fake's commands left, so the server's handlers can end
func (f *fakeHF) stopCommands() {
	f.mu.Lock()
	groups := slices.Collect(func(yield func(int) bool) {
		for g := range f.sbx.groups {
			if !yield(g) {
				return
			}
		}
	})
	dir := f.sbx.execDir
	f.mu.Unlock()
	pids, _ := filepath.Glob(filepath.Join(dir, "*.pid"))
	for _, name := range pids {
		raw, _ := os.ReadFile(name)
		if g, err := strconv.Atoi(strings.TrimSpace(string(raw))); err == nil && g > 1 {
			groups = append(groups, g)
		}
	}
	for _, g := range groups {
		_ = syscall.Kill(-g, syscall.SIGKILL)
	}
}

// sbxRoute serves a Job's sbx-server route once HF's proxy finds the Job running and the token
func (f *fakeHF) sbxRoute(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m := sbxHost.FindStringSubmatch(r.Host)
		var j *fakeJob
		if m != nil {
			j = f.job(m[1])
		}
		f.mu.Lock()
		running := j != nil && j.stage == "RUNNING"
		var token string
		if j != nil {
			token = j.token
		}
		f.mu.Unlock()
		switch {
		case !running:
			// HF's proxy answers for a Job whose port nothing listens on
			w.WriteHeader(http.StatusBadGateway)
			_, _ = io.WriteString(w, "<html>502 Bad Gateway</html>")
		case token == "" || r.Header.Get("X-Sandbox-Token") != token:
			sbxError(w, http.StatusForbidden, "invalid or missing X-Sandbox-Token")
		default:
			h(w, r)
		}
	}
}

func sbxError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

// cutAnswer serves the route, then sends its head and only the first cut bytes of its body
func cutAnswer(w http.ResponseWriter, r *http.Request, h http.HandlerFunc, cut int) {
	rec := httptest.NewRecorder()
	h(rec, r)
	body := rec.Body.Bytes()
	conn, buf, err := w.(http.Hijacker).Hijack()
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()
	_, _ = fmt.Fprintf(buf, "HTTP/1.1 %d %s\r\nContent-Type: %s\r\nContent-Length: %d\r\n\r\n",
		rec.Code, http.StatusText(rec.Code), rec.Header().Get("Content-Type"), len(body))
	_, _ = buf.Write(body[:min(cut, len(body))])
	_ = buf.Flush()
}

// fakeExec is POST /v1/exec's body as sbx-server reads it
type fakeExec struct {
	Cmd        json.RawMessage   `json:"cmd"`
	Shell      *bool             `json:"shell"`
	Env        map[string]string `json:"env"`
	Cwd        string            `json:"cwd"`
	Timeout    *float64          `json:"timeout"`
	Stdin      *string           `json:"stdin"`
	Background bool              `json:"background"`
}

// output is one stdout or stderr read, or a stream's end
type output struct {
	stream string
	data   []byte
	end    bool
}

// exec runs argv on the host in a process group of its own and streams NDJSON as sbx-server does
func (f *fakeHF) exec(w http.ResponseWriter, r *http.Request) {
	var b fakeExec
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&b); err != nil {
		sbxError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	var argv []string
	if err := json.Unmarshal(b.Cmd, &argv); err != nil || len(argv) == 0 || b.Shell == nil ||
		*b.Shell || b.Background || b.Stdin != nil {
		sbxError(w, http.StatusBadRequest, "cmd must be a non-empty array with shell false")
		return
	}
	if b.Timeout == nil || *b.Timeout <= 0 {
		f.t.Errorf("an exec of %q came without a timeout", argv)
		sbxError(w, http.StatusBadRequest, "the fake wants a timeout on every exec")
		return
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	var ping time.Duration
	f.mu.Lock()
	cmd.Env, cmd.Dir, ping = f.sbx.env(), b.Cwd, f.sbx.pingEvery
	f.mu.Unlock()
	for k, v := range b.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()
	if err := cmd.Start(); err != nil {
		sbxError(w, http.StatusInternalServerError, "spawn failed: "+err.Error())
		return
	}
	pid := cmd.Process.Pid
	f.mu.Lock()
	f.sbx.groups[pid] = true
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		delete(f.sbx.groups, pid)
		f.mu.Unlock()
	}()
	f.mu.Lock()
	hold, gate := f.holdExecs > 0, f.execGate
	if hold {
		f.holdExecs--
	}
	f.mu.Unlock()
	if hold {
		select {
		case <-gate:
		case <-r.Context().Done():
		}
	}
	var timedOut atomic.Bool
	timer := time.AfterFunc(time.Duration(*b.Timeout*float64(time.Second)), func() {
		timedOut.Store(true)
		_ = syscall.Kill(-pid, syscall.SIGKILL)
	})
	defer timer.Stop()
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	// The client may go, and the command runs on as sbx-server's does, its output dropped
	alive := true
	send := func(ev map[string]any) {
		line, _ := json.Marshal(ev)
		if !alive {
			return
		}
		if _, err := w.Write(append(line, '\n')); err != nil {
			alive = false
			return
		}
		if err := http.NewResponseController(w).Flush(); err != nil {
			alive = false
		}
	}
	send(map[string]any{"event": "start", "pid": pid})
	events := make(chan output)
	pump := func(name string, rd io.Reader) {
		buf := make([]byte, fakeChunk)
		for {
			n, err := rd.Read(buf)
			if n > 0 {
				events <- output{stream: name, data: slices.Clone(buf[:n])}
			}
			if err != nil {
				events <- output{stream: name, end: true}
				return
			}
		}
	}
	go pump("stdout", stdout)
	go pump("stderr", stderr)
	silence := time.NewTimer(ping)
	defer silence.Stop()
	start := time.Now()
	for open := 2; open > 0; {
		select {
		case ev := <-events:
			if ev.end {
				open--
				continue
			}
			// JSON holds text alone, so bytes that are not UTF-8 are replaced as from_utf8_lossy does
			send(map[string]any{"event": ev.stream, "data": string(ev.data)})
			silence.Reset(ping)
		case <-silence.C:
			send(map[string]any{"event": "ping"})
			silence.Reset(ping)
		}
	}
	_ = cmd.Wait()
	exit := map[string]any{"event": "exit", "exit_code": nil, "signal": nil,
		"timed_out": timedOut.Load(), "duration_ms": time.Since(start).Milliseconds()}
	if ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
		exit["signal"] = int(ws.Signal())
	} else {
		exit["exit_code"] = cmd.ProcessState.ExitCode()
	}
	send(exit)
}

// queryPath is the request's path, which sbx-server takes only absolute
func queryPath(w http.ResponseWriter, r *http.Request) (string, bool) {
	p := r.URL.Query().Get("path")
	if !filepath.IsAbs(p) {
		sbxError(w, http.StatusBadRequest, "path must be absolute")
		return "", false
	}
	return p, true
}

// queryInt reads an optional non-negative number, ok false once it answered a 400
func queryInt(w http.ResponseWriter, r *http.Request, name string) (int64, bool, bool) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return 0, false, true
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < 0 {
		sbxError(w, http.StatusBadRequest, "invalid "+name)
		return 0, false, false
	}
	return n, true, true
}

// fileError answers an OS error as sbx-server does
func fileError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, fs.ErrNotExist):
		sbxError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, syscall.EISDIR):
		sbxError(w, http.StatusConflict, err.Error())
	case errors.Is(err, fs.ErrPermission), errors.Is(err, syscall.ENOTDIR):
		sbxError(w, http.StatusBadRequest, err.Error())
	default:
		sbxError(w, http.StatusInternalServerError, err.Error())
	}
}

// read sends a regular file's bytes from offset, length clamped to its size
func (f *fakeHF) read(w http.ResponseWriter, r *http.Request) {
	name, ok := queryPath(w, r)
	if !ok {
		return
	}
	offset, _, ok := queryInt(w, r, "offset")
	if !ok {
		return
	}
	length, given, ok := queryInt(w, r, "length")
	if !ok {
		return
	}
	info, err := os.Stat(name)
	switch {
	case err != nil:
		fileError(w, err)
		return
	case info.IsDir():
		sbxError(w, http.StatusConflict, "is a directory")
		return
	case !info.Mode().IsRegular():
		sbxError(w, http.StatusBadRequest, "not a regular file")
		return
	}
	file, err := os.Open(name)
	if err != nil {
		fileError(w, err)
		return
	}
	defer func() { _ = file.Close() }()
	left := max(info.Size()-offset, 0)
	if !given || length > left {
		length = left
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		fileError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	w.WriteHeader(http.StatusOK)
	_, _ = io.CopyN(w, file, length)
}

// write puts the body at offset, truncating only without one, then sets truncate_to and mode
func (f *fakeHF) write(w http.ResponseWriter, r *http.Request) {
	name, ok := queryPath(w, r)
	if !ok {
		return
	}
	if r.ContentLength < 0 {
		sbxError(w, http.StatusLengthRequired,
			"chunked request bodies not supported; send Content-Length")
		return
	}
	q := r.URL.Query()
	offset, atOffset, ok := queryInt(w, r, "offset")
	if !ok {
		return
	}
	truncTo, truncate, ok := queryInt(w, r, "truncate_to")
	if !ok {
		return
	}
	var mode uint64
	if raw := q.Get("mode"); raw != "" {
		var err error
		if mode, err = strconv.ParseUint(raw, 8, 32); err != nil || mode > 0o7777 {
			sbxError(w, http.StatusBadRequest, "invalid mode")
			return
		}
	}
	f.mu.Lock()
	f.sbx.wrote = append(f.sbx.wrote, name)
	f.mu.Unlock()
	if q.Get("mkdir") != "false" {
		if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
			fileError(w, err)
			return
		}
	}
	flags := os.O_CREATE | os.O_WRONLY
	if !atOffset {
		flags |= os.O_TRUNC
	}
	file, err := os.OpenFile(name, flags, 0o644)
	if err != nil {
		fileError(w, err)
		return
	}
	defer func() { _ = file.Close() }()
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		fileError(w, err)
		return
	}
	if _, err := io.Copy(file, r.Body); err != nil {
		fileError(w, err)
		return
	}
	if truncate {
		if err := file.Truncate(truncTo); err != nil {
			fileError(w, err)
			return
		}
	}
	if q.Get("mode") != "" {
		if err := file.Chmod(fs.FileMode(mode).Perm()); err != nil {
			fileError(w, err)
			return
		}
	}
	info, err := file.Stat()
	if err != nil {
		fileError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"path": name, "size": info.Size()})
}

// entryOf is one list or stat entry as sbx-server writes it, from symlink metadata
func entryOf(name string, info fs.FileInfo) map[string]any {
	typ := "file"
	switch {
	case info.IsDir():
		typ = "dir"
	case info.Mode()&fs.ModeSymlink != 0:
		typ = "symlink"
	}
	var perm uint32
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		perm = uint32(st.Mode) & 0o7777
	}
	return map[string]any{
		"name": filepath.Base(name), "path": name, "type": typ, "size": info.Size(),
		"mtime_ms": info.ModTime().UnixMilli(), "mode": strconv.FormatUint(uint64(perm), 8),
	}
}

// list answers a directory's entries sorted by name, a page after the name given
func (f *fakeHF) list(w http.ResponseWriter, r *http.Request) {
	name, ok := queryPath(w, r)
	if !ok {
		return
	}
	limit, given, ok := queryInt(w, r, "limit")
	if !ok {
		return
	}
	if !given || limit == 0 {
		limit = fakeListDefault
	}
	limit = min(limit, fakeListMax)
	f.mu.Lock()
	if f.sbx.listMax > 0 {
		limit = min(limit, int64(f.sbx.listMax))
	}
	f.mu.Unlock()
	info, err := os.Stat(name)
	if err != nil {
		fileError(w, err)
		return
	}
	if !info.IsDir() {
		sbxError(w, http.StatusBadRequest, "not a directory")
		return
	}
	dirents, err := os.ReadDir(name)
	if err != nil {
		fileError(w, err)
		return
	}
	after := r.URL.Query().Get("after")
	entries := []map[string]any{}
	var next any
	for _, d := range dirents {
		if after != "" && d.Name() <= after {
			continue
		}
		if int64(len(entries)) == limit {
			next = entries[len(entries)-1]["name"]
			break
		}
		full := filepath.Join(name, d.Name())
		info, err := os.Lstat(full)
		if err != nil {
			continue
		}
		entries = append(entries, entryOf(full, info))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"entries": entries, "truncated": next != nil, "next": next,
	})
}

// stat describes a path from its symlink metadata
func (f *fakeHF) stat(w http.ResponseWriter, r *http.Request) {
	name, ok := queryPath(w, r)
	if !ok {
		return
	}
	info, err := os.Lstat(name)
	if err != nil {
		fileError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, entryOf(name, info))
}

// remove deletes a path, a directory with its contents only when recursive
func (f *fakeHF) remove(w http.ResponseWriter, r *http.Request) {
	name, ok := queryPath(w, r)
	if !ok {
		return
	}
	if _, err := os.Lstat(name); err != nil {
		fileError(w, err)
		return
	}
	var err error
	if r.URL.Query().Get("recursive") == "true" {
		err = os.RemoveAll(name)
	} else {
		err = os.Remove(name)
	}
	if err != nil {
		fileError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"deleted": name})
}

// execLines are the commands every exec the fake got asked for
func (f *fakeHF) execLines() [][]string {
	var out [][]string
	for _, r := range f.requestsTo("POST", "/v1/exec") {
		var b fakeExec
		var argv []string
		if json.Unmarshal(r.body, &b) == nil && json.Unmarshal(b.Cmd, &argv) == nil {
			out = append(out, argv)
		}
	}
	return out
}

// ndjson joins events as an exec answer
func ndjson(events ...string) string {
	var b strings.Builder
	for _, e := range events {
		b.WriteString(e + "\n")
	}
	return b.String()
}
