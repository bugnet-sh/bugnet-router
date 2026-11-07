package e2b

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/provider/e2b/api"
	filesystempb "github.com/oynozan/bugnet-router/internal/provider/e2b/envdpb/filesystem"
	"github.com/oynozan/bugnet-router/internal/provider/e2b/envdpb/filesystem/filesystemconnect"
	processpb "github.com/oynozan/bugnet-router/internal/provider/e2b/envdpb/process"
	"github.com/oynozan/bugnet-router/internal/provider/e2b/envdpb/process/processconnect"
)

const (
	fakeKey    = "e2b_fake00000000000000000000000000000000"
	fakeDomain = "fake.e2b.test"
	fakeEnvd   = "0.2.0"
)

// fakeTemplates are the templates the fake account has built
var fakeTemplates = []Template{
	{Name: "4x8", TemplateID: "bugnet-4x8:v1", VCPU: 4, MemoryMiB: 8192, DiskGiB: 10},
	{Name: "2x2", TemplateID: "bugnet-2x2", VCPU: 2, MemoryMiB: 2048, DiskGiB: 10},
}

var fakeIdemKey = regexp.MustCompile(`^[a-z0-9-]{1,255}$`)

// fault answers matching REST calls with an error, after serving them when after is set
type fault struct {
	status    int
	errorCode string
	message   string
	header    map[string]string
	drop      bool
	after     bool
	times     int
}

type fakeSandbox struct {
	id, templateID, token string
	metadata              map[string]string
	started, endAt        time.Time
	state                 string
	envdDown              int
}

type createCall struct {
	header http.Header
	body   map[string]any
}

type startCall struct {
	header http.Header
	msg    *processpb.StartRequest
}

type idemEntry struct {
	request, reply []byte
}

// filesCall is one request to envd's /files endpoint as the fake saw it
type filesCall struct {
	method, path, encoding string
	protoMajor             int
}

// fakeE2B serves E2B's REST API and proxied envd from memory and the host, for trusted tests
type fakeE2B struct {
	t    *testing.T
	rest *httptest.Server
	envd *httptest.Server

	// noRange makes envd ignore Range headers like a server without range support
	noRange bool
	// noContentRange drops Content-Range from partial answers like a proxy that strips it
	noContentRange bool
	// wrongRangeStart answers a ranged GET with 206 and the file from byte 0
	wrongRangeStart bool
	pageSize        int
	// noSpace answers every upload with envd's 507 once its body arrived
	noSpace atomic.Bool
	// shortBody declares a whole file on download and sends half of it
	shortBody atomic.Bool
	// gzipBody labels downloads gzip encoded like a proxy that ignores Accept-Encoding
	gzipBody atomic.Bool

	mu       sync.Mutex
	boxes    map[string]*fakeSandbox
	nextID   int
	idem     map[string]*idemEntry
	faults   map[string]*fault
	calls    map[string]int
	keys     []string
	creates  []createCall
	starts   []startCall
	uploads  []url.Values
	ranges   []string
	files    []filesCall
	moves    [][2]string
	scripted bool
	script   []*processpb.ProcessEvent
	cleanup  []string
	running  atomic.Int64
	// listLimited makes envd refuse every listing as rate limited
	listLimited atomic.Bool
}

func newFake(t *testing.T) *fakeE2B {
	t.Helper()
	f := &fakeE2B{
		t: t, boxes: map[string]*fakeSandbox{}, idem: map[string]*idemEntry{},
		faults: map[string]*fault{}, calls: map[string]int{},
	}
	rest := http.NewServeMux()
	for pattern, h := range map[string]http.HandlerFunc{
		"POST /v2/sandboxes":           f.create,
		"GET /v2/sandboxes":            f.list,
		"GET /sandboxes/{id}":          f.get,
		"DELETE /sandboxes/{id}":       f.kill,
		"POST /sandboxes/{id}/timeout": f.timeout,
	} {
		rest.HandleFunc(pattern, f.gate(pattern, h))
	}
	f.rest = httptest.NewServer(rest)

	envd := http.NewServeMux()
	envd.Handle(processconnect.NewProcessHandler(fakeProcess{f: f}))
	envd.Handle(filesystemconnect.NewFilesystemHandler(fakeFilesystem{f: f}))
	envd.HandleFunc("GET /files", f.download)
	envd.HandleFunc("POST /files", f.upload)
	envd.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	f.envd = httptest.NewServer(f.proxy(envd))

	t.Cleanup(func() {
		f.rest.Close()
		f.envd.Close()
		f.mu.Lock()
		defer f.mu.Unlock()
		for _, p := range f.cleanup {
			_ = os.RemoveAll(p)
		}
	})
	return f
}

// provider builds an adapter that calls the fake's REST and envd servers
func (f *fakeE2B) provider(t *testing.T, key string) *Provider {
	t.Helper()
	p, err := New(Options{
		APIKey: key, Templates: fakeTemplates, BaseURL: f.rest.URL, EnvdURL: f.envd.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	p.pollInterval = 10 * time.Millisecond
	p.retryBase = time.Millisecond
	return p
}

func (f *fakeE2B) inject(pattern string, ft fault) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.faults[pattern] = &ft
}

func (f *fakeE2B) clearFaults() {
	f.mu.Lock()
	defer f.mu.Unlock()
	clear(f.faults)
}

func (f *fakeE2B) take(pattern string, h http.Header) *fault {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[pattern]++
	if pattern == "POST /v2/sandboxes" {
		f.keys = append(f.keys, h.Get("Idempotency-Key"))
	}
	ft := f.faults[pattern]
	if ft == nil {
		return nil
	}
	if ft.times > 0 {
		ft.times--
		if ft.times == 0 {
			delete(f.faults, pattern)
		}
	}
	return ft
}

func (f *fakeE2B) callCount(pattern string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[pattern]
}

// gate checks the API key and applies any fault injected for the route
func (f *fakeE2B) gate(pattern string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-API-Key") != fakeKey {
			writeError(w, http.StatusUnauthorized, "", "Invalid API key, please visit e2b.dev")
			return
		}
		ft := f.take(pattern, r.Header)
		if ft == nil {
			h(w, r)
			return
		}
		if ft.after {
			h(httptest.NewRecorder(), r)
		}
		if ft.drop {
			conn, _, err := http.NewResponseController(w).Hijack()
			if err == nil {
				_ = conn.Close()
			}
			return
		}
		for k, v := range ft.header {
			w.Header().Set(k, v)
		}
		writeError(w, ft.status, ft.errorCode, ft.message)
	}
}

func writeError(w http.ResponseWriter, status int, errorCode, message string) {
	e := api.Error{Code: int32(status), Message: message}
	if errorCode != "" {
		e.ErrorCode = &errorCode
	}
	writeJSON(w, status, e)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

/* Sandboxes */

// add places a running sandbox directly, as another client of the same account would
func (f *fakeE2B) add(metadata map[string]string) provider.Ref {
	f.mu.Lock()
	defer f.mu.Unlock()
	box := f.place("bugnet-2x2", metadata, 300)
	return provider.Ref{Provider: "e2b", ID: box.id, Meta: map[string]string{
		"arch": "amd64", "domain": fakeDomain, "envd_token": box.token,
	}}
}

func (f *fakeE2B) place(templateID string, metadata map[string]string, timeout int) *fakeSandbox {
	f.nextID++
	now := time.Now().UTC()
	box := &fakeSandbox{
		id:         fmt.Sprintf("ifake%016d", f.nextID),
		templateID: templateID,
		token:      fmt.Sprintf("tok-%d", f.nextID),
		metadata:   metadata,
		started:    now,
		endAt:      now.Add(time.Duration(timeout) * time.Second),
		state:      "running",
	}
	f.boxes[box.id] = box
	return box
}

func (f *fakeE2B) box(id string) (fakeSandbox, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	box, ok := f.boxes[id]
	if !ok {
		return fakeSandbox{}, false
	}
	return *box, true
}

func (f *fakeE2B) setBox(id string, change func(*fakeSandbox)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	change(f.boxes[id])
}

func (f *fakeE2B) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.boxes)
}

func (f *fakeE2B) create(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		return
	}
	var req api.NewSandboxV2
	var fields map[string]any
	if json.Unmarshal(raw, &req) != nil || json.Unmarshal(raw, &fields) != nil {
		writeError(w, http.StatusBadRequest, "", "request body is not JSON")
		return
	}
	key, keyed := r.Header.Get("Idempotency-Key"), len(r.Header.Values("Idempotency-Key")) > 0
	if keyed && !fakeIdemKey.MatchString(key) {
		writeError(w, http.StatusBadRequest, "", "Idempotency-Key does not match ^[a-z0-9-]+$")
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.creates = append(f.creates, createCall{header: r.Header.Clone(), body: fields})
	if e := f.idem[key]; keyed && e != nil {
		if !bytes.Equal(e.request, raw) {
			writeError(w, http.StatusConflict, "idempotency_request_mismatch",
				"Idempotency-Key was used with different parameters")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write(e.reply)
		return
	}
	if !slices.ContainsFunc(fakeTemplates, func(t Template) bool {
		return t.TemplateID == req.TemplateID
	}) {
		writeError(w, http.StatusBadRequest, "", "template '"+req.TemplateID+"' not found")
		return
	}
	timeout := 300
	if req.Timeout != nil {
		timeout = int(*req.Timeout)
	}
	if timeout < 1 {
		writeError(w, http.StatusBadRequest, "", "timeout must be at least 1")
		return
	}
	var metadata map[string]string
	if req.Metadata != nil {
		metadata = maps.Clone(*req.Metadata)
	}
	box := f.place(req.TemplateID, metadata, timeout)
	domain := fakeDomain
	reply, _ := json.Marshal(api.Sandbox{
		SandboxID: box.id, TemplateID: box.templateID, ClientID: "6532622b",
		EnvdVersion: fakeEnvd, EnvdAccessToken: &box.token, Domain: &domain,
	})
	if keyed {
		f.idem[key] = &idemEntry{request: raw, reply: reply}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write(reply)
}

func (f *fakeE2B) lastCreate() createCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.creates) == 0 {
		return createCall{}
	}
	return f.creates[len(f.creates)-1]
}

// createKeys returns the Idempotency-Key of every create call, faulted ones included
func (f *fakeE2B) createKeys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.keys)
}

// list filters like E2B, by a metadata query whose keys and values are each URL encoded
func (f *fakeE2B) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	want := map[string]string{}
	if m := q.Get("metadata"); m != "" {
		pairs, err := url.ParseQuery(m)
		if err != nil {
			writeError(w, http.StatusBadRequest, "", "metadata is not a query string")
			return
		}
		for k, vs := range pairs {
			key, err1 := url.QueryUnescape(k)
			value, err2 := url.QueryUnescape(vs[0])
			if err1 != nil || err2 != nil {
				writeError(w, http.StatusBadRequest, "", "metadata is not URL encoded")
				return
			}
			want[key] = value
		}
	}
	states := map[string]bool{"running": true}
	if s := q.Get("state"); s != "" {
		states = map[string]bool{}
		for _, st := range strings.Split(s, ",") {
			if st != "running" && st != "paused" {
				writeError(w, http.StatusBadRequest, "", "unknown state "+st)
				return
			}
			states[st] = true
		}
	}
	limit := 100
	if l := q.Get("limit"); l != "" {
		n, err := strconv.Atoi(l)
		if err != nil || n < 1 || n > 100 {
			writeError(w, http.StatusBadRequest, "", "limit must be 1 to 100")
			return
		}
		limit = n
	}
	if f.pageSize > 0 {
		limit = min(limit, f.pageSize)
	}
	start := 0
	if tok := q.Get("nextToken"); tok != "" {
		start, _ = strconv.Atoi(tok)
	}

	f.mu.Lock()
	var match []*fakeSandbox
	for _, box := range f.boxes {
		if states[box.state] && hasLabels(box.metadata, want) {
			match = append(match, box)
		}
	}
	slices.SortFunc(match, func(a, b *fakeSandbox) int { return strings.Compare(a.id, b.id) })
	page := []api.ListedSandbox{}
	end := min(start+limit, len(match))
	for _, box := range match[min(start, end):end] {
		md := api.SandboxMetadata(maps.Clone(box.metadata))
		page = append(page, api.ListedSandbox{
			SandboxID: box.id, TemplateID: box.templateID, ClientID: "6532622b",
			StartedAt: box.started, EndAt: box.endAt, CpuCount: 2, MemoryMB: 2048,
			DiskSizeMB: 10240, Metadata: &md, State: api.SandboxState(box.state),
			EnvdVersion: fakeEnvd,
		})
	}
	f.mu.Unlock()
	if end < len(match) {
		w.Header().Set("X-Next-Token", strconv.Itoa(end))
	}
	writeJSON(w, http.StatusOK, page)
}

func (f *fakeE2B) get(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := r.PathValue("id")
	box, ok := f.boxes[id]
	if !ok {
		writeError(w, http.StatusNotFound, "", "sandbox \""+id+"\" not found")
		return
	}
	domain, md := fakeDomain, api.SandboxMetadata(maps.Clone(box.metadata))
	writeJSON(w, http.StatusOK, api.SandboxDetail{
		SandboxID: box.id, TemplateID: box.templateID, ClientID: "6532622b",
		StartedAt: box.started, EndAt: box.endAt, CpuCount: 2, MemoryMB: 2048,
		DiskSizeMB: 10240, Metadata: &md, State: api.SandboxState(box.state),
		EnvdVersion: fakeEnvd, EnvdAccessToken: &box.token, Domain: &domain,
	})
}

func (f *fakeE2B) kill(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := r.PathValue("id")
	if _, ok := f.boxes[id]; !ok {
		writeError(w, http.StatusNotFound, "", "sandbox \""+id+"\" not found")
		return
	}
	delete(f.boxes, id)
	w.WriteHeader(http.StatusNoContent)
}

func (f *fakeE2B) timeout(w http.ResponseWriter, r *http.Request) {
	var req api.SandboxTimeoutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Timeout < 0 {
		writeError(w, http.StatusBadRequest, "", "timeout must be a whole number of seconds")
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	id := r.PathValue("id")
	box, ok := f.boxes[id]
	if !ok {
		writeError(w, http.StatusNotFound, "", "sandbox \""+id+"\" not found")
		return
	}
	box.endAt = time.Now().UTC().Add(time.Duration(req.Timeout) * time.Second)
	w.WriteHeader(http.StatusNoContent)
}

/* Proxy and envd */

// proxy routes by the sandbox headers and refuses gone sandboxes and wrong tokens like E2B's
func (f *fakeE2B) proxy(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("E2b-Sandbox-Id")
		f.mu.Lock()
		box, down := f.boxes[id], false
		if box != nil && box.envdDown > 0 {
			box.envdDown--
			down = true
		}
		f.mu.Unlock()
		switch {
		case box == nil || r.Header.Get("E2b-Sandbox-Port") != envdPort:
			writeJSON(w, http.StatusBadGateway, map[string]any{
				"sandboxId": id, "code": 502,
				"message": "The sandbox " + id + " was not found",
			})
		case down:
			writeJSON(w, http.StatusBadGateway, map[string]any{
				"sandboxId": id, "code": 502,
				"message": "The sandbox is running but port is not open",
			})
		case r.URL.Path != "/health" && r.Header.Get("X-Access-Token") != box.token:
			http.Error(w, "unauthorized access, please provide a valid access token",
				http.StatusUnauthorized)
		default:
			next.ServeHTTP(w, r)
		}
	})
}

func basicUser(h http.Header) string {
	raw, ok := strings.CutPrefix(h.Get("Authorization"), "Basic ")
	if !ok {
		return ""
	}
	b, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return ""
	}
	user, _, _ := strings.Cut(string(b), ":")
	return user
}

type fakeProcess struct {
	processconnect.UnimplementedProcessHandler
	f *fakeE2B
}

// replyExec answers every later Start with these events instead of running a command
func (f *fakeE2B) replyExec(events ...*processpb.ProcessEvent) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scripted, f.script = true, events
}

func (f *fakeE2B) lastStart() startCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.starts) == 0 {
		return startCall{}
	}
	return f.starts[len(f.starts)-1]
}

func (f *fakeE2B) startCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.starts)
}

func (p fakeProcess) Start(
	ctx context.Context, req *connect.Request[processpb.StartRequest],
	stream *connect.ServerStream[processpb.StartResponse],
) error {
	f := p.f
	f.mu.Lock()
	msg, _ := proto.Clone(req.Msg).(*processpb.StartRequest)
	f.starts = append(f.starts, startCall{header: req.Header().Clone(), msg: msg})
	scripted, script := f.scripted, f.script
	f.mu.Unlock()
	if basicUser(req.Header()) != "root" {
		return connect.NewError(connect.CodePermissionDenied,
			errors.New("fake envd runs commands only as root"))
	}
	if req.Msg.Stdin == nil || req.Msg.GetStdin() {
		return connect.NewError(connect.CodeInvalidArgument,
			errors.New("fake envd runs commands only without stdin"))
	}
	var sendMu sync.Mutex
	send := func(ev *processpb.ProcessEvent) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(&processpb.StartResponse{Event: ev})
	}
	if scripted {
		for _, ev := range script {
			if err := send(ev); err != nil {
				return err
			}
		}
		return nil
	}

	cfg := req.Msg.GetProcess()
	cmd := exec.Command(cfg.GetCmd(), cfg.GetArgs()...)
	cmd.Dir = cfg.GetCwd()
	cmd.Env = cmd.Environ()
	for k, v := range cfg.GetEnvs() {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("error starting process: %w", err))
	}
	f.running.Add(1)
	defer f.running.Add(-1)
	exited := make(chan struct{})
	defer close(exited)
	go func() {
		select {
		case <-ctx.Done():
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		case <-exited:
		}
	}()

	_ = send(startEvent(uint32(cmd.Process.Pid)))
	if req.Header().Get("Keepalive-Ping-Interval") != "" {
		_ = send(&processpb.ProcessEvent{Event: &processpb.ProcessEvent_Keepalive{
			Keepalive: &processpb.ProcessEvent_KeepAlive{},
		}})
	}
	var pipes sync.WaitGroup
	for _, pipe := range []struct {
		r      io.Reader
		stderr bool
	}{{stdout, false}, {stderr, true}} {
		pipes.Go(func() {
			buf := make([]byte, 4096)
			for {
				n, err := pipe.r.Read(buf)
				if n > 0 {
					data := slices.Clone(buf[:n])
					if pipe.stderr {
						_ = send(stderrEvent(data))
					} else {
						_ = send(stdoutEvent(data))
					}
				}
				if err != nil {
					return
				}
			}
		})
	}
	pipes.Wait()
	waitErr := cmd.Wait()
	if ctx.Err() != nil {
		return connect.NewError(connect.CodeCanceled, ctx.Err())
	}
	end := &processpb.ProcessEvent_EndEvent{
		ExitCode: int32(cmd.ProcessState.ExitCode()), Exited: cmd.ProcessState.Exited(),
		Status: cmd.ProcessState.String(),
	}
	if waitErr != nil {
		end.Error = proto.String(waitErr.Error())
	}
	return send(&processpb.ProcessEvent{Event: &processpb.ProcessEvent_End{End: end}})
}

func startEvent(pid uint32) *processpb.ProcessEvent {
	return &processpb.ProcessEvent{Event: &processpb.ProcessEvent_Start{
		Start: &processpb.ProcessEvent_StartEvent{Pid: pid},
	}}
}

func stdoutEvent(data []byte) *processpb.ProcessEvent {
	return &processpb.ProcessEvent{Event: &processpb.ProcessEvent_Data{
		Data: &processpb.ProcessEvent_DataEvent{
			Output: &processpb.ProcessEvent_DataEvent_Stdout{Stdout: data},
		},
	}}
}

func stderrEvent(data []byte) *processpb.ProcessEvent {
	return &processpb.ProcessEvent{Event: &processpb.ProcessEvent_Data{
		Data: &processpb.ProcessEvent_DataEvent{
			Output: &processpb.ProcessEvent_DataEvent_Stderr{Stderr: data},
		},
	}}
}

func endEvent(code int32) *processpb.ProcessEvent {
	return &processpb.ProcessEvent{Event: &processpb.ProcessEvent_End{
		End: &processpb.ProcessEvent_EndEvent{
			ExitCode: code, Exited: true, Status: fmt.Sprintf("exit status %d", code),
		},
	}}
}

type fakeFilesystem struct {
	filesystemconnect.UnimplementedFilesystemHandler
	f *fakeE2B
}

func (h fakeFilesystem) ListDir(
	_ context.Context, req *connect.Request[filesystempb.ListDirRequest],
) (*connect.Response[filesystempb.ListDirResponse], error) {
	if h.f.listLimited.Load() {
		return nil, connect.NewError(connect.CodeResourceExhausted,
			errors.New("too many requests"))
	}
	if basicUser(req.Header()) != "root" {
		return nil, connect.NewError(connect.CodePermissionDenied,
			errors.New("fake envd lists only as root"))
	}
	root, depth := req.Msg.GetPath(), int(req.Msg.GetDepth())
	if depth < 1 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("depth below 1"))
	}
	info, err := os.Stat(root)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil, connect.NewError(connect.CodeNotFound, err)
	case err != nil:
		return nil, connect.NewError(connect.CodeInternal, err)
	case !info.IsDir():
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("path %s is not a directory", root))
	}
	resp := &filesystempb.ListDirResponse{}
	err = filepath.WalkDir(root, func(name string, _ fs.DirEntry, err error) error {
		if err != nil || name == root {
			return err
		}
		rel, _ := filepath.Rel(root, name)
		if strings.Count(rel, string(filepath.Separator))+1 > depth {
			return fs.SkipDir
		}
		e, err := fakeEntry(name)
		if err != nil {
			return err
		}
		resp.Entries = append(resp.Entries, e)
		return nil
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(resp), nil
}

func (h fakeFilesystem) Stat(
	_ context.Context, req *connect.Request[filesystempb.StatRequest],
) (*connect.Response[filesystempb.StatResponse], error) {
	if basicUser(req.Header()) != "root" {
		return nil, connect.NewError(connect.CodePermissionDenied,
			errors.New("fake envd stats only as root"))
	}
	entry, err := fakeEntry(req.Msg.GetPath())
	if err != nil {
		return nil, fakeFsError(err)
	}
	return connect.NewResponse(&filesystempb.StatResponse{Entry: entry}), nil
}

// Move renames like envd, making the destination's parents first
func (h fakeFilesystem) Move(
	_ context.Context, req *connect.Request[filesystempb.MoveRequest],
) (*connect.Response[filesystempb.MoveResponse], error) {
	if basicUser(req.Header()) != "root" {
		return nil, connect.NewError(connect.CodePermissionDenied,
			errors.New("fake envd moves only as root"))
	}
	from, to := req.Msg.GetSource(), req.Msg.GetDestination()
	h.f.mu.Lock()
	h.f.moves = append(h.f.moves, [2]string{from, to})
	h.f.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(to), 0o755); err != nil {
		return nil, fakeFsError(err)
	}
	if err := os.Rename(from, to); err != nil {
		return nil, fakeFsError(err)
	}
	entry, err := fakeEntry(to)
	if err != nil {
		return nil, fakeFsError(err)
	}
	return connect.NewResponse(&filesystempb.MoveResponse{Entry: entry}), nil
}

// Remove deletes like envd, with os.RemoveAll, so a missing path is no error
func (h fakeFilesystem) Remove(
	_ context.Context, req *connect.Request[filesystempb.RemoveRequest],
) (*connect.Response[filesystempb.RemoveResponse], error) {
	if basicUser(req.Header()) != "root" {
		return nil, connect.NewError(connect.CodePermissionDenied,
			errors.New("fake envd removes only as root"))
	}
	if err := os.RemoveAll(req.Msg.GetPath()); err != nil {
		return nil, fakeFsError(err)
	}
	return connect.NewResponse(&filesystempb.RemoveResponse{}), nil
}

// fakeEntry describes name like envd, a symlink flagged and typed by its target
func fakeEntry(name string) (*filesystempb.EntryInfo, error) {
	info, err := os.Lstat(name)
	if err != nil {
		return nil, err
	}
	e := &filesystempb.EntryInfo{
		Name: info.Name(), Path: name, Size: info.Size(), Mode: uint32(info.Mode().Perm()),
		Permissions: info.Mode().String(), ModifiedTime: timestamppb.New(info.ModTime()),
	}
	kind := info.Mode()
	if kind&fs.ModeSymlink != 0 {
		e.IsSymlink = true
		if target, err := os.Readlink(name); err == nil {
			e.SymlinkTarget = &target
		}
		if followed, err := os.Stat(name); err == nil {
			kind = followed.Mode()
		}
	}
	switch {
	case kind.IsRegular():
		e.Type = filesystempb.FileType_FILE_TYPE_FILE
	case kind.IsDir():
		e.Type = filesystempb.FileType_FILE_TYPE_DIRECTORY
	case kind&fs.ModeSymlink != 0:
		e.Type = filesystempb.FileType_FILE_TYPE_SYMLINK
	}
	return e, nil
}

func fakeFsError(err error) error {
	if errors.Is(err, fs.ErrNotExist) {
		return connect.NewError(connect.CodeNotFound, err)
	}
	return connect.NewError(connect.CodeInternal, err)
}

func fakeFilesError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, fs.ErrNotExist):
		status = http.StatusNotFound
	case errors.Is(err, syscall.EISDIR), errors.Is(err, syscall.ENOTDIR):
		status = http.StatusBadRequest
	}
	writeJSON(w, status, map[string]any{"code": status, "message": err.Error()})
}

// seeFiles records a request to /files
func (f *fakeE2B) seeFiles(r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.files = append(f.files, filesCall{
		method: r.Method, path: r.URL.Query().Get("path"),
		encoding: r.Header.Get("Accept-Encoding"), protoMajor: r.ProtoMajor,
	})
}

func (f *fakeE2B) filesSeen() []filesCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.files)
}

func (f *fakeE2B) movesSeen() [][2]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.moves)
}

func (f *fakeE2B) download(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f.seeFiles(r)
	f.mu.Lock()
	f.ranges = append(f.ranges, r.Header.Get("Range"))
	f.mu.Unlock()
	if q.Get("username") != "root" {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"code": 401, "message": "bad user"})
		return
	}
	enc := r.Header.Get("Accept-Encoding")
	if r.Header.Get("Range") != "" && enc != "" && enc != "identity" {
		writeJSON(w, http.StatusNotAcceptable, map[string]any{
			"code": 406, "message": "identity encoding not acceptable for Range",
		})
		return
	}
	file, err := os.Open(q.Get("path"))
	if err != nil {
		fakeFilesError(w, err)
		return
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		fakeFilesError(w, err)
		return
	}
	if info.IsDir() {
		fakeFilesError(w, fmt.Errorf("path %s: %w", q.Get("path"), syscall.EISDIR))
		return
	}
	if f.noRange {
		r.Header.Del("Range")
	}
	if f.gzipBody.Load() {
		w.Header().Set("Content-Encoding", "gzip")
	}
	if f.shortBody.Load() {
		w.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))
		w.WriteHeader(http.StatusOK)
		_, _ = io.CopyN(w, file, info.Size()/2)
		return
	}
	if f.wrongRangeStart && r.Header.Get("Range") != "" {
		w.Header().Set("Content-Range",
			fmt.Sprintf("bytes 0-%d/%d", info.Size()-1, info.Size()))
		w.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = io.Copy(w, file)
		return
	}
	if f.noContentRange {
		w = dropContentRange{w}
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	http.ServeContent(w, r, "", info.ModTime(), file)
}

type dropContentRange struct{ http.ResponseWriter }

func (d dropContentRange) WriteHeader(status int) {
	if status == http.StatusPartialContent {
		d.Header().Del("Content-Range")
	}
	d.ResponseWriter.WriteHeader(status)
}

// upload writes like envd, truncating in place, new files get 0644 and replaced ones keep theirs
func (f *fakeE2B) upload(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f.seeFiles(r)
	f.mu.Lock()
	f.uploads = append(f.uploads, q)
	f.mu.Unlock()
	name := q.Get("path")
	switch {
	case q.Get("username") != "root":
		writeJSON(w, http.StatusUnauthorized, map[string]any{"code": 401, "message": "bad user"})
		return
	case name == "" || r.Header.Get("Content-Type") != "application/octet-stream":
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"code": 400, "message": "octet-stream uploads need a path",
		})
		return
	}
	f.track(name)
	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		fakeFilesError(w, err)
		return
	}
	file, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		fakeFilesError(w, err)
		return
	}
	_, err = io.Copy(file, r.Body)
	cerr := file.Close()
	switch {
	case err != nil:
		return
	case f.noSpace.Load():
		writeJSON(w, http.StatusInsufficientStorage, map[string]any{
			"code": 507, "message": "not enough disk space available",
		})
		return
	case cerr != nil:
		fakeFilesError(w, cerr)
		return
	}
	writeJSON(w, http.StatusOK, []map[string]string{
		{"path": name, "name": filepath.Base(name), "type": "file"},
	})
}

func (f *fakeE2B) rangesAsked() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.ranges)
}

func (f *fakeE2B) lastUpload() url.Values {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.uploads) == 0 {
		return nil
	}
	return f.uploads[len(f.uploads)-1]
}

// track remembers the topmost path a write is about to create so cleanup removes it
func (f *fakeE2B) track(name string) {
	top := ""
	for p := name; p != "/" && p != "."; p = filepath.Dir(p) {
		if _, err := os.Lstat(p); err == nil {
			break
		}
		top = p
	}
	if top != "" {
		f.mu.Lock()
		f.cleanup = append(f.cleanup, top)
		f.mu.Unlock()
	}
}
