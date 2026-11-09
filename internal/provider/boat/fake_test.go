package boat

import (
	"bytes"
	"cmp"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
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
	"path"
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
)

// fakeHostKey is the host key the fake's sshd reports
const fakeHostKey = "ssh-ed25519 " +
	"AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl"

const (
	fakeKey     = "boat_fake_key"
	fakeFileCap = 5 << 20
	idAlphabet  = "23456789abcdefghjkmnpqrstuvwxyz"
)

var (
	fakeLabelKey = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]{0,62}$`)
	octalMode    = regexp.MustCompile(`^[0-7]{3,4}$`)
	fakeTypes    = map[string][2]int{
		"small": {2, 4}, "default": {4, 8}, "large": {8, 16}, "xlarge": {16, 32},
	}
)

// fault answers matching calls with an error envelope, after serving them when after is set
type fault struct {
	status  int
	code    string
	message string
	top     map[string]any
	details map[string]any
	header  map[string]string
	drop    bool
	after   bool
	times   int
}

type fakeSandbox struct {
	id           string
	labels       map[string]string
	typ          string
	ttl          int
	archiveAfter time.Time
	created      time.Time
	state        string
	startPolls   int
	deleted      bool
	procs        map[int]*fakeProc
	ip           any
	sshEndpoint  any
	// degradedSince and stopText set Boat's health and error fields, empty for none
	degradedSince time.Time
	stopText      string
}

// fakeProc is one detached command, its logs kept whole so every attach can replay them
type fakeProc struct {
	mu       sync.Mutex
	stdout   []byte
	stderr   []byte
	done     bool
	exitCode *int
	signal   string
	changed  chan struct{}
	cmd      *exec.Cmd
}

func (p *fakeProc) add(stderr bool, b []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if stderr {
		p.stderr = append(p.stderr, b...)
	} else {
		p.stdout = append(p.stdout, b...)
	}
	close(p.changed)
	p.changed = make(chan struct{})
}

func (p *fakeProc) finish(code *int, signal string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.done, p.exitCode, p.signal = true, code, signal
	close(p.changed)
	p.changed = make(chan struct{})
}

func (p *fakeProc) since(stdout, stderr int) ([]byte, []byte, bool, <-chan struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.stdout[min(stdout, len(p.stdout)):]),
		slices.Clone(p.stderr[min(stderr, len(p.stderr)):]), p.done, p.changed
}

type createCall struct {
	header http.Header
	body   map[string]any
}

type commandCall struct {
	body map[string]any
}

type attachCall struct {
	process        int
	stdout, stderr int
}

type keyRecord struct {
	id   string
	body string
}

type uploadGrant struct {
	sandbox, path string
	method        string
	expires       time.Time
}

// fakeBoat serves Boat's API from memory, its commands run on the host and its files live in /tmp
type fakeBoat struct {
	t   *testing.T
	srv *httptest.Server
	tmp string

	// startPolls is how many reads see a new sandbox still provisioning
	startPolls int
	// endState is what a sandbox becomes after provisioning, ready when empty
	endState string
	pageSize int
	// chunkBytes caps the stdout bytes in one stream line
	chunkBytes int
	// reconnectEvery ends an attach stream with a reconnect line after so many stdout lines
	reconnectEvery int
	// dropEvery cuts an attach stream without a last line after so many stdout lines
	dropEvery int
	// rewind starts a reopened stream this many bytes before the offset asked for
	rewind int
	// heartbeat sends heartbeat lines at this interval while a command is quiet
	heartbeat time.Duration
	// script answers every attach stream with these lines and runs no command
	script *string
	// listLimit cuts a directory listing to so many entries and marks it truncated
	listLimit int
	// drift writes sandbox and command times in another layout, or as numbers when "types"
	drift string
	// uploadBase serves upload URLs from another server instead of the fake itself
	uploadBase string
	// ignoreRange answers a ranged download with the whole file and 200
	ignoreRange bool
	// shortDownload aborts every download halfway, its length already sent
	shortDownload bool
	// holdUploads keeps every upload from reading its body until it is closed
	holdUploads chan struct{}
	// wholeUploads writes a signed PUT's path only once its whole body arrived, as live Boat did
	wholeUploads bool
	// downloadEncoding is the Content-Encoding every signed download claims
	downloadEncoding string
	// expired answers every signed URL as one past its expiry, under mu
	expired bool
	// shiftRange moves the start of every ranged download this many bytes past the one asked for
	shiftRange int64

	mu sync.Mutex
	// limits is the GET /limits reply, limitsStatus its status when not 200
	limits       string
	limitsStatus int
	limitsCalls  int
	// rateHeader is the RateLimit header of the GET /limits answer, none when empty
	rateHeader string
	// me is the GET /me reply, meStatus its status when not 200
	me        string
	meStatus  int
	meCalls   int
	streams   int
	grow      map[string][]byte
	swaps     map[string]func()
	sandboxes map[string]*fakeSandbox
	order     []string
	keys      map[string]keyRecord
	faults    map[string]*fault
	calls     map[string]int
	creates   []createCall
	commands  []commandCall
	attaches  []attachCall
	listed    []url.Values
	kills     []int
	uploads   []http.Header
	bodies    map[string][]int
	perms     []string
	grants    map[string]uploadGrant
	ttls      []int
	downloads []http.Header
	moves     [][2]string
	nextPID   int
	// hosts holds the token of each hosted port by sandbox and port
	hosts  map[string]string
	hosted []string
	// hostURL replaces every hosted URL when set, an empty one leaving the url field out
	hostURL *string
	// sshKeys lists each key line sent to /sshkey, and sshReply replaces its reply when set
	sshKeys  []string
	sshReply map[string]any
	requests atomic.Int64
	cleanup  []string
	cmds     []*exec.Cmd
}

func newFake(t *testing.T) *fakeBoat {
	t.Helper()
	tmp, err := filepath.EvalSymlinks("/tmp")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeBoat{
		t: t, tmp: tmp, sandboxes: map[string]*fakeSandbox{}, keys: map[string]keyRecord{},
		faults: map[string]*fault{}, calls: map[string]int{}, bodies: map[string][]int{},
		grants: map[string]uploadGrant{}, grow: map[string][]byte{}, swaps: map[string]func(){},
		hosts: map[string]string{},
	}
	mux := http.NewServeMux()
	const sb = "/api/v1/sandboxes/{id}"
	for pattern, h := range map[string]http.HandlerFunc{
		"POST /api/v1/sandboxes":               f.create,
		"GET /api/v1/sandboxes":                f.list,
		"GET " + sb:                            f.get,
		"PATCH " + sb:                          f.update,
		"DELETE " + sb:                         f.delete,
		"POST " + sb + "/commands":             f.command,
		"GET " + sb + "/commands/{pid}/stream": f.attach,
		"DELETE " + sb + "/commands/{pid}":     f.stopCommand,
		"GET " + sb + "/files":                 f.readFile,
		"PUT " + sb + "/files":                 f.writeFile,
		"GET " + sb + "/files/list":            f.listFiles,
		"GET " + sb + "/files/info":            f.fileInfo,
		"POST " + sb + "/files/folder":         f.folder,
		"POST " + sb + "/files/permissions":    f.permissions,
		"POST " + sb + "/files/upload-url":     f.uploadURL,
		"POST " + sb + "/files/download-url":   f.downloadURL,
		"POST " + sb + "/files/move":           f.move,
		"POST " + sb + "/host":                 f.host,
		"POST " + sb + "/sshkey":               f.sshKey,
		"DELETE " + sb + "/files":              f.deleteFile,
		"GET /api/v1/limits":                   f.readLimits,
		"GET /api/v1/me":                       f.readMe,
	} {
		mux.HandleFunc(pattern, f.gate(pattern, h))
	}
	mux.HandleFunc("PUT /upload/{token}", f.upload)
	mux.HandleFunc("GET /download/{token}", f.download)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("adapter called the unexpected route %s %s", r.Method, r.URL.Path)
		f.fail(w, http.StatusNotFound, "not_found", "not in the fake", nil, nil)
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(func() {
		f.srv.Close()
		f.mu.Lock()
		defer f.mu.Unlock()
		for _, cmd := range f.cmds {
			if cmd.Process != nil {
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			}
		}
		for _, p := range f.cleanup {
			_ = os.RemoveAll(p)
		}
	})
	return f
}

// provider builds an adapter whose API is the fake and that polls and reattaches quickly
func (f *fakeBoat) provider(t *testing.T, key string) *Provider {
	t.Helper()
	p, err := New(Options{APIKey: key, BaseURL: f.srv.URL + "/api/v1"})
	if err != nil {
		t.Fatal(err)
	}
	p.pollInterval = 10 * time.Millisecond
	p.reattachWait = 10 * time.Millisecond
	return p
}

func (f *fakeBoat) inject(pattern string, ft fault) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.faults[pattern] = &ft
}

func (f *fakeBoat) take(pattern string) *fault {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[pattern]++
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

func (f *fakeBoat) callCount(pattern string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[pattern]
}

// totalCalls counts every API call the fake served
func (f *fakeBoat) totalCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		n += c
	}
	return n
}

// gate checks the key, keeps the body size and applies any fault injected for the route
func (f *fakeBoat) gate(pattern string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+fakeKey {
			f.fail(w, http.StatusUnauthorized, "unauthorized", "Unauthorized", nil, nil)
			return
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(raw))
		f.mu.Lock()
		f.bodies[pattern] = append(f.bodies[pattern], len(raw))
		f.mu.Unlock()
		ft := f.take(pattern)
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
		f.fail(w, ft.status, ft.code, ft.message, ft.top, ft.details)
	}
}

// fail writes Boat's error envelope with any extra top-level fields and details
func (f *fakeBoat) fail(
	w http.ResponseWriter, status int, code, message string, top, details map[string]any,
) {
	id := fmt.Sprintf("req_fake%04d", f.requests.Add(1))
	inner := map[string]any{"code": code, "message": message, "status": status}
	if details != nil {
		inner["details"] = details
	}
	body := map[string]any{
		"ok": false, "type": "sandbox.error", "status": status, "code": code,
		"message": message, "error": inner, "requestId": id,
	}
	maps.Copy(body, top)
	writeJSON(w, status, body)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func randomText(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	for i := range b {
		b[i] = idAlphabet[int(b[i])%len(idAlphabet)]
	}
	return string(b)
}

/* Sandboxes */

func (f *fakeBoat) sandboxJSON(s *fakeSandbox) map[string]any {
	shape := fakeTypes[s.typ]
	sb := map[string]any{
		"id": s.id, "name": "Sandbox " + s.created.Format("2006-01-02 15:04"), "state": s.state,
		"labels": s.labels, "type": s.typ, "vcpu": shape[0], "memoryGB": shape[1],
		"billingMultiplier": 1, "machineProvider": "baremetal", "url": nil, "ip": s.ip,
		"createdAt": f.when(s.created), "updatedAt": f.when(s.created),
		"archiveAfter": f.when(s.archiveAfter), "ttlSeconds": s.ttl,
		"desktopAvailable": false, "snapshotAvailable": false, "snapshots": false,
		"monitoring": false, "hydration": nil, "team": nil, "access": "owner",
		"health": "ok", "lastReboot": nil, "futureField": map[string]any{"nested": []any{1}},
	}
	if !s.degradedSince.IsZero() {
		sb["health"], sb["healthReason"] = "degraded", "tunnel unreachable"
		sb["degradedSince"] = f.when(s.degradedSince)
	}
	if s.stopText != "" {
		sb["error"] = s.stopText
	}
	if s.sshEndpoint != nil {
		sb["sshEndpoint"] = s.sshEndpoint
	}
	if f.drift == "types" {
		sb["error"] = map[string]any{"reason": "none"}
	}
	return sb
}

// when writes a time as Boat documents it, or as the drift knob says
func (f *fakeBoat) when(t time.Time) any {
	switch f.drift {
	case "types":
		return t.Unix()
	case "":
		return t.Format(time.RFC3339)
	}
	return t.Format("2006-01-02 15:04:05Z07:00")
}

// live returns the sandbox the request names unless it is gone, answering 404 for it
func (f *fakeBoat) live(w http.ResponseWriter, r *http.Request) *fakeSandbox {
	s := f.sandboxes[r.PathValue("id")]
	if s == nil || s.deleted {
		f.fail(w, http.StatusNotFound, "not_found", "Sandbox not found", nil, nil)
		return nil
	}
	return s
}

// host registers a port's route once and answers the same URL and token every later time
func (f *fakeBoat) host(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	var body struct {
		Port int `json:"port"`
	}
	if json.Unmarshal(raw, &body) != nil || body.Port < 1 || body.Port > 65535 {
		f.fail(w, http.StatusBadRequest, "invalid_port", "port must be 1 to 65535", nil, nil)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hosted = append(f.hosted, string(bytes.TrimSpace(raw)))
	s := f.live(w, r)
	if s == nil {
		return
	}
	key := s.id + "/" + strconv.Itoa(body.Port)
	if f.hosts[key] == "" {
		f.hosts[key] = randomText(24)
	}
	reply := map[string]any{
		"ok": true, "type": "port.hosted", "success": true, "port": body.Port,
		"isProtected": true, "access": "private",
		"url": "https://" + s.id + "-" + strconv.Itoa(body.Port) + ".on.boat.dev?_token=" +
			f.hosts[key],
	}
	if f.hostURL != nil {
		reply["url"] = *f.hostURL
		if *f.hostURL == "" {
			delete(reply, "url")
		}
	}
	writeJSON(w, http.StatusOK, reply)
}

func (f *fakeBoat) sshKey(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Key string `json:"key"`
	}
	if json.NewDecoder(r.Body).Decode(&body) != nil || body.Key == "" {
		f.fail(w, http.StatusBadRequest, "invalid_key", "key is required", nil, nil)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sshKeys = append(f.sshKeys, body.Key)
	if f.live(w, r) == nil {
		return
	}
	reply := map[string]any{
		"ok": true, "type": "ssh_key.configured", "success": true, "machineIp": "203.0.113.10",
		"sshUser": "user", "hostKey": fakeHostKey,
	}
	if f.sshReply != nil {
		reply = f.sshReply
	}
	writeJSON(w, http.StatusOK, reply)
}

func (f *fakeBoat) setSSHReply(reply map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sshReply = reply
}

func (f *fakeBoat) sentSSHKeys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.sshKeys)
}

func (f *fakeBoat) hostToken(id string, port int) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hosts[id+"/"+strconv.Itoa(port)]
}

// hostBodies lists the body of every host call, oldest first
func (f *fakeBoat) hostBodies() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.hosted)
}

func (f *fakeBoat) setHostURL(u string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hostURL = &u
}

// add places a ready sandbox directly, as another create would have
func (f *fakeBoat) add(labels map[string]string) *fakeSandbox {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now().UTC().Truncate(time.Second)
	s := &fakeSandbox{
		id: "bx_" + randomText(8), labels: labels, typ: "small", ttl: 3600,
		archiveAfter: now.Add(time.Hour), created: now, state: "ready", procs: map[int]*fakeProc{},
	}
	f.sandboxes[s.id] = s
	f.order = append(f.order, s.id)
	return s
}

func (f *fakeBoat) sandbox(id string) (fakeSandbox, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.sandboxes[id]
	if s == nil {
		return fakeSandbox{}, false
	}
	return *s, true
}

func (f *fakeBoat) liveCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, s := range f.sandboxes {
		if !s.deleted {
			n++
		}
	}
	return n
}

// create checks the body like Boat and replays the sandbox of a repeated idempotency key
func (f *fakeBoat) create(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		f.fail(w, http.StatusBadRequest, "invalid_json", "Request body must be valid JSON.", nil,
			nil)
		return
	}
	var req struct {
		Type        *string           `json:"type"`
		TTL         *int              `json:"ttlSeconds"`
		Labels      map[string]string `json:"labels"`
		FailFast    bool              `json:"failFast"`
		Count       *int              `json:"count"`
		HostedPorts []any             `json:"hostedPorts"`
	}
	if err := json.Unmarshal(raw, &req); err != nil {
		f.fail(w, http.StatusBadRequest, "invalid_json", err.Error(), nil, nil)
		return
	}
	typ := "default"
	if req.Type != nil {
		typ = *req.Type
	}
	ttl := 3600
	if req.TTL != nil {
		ttl = *req.TTL
	}
	msg := checkCreate(typ, ttl, req.Labels, req.FailFast, req.Count, req.HostedPorts)
	if msg != "" {
		f.fail(w, http.StatusBadRequest, "invalid_request", msg, nil, nil)
		return
	}
	canonical, _ := json.Marshal(body)
	key := r.Header.Get("Idempotency-Key")
	f.mu.Lock()
	defer f.mu.Unlock()
	f.creates = append(f.creates, createCall{header: r.Header.Clone(), body: body})
	if key != "" {
		if rec, ok := f.keys[key]; ok {
			if rec.body != string(canonical) {
				f.fail(w, http.StatusConflict, "idempotency_key_reused",
					"Idempotency-Key was used with another body.", nil, nil)
				return
			}
			s := f.sandboxes[rec.id]
			status := "provisioning"
			if s.state == "ready" {
				status = "ready"
			}
			writeJSON(w, http.StatusAccepted, map[string]any{
				"ok": true, "type": "sandbox.created", "status": status, "ttlSeconds": s.ttl,
				"sandbox": f.sandboxJSON(s),
			})
			return
		}
	}
	now := time.Now().UTC().Truncate(time.Second)
	s := &fakeSandbox{
		id: "bx_" + randomText(8), labels: req.Labels, typ: typ, ttl: ttl,
		archiveAfter: now.Add(time.Duration(ttl) * time.Second), created: now,
		state: "provisioning", startPolls: f.startPolls, procs: map[int]*fakeProc{},
	}
	if s.labels == nil {
		s.labels = map[string]string{}
	}
	f.sandboxes[s.id] = s
	f.order = append(f.order, s.id)
	if key != "" {
		f.keys[key] = keyRecord{id: s.id, body: string(canonical)}
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"ok": true, "type": "sandbox.created", "status": "provisioning", "ttlSeconds": ttl,
		"sandbox": f.sandboxJSON(s),
	})
}

func checkCreate(
	typ string, ttl int, labels map[string]string, failFast bool, count *int, ports []any,
) string {
	if _, ok := fakeTypes[typ]; !ok {
		return "unknown type " + typ
	}
	if ttl < 1 || ttl > 2592000 {
		return "ttlSeconds must be 1 to 2592000"
	}
	if len(labels) > 20 {
		return "at most 20 labels"
	}
	for k, v := range labels {
		if !fakeLabelKey.MatchString(k) || len(v) > 256 {
			return "label " + k + " is invalid"
		}
	}
	if failFast && count != nil {
		return "count_with_fail_fast"
	}
	if failFast && ports != nil {
		return "fail_fast_with_hosted_ports"
	}
	return ""
}

func (f *fakeBoat) lastCreate() createCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.creates) == 0 {
		return createCall{}
	}
	return f.creates[len(f.creates)-1]
}

func (f *fakeBoat) createCalls() []createCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.creates)
}

func (f *fakeBoat) createCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.creates)
}

// get moves a provisioning sandbox on, and answers a cancelled one once before it is gone
func (f *fakeBoat) get(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.live(w, r)
	if s == nil {
		return
	}
	if s.state == "provisioning" && s.startPolls > 0 {
		s.startPolls--
	} else if s.state == "provisioning" {
		s.state = cmp.Or(f.endState, "ready")
	}
	sb := f.sandboxJSON(s)
	if s.state == "cancelled" {
		s.deleted = true
		sb = map[string]any{"id": s.id, "state": "cancelled", "error": "no machine was ready"}
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "type": "sandbox.info", "sandbox": sb})
}

func (f *fakeBoat) update(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TTL *float64 `json:"ttlSeconds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.TTL == nil ||
		*req.TTL != float64(int(*req.TTL)) || *req.TTL < 1 || *req.TTL > 2592000 {
		f.fail(w, http.StatusBadRequest, "invalid_ttl", "ttlSeconds must be 1 to 2592000", nil,
			nil)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.live(w, r)
	if s == nil {
		return
	}
	s.ttl = int(*req.TTL)
	s.archiveAfter = time.Now().UTC().Add(time.Duration(s.ttl) * time.Second)
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "type": "sandbox.info", "sandbox": f.sandboxJSON(s),
	})
}

// delete needs the confirm header like Boat and kills the sandbox's commands
func (f *fakeBoat) delete(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.live(w, r)
	if s == nil {
		return
	}
	if r.Header.Get("X-Ascii-Confirm-Delete") != s.id {
		f.fail(w, http.StatusConflict, "delete_confirmation_required",
			"X-Ascii-Confirm-Delete must equal the sandbox id.", nil, nil)
		return
	}
	s.deleted = true
	for _, p := range s.procs {
		p.mu.Lock()
		if p.cmd != nil && !p.done {
			_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
		}
		p.mu.Unlock()
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"ok": true, "type": "sandbox.deleting", "operation": map[string]any{
			"id": "bdop_" + strings.Repeat("a", 32), "kind": "sandbox", "targetId": s.id,
			"reason": "explicit", "status": "pending", "stage": "removing", "attemptCount": 0,
			"requestedAt": time.Now().UTC().Format(time.RFC3339), "completedAt": nil,
		},
	})
}

// list filters on every label=key=value and pages with an index cursor
func (f *fakeBoat) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	want := map[string]string{}
	for _, l := range q["label"] {
		k, v, ok := strings.Cut(l, "=")
		if !ok {
			f.fail(w, http.StatusBadRequest, "invalid_label", "label must be key=value", nil, nil)
			return
		}
		want[k] = v
	}
	limit := 100
	if l := q.Get("limit"); l != "" {
		n, err := strconv.Atoi(l)
		if err != nil || n < 1 || n > 200 {
			f.fail(w, http.StatusBadRequest, "invalid_limit", "limit must be 1 to 200", nil, nil)
			return
		}
		limit = n
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listed = append(f.listed, q)
	if f.pageSize > 0 {
		limit = min(limit, f.pageSize)
	}
	var match []*fakeSandbox
	for _, id := range f.order {
		s := f.sandboxes[id]
		if !s.deleted && labelsHave(s.labels, want) {
			match = append(match, s)
		}
	}
	start, _ := strconv.Atoi(q.Get("cursor"))
	start = min(start, len(match))
	end := min(start+limit, len(match))
	items := []map[string]any{}
	for _, s := range match[start:end] {
		items = append(items, f.sandboxJSON(s))
	}
	var next any
	if end < len(match) {
		next = strconv.Itoa(end)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "type": "sandbox.list", "sandboxes": items,
		"pageInfo": map[string]any{"nextCursor": next, "hasMore": end < len(match), "limit": limit},
	})
}

func (f *fakeBoat) listQueries() []url.Values {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.listed)
}

func labelsHave(have, want map[string]string) bool {
	for k, v := range want {
		if got, ok := have[k]; !ok || got != v {
			return false
		}
	}
	return true
}

/* Commands */

func (f *fakeBoat) command(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		f.fail(w, http.StatusBadRequest, "invalid_json", "Request body must be valid JSON.", nil,
			nil)
		return
	}
	line, _ := body["command"].(string)
	if body["detached"] != true || line == "" {
		f.t.Errorf("command %v is not detached", body)
		f.fail(w, http.StatusBadRequest, "invalid_request", "the fake runs detached commands only",
			nil, nil)
		return
	}
	f.mu.Lock()
	s := f.live(w, r)
	if s == nil {
		f.mu.Unlock()
		return
	}
	f.commands = append(f.commands, commandCall{body: body})
	f.nextPID++
	pid := f.nextPID
	proc := &fakeProc{changed: make(chan struct{})}
	s.procs[pid] = proc
	scripted := f.script != nil
	f.mu.Unlock()

	started := map[string]any{
		"ok": true, "type": "command.started", "success": true, "processId": pid, "pid": 4000 + pid,
		"command": line, "startedAt": f.when(time.Now().UTC()),
		"logPath": fmt.Sprintf("/home/user/.ascii/processes/%d.log", pid),
	}
	if f.drift == "types" {
		started["pid"], started["commandId"] = strconv.Itoa(4000+pid), pid
	}
	if scripted {
		writeJSON(w, http.StatusOK, started)
		return
	}
	cmd := exec.Command("sh", "-c", line)
	cmd.Dir = f.tmp
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdout, err1 := cmd.StdoutPipe()
	stderr, err2 := cmd.StderrPipe()
	if err := errors.Join(err1, err2, cmd.Start()); err != nil {
		f.fail(w, http.StatusInternalServerError, "command_failed", err.Error(), nil, nil)
		return
	}
	f.mu.Lock()
	f.cmds = append(f.cmds, cmd)
	f.mu.Unlock()
	proc.mu.Lock()
	proc.cmd = cmd
	proc.mu.Unlock()
	go func() {
		var pipes sync.WaitGroup
		for i, pipe := range []io.Reader{stdout, stderr} {
			pipes.Go(func() {
				buf := make([]byte, 32<<10)
				for {
					n, err := pipe.Read(buf)
					if n > 0 {
						proc.add(i == 1, buf[:n])
					}
					if err != nil {
						return
					}
				}
			})
		}
		pipes.Wait()
		_ = cmd.Wait()
		st := cmd.ProcessState.Sys().(syscall.WaitStatus)
		if st.Signaled() {
			proc.finish(nil, signalNames[st.Signal()])
			return
		}
		code := st.ExitStatus()
		proc.finish(&code, "")
	}()
	writeJSON(w, http.StatusOK, started)
}

var signalNames = map[syscall.Signal]string{
	syscall.SIGKILL: "SIGKILL", syscall.SIGTERM: "SIGTERM", syscall.SIGINT: "SIGINT",
	syscall.SIGHUP: "SIGHUP", syscall.SIGSEGV: "SIGSEGV",
}

func (f *fakeBoat) commandCalls() []commandCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.commands)
}

func (f *fakeBoat) lastCommand() map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.commands) == 0 {
		return nil
	}
	return f.commands[len(f.commands)-1].body
}

func (f *fakeBoat) proc(w http.ResponseWriter, r *http.Request) (*fakeProc, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.live(w, r)
	if s == nil {
		return nil, 0
	}
	pid, _ := strconv.Atoi(r.PathValue("pid"))
	p := s.procs[pid]
	if p == nil {
		f.fail(w, http.StatusNotFound, "not_found", "Process not found", nil, nil)
	}
	return p, pid
}

// attach streams a command's logs from the offsets, cut short as the knobs say
func (f *fakeBoat) attach(w http.ResponseWriter, r *http.Request) {
	proc, pid := f.proc(w, r)
	if proc == nil {
		return
	}
	q := r.URL.Query()
	so, _ := strconv.Atoi(q.Get("stdoutOffset"))
	se, _ := strconv.Atoi(q.Get("stderrOffset"))
	f.mu.Lock()
	f.attaches = append(f.attaches, attachCall{process: pid, stdout: so, stderr: se})
	script, chunk := f.script, f.chunkBytes
	reconnectEvery, dropEvery, rewind, beat := f.reconnectEvery, f.dropEvery, f.rewind, f.heartbeat
	f.streams++
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.streams--
		f.mu.Unlock()
	}()
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	rc := http.NewResponseController(w)
	if script != nil {
		_, _ = io.WriteString(w, *script)
		return
	}
	if so > 0 {
		so = max(0, so-rewind)
	}
	enc := json.NewEncoder(w)
	_ = enc.Encode(map[string]any{
		"type": "attached", "processId": pid, "stdoutOffset": so, "stderrOffset": se,
	})
	_ = rc.Flush()
	var tick <-chan time.Time
	if beat > 0 {
		ticker := time.NewTicker(beat)
		defer ticker.Stop()
		tick = ticker.C
	}
	lines := 0
	for {
		out, errb, done, changed := proc.since(so, se)
		if len(errb) > 0 {
			se += len(errb)
			_ = enc.Encode(map[string]any{"type": "stderr", "data": string(errb), "nextOffset": se})
		}
		for len(out) > 0 {
			n := len(out)
			if chunk > 0 {
				n = min(n, chunk)
			}
			so += n
			_ = enc.Encode(map[string]any{
				"type": "stdout", "data": string(out[:n]), "nextOffset": so,
			})
			out = out[n:]
			lines++
			if reconnectEvery > 0 && lines%reconnectEvery == 0 {
				_ = enc.Encode(map[string]any{
					"type": "reconnect", "stdoutOffset": so, "stderrOffset": se,
				})
				return
			}
			if dropEvery > 0 && lines%dropEvery == 0 {
				_ = rc.Flush()
				if conn, _, err := rc.Hijack(); err == nil {
					_ = conn.Close()
				}
				return
			}
		}
		_ = rc.Flush()
		if done && len(errb) == 0 && len(out) == 0 {
			proc.mu.Lock()
			exit := map[string]any{
				"type": "exit", "status": "exited", "exitCode": proc.exitCode,
				"signal": nil, "stdoutOffset": so, "stderrOffset": se,
			}
			if proc.signal != "" {
				exit["signal"] = proc.signal
			}
			proc.mu.Unlock()
			_ = enc.Encode(exit)
			return
		}
		select {
		case <-changed:
		case <-tick:
			_ = enc.Encode(map[string]any{"type": "heartbeat"})
			_ = rc.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

// stopCommand ends the command's process group as Boat's SIGTERM does
func (f *fakeBoat) stopCommand(w http.ResponseWriter, r *http.Request) {
	proc, pid := f.proc(w, r)
	if proc == nil {
		return
	}
	f.mu.Lock()
	f.kills = append(f.kills, pid)
	f.mu.Unlock()
	proc.mu.Lock()
	running := proc.cmd != nil && !proc.done
	if running {
		_ = syscall.Kill(-proc.cmd.Process.Pid, syscall.SIGTERM)
	}
	proc.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "type": "command.stopped", "processId": pid, "running": false,
		"signaled": running, "command": nil,
	})
}

func (f *fakeBoat) killed() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.kills)
}

func (f *fakeBoat) attachCalls() []attachCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.attaches)
}

// openStreams counts the attach streams the fake is still serving
func (f *fakeBoat) openStreams() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.streams
}

// running counts the host commands that have not exited
func (f *fakeBoat) running() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, s := range f.sandboxes {
		for _, p := range s.procs {
			p.mu.Lock()
			if p.cmd != nil && !p.done {
				n++
			}
			p.mu.Unlock()
		}
	}
	return n
}

/* Files */

// resolve refuses paths that do not canonicalize under /tmp or /home/user like Boat's agent
func (f *fakeBoat) resolve(w http.ResponseWriter, name string) (string, bool) {
	if !path.IsAbs(name) {
		name = path.Join("/home/user", name)
	}
	name = path.Clean(name)
	existing, rest := name, ""
	for {
		if _, err := os.Lstat(existing); err == nil || existing == "/" {
			break
		}
		rest = path.Join(path.Base(existing), rest)
		existing = path.Dir(existing)
	}
	canonical, err := filepath.EvalSymlinks(existing)
	if err == nil {
		canonical = path.Join(canonical, rest)
	}
	for _, root := range []string{f.tmp, "/home/user"} {
		if err == nil && canonical == root {
			// The sandbox's roots are directories, though the host's /tmp may be a link
			return canonical, true
		}
		if err == nil && strings.HasPrefix(canonical, root+"/") {
			return name, true
		}
	}
	f.fail(w, http.StatusBadRequest, "invalid_path",
		"path must resolve under /home/user or /tmp", nil, nil)
	return "", false
}

// fileError answers a failed file call, a missing path as Boat's sandbox agent does
func (f *fakeBoat) fileError(w http.ResponseWriter, err error) {
	var pe *fs.PathError
	var le *os.LinkError
	switch {
	case errors.Is(err, fs.ErrNotExist) && errors.As(err, &pe):
		f.fail(w, http.StatusBadRequest, "sandbox_direct_failed",
			"No such file or directory: "+pe.Path, nil, nil)
	case errors.Is(err, fs.ErrNotExist) && errors.As(err, &le):
		f.fail(w, http.StatusBadRequest, "sandbox_direct_failed",
			"No such file or directory: "+le.Old, nil, nil)
	case errors.Is(err, syscall.EISDIR), errors.Is(err, syscall.ENOTDIR):
		f.fail(w, http.StatusBadRequest, "invalid_path", err.Error(), nil, nil)
	default:
		f.fail(w, http.StatusBadRequest, "file_error", err.Error(), nil, nil)
	}
}

// track remembers the topmost path a write is about to create so cleanup removes it
func (f *fakeBoat) track(name string) {
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

// liveFile checks the sandbox and the path of a file call
func (f *fakeBoat) liveFile(w http.ResponseWriter, r *http.Request, name string) (string, bool) {
	f.mu.Lock()
	s := f.live(w, r)
	f.mu.Unlock()
	if s == nil {
		return "", false
	}
	return f.resolve(w, name)
}

func (f *fakeBoat) entryJSON(name string, info fs.FileInfo) map[string]any {
	e := map[string]any{
		"name": path.Base(name), "path": name, "isDir": info.IsDir(),
		"isSymlink": info.Mode()&fs.ModeSymlink != 0, "size": info.Size(),
		"mode":       fmt.Sprintf("%04o", info.Mode().Perm()),
		"modifiedAt": info.ModTime().UTC().Format(time.RFC3339Nano),
	}
	switch f.drift {
	case "types":
		e["modifiedAt"], e["type"] = info.ModTime().Unix(), 3
	case "layout":
		e["modifiedAt"] = info.ModTime().UTC().Format("2006-01-02 15:04:05.999999999Z07:00")
	}
	return e
}

func (f *fakeBoat) readFile(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	name, ok := f.liveFile(w, r, q.Get("path"))
	if !ok {
		return
	}
	info, err := os.Stat(name)
	if err != nil {
		f.fileError(w, err)
		return
	}
	if info.IsDir() {
		f.fail(w, http.StatusBadRequest, "invalid_path", name+" is a directory", nil, nil)
		return
	}
	if info.Size() > fakeFileCap {
		f.fail(w, http.StatusBadRequest, "file_too_large", "Files up to 5 MiB", nil, nil)
		return
	}
	data, err := os.ReadFile(name)
	if err != nil {
		f.fileError(w, err)
		return
	}
	enc, content := "utf8", string(data)
	if q.Get("encoding") == "base64" {
		enc, content = "base64", base64.StdEncoding.EncodeToString(data)
	}
	reply := map[string]any{
		"ok": true, "type": "file.read", "success": true, "path": name, "encoding": enc,
		"size": len(data), "content": content,
	}
	if f.drift == "types" {
		reply["type"], reply["success"], reply["size"] = 5, "yes", float64(len(data))+0.5
	}
	writeJSON(w, http.StatusOK, reply)
}

// replace writes data next to name and renames it over, leaving the 0644 of a new file
func replace(name string, data []byte) error {
	tmp := filepath.Join(filepath.Dir(name), "."+filepath.Base(name)+".boat-tmp")
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Chmod(tmp, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, name)
}

// writeFile needs the parent to exist, since Boat's docs do not promise to create it
func (f *fakeBoat) writeFile(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Path     string `json:"path"`
		Content  string `json:"content"`
		Encoding string `json:"encoding"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		f.fail(w, http.StatusBadRequest, "invalid_json", "Request body must be valid JSON.", nil,
			nil)
		return
	}
	name, ok := f.liveFile(w, r, req.Path)
	if !ok {
		return
	}
	data := []byte(req.Content)
	if req.Encoding == "base64" {
		var err error
		if data, err = base64.StdEncoding.DecodeString(req.Content); err != nil {
			f.fail(w, http.StatusBadRequest, "invalid_base64", err.Error(), nil, nil)
			return
		}
	}
	if len(data) > fakeFileCap {
		f.fail(w, http.StatusBadRequest, "file_too_large", "Files up to 5 MiB", nil, nil)
		return
	}
	if info, err := os.Stat(filepath.Dir(name)); err != nil || !info.IsDir() {
		f.fail(w, http.StatusBadRequest, "invalid_path", "parent directory is missing", nil, nil)
		return
	}
	f.track(name)
	if err := replace(name, data); err != nil {
		f.fileError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "type": "file.written", "success": true, "path": name,
		"encoding": req.Encoding, "size": len(data),
	})
}

func (f *fakeBoat) listFiles(w http.ResponseWriter, r *http.Request) {
	name, ok := f.liveFile(w, r, r.URL.Query().Get("path"))
	if !ok {
		return
	}
	dirents, err := os.ReadDir(name)
	if err != nil {
		f.fileError(w, err)
		return
	}
	f.mu.Lock()
	limit := f.listLimit
	f.mu.Unlock()
	truncated := limit > 0 && len(dirents) > limit
	if truncated {
		dirents = dirents[:limit]
	}
	files := []map[string]any{}
	for _, d := range dirents {
		info, err := os.Lstat(filepath.Join(name, d.Name()))
		if err != nil {
			continue
		}
		files = append(files, f.entryJSON(path.Join(name, d.Name()), info))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "type": "file.list", "success": true, "path": name, "files": files,
		"truncated": truncated,
	})
}

// swapAfterInfo runs swap once the next info of name is taken, its answer still the old entry
func (f *fakeBoat) swapAfterInfo(name string, swap func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.swaps[name] = swap
}

// growAfterInfo appends extra to a file once its next info is taken, as a writer in the sandbox
func (f *fakeBoat) growAfterInfo(name string, extra []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.grow[name] = extra
}

func (f *fakeBoat) fileInfo(w http.ResponseWriter, r *http.Request) {
	name, ok := f.liveFile(w, r, r.URL.Query().Get("path"))
	if !ok {
		return
	}
	info, err := os.Lstat(name)
	if err != nil {
		f.fileError(w, err)
		return
	}
	f.mu.Lock()
	extra, grows := f.grow[name]
	delete(f.grow, name)
	swap := f.swaps[name]
	delete(f.swaps, name)
	f.mu.Unlock()
	if swap != nil {
		swap()
	}
	if grows {
		file, err := os.OpenFile(name, os.O_WRONLY|os.O_APPEND, 0)
		if err == nil {
			_, err = file.Write(extra)
			err = errors.Join(err, file.Close())
		}
		if err != nil {
			f.t.Errorf("growing %s: %v", name, err)
		}
	}
	body := f.entryJSON(name, info)
	body["ok"], body["type"], body["success"] = true, "file.info", true
	writeJSON(w, http.StatusOK, body)
}

func (f *fakeBoat) folder(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Path string `json:"path"`
		Mode string `json:"mode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		f.fail(w, http.StatusBadRequest, "invalid_json", "Request body must be valid JSON.", nil,
			nil)
		return
	}
	name, ok := f.liveFile(w, r, req.Path)
	if !ok {
		return
	}
	f.track(name)
	if err := os.MkdirAll(name, 0o755); err != nil {
		f.fileError(w, err)
		return
	}
	info, _ := os.Lstat(name)
	body := f.entryJSON(name, info)
	body["ok"], body["type"], body["success"] = true, "folder.created", true
	writeJSON(w, http.StatusOK, body)
}

func (f *fakeBoat) permissions(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Path string `json:"path"`
		Mode string `json:"mode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || !octalMode.MatchString(req.Mode) {
		f.fail(w, http.StatusBadRequest, "invalid_mode", "mode must be octal bits", nil, nil)
		return
	}
	name, ok := f.liveFile(w, r, req.Path)
	if !ok {
		return
	}
	mode, _ := strconv.ParseUint(req.Mode, 8, 32)
	if err := os.Chmod(name, fs.FileMode(mode)); err != nil {
		f.fileError(w, err)
		return
	}
	f.mu.Lock()
	f.perms = append(f.perms, name+" "+req.Mode)
	f.mu.Unlock()
	info, _ := os.Lstat(name)
	body := f.entryJSON(name, info)
	body["ok"], body["type"], body["success"] = true, "file.permissions_set", true
	writeJSON(w, http.StatusOK, body)
}

func (f *fakeBoat) permissionCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.perms)
}

func (f *fakeBoat) bodySizes(pattern string) []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.bodies[pattern])
}

func (f *fakeBoat) uploadURL(w http.ResponseWriter, r *http.Request) {
	f.grant(w, r, http.MethodPut, "/upload/", "file.upload_url")
}

func (f *fakeBoat) downloadURL(w http.ResponseWriter, r *http.Request) {
	f.grant(w, r, http.MethodGet, "/download/", "file.download_url")
}

// grant signs a URL bound to one path and one method, as both URL routes do
func (f *fakeBoat) grant(w http.ResponseWriter, r *http.Request, method, route, kind string) {
	var req struct {
		Path string `json:"path"`
		TTL  *int   `json:"ttlSeconds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		f.fail(w, http.StatusBadRequest, "invalid_json", "Request body must be valid JSON.", nil,
			nil)
		return
	}
	ttl := 900
	if req.TTL != nil {
		ttl = *req.TTL
	}
	if ttl < 1 || ttl > 3600 {
		f.fail(w, http.StatusBadRequest, "invalid_ttl", "ttlSeconds must be 1 to 3600", nil, nil)
		return
	}
	name, ok := f.liveFile(w, r, req.Path)
	if !ok {
		return
	}
	token := make([]byte, 16)
	_, _ = rand.Read(token)
	grant := uploadGrant{
		sandbox: r.PathValue("id"), path: name, method: method,
		expires: time.Now().Add(time.Duration(ttl) * time.Second),
	}
	f.mu.Lock()
	f.grants[hex.EncodeToString(token)] = grant
	f.ttls = append(f.ttls, ttl)
	f.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "type": kind, "method": method, "path": name,
		"url":       cmp.Or(f.uploadBase, f.srv.URL) + route + hex.EncodeToString(token),
		"expiresAt": grant.expires.UTC().Format(time.RFC3339),
	})
}

// signed finds the live grant for a URL token and method, answering 403 like Boat otherwise
func (f *fakeBoat) signed(
	w http.ResponseWriter, r *http.Request, method string,
) (uploadGrant, bool) {
	if r.Header.Get("Authorization") != "" {
		f.t.Errorf("a signed URL request carried an Authorization header")
	}
	f.mu.Lock()
	grant, ok := f.grants[r.PathValue("token")]
	s := f.sandboxes[grant.sandbox]
	expired := f.expired || time.Now().After(grant.expires)
	f.mu.Unlock()
	if !ok || grant.method != method || expired || s == nil || s.deleted {
		http.Error(w, "invalid or expired URL", http.StatusForbidden)
		return uploadGrant{}, false
	}
	return grant, true
}

// upload streams the body into the granted path at once and keeps a cut upload, unless whole
func (f *fakeBoat) upload(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.uploads = append(f.uploads, r.Header.Clone())
	hold, whole := f.holdUploads, f.wholeUploads
	f.mu.Unlock()
	grant, ok := f.signed(w, r, http.MethodPut)
	if !ok {
		return
	}
	f.track(grant.path)
	if err := os.MkdirAll(filepath.Dir(grant.path), 0o755); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if whole {
		data, err := io.ReadAll(r.Body)
		if err != nil || int64(len(data)) != r.ContentLength {
			return
		}
		if err := os.WriteFile(grant.path, data, 0o644); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "size": len(data)})
		return
	}
	file, err := os.OpenFile(grant.path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer func() { _ = file.Close() }()
	if hold != nil {
		<-hold
	}
	n, err := io.Copy(file, r.Body)
	if err != nil {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "size": n})
}

// download serves the granted file with Range, the whole of it when ignoreRange is set
func (f *fakeBoat) download(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.downloads = append(f.downloads, r.Header.Clone())
	ignore, short, shift, enc := f.ignoreRange, f.shortDownload, f.shiftRange, f.downloadEncoding
	f.mu.Unlock()
	grant, ok := f.signed(w, r, http.MethodGet)
	if !ok {
		return
	}
	file, err := os.Open(grant.path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		http.Error(w, "not a regular file", http.StatusBadRequest)
		return
	}
	if ignore {
		r.Header.Del("Range")
	}
	from, to, ok := strings.Cut(strings.TrimPrefix(r.Header.Get("Range"), "bytes="), "-")
	if start, err := strconv.ParseInt(from, 10, 64); shift != 0 && ok && err == nil {
		r.Header.Set("Range", "bytes="+strconv.FormatInt(start+shift, 10)+"-"+to)
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	if enc != "" {
		w.Header().Set("Content-Encoding", enc)
	}
	if short {
		w.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))
		_, _ = io.CopyN(w, file, info.Size()/2)
		_ = http.NewResponseController(w).Flush()
		panic(http.ErrAbortHandler)
	}
	http.ServeContent(w, r, "", info.ModTime(), file)
}

func (f *fakeBoat) move(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Source      string `json:"source"`
		Destination string `json:"destination"`
		Overwrite   bool   `json:"overwrite"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		f.fail(w, http.StatusBadRequest, "invalid_json", "Request body must be valid JSON.", nil,
			nil)
		return
	}
	from, ok := f.liveFile(w, r, req.Source)
	if !ok {
		return
	}
	to, ok := f.resolve(w, req.Destination)
	if !ok {
		return
	}
	if _, err := os.Lstat(to); err == nil && !req.Overwrite {
		f.fail(w, http.StatusConflict, "file_exists", "destination exists", nil, nil)
		return
	}
	f.track(to)
	if err := os.MkdirAll(filepath.Dir(to), 0o755); err != nil {
		f.fileError(w, err)
		return
	}
	if err := os.Rename(from, to); err != nil {
		f.fileError(w, err)
		return
	}
	f.mu.Lock()
	f.moves = append(f.moves, [2]string{from, to})
	f.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "type": "file.moved", "success": true, "path": from, "destination": to,
	})
}

func (f *fakeBoat) deleteFile(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	name, ok := f.liveFile(w, r, q.Get("path"))
	if !ok {
		return
	}
	if name == f.tmp || name == "/home/user" {
		f.fail(w, http.StatusBadRequest, "invalid_path", "the root cannot be deleted", nil, nil)
		return
	}
	remove := os.Remove
	if q.Get("recursive") == "true" {
		remove = os.RemoveAll
	}
	if _, err := os.Lstat(name); err != nil {
		f.fileError(w, err)
		return
	}
	if err := remove(name); err != nil {
		f.fileError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "type": "file.deleted", "path": name})
}

// expireGrants makes every signed URL answer as one past its expiry
func (f *fakeBoat) expireGrants() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.expired = true
}

// grantTTLs lists the ttlSeconds of every signed URL asked for, oldest first
func (f *fakeBoat) grantTTLs() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.ttls)
}

// downloadHeaders lists the request headers of every signed download, oldest first
func (f *fakeBoat) downloadHeaders() []http.Header {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.downloads)
}

// movesSeen lists every move as its source and destination, oldest first
func (f *fakeBoat) movesSeen() [][2]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.moves)
}

func (f *fakeBoat) uploadCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.uploads)
}

// fakeDir is a directory under /tmp, where Boat's file calls may reach
func fakeDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "bugnet-boat-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func (f *fakeBoat) setLimits(status int, body string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.limitsStatus, f.limits = status, body
}

func (f *fakeBoat) readLimits(w http.ResponseWriter, _ *http.Request) {
	f.mu.Lock()
	status, body := f.limitsStatus, f.limits
	f.limitsCalls++
	f.mu.Unlock()
	if status != 0 && status != http.StatusOK {
		f.fail(w, status, "forbidden", "the key lacks account.read", nil, nil)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	f.mu.Lock()
	rate := f.rateHeader
	f.mu.Unlock()
	if rate != "" {
		w.Header().Set("RateLimit", rate)
	}
	_, _ = io.WriteString(w, body)
}

func (f *fakeBoat) setMe(status int, body string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.meStatus, f.me = status, body
}

func (f *fakeBoat) readMe(w http.ResponseWriter, _ *http.Request) {
	f.mu.Lock()
	status, body := f.meStatus, f.me
	f.meCalls++
	f.mu.Unlock()
	if status != 0 && status != http.StatusOK {
		f.fail(w, status, "forbidden", "the key lacks account.read", nil, nil)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, body)
}

// setNetwork sets the ip and sshEndpoint a sandbox reports, nil for JSON null
func (f *fakeBoat) setNetwork(id string, ip, sshEndpoint any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sandboxes[id].ip, f.sandboxes[id].sshEndpoint = ip, sshEndpoint
}

// setState moves a sandbox to a state
func (f *fakeBoat) setState(id, state string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sandboxes[id].state = state
}

// degrade makes Boat report a sandbox degraded since then
func (f *fakeBoat) degrade(id string, since time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sandboxes[id].degradedSince = since
}

// stopWith moves a sandbox to a state with Boat's stop text
func (f *fakeBoat) stopWith(id, state, text string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sandboxes[id].state, f.sandboxes[id].stopText = state, text
}
