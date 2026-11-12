package hf

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeServerName is the name httptest's certificate holds, which the routed transport verifies
const fakeServerName = "example.com"

// fault answers matching calls with an error, every one when times is 0
type fault struct {
	status int
	// message is written as the error field of the Hub's error envelope
	message string
	header  map[string]string
	// raw replaces the envelope as the answer's body when set
	raw string
	// drop closes the connection once the request has arrived, with no answer
	drop bool
	// cut sends the route's own head and only the first cut bytes of its body, then closes
	cut int
	// echo adds the request's Authorization header to the message
	echo bool
	// served runs the route before the fault answers, so the call took effect unseen
	served bool
	// after lets that many matching calls through before the fault answers
	after int
	times int
	// only limits the fault to the calls whose body holds it, when set
	only string
	// serve answers a sandbox call in place of its route, when set
	serve http.HandlerFunc
}

// recorded is one request the fake served, kept for the secret and pacing checks
type recorded struct {
	method, host, path string
	query              url.Values
	header             http.Header
	body               []byte
	at                 time.Time
}

// fakeJob is one Job of the fake namespace
type fakeJob struct {
	id, flavor, stage, message, cancelReason string
	labels                                   map[string]string
	createdAt                                time.Time
	// finishedAt is when the Job ended, left out of its answers while zero
	finishedAt time.Time
	timeout    int
	// runAfter turns a SCHEDULING Job RUNNING on its runAfter-th read, never when 0
	runAfter, reads int
	// spec is the create body a POST sent, nil for a Job a test added
	spec *fakeSpec
	// token is the SBX_TOKEN secret its sbx-server checks
	token string
	// unlisted hides the Job from every list, as a list that lags behind a create would
	unlisted bool
	// health replaces the sbx-server's /health body when set
	health string
}

// fakeSpec is a create body as HF's schema reads it
type fakeSpec struct {
	DockerImage    string            `json:"dockerImage"`
	Command        []string          `json:"command"`
	Arguments      []string          `json:"arguments"`
	Environment    map[string]string `json:"environment"`
	Secrets        map[string]string `json:"secrets"`
	Flavor         string            `json:"flavor"`
	Arch           string            `json:"arch"`
	TimeoutSeconds *int              `json:"timeoutSeconds"`
	Attempts       *int              `json:"attempts"`
	Labels         map[string]string `json:"labels"`
	Expose         *struct {
		Ports       []int `json:"ports"`
		PortsPublic []int `json:"portsPublic"`
	} `json:"expose"`
}

// json writes the Job with the field shapes of HF's Job object
func (j *fakeJob) json() map[string]any {
	var message any
	if j.message != "" {
		message = j.message
	}
	status := map[string]any{"stage": j.stage, "message": message}
	if j.cancelReason != "" {
		status["cancelReason"] = j.cancelReason
	}
	out := map[string]any{
		"id": j.id, "type": "job", "createdAt": j.createdAt.Format(time.RFC3339Nano),
		"flavor": j.flavor, "labels": j.labels, "timeout": j.timeout, "status": status,
		"arch": "amd64", "dockerImage": "ghcr.io/bugnet-sh/bugnet-base@sha256:2a47",
		"command": []string{"/bin/sh", "-c", "exec /tmp/.sbx-server"}, "arguments": []string{},
		"environment": map[string]string{"SBX_PORT": "49983"}, "secrets": []string{"SBX_TOKEN"},
		"owner":     map[string]any{"id": "6650", "name": testNamespace, "type": "org"},
		"createdBy": map[string]any{"id": "6651", "name": "bugnet-bot"},
		// Fields the adapter never reads, which it must ignore
		"durations": map[string]any{"schedulingSecs": 4, "runningSecs": 0, "totalSecs": 4},
		"retry":     0,
	}
	if !j.finishedAt.IsZero() {
		out["finishedAt"] = j.finishedAt.Format(time.RFC3339Nano)
	}
	if sp := j.spec; sp != nil {
		// A Job read shows the environment and the secrets' names, never their values
		out["dockerImage"], out["command"], out["arguments"] = sp.DockerImage, sp.Command,
			sp.Arguments
		out["environment"], out["secrets"] = sp.Environment, slices.Sorted(maps.Keys(sp.Secrets))
		out["expose"] = sp.Expose
	}
	return out
}

// fakeHF is an in-process HF Jobs API, reached through a transport that routes huggingface.co
type fakeHF struct {
	t   *testing.T
	srv *httptest.Server

	mu       sync.Mutex
	requests []recorded
	// dials are the addresses the adapter's transport dialled
	dials  []string
	faults map[string]*fault
	// hardware is what GET /api/jobs/hardware answers
	hardware []byte
	jobs     []*fakeJob
	nextID   int
	pageSize int
	// nextLink replaces the link of every next page when set
	nextLink string
	// pace answers 429 to a call sooner than 70% of 1/pace after the last one served, 0 for none
	pace       int
	lastServed time.Time
	// paced counts the 429s the pace answered
	paced int
	// runAfter is each created Job's runAfter
	runAfter int
	// unlistNew hides every Job created from the lists
	unlistNew bool
	// unfiltered makes the list ignore its label and stage filters
	unfiltered bool
	// cancelEnded is the status a cancel of an ended Job answers, 200 with the Job when 0
	cancelEnded int
	// createDelay holds each create's answer back once its Job exists
	createDelay time.Duration
	// show rewrites each Job the fake answers, as HF's read may differ from what was sent
	show func(job map[string]any)
	// sbx is where every Job's sbx-server runs its commands and keeps its files
	sbx fakeSBX
	// holdExecs holds that many exec answers once their command started, until execGate closes
	holdExecs int
	execGate  chan struct{}
}

func newFake(t *testing.T) *fakeHF {
	t.Helper()
	f := &fakeHF{
		t: t, faults: map[string]*fault{}, hardware: bytes.Clone(compiledHardware),
		pageSize: 50, runAfter: 1, sbx: newFakeSBX(t),
	}
	mux := http.NewServeMux()
	for pattern, h := range map[string]http.HandlerFunc{
		"GET /api/jobs/hardware":          f.listHardware,
		"GET /api/jobs/{ns}":              f.listJobs,
		"POST /api/jobs/{ns}":             f.createJob,
		"GET /api/jobs/{ns}/{id}":         f.getJob,
		"POST /api/jobs/{ns}/{id}/cancel": f.cancelJob,
		authCheckRoute:                    f.authCheck,
	} {
		mux.HandleFunc(pattern, f.gate(pattern, h))
	}
	mux.HandleFunc(healthRoute, f.sbxGate(healthRoute, f.health))
	for pattern, h := range map[string]http.HandlerFunc{
		sbxExecRoute: f.exec, sbxReadRoute: f.read, sbxWriteRoute: f.write,
		sbxListRoute: f.list, sbxStatRoute: f.stat, sbxDeleteRoute: f.remove,
	} {
		mux.HandleFunc(pattern, f.sbxGate(pattern, f.counted(pattern, f.sbxRoute(h))))
	}
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("adapter called the unexpected route %s %s", r.Method, r.URL.Path)
		writeError(w, http.StatusNotFound, "not in the fake")
	})
	f.srv = httptest.NewTLSServer(mux)
	t.Cleanup(f.srv.Close)
	// Commands left running hold their handlers, which the server's Close waits for
	t.Cleanup(f.stopCommands)
	return f
}

// transport sends every connection to the fake whatever host it names, recording the host
func (f *fakeHF) transport() *http.Transport {
	tr := f.srv.Client().Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.ServerName = fakeServerName
	addr := f.srv.Listener.Addr().String()
	tr.DialContext = func(ctx context.Context, network, to string) (net.Conn, error) {
		f.mu.Lock()
		f.dials = append(f.dials, to)
		f.mu.Unlock()
		var d net.Dialer
		return d.DialContext(ctx, network, addr)
	}
	return tr
}

// provider builds an adapter whose Jobs and sandbox clients reach the fake
func (f *fakeHF) provider(t *testing.T, opts Options) *Provider {
	t.Helper()
	p, err := newProvider(opts)
	if err != nil {
		t.Fatal(err)
	}
	p.jobs.client.Transport = f.transport()
	p.sbx.client.Transport = f.transport()
	p.execDir, p.setsid, p.mv = f.sbx.execDir, filepath.Join(f.sbx.bin, "setsid"),
		filepath.Join(f.sbx.bin, "mv")
	p.retryWait = 10 * time.Millisecond
	return p
}

// inject makes the next calls to the route pattern fail
func (f *fakeHF) inject(pattern string, ft fault) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.faults[pattern] = &ft
}

// clear drops the route pattern's fault
func (f *fakeHF) clear(pattern string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.faults, pattern)
}

// addJob adds a Job in stage with labels and returns it
func (f *fakeHF) addJob(stage string, labels map[string]string) *fakeJob {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.newJob(stage, labels)
}

// newJob adds a Job with f.mu held
func (f *fakeHF) newJob(stage string, labels map[string]string) *fakeJob {
	f.nextID++
	j := &fakeJob{
		id: fmt.Sprintf("6911%020x", f.nextID), flavor: "t4-small", stage: stage,
		labels: labels, createdAt: time.Now().UTC(), timeout: 5400,
	}
	f.jobs = append(f.jobs, j)
	return j
}

// answerOf is the Job as the fake answers it, with f.mu held
func (f *fakeHF) answerOf(j *fakeJob) map[string]any {
	out := j.json()
	if f.show != nil {
		f.show(out)
	}
	return out
}

// job returns the Job with id, nil when the fake has none
func (f *fakeHF) job(id string) *fakeJob {
	f.mu.Lock()
	defer f.mu.Unlock()
	if i := slices.IndexFunc(f.jobs, func(j *fakeJob) bool { return j.id == id }); i >= 0 {
		return f.jobs[i]
	}
	return nil
}

// stageOf is the stage of the Job with id, empty when the fake has none
func (f *fakeHF) stageOf(id string) string {
	j := f.job(id)
	if j == nil {
		return ""
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return j.stage
}

// set changes a Job under the fake's lock
func (f *fakeHF) set(j *fakeJob, change func(j *fakeJob)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	change(j)
}

// jobCount is how many Jobs the namespace holds, ended ones too
func (f *fakeHF) jobCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.jobs)
}

// requestsTo returns the requests made to one method and path
func (f *fakeHF) requestsTo(method, path string) []recorded {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []recorded
	for _, r := range f.requests {
		if r.method == method && r.path == path {
			out = append(out, r)
		}
	}
	return out
}

// dialed returns the addresses the adapter's transport dialled
func (f *fakeHF) dialed() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.dials)
}

// pacedCount returns how many 429s the pace answered
func (f *fakeHF) pacedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.paced
}

// all returns every request the fake got
func (f *fakeHF) all() []recorded {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.requests)
}

// record keeps the request, leaving its body readable again, and returns when it came
func (f *fakeHF) record(r *http.Request) time.Time {
	body, _ := io.ReadAll(r.Body)
	r.Body = io.NopCloser(bytes.NewReader(body))
	now := time.Now()
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, recorded{
		method: r.Method, host: r.Host, path: r.URL.Path, query: r.URL.Query(),
		header: r.Header.Clone(), body: body, at: now,
	})
	return now
}

// takeFault returns the route's fault for a call with body, counting it, with f.mu held
func (f *fakeHF) takeFault(pattern string, body []byte) *fault {
	ft := f.faults[pattern]
	if ft != nil && ft.only != "" && !bytes.Contains(body, []byte(ft.only)) {
		return nil
	}
	if ft != nil && ft.after > 0 {
		ft.after--
		return nil
	}
	if ft != nil && ft.times > 0 {
		if ft.times--; ft.times == 0 {
			delete(f.faults, pattern)
		}
	}
	return ft
}

// gate records the request, then applies the pace, a fault and the token check
func (f *fakeHF) gate(pattern string, h http.HandlerFunc) http.HandlerFunc {
	serve := func(w http.ResponseWriter, r *http.Request) {
		if pattern != "GET /api/jobs/hardware" {
			if r.Header.Get("Authorization") != "Bearer "+fakeToken {
				writeError(w, http.StatusUnauthorized,
					"Invalid credentials in Authorization header")
				return
			}
			if r.PathValue("ns") != testNamespace {
				writeError(w, http.StatusNotFound, "Namespace not found")
				return
			}
		}
		h(w, r)
	}
	return func(w http.ResponseWriter, r *http.Request) {
		now := f.record(r)
		f.mu.Lock()
		if f.pace > 0 && !f.lastServed.IsZero() &&
			now.Sub(f.lastServed) < time.Second*7/10/time.Duration(f.pace) {
			f.paced++
			f.mu.Unlock()
			w.Header().Set("RateLimit", `"api";r=0;t=1`)
			w.Header().Set("RateLimit-Policy", `"fixed window";"api";q=1000;w=300`)
			writeError(w, http.StatusTooManyRequests, "You have exceeded the rate limit")
			return
		}
		f.lastServed = now
		ft := f.takeFault(pattern, nil)
		f.mu.Unlock()
		if ft == nil {
			serve(w, r)
			return
		}
		if ft.served {
			serve(httptest.NewRecorder(), r)
		}
		ft.answer(w, r)
	}
}

// sbxGate records a call to a Job's sbx-server and applies a fault, none of the Hub's checks
func (f *fakeHF) sbxGate(pattern string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f.record(r)
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		f.mu.Lock()
		ft := f.takeFault(pattern, body)
		f.mu.Unlock()
		switch {
		case ft == nil:
			h(w, r)
			return
		case ft.cut > 0:
			cutAnswer(w, r, h, ft.cut)
			return
		case ft.serve != nil:
			ft.serve(w, r)
			return
		}
		if ft.served {
			h(httptest.NewRecorder(), r)
		}
		ft.answer(w, r)
	}
}

// answer writes the fault, the request's Authorization header in its message when it echoes
func (ft *fault) answer(w http.ResponseWriter, r *http.Request) {
	if ft.echo {
		echoed := *ft
		echoed.message += ", got " + r.Header.Get("Authorization")
		ft = &echoed
	}
	ft.write(w)
}

func (ft *fault) write(w http.ResponseWriter) {
	if ft.drop {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
		return
	}
	for k, v := range ft.header {
		w.Header().Set(k, v)
	}
	if ft.raw != "" {
		w.WriteHeader(ft.status)
		_, _ = io.WriteString(w, ft.raw)
		return
	}
	writeError(w, ft.status, ft.message)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (f *fakeHF) listHardware(w http.ResponseWriter, _ *http.Request) {
	f.mu.Lock()
	raw := bytes.Clone(f.hardware)
	f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, _ = w.Write(raw)
}

func (f *fakeHF) getJob(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	i := slices.IndexFunc(f.jobs, func(j *fakeJob) bool { return j.id == r.PathValue("id") })
	if i < 0 {
		f.mu.Unlock()
		writeError(w, http.StatusNotFound, "Job not found")
		return
	}
	j := f.jobs[i]
	j.reads++
	if j.stage == "SCHEDULING" && j.runAfter > 0 && j.reads >= j.runAfter {
		j.stage = "RUNNING"
	}
	out := f.answerOf(j)
	f.mu.Unlock()
	writeJSON(w, http.StatusOK, out)
}

// listJobs answers the Jobs holding every label in any stage asked, a page at a time
func (f *fakeHF) listJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	from, _ := strconv.Atoi(q.Get("cursor"))
	f.mu.Lock()
	var match []map[string]any
	for _, j := range f.jobs {
		if !j.unlisted && (f.unfiltered || (len(q["stage"]) == 0 ||
			slices.Contains(q["stage"], j.stage)) && carries(j, q)) {
			match = append(match, f.answerOf(j))
		}
	}
	size, override := f.pageSize, f.nextLink
	f.mu.Unlock()
	from = min(max(from, 0), len(match))
	to := min(from+size, len(match))
	if to < len(match) {
		next := override
		if next == "" {
			q.Set("cursor", strconv.Itoa(to))
			next = "https://huggingface.co" + r.URL.Path + "?" + q.Encode()
		}
		w.Header().Set("Link", "<"+next+`>; rel="next"`)
	}
	writeJSON(w, http.StatusOK, append([]map[string]any{}, match[from:to]...))
}

// carries reports whether j holds every key=value of the query's label filters
func carries(j *fakeJob, q url.Values) bool {
	for _, kv := range q["label"] {
		k, v, _ := strings.Cut(kv, "=")
		if got, ok := j.labels[k]; !ok || got != v {
			return false
		}
	}
	return true
}

// authCheckRoute answers whether the token may write Jobs in the namespace
const authCheckRoute = "POST /api/jobs/{ns}/auth-check/write"

// authCheck answers 200 with no body, as the gate already refused a wrong token or namespace
func (f *fakeHF) authCheck(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// healthRoute is sbx-server's health route, reached on a Job's own host
const healthRoute = "GET /health"

var (
	// fakeLabelKey, fakeLabelValue, fakeEnvKey and fakeSecretKey are HF's schema patterns
	fakeLabelKey   = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,100}$`)
	fakeLabelValue = regexp.MustCompile(`^[a-zA-Z0-9_-]{0,100}$`)
	fakeEnvKey     = regexp.MustCompile(`^[a-zA-Z][_a-zA-Z0-9]+$`)
	fakeSecretKey  = regexp.MustCompile(`^[a-zA-Z][_a-zA-Z0-9]*$`)
	// sbxHost is the host of a Job's sbx-server port
	sbxHost = regexp.MustCompile(`^([A-Za-z0-9_-]+)--49983\.hf\.jobs$`)
)

// createJob checks the body as HF's create schema does and starts a SCHEDULING Job
func (f *fakeHF) createJob(w http.ResponseWriter, r *http.Request) {
	var sp fakeSpec
	dec := json.NewDecoder(r.Body)
	// The adapter sends nothing HF's schema lacks
	dec.DisallowUnknownFields()
	if err := dec.Decode(&sp); err != nil {
		writeError(w, http.StatusUnprocessableEntity, "Invalid body: "+err.Error())
		return
	}
	if why := sp.refused(); why != "" {
		writeError(w, http.StatusUnprocessableEntity, why)
		return
	}
	f.mu.Lock()
	j := f.newJob("SCHEDULING", sp.Labels)
	j.flavor, j.spec, j.token, j.runAfter = sp.Flavor, &sp, sp.Secrets["SBX_TOKEN"], f.runAfter
	j.unlisted = f.unlistNew
	if sp.TimeoutSeconds != nil {
		j.timeout = *sp.TimeoutSeconds
	}
	out, delay := f.answerOf(j), f.createDelay
	f.mu.Unlock()
	time.Sleep(delay)
	writeJSON(w, http.StatusOK, out)
}

// refused says why HF's schema would refuse the body, empty when it takes it
func (sp *fakeSpec) refused() string {
	if _, ok := compiled[sp.Flavor]; !ok {
		return "flavor must be one of the hardware flavors"
	}
	switch {
	case sp.DockerImage == "":
		return "dockerImage must not be empty"
	case len(sp.Command) == 0 || slices.Contains(sp.Command, ""):
		return "command must hold non-empty strings"
	case sp.Arch != "" && sp.Arch != "amd64" && sp.Arch != "arm64":
		return "arch must be amd64 or arm64"
	case sp.TimeoutSeconds != nil && *sp.TimeoutSeconds <= 0:
		return "timeoutSeconds must be greater than 0"
	case sp.Attempts != nil && *sp.Attempts < 1:
		return "attempts must be at least 1"
	}
	for k, v := range sp.Labels {
		if !fakeLabelKey.MatchString(k) || !fakeLabelValue.MatchString(v) {
			return "labels must match the label patterns"
		}
	}
	for k := range sp.Environment {
		if !fakeEnvKey.MatchString(k) {
			return "environment keys must match the pattern"
		}
	}
	for k := range sp.Secrets {
		if !fakeSecretKey.MatchString(k) {
			return "secrets keys must match the pattern"
		}
	}
	if e := sp.Expose; e != nil {
		for _, port := range e.PortsPublic {
			if !slices.Contains(e.Ports, port) {
				return "portsPublic must be a subset of ports"
			}
		}
	}
	return ""
}

// cancelJob cancels a live Job and answers the Job, an ended one as cancelEnded says
func (f *fakeHF) cancelJob(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	i := slices.IndexFunc(f.jobs, func(j *fakeJob) bool { return j.id == r.PathValue("id") })
	if i < 0 {
		f.mu.Unlock()
		writeError(w, http.StatusNotFound, "Job not found")
		return
	}
	j, status := f.jobs[i], f.cancelEnded
	if j.stage == "SCHEDULING" || j.stage == "RUNNING" {
		j.stage, status, j.finishedAt = "CANCELED", 0, time.Now().UTC()
	}
	out := f.answerOf(j)
	f.mu.Unlock()
	if status != 0 {
		writeError(w, status, "Job has already finished")
		return
	}
	writeJSON(w, http.StatusOK, out)
}

// health answers as sbx-server 0.7.0 does once its Job runs, with more for its own token
func (f *fakeHF) health(w http.ResponseWriter, r *http.Request) {
	m := sbxHost.FindStringSubmatch(r.Host)
	var j *fakeJob
	if m != nil {
		j = f.job(m[1])
	}
	f.mu.Lock()
	running := j != nil && j.stage == "RUNNING"
	var token, body string
	if j != nil {
		token, body = j.token, j.health
	}
	f.mu.Unlock()
	switch {
	case !running:
		// HF's proxy answers for a Job whose port nothing listens on yet
		w.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(w, "<html>502 Bad Gateway</html>")
		return
	case body != "":
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
		return
	}
	out := map[string]any{"status": "ok", "protocol": 3}
	if token != "" && r.Header.Get("X-Sandbox-Token") == token {
		out["version"], out["uptime_ms"], out["sandboxes"] = "0.7.0", 1200, 1
		out["mode"], out["auth"] = "dedicated", "required"
		out["landlock"] = map[string]any{"abi": 0, "features": []string{}}
	}
	writeJSON(w, http.StatusOK, out)
}
