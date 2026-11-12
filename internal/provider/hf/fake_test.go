package hf

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
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
	// echo adds the request's Authorization header to the message
	echo  bool
	times int
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
	timeout                                  int
	// runAfter turns a SCHEDULING Job RUNNING on its runAfter-th read, never when 0
	runAfter, reads int
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
	return map[string]any{
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
}

func newFake(t *testing.T) *fakeHF {
	t.Helper()
	f := &fakeHF{
		t: t, faults: map[string]*fault{}, hardware: bytes.Clone(compiledHardware),
		pageSize: 50,
	}
	mux := http.NewServeMux()
	for pattern, h := range map[string]http.HandlerFunc{
		"GET /api/jobs/hardware":  f.listHardware,
		"GET /api/jobs/{ns}":      f.listJobs,
		"GET /api/jobs/{ns}/{id}": f.getJob,
	} {
		mux.HandleFunc(pattern, f.gate(pattern, h))
	}
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("adapter called the unexpected route %s %s", r.Method, r.URL.Path)
		writeError(w, http.StatusNotFound, "not in the fake")
	})
	f.srv = httptest.NewTLSServer(mux)
	t.Cleanup(f.srv.Close)
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

// provider builds an adapter whose Jobs client reaches the fake
func (f *fakeHF) provider(t *testing.T, opts Options) *Provider {
	t.Helper()
	p, err := newProvider(opts)
	if err != nil {
		t.Fatal(err)
	}
	p.jobs.client.Transport = f.transport()
	return p
}

// inject makes the next calls to the route pattern fail
func (f *fakeHF) inject(pattern string, ft fault) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.faults[pattern] = &ft
}

// addJob adds a Job in stage with labels and returns it
func (f *fakeHF) addJob(stage string, labels map[string]string) *fakeJob {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	j := &fakeJob{
		id: fmt.Sprintf("6911%020x", f.nextID), flavor: "t4-small", stage: stage,
		labels: labels, createdAt: time.Now().UTC(), timeout: 5400,
	}
	f.jobs = append(f.jobs, j)
	return j
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

// gate records the request, then applies the pace, a fault and the token check
func (f *fakeHF) gate(pattern string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		now := time.Now()
		f.mu.Lock()
		f.requests = append(f.requests, recorded{
			method: r.Method, host: r.Host, path: r.URL.Path, query: r.URL.Query(),
			header: r.Header.Clone(), body: body, at: now,
		})
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
		ft := f.faults[pattern]
		if ft != nil {
			if ft.times > 0 {
				if ft.times--; ft.times == 0 {
					delete(f.faults, pattern)
				}
			}
		}
		f.mu.Unlock()
		if ft != nil {
			if ft.echo {
				echoed := *ft
				echoed.message += ", got " + r.Header.Get("Authorization")
				ft = &echoed
			}
			ft.write(w)
			return
		}
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
	out := j.json()
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
		if (len(q["stage"]) == 0 || slices.Contains(q["stage"], j.stage)) && hasLabels(j, q) {
			match = append(match, j.json())
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

// hasLabels reports whether j holds every key=value of the query's label filters
func hasLabels(j *fakeJob, q url.Values) bool {
	for _, kv := range q["label"] {
		k, v, _ := strings.Cut(kv, "=")
		if got, ok := j.labels[k]; !ok || got != v {
			return false
		}
	}
	return true
}
