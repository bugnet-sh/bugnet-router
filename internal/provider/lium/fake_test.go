package lium

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"
)

const fakeKey = "lium-fake-key"

// fault answers matching calls with an error envelope, after serving them when after is set
type fault struct {
	status  int
	code    string
	message string
	header  map[string]string
	// raw replaces the envelope as the answer's body when set
	raw   string
	drop  bool
	after bool
	times int
	// skip lets this many matching calls through before the fault applies
	skip int
	// then takes this fault's place once its times are spent
	then *fault
}

// fakeNode is one node of the listing, its fields as GET /executors writes them
type fakeNode struct {
	id, gpuClass, machine, tier, country, cpuModel string
	gpus, free, vcpu                               int
	ramGiB, diskGiB                                float64
	// price is USD per GPU and hour, written as null when unpriced is set
	price    float64
	unpriced bool
	// pending is pending_price_per_hour, null when nil
	pending      *float64
	notice       bool
	removal      bool
	unreliable   bool
	downloadMbps float64
	// noMachine writes machine_name as null
	noMachine bool
}

// euNode is a node every rule of design 3.5 lets lium-rtx3090 rent, in Germany
func euNode(id string) fakeNode {
	return fakeNode{
		id: id, gpuClass: "NVIDIA GeForce RTX 3090", machine: "NVIDIA GeForce RTX 3090",
		tier: "secure", country: "DE", cpuModel: "AMD Ryzen 9 5950X 16-Core Processor",
		gpus: 1, free: 1, vcpu: 16, ramGiB: 64, diskGiB: 500, price: 0.28, downloadMbps: 900,
	}
}

// json writes the node with the field shapes GET /executors used on 2026-11-11
func (n fakeNode) json() map[string]any {
	kib := func(gib float64) float64 { return gib * 1024 * 1024 }
	var price any = n.price
	if n.unpriced {
		price = nil
	}
	var pending any
	if n.pending != nil {
		pending = *n.pending
	}
	var notice, removal any
	if n.notice {
		notice = map[string]any{
			"starting_at": "2026-11-12T00:00:00Z", "expiring_at": "2026-11-12T06:00:00Z",
			"permanent_removal": false,
		}
	}
	if n.removal {
		removal = "2026-11-13T00:00:00Z"
	}
	var machine any = n.machine
	if n.noMachine {
		machine = nil
	}
	return map[string]any{
		"id": n.id, "machine_name": machine, "gpu_class": n.gpuClass,
		"gpu_class_display": n.gpuClass, "gpu_count": n.gpus, "available_gpu_count": n.free,
		"min_gpu_count_for_rental": nil, "is_whole_host_free": n.free == n.gpus,
		"price_per_gpu": price, "pending_price_per_hour": pending,
		"price_change_effective_date": nil, "tier": n.tier,
		"location": map[string]any{
			"id": "loc-" + n.id, "country": "Somewhere", "country_code": n.country,
			"city": "City", "lat": 50.1, "lon": 8.6, "source": nil,
		},
		"scheduled_notice": notice, "removal_at": removal,
		"frequent_rent_failures": n.unreliable, "reliability_score": 0.98,
		"effective_download_speed_mbps": n.downloadMbps,
		"specs": map[string]any{
			"cpu": map[string]any{"count": n.vcpu, "model": n.cpuModel},
			"ram": map[string]any{"total": kib(n.ramGiB), "pod_reserve": 4194304},
			"hard_disk": map[string]any{
				"total": int64(kib(n.diskGiB)), "free": int64(kib(n.diskGiB) / 2),
				"disk_type": "nvme",
			},
			"gpu":            map[string]any{"count": n.gpus, "details": []any{}},
			"sysbox_runtime": true, "kernel": "6.8.0",
		},
		// Fields the adapter never reads, which it must ignore
		"miner_hotkey": "5F7X", "nvlink": nil, "interconnect": map[string]any{"matrix": nil},
		"tier_future": "a value Lium may add",
	}
}

// recorded is one request the fake served, kept for the secret and pacing checks
type recorded struct {
	method, path string
	query        url.Values
	header       http.Header
	body         []byte
	at           time.Time
}

type fakeLium struct {
	t   *testing.T
	srv *httptest.Server

	mu       sync.Mutex
	nodes    []fakeNode
	faults   map[string]*fault
	requests []recorded
	// unpaged answers every node to every listing call, whatever page and size ask
	unpaged bool
	nextID  int

	pods   map[string]*fakePod
	events map[string][]fakeEvent
	// rentFaults refuse rents of one node, by its id
	rentFaults map[string]*fault
	// idem keeps each Idempotency-Key's first body and answer
	idem      map[string]idemRecord
	templates []map[string]any
	// checkIssues is what POST /templates/check answers
	checkIssues []map[string]any
	// userID is the id GET /users/me answers, none when empty
	userID string
	// startStatus is the status a rented pod starts in
	startStatus string
	// podIP is the executor address of every pod rented
	podIP string

	// sh is where the pods' sshds run commands, sshds each pod's sshd by pod id
	sh    fakeShell
	sshds map[string]*fakeSSHD
	// dialed are the addresses the adapter dialled, execs the command lines the pods ran
	dialed, execs []string
	// signals are the signal requests the pods got
	signals []string
	// sshDown refuses every SSH dial, as a pod whose sshd died would
	sshDown bool
}

func newFake(t *testing.T) *fakeLium {
	t.Helper()
	f := &fakeLium{
		t: t, faults: map[string]*fault{}, pods: map[string]*fakePod{},
		events: map[string][]fakeEvent{}, rentFaults: map[string]*fault{},
		idem: map[string]idemRecord{}, userID: fakeUserID, startStatus: "PENDING",
		podIP: "203.0.113.7", sh: newFakeShell(t), sshds: map[string]*fakeSSHD{},
	}
	mux := http.NewServeMux()
	for pattern, h := range map[string]http.HandlerFunc{
		"GET /executors":                       f.listNodes,
		"GET /pods":                            f.listPods,
		"GET /pods/{id}":                       f.getPod,
		"GET /pods/{id}/events":                f.podEvents,
		"DELETE /pods/{id}":                    f.deletePod,
		"POST /pods/{id}/schedule-removal":     f.scheduleRemoval,
		"POST /executors/{executor_uuid}/rent": f.rent,
		"GET /templates":                       f.listTemplates,
		"POST /templates":                      f.makeTemplate,
		"POST /templates/check":                f.checkTemplate,
		"GET /users/me":                        f.me,
	} {
		mux.HandleFunc(pattern, f.gate(pattern, h))
	}
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("adapter called the unexpected route %s %s", r.Method, r.URL.Path)
		writeError(w, http.StatusNotFound, "not_found", "not in the fake")
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	t.Cleanup(f.closeSSHDs)
	return f
}

// provider builds the adapter on the fake without its listing loop, so a test drives refresh
func (f *fakeLium) provider(t *testing.T, opts Options, now func() time.Time) *Provider {
	t.Helper()
	opts.BaseURL = f.srv.URL
	p, err := newProvider(opts, now)
	if err != nil {
		t.Fatal(err)
	}
	// Go resends a keyed rent itself when a reused connection drops, which would spend a fault
	p.api.client.Transport = &http.Transport{DisableKeepAlives: true}
	p.relistEvery = time.Millisecond
	p.dial, p.execDir, p.setsid = f.dialSSH, f.sh.execDir, f.sh.setsid
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func (f *fakeLium) setNodes(nodes ...fakeNode) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nodes = slices.Clone(nodes)
}

func (f *fakeLium) inject(pattern string, ft fault) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.faults[pattern] = &ft
}

func (f *fakeLium) clearFaults() {
	f.mu.Lock()
	defer f.mu.Unlock()
	clear(f.faults)
}

func (f *fakeLium) requestsTo(method, path string) []recorded {
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

// take records the request and returns the fault for its route, nil when none applies
func (f *fakeLium) take(pattern string, r *http.Request, body []byte) *fault {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, recorded{
		method: r.Method, path: r.URL.Path, query: r.URL.Query(), header: r.Header.Clone(),
		body: body, at: time.Now(),
	})
	ft := f.faults[pattern]
	if ft == nil {
		return nil
	}
	if ft.skip > 0 {
		ft.skip--
		return nil
	}
	if ft.times > 0 {
		ft.times--
		if ft.times == 0 {
			delete(f.faults, pattern)
			if ft.then != nil {
				f.faults[pattern] = ft.then
			}
		}
	}
	return ft
}

// gate records the call, checks the key on every route but the public listing, and applies faults
func (f *fakeLium) gate(pattern string, h http.HandlerFunc) http.HandlerFunc {
	public := pattern == "GET /executors"
	return func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		ft := f.take(pattern, r, body)
		if !public && r.Header.Get("X-API-Key") != fakeKey {
			writeError(w, http.StatusUnauthorized, "unauthorized", "Missing or invalid API key")
			return
		}
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
		if ft.raw != "" {
			w.WriteHeader(ft.status)
			_, _ = io.WriteString(w, ft.raw)
			return
		}
		writeError(w, ft.status, ft.code, ft.message)
	}
}

// writeError answers with Lium's error envelope
func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("X-Request-Id", "req-fake")
	writeJSON(w, status, map[string]any{
		"success": false, "message": message, "status_code": status, "request_id": "req-fake",
		"error": map[string]any{
			"code": code, "message": message, "hint": "a hint", "request_id": "req-fake",
		},
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// listNodes serves the listing a page at a time, hiding unreliable or slow nodes when asked
func (f *fakeLium) listNodes(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	page, size := 1, 100
	if v, err := strconv.Atoi(q.Get("page")); err == nil {
		page = v
	}
	if v, err := strconv.Atoi(q.Get("size")); err == nil {
		size = v
	}
	floor, _ := strconv.ParseFloat(q.Get("min_download_mbps"), 64)
	f.mu.Lock()
	var shown []map[string]any
	for _, n := range f.nodes {
		if q.Get("reliable_only") == "true" && n.unreliable || n.downloadMbps < floor {
			continue
		}
		shown = append(shown, n.json())
	}
	unpaged := f.unpaged
	f.mu.Unlock()
	from, to := min((page-1)*size, len(shown)), min(page*size, len(shown))
	out := shown[from:to]
	if unpaged {
		out = shown
	}
	if out == nil {
		out = []map[string]any{}
	}
	writeJSON(w, http.StatusOK, out)
}
