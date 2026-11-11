package lium

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// liveFake serves three EU RTX 3090 nodes and an adapter holding a fresh listing of them
func liveFake(t *testing.T, opts ...func(*Options)) (*fakeLium, *Provider) {
	t.Helper()
	f := newFake(t)
	a, b, c := euNode("node-a"), euNode("node-b"), euNode("node-c")
	a.price, b.price, c.price = 0.25, 0.28, 0.30
	f.setNodes(a, b, c)
	o := testOptions()
	for _, change := range opts {
		change(&o)
	}
	p := f.provider(t, o, time.Now)
	p.pollEvery = time.Millisecond
	if err := p.nodes.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	return f, p
}

// relist reads the fake's listing again, as the adapter's loop would
func relist(t *testing.T, p *Provider) {
	t.Helper()
	if err := p.nodes.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func createReq(key string) provider.CreateRequest {
	return provider.CreateRequest{
		IdempotencyKey: key, Size: provider.Size{Name: "lium-rtx3090"},
		Labels: placementLabels(), Lifetime: 15 * time.Minute,
	}
}

// mustCreate creates and fails the test on an error
func mustCreate(t *testing.T, p *Provider, req provider.CreateRequest) provider.Ref {
	t.Helper()
	ref, err := p.Create(context.Background(), req)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	return ref
}

// wantCode fails the test unless err carries code and the fail over flag
func wantCode(t *testing.T, err error, code provider.Code, failOver bool) {
	t.Helper()
	e, ok := provider.AsError(err)
	if !ok || e.Code != code || e.CanFailOver != failOver {
		t.Fatalf("got %v, want %s with fail over %v", err, code, failOver)
	}
}

// rents lists the rent requests the fake saw, of every node
func (f *fakeLium) rents() []recorded {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []recorded
	for _, r := range f.requests {
		if r.method == "POST" && strings.HasPrefix(r.path, "/executors/") &&
			strings.HasSuffix(r.path, "/rent") {
			out = append(out, r)
		}
	}
	return out
}

// podCount is how many pods the fake holds, of every name
func (f *fakeLium) podCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pods)
}

func rentNode(r recorded) string {
	return strings.TrimSuffix(strings.TrimPrefix(r.path, "/executors/"), "/rent")
}

// checkReq is the image check's create, which carries no idempotency key
func checkReq() provider.CreateRequest {
	req := createReq("")
	req.Labels = map[string]string{"bugnet_image_check": "1"}
	return req
}

// refuseCalls fails the picked matching calls before anything is written, as a refused dial does
type refuseCalls struct {
	next  http.RoundTripper
	match func(*http.Request) bool
	// picks are the indexes, among the matching calls, that are refused
	picks map[int]bool
	n     atomic.Int32
}

func (r *refuseCalls) RoundTrip(req *http.Request) (*http.Response, error) {
	if r.match(req) && r.picks[int(r.n.Add(1))-1] {
		return nil, errors.New("dial tcp 192.0.2.1:443: connect: connection refused")
	}
	return r.next.RoundTrip(req)
}

func isRent(req *http.Request) bool {
	return req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/rent")
}

func isPodList(req *http.Request) bool {
	return req.Method == http.MethodGet && req.URL.Path == "/pods"
}

func TestLiumCreate(t *testing.T) {
	f, p := liveFake(t)
	ref := mustCreate(t, p, createReq(testKey))
	name := podName(testKey, placementLabels())
	eq(t, ref.Provider, "lium")
	pods := f.podsNamed(name)
	if len(pods) != 1 || pods[0].id != ref.ID || pods[0].node != "node-a" {
		t.Fatalf("pods named %s: %+v, ref %+v", name, pods, ref)
	}
	created, err := time.Parse(time.RFC3339, ref.Meta["created_at"])
	if err != nil || time.Since(created) > time.Minute || created.After(time.Now()) {
		t.Fatalf("created_at %q", ref.Meta["created_at"])
	}
	delete(ref.Meta, "created_at")
	eq(t, ref.Meta, map[string]string{
		"pod_name": name, "node": "node-a", "hourly_micros": "250000", "arch": "amd64",
		"size": "lium-rtx3090",
	})

	rents := f.rents()
	if len(rents) != 1 {
		t.Fatalf("%d rents", len(rents))
	}
	r := rents[0]
	eq(t, rentNode(r), "node-a")
	eq(t, r.header.Get("Idempotency-Key"), testKey+":node-a")
	eq(t, r.header.Get("X-Customer-Ref"), "0b8d1c2e-3f4a-4b5c-8d6e-7f8091a2b3c4")
	eq(t, r.header.Get("Content-Type"), "application/json")
	eq(t, len(f.requestsTo("POST", "/templates")), 1)
	tmpl := pods[0].template
	eq(t, string(r.body), `{"pod_name":"`+name+`","template_id":"`+tmpl+`",`+
		`"user_public_key":["`+p.podPublicKey(name)+`"],"initial_port_count":1,`+
		`"enable_jupyter":false,"enable_backups":false,"enable_volume_encryption":false,`+
		`"skip_add_agent_ssh_key":true,"termination_hours":3,`+
		`"expected_price_per_gpu_hour":0.25}`)

	// The same key adopts the pod it rented, renting nothing more
	again := mustCreate(t, p, createReq(testKey))
	eq(t, again.ID, ref.ID)
	eq(t, again.Meta["node"], "node-a")
	eq(t, again.Meta["hourly_micros"], "250000")
	eq(t, again.Meta["pod_name"], name)
	eq(t, len(f.rents()), 1)
	eq(t, len(f.requestsTo("POST", "/templates")), 1)
}

func TestLiumTerminationHours(t *testing.T) {
	for _, tc := range []struct {
		name        string
		termination string
		maxLifetime time.Duration
		lifetime    time.Duration
		want        int
	}{
		{"max lifetime of 2 h", "", 2 * time.Hour, 15 * time.Minute, 3},
		{"max lifetime named", "max_lifetime", 2 * time.Hour, 15 * time.Minute, 3},
		{"max lifetime of 55 min", "max_lifetime", 55 * time.Minute, 15 * time.Minute, 1},
		{"max lifetime of 3 h", "max_lifetime", 3 * time.Hour, 15 * time.Minute, 4},
		{"lifetime of 15 min", "lifetime", 2 * time.Hour, 15 * time.Minute, 1},
		{"lifetime of 60 min", "lifetime", 2 * time.Hour, time.Hour, 1},
		{"lifetime of 65 min", "lifetime", 2 * time.Hour, 65 * time.Minute, 2},
		{"at most 720 h", "max_lifetime", 800 * time.Hour, 15 * time.Minute, 720},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, p := liveFake(t, func(o *Options) {
				o.Termination, o.MaxLifetime = tc.termination, tc.maxLifetime
			})
			req := createReq(freshKey())
			req.Lifetime = tc.lifetime
			mustCreate(t, p, req)
			var body struct {
				Hours int `json:"termination_hours"`
			}
			if err := json.Unmarshal(f.rents()[0].body, &body); err != nil {
				t.Fatal(err)
			}
			eq(t, body.Hours, tc.want)
		})
	}
	opts := testOptions()
	opts.Termination = "forever"
	if _, err := newProvider(opts, time.Now); err == nil ||
		!strings.Contains(err.Error(), "termination") {
		t.Fatalf("termination forever gave %v", err)
	}
}

func TestLiumCreateRefuses(t *testing.T) {
	f, p := liveFake(t)
	for name, tc := range map[string]struct {
		change   func(*provider.CreateRequest)
		code     provider.Code
		failOver bool
	}{
		"builder": {func(r *provider.CreateRequest) { r.Builder = true }, provider.CodeInvalid,
			false},
		"unknown size": {func(r *provider.CreateRequest) { r.Size.Name = "lium-t4" },
			provider.CodeInvalid, false},
		"no lifetime": {func(r *provider.CreateRequest) { r.Lifetime = 0 },
			provider.CodeInvalid, false},
		"past max lifetime": {func(r *provider.CreateRequest) { r.Lifetime = 3 * time.Hour },
			provider.CodeInvalid, false},
		"image by tag": {
			func(r *provider.CreateRequest) { r.Image = "ghcr.io/bugnet-sh/bugnet-base:1" },
			provider.CodeInvalid, true},
		"no node of the size": {
			func(r *provider.CreateRequest) { r.Size.Name = "lium-h200" },
			provider.CodeCapacity, true},
	} {
		t.Run(name, func(t *testing.T) {
			req := createReq(freshKey())
			tc.change(&req)
			_, err := p.Create(context.Background(), req)
			wantCode(t, err, tc.code, tc.failOver)
		})
	}
	eq(t, len(f.rents()), 0)

	t.Run("no image", func(t *testing.T) {
		f, p := liveFake(t, func(o *Options) { o.Image = "" })
		_, err := p.Create(context.Background(), createReq(freshKey()))
		wantCode(t, err, provider.CodeInvalid, true)
		eq(t, len(f.rents()), 0)
	})
	t.Run("a stale listing", func(t *testing.T) {
		f := newFake(t)
		f.setNodes(euNode("node-a"))
		c := newClock()
		p := f.provider(t, testOptions(), c.now)
		if err := p.nodes.refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
		c.add(listingStale + time.Second)
		_, err := p.Create(context.Background(), createReq(freshKey()))
		wantCode(t, err, provider.CodeCapacity, true)
		eq(t, len(f.rents()), 0)
	})
}

func TestLiumImageOverridePicksTemplate(t *testing.T) {
	f, p := liveFake(t)
	other := "ghcr.io/bugnet-sh/bugnet-base:2026.11.4@sha256:" +
		"b0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecf"
	req := createReq("")
	req.Image, req.Labels = other, map[string]string{"bugnet_image_check": "1"}
	ref := mustCreate(t, p, req)
	pod, _ := f.pod(ref.ID)
	var tmpl map[string]any
	f.mu.Lock()
	for _, tm := range f.templates {
		if tm["id"] == pod.template {
			tmpl = tm
		}
	}
	f.mu.Unlock()
	if tmpl == nil || tmpl["docker_image_digest"] != "sha256:"+
		"b0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecf" {
		t.Fatalf("pod started from template %v", tmpl)
	}
	// The configured image keeps its own template
	ref = mustCreate(t, p, createReq(freshKey()))
	pod2, _ := f.pod(ref.ID)
	if pod2.template == pod.template {
		t.Fatal("the configured image took the image check's template")
	}
}

func TestEmptyKeyIsFresh(t *testing.T) {
	f, p := liveFake(t)
	req := createReq("")
	req.Labels = map[string]string{"bugnet_image_check": "1"}
	a := mustCreate(t, p, req)
	relist(t, p)
	b := mustCreate(t, p, req)
	if a.ID == b.ID || a.Meta["pod_name"] == b.Meta["pod_name"] {
		t.Fatalf("two creates with no key gave %+v and %+v", a, b)
	}
	for _, ref := range []provider.Ref{a, b} {
		if !strings.HasPrefix(ref.Meta["pod_name"], "bx.") ||
			!strings.HasSuffix(ref.Meta["pod_name"], ".i.1") {
			t.Fatalf("pod name %q", ref.Meta["pod_name"])
		}
	}
	rents := f.rents()
	if len(rents) != 2 {
		t.Fatalf("%d rents", len(rents))
	}
	var keys []string
	for _, r := range rents {
		if v := r.header.Values("Idempotency-Key"); len(v) != 0 {
			t.Fatalf("a create with no key sent Idempotency-Key %v", v)
		}
		eq(t, r.header.Get("X-Customer-Ref"), "bx.check")
		var body struct {
			Keys []string `json:"user_public_key"`
		}
		if err := json.Unmarshal(r.body, &body); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, body.Keys...)
	}
	if len(keys) != 2 || keys[0] == keys[1] {
		t.Fatalf("SSH keys %v", keys)
	}
	// Adoption is skipped, so no listing ran
	eq(t, len(f.requestsTo("GET", "/pods")), 0)

	t.Run("a lost answer rents no second node", func(t *testing.T) {
		f, p := liveFake(t)
		// The first rent takes node-a and loses its answer, so the resend finds node-a taken
		f.inject(rentRoute, fault{drop: true, after: true, times: 1})
		ref := mustCreate(t, p, req)
		eq(t, f.podCount(), 1)
		pods := f.podsNamed(ref.Meta["pod_name"])
		if len(pods) != 1 || pods[0].id != ref.ID || pods[0].node != "node-a" {
			t.Fatalf("pods %+v, ref %+v", pods, ref)
		}
		eq(t, len(f.rents()), 2)
		for _, r := range f.rents() {
			eq(t, rentNode(r), "node-a")
		}
	})
}

func TestLiumKeylessListsAgain(t *testing.T) {
	for name, listing := range map[string]fault{
		"the listing lags": {status: 200, raw: "[]", times: 1},
		"the listing is rate limited": {status: 429, code: "rate_limited", times: 1,
			header: map[string]string{"Retry-After": "0"}},
		"the listing gets a server error": {status: 500, code: "internal_error", times: 1},
	} {
		t.Run(name, func(t *testing.T) {
			f, p := liveFake(t)
			f.inject(rentRoute, fault{drop: true, after: true, times: 1})
			f.inject(podsRoute, listing)
			ref := mustCreate(t, p, checkReq())
			// The second listing shows the pod the first rent made
			eq(t, len(f.requestsTo("GET", "/pods")), 2)
			pods := f.podsNamed(ref.Meta["pod_name"])
			if len(pods) != 1 || pods[0].id != ref.ID || pods[0].node != "node-a" {
				t.Fatalf("pods %+v, ref %+v", pods, ref)
			}
			eq(t, f.podCount(), 1)
			eq(t, len(f.rents()), 2)
		})
	}
	t.Run("the pod never shows", func(t *testing.T) {
		f, p := liveFake(t)
		p.relistEvery = 20 * time.Millisecond
		f.inject(rentRoute, fault{drop: true, after: true, times: 1})
		f.inject(podsRoute, fault{status: 200, raw: "[]"})
		_, err := p.Create(context.Background(), checkReq())
		wantCode(t, err, provider.CodeOutcomeUnknown, false)
		eq(t, f.podCount(), 1)
		eq(t, len(f.rents()), 2)
		for _, r := range f.rents() {
			eq(t, rentNode(r), "node-a")
		}
		// The answer names the pod, which no retry of the saga finds without a key
		name := rentedName(t, f)
		eq(t, len(f.podsNamed(name)), 1)
		if !strings.Contains(err.Error(), name) {
			t.Fatalf("the answer %q does not name pod %s", err, name)
		}
		lists := f.requestsTo("GET", "/pods")
		eq(t, len(lists), 1+3)
		for i := 1; i < len(lists); i++ {
			if gap := lists[i].at.Sub(lists[i-1].at); gap < p.relistEvery {
				t.Fatalf("listing %d came %v after the one before", i, gap)
			}
		}
	})
	t.Run("past the create budget", func(t *testing.T) {
		f, p := liveFake(t)
		f.inject(rentRoute, fault{drop: true, after: true, times: 1})
		// The resend waits on the create pace past the budget, so the listings run after it
		lim := rate.NewLimiter(rate.Every(time.Hour), 1)
		p.api.creates, p.createBudget = lim, 100*time.Millisecond
		ref, err := p.Create(context.Background(), checkReq())
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		eq(t, f.podsNamed(ref.Meta["pod_name"])[0].id, ref.ID)
		eq(t, f.podCount(), 1)
		eq(t, len(f.rents()), 1)
	})
}

// rentedName is the pod name the first rent the fake saw asked for
func rentedName(t *testing.T, f *fakeLium) string {
	t.Helper()
	var body struct {
		Name string `json:"pod_name"`
	}
	if err := json.Unmarshal(f.rents()[0].body, &body); err != nil {
		t.Fatal(err)
	}
	return body.Name
}

func TestLiumAdoptsLivePod(t *testing.T) {
	for _, status := range []string{"PENDING", "START_PENDING", "RUNNING"} {
		t.Run(status, func(t *testing.T) {
			f, p := liveFake(t)
			pod := f.addPod(fakePod{
				name: podName(testKey, placementLabels()), status: status, node: "node-x",
				price: 0.31, created: time.Date(2026, 11, 11, 11, 0, 0, 123000, time.UTC),
			})
			// Another key's pod and a pod of another name are left alone
			f.addPod(fakePod{name: podName(freshKey(), placementLabels())})
			f.addPod(fakePod{name: "someone-else"})
			ref := mustCreate(t, p, createReq(testKey))
			eq(t, ref, provider.Ref{Provider: "lium", ID: pod.id, Meta: map[string]string{
				"pod_name": pod.name, "node": "node-x", "hourly_micros": "310000",
				"arch": "amd64", "size": "lium-rtx3090", "created_at": "2026-11-11T11:00:00Z",
			}})
			eq(t, len(f.rents()), 0)
		})
	}
}

func TestLiumAdoptSkipsTerminal(t *testing.T) {
	for _, status := range []string{
		"DELETING", "CREATION_FAILED", "BROKEN", "FAILED", "REBOOT_PENDING", "REBOOT_FAILED",
		"STOPPED", "STOP_PENDING",
	} {
		t.Run(status, func(t *testing.T) {
			f, p := liveFake(t)
			pod := f.addPod(fakePod{name: podName(testKey, placementLabels()), status: status})
			_, err := p.Create(context.Background(), createReq(testKey))
			wantCode(t, err, provider.CodeCapacity, true)
			eq(t, len(f.rents()), 0)
			deletes := f.requestsTo("DELETE", "/pods/"+pod.id)
			if status == "DELETING" {
				eq(t, len(deletes), 0)
				return
			}
			if len(deletes) != 1 || deletes[0].query.Get("expected_name") != pod.name {
				t.Fatalf("deletes %+v", deletes)
			}
			if _, ok := f.pod(pod.id); ok {
				t.Fatal("the dead pod is still there")
			}
		})
	}
	t.Run("a live pod beside a dead one", func(t *testing.T) {
		f, p := liveFake(t)
		dead := f.addPod(fakePod{name: podName(testKey, placementLabels()), status: "FAILED"})
		live := f.addPod(fakePod{name: podName(testKey, placementLabels()), status: "RUNNING"})
		ref := mustCreate(t, p, createReq(testKey))
		eq(t, ref.ID, live.id)
		if _, ok := f.pod(dead.id); ok {
			t.Fatal("the failed pod was kept, it would bill again after a reboot")
		}
	})
	for _, status := range []string{"FAILED", "BROKEN", "STOPPED"} {
		t.Run(status+" whose delete fails", func(t *testing.T) {
			f, p := liveFake(t)
			pod := f.addPod(fakePod{name: podName(testKey, placementLabels()), status: status})
			f.inject("DELETE /pods/{id}", fault{status: 502, code: "upstream_unavailable"})
			_, err := p.Create(context.Background(), createReq(testKey))
			wantCode(t, err, provider.CodeInternal, false)
			eq(t, len(f.requestsTo("DELETE", "/pods/"+pod.id)), 1)
			if _, ok := f.pod(pod.id); !ok {
				t.Fatal("the fake lost the pod its delete refused")
			}
			eq(t, len(f.rents()), 0)
		})
	}
	t.Run("a live pod beside a dead one whose delete fails", func(t *testing.T) {
		f, p := liveFake(t)
		f.addPod(fakePod{name: podName(testKey, placementLabels()), status: "FAILED"})
		live := f.addPod(fakePod{name: podName(testKey, placementLabels()), status: "RUNNING"})
		f.inject("DELETE /pods/{id}", fault{status: 502, code: "upstream_unavailable"})
		ref := mustCreate(t, p, createReq(testKey))
		eq(t, ref.ID, live.id)
	})
	t.Run("an unknown status", func(t *testing.T) {
		f, p := liveFake(t)
		f.addPod(fakePod{name: podName(testKey, placementLabels()), status: "HIBERNATED"})
		_, err := p.Create(context.Background(), createReq(testKey))
		wantCode(t, err, provider.CodeInternal, false)
		eq(t, len(f.rents()), 0)
	})
}

func TestLiumRetrySameNode(t *testing.T) {
	f, p := liveFake(t)
	// The rent and its one retry lose their answers, and nothing was rented
	f.inject(rentRoute, fault{drop: true, times: 2})
	_, err := p.Create(context.Background(), createReq(testKey))
	wantCode(t, err, provider.CodeOutcomeUnknown, false)
	eq(t, len(f.rents()), 2)

	// The listing moves on without node-a, the node the key rented
	b, c := euNode("node-b"), euNode("node-c")
	f.setNodes(b, c)
	if err := p.nodes.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	ref := mustCreate(t, p, createReq(testKey))
	eq(t, ref.Meta["node"], "node-a")
	rents := f.rents()
	if len(rents) != 3 {
		t.Fatalf("%d rents", len(rents))
	}
	for _, r := range rents {
		eq(t, rentNode(r), "node-a")
		eq(t, r.header.Get("Idempotency-Key"), testKey+":node-a")
		eq(t, string(r.body), string(rents[0].body))
		eq(t, r.header.Get("X-Customer-Ref"), rents[0].header.Get("X-Customer-Ref"))
	}
	created, err := time.Parse(time.RFC3339, ref.Meta["created_at"])
	if err != nil || created.After(rents[0].at) {
		t.Fatalf("created_at %q comes after the first rent at %v", ref.Meta["created_at"],
			rents[0].at)
	}

	t.Run("a retry that never leaves", func(t *testing.T) {
		f, p := liveFake(t)
		f.inject(rentRoute, fault{drop: true, times: 2})
		_, err := p.Create(context.Background(), createReq(testKey))
		wantCode(t, err, provider.CodeOutcomeUnknown, false)
		// The next rent waits on the create pace past the budget, so it is never sent
		lim := rate.NewLimiter(rate.Every(time.Hour), 1)
		lim.Allow()
		p.api.creates, p.createBudget = lim, 100*time.Millisecond
		_, err = p.Create(context.Background(), createReq(testKey))
		wantCode(t, err, provider.CodeOutcomeUnknown, false)
		eq(t, len(f.rents()), 2)
	})
}

// lostRental creates testKey once, its rent taking node-a while both answers and a listing miss it
func lostRental(t *testing.T, f *fakeLium, p *Provider) {
	t.Helper()
	f.inject(rentRoute, fault{drop: true, after: true, times: 2})
	f.inject(podsRoute, fault{status: 200, raw: "[]", skip: 1, times: 1})
	_, err := p.Create(context.Background(), createReq(testKey))
	wantCode(t, err, provider.CodeOutcomeUnknown, false)
	eq(t, f.podCount(), 1)
	eq(t, len(f.rents()), 2)
}

func TestLiumRetryListingFailsAfterUncertain(t *testing.T) {
	for name, listing := range map[string]fault{
		"503": {status: 503, code: "upstream_unavailable", times: 1},
		"500": {status: 500, code: "internal_error", times: 1},
		"429": {status: 429, code: "rate_limited", times: 1,
			header: map[string]string{"Retry-After": "0"}},
		"401":                      {status: 401, code: "unauthorized", times: 1},
		"403 insufficient_balance": {status: 403, code: "insufficient_balance", times: 1},
		"dropped once sent":        {drop: true, times: 1},
	} {
		t.Run(name, func(t *testing.T) {
			f, p := liveFake(t)
			lostRental(t, f, p)
			// The saga's retry of the key opens with a listing that fails
			f.inject(podsRoute, listing)
			_, err := p.Create(context.Background(), createReq(testKey))
			wantCode(t, err, provider.CodeOutcomeUnknown, false)
			eq(t, f.podCount(), 1)
			eq(t, len(f.rents()), 2)
			// The next retry lists the pod and adopts it
			ref := mustCreate(t, p, createReq(testKey))
			eq(t, len(f.podsNamed(ref.Meta["pod_name"])), 1)
			eq(t, f.podsNamed(ref.Meta["pod_name"])[0].id, ref.ID)
			eq(t, len(f.rents()), 2)
		})
	}
	t.Run("never sent", func(t *testing.T) {
		f, p := liveFake(t)
		lostRental(t, f, p)
		refuse := &refuseCalls{next: p.api.client.Transport, match: isPodList,
			picks: map[int]bool{0: true}}
		p.api.client.Transport = refuse
		_, err := p.Create(context.Background(), createReq(testKey))
		wantCode(t, err, provider.CodeOutcomeUnknown, false)
		eq(t, f.podCount(), 1)
		eq(t, len(f.rents()), 2)
	})
}

func TestLiumTransportErrorRetriesOnce(t *testing.T) {
	t.Run("the retry answers", func(t *testing.T) {
		f, p := liveFake(t)
		f.inject(rentRoute, fault{drop: true, times: 1})
		mustCreate(t, p, createReq(testKey))
		rents := f.rents()
		eq(t, len(rents), 2)
		eq(t, string(rents[1].body), string(rents[0].body))
	})
	t.Run("the first rent rented", func(t *testing.T) {
		f, p := liveFake(t)
		f.inject(rentRoute, fault{drop: true, after: true, times: 1})
		ref := mustCreate(t, p, createReq(testKey))
		eq(t, len(f.podsNamed(ref.Meta["pod_name"])), 1)
		eq(t, len(f.rents()), 2)
	})
	t.Run("the listing finds the pod", func(t *testing.T) {
		f, p := liveFake(t)
		// Lium rented but lost both answers and its record of the key
		f.inject(rentRoute, fault{drop: true, times: 2})
		pod := f.addPod(fakePod{name: podName(testKey, placementLabels()), status: "PENDING"})
		f.inject(podsRoute, fault{status: 200, raw: "[]", times: 1})
		ref := mustCreate(t, p, createReq(testKey))
		eq(t, ref.ID, pod.id)
	})
	for name, resend := range map[string]fault{
		"the resend is rate limited": {status: 429, code: "rate_limited", times: 1,
			header: map[string]string{"Retry-After": "0"}},
		"the resend gets a server error": {status: 502, code: "upstream_unavailable", times: 1},
	} {
		t.Run(name, func(t *testing.T) {
			f, p := liveFake(t)
			// The first rent takes node-a and loses its answer, and both listings miss the pod
			f.inject(rentRoute, fault{drop: true, after: true, times: 1, then: &resend})
			f.inject(podsRoute, fault{status: 200, raw: "[]", times: 2})
			_, err := p.Create(context.Background(), createReq(testKey))
			wantCode(t, err, provider.CodeOutcomeUnknown, false)
			eq(t, len(f.rents()), 2)
			eq(t, f.podCount(), 1)
			// The key's retry adopts the pod once Lium lists it
			ref := mustCreate(t, p, createReq(testKey))
			eq(t, ref.Meta["node"], "node-a")
			eq(t, f.podCount(), 1)
			eq(t, len(f.rents()), 2)
		})
	}
}

func TestLiumServerErrorRetriesOnce(t *testing.T) {
	t.Run("the retry answers", func(t *testing.T) {
		f, p := liveFake(t)
		f.inject(rentRoute, fault{status: 502, code: "upstream_unavailable", times: 1})
		mustCreate(t, p, createReq(testKey))
		eq(t, len(f.rents()), 2)
	})
	t.Run("both fail", func(t *testing.T) {
		f, p := liveFake(t)
		f.inject(rentRoute, fault{status: 500, code: "internal_error", times: 2})
		_, err := p.Create(context.Background(), createReq(testKey))
		wantCode(t, err, provider.CodeInternal, true)
		eq(t, len(f.rents()), 2)
		// A later create of the key still goes to the node first
		mustCreate(t, p, createReq(testKey))
		for _, r := range f.rents() {
			eq(t, rentNode(r), "node-a")
		}
	})
	t.Run("both fail and the listing fails", func(t *testing.T) {
		f, p := liveFake(t)
		f.inject(rentRoute, fault{status: 500, code: "internal_error", after: true, times: 1,
			then: &fault{status: 502, code: "upstream_unavailable", times: 1}})
		f.inject(podsRoute, fault{status: 503, code: "upstream_unavailable", skip: 1, times: 1})
		_, err := p.Create(context.Background(), createReq(testKey))
		wantCode(t, err, provider.CodeOutcomeUnknown, false)
		eq(t, f.podCount(), 1)
		eq(t, len(f.rents()), 2)
	})
	t.Run("the listing lags behind both", func(t *testing.T) {
		f, p := liveFake(t)
		// The first send rented node-a, and the listing after the sends misses it once
		f.inject(rentRoute, fault{status: 500, code: "internal_error", after: true, times: 1,
			then: &fault{status: 502, code: "upstream_unavailable", times: 1}})
		f.inject(podsRoute, fault{status: 200, raw: "[]", skip: 1, times: 1})
		ref := mustCreate(t, p, createReq(testKey))
		eq(t, f.podsNamed(ref.Meta["pod_name"])[0].id, ref.ID)
		eq(t, len(f.requestsTo("GET", "/pods")), 3)
		eq(t, f.podCount(), 1)
		eq(t, len(f.rents()), 2)
	})
	// Each answer after the server error rents nothing, so nothing listed means Lium rented none
	for name, then := range map[string]fault{
		"rate limited": {status: 429, code: "rate_limited", times: 1,
			header: map[string]string{"Retry-After": "0"}},
		"unauthorized":         {status: 401, code: "unauthorized", times: 1},
		"insufficient_balance": {status: 403, code: "insufficient_balance", times: 1},
		"image_not_found":      {status: 400, code: "image_not_found", times: 1},
		"an unknown 4xx":       {status: 400, code: "brand_new_code", times: 1},
		"template_not_found":   {status: 404, code: "template_not_found", times: 1},
		"node_unavailable":     {status: 400, code: "node_unavailable", times: 1},
	} {
		t.Run("then "+name, func(t *testing.T) {
			f, p := liveFake(t)
			f.inject(rentRoute, fault{status: 500, code: "internal_error", times: 1, then: &then})
			_, err := p.Create(context.Background(), createReq(testKey))
			wantCode(t, err, provider.CodeInternal, true)
			eq(t, codeOf(err), then.code)
			eq(t, len(f.rents()), 2)
			eq(t, f.podCount(), 0)
			// The opening listing, the one after the sends and one more
			eq(t, len(f.requestsTo("GET", "/pods")), 3)
			eq(t, len(f.requestsTo("POST", "/templates")), 1)
		})
	}
	t.Run("then a resend that never leaves", func(t *testing.T) {
		f, p := liveFake(t)
		p.api.client.Transport = &refuseCalls{next: p.api.client.Transport, match: isRent,
			picks: map[int]bool{1: true}}
		f.inject(rentRoute, fault{status: 500, code: "internal_error", times: 1})
		_, err := p.Create(context.Background(), createReq(testKey))
		wantCode(t, err, provider.CodeInternal, true)
		eq(t, len(f.rents()), 1)
		eq(t, f.podCount(), 0)
	})
	t.Run("one that rented, then a resend that never leaves", func(t *testing.T) {
		f, p := liveFake(t)
		p.api.client.Transport = &refuseCalls{next: p.api.client.Transport, match: isRent,
			picks: map[int]bool{1: true}}
		f.inject(rentRoute, fault{status: 500, code: "internal_error", after: true, times: 1})
		f.inject(podsRoute, fault{status: 200, raw: "[]", skip: 1, times: 1})
		ref := mustCreate(t, p, createReq(testKey))
		eq(t, f.podsNamed(ref.Meta["pod_name"])[0].id, ref.ID)
		eq(t, f.podCount(), 1)
		eq(t, len(f.rents()), 1)
	})
	t.Run("without a key, then the node is taken", func(t *testing.T) {
		f, p := liveFake(t)
		// The first send rented node-a, so the resend finds the node our pod holds
		f.inject(rentRoute, fault{status: 500, code: "internal_error", after: true, times: 1})
		f.inject(podsRoute, fault{status: 200, raw: "[]"})
		_, err := p.Create(context.Background(), checkReq())
		wantCode(t, err, provider.CodeOutcomeUnknown, false)
		if !strings.Contains(err.Error(), rentedName(t, f)) {
			t.Fatalf("the answer %q names no pod", err)
		}
		eq(t, f.podCount(), 1)
		eq(t, len(f.rents()), 2)
		for _, r := range f.rents() {
			eq(t, rentNode(r), "node-a")
		}
	})
	t.Run("after lost answers", func(t *testing.T) {
		f, p := liveFake(t)
		f.inject(rentRoute, fault{drop: true, times: 2})
		_, err := p.Create(context.Background(), createReq(testKey))
		wantCode(t, err, provider.CodeOutcomeUnknown, false)
		// The key's retry gets two server errors, which say nothing of the lost sends
		f.inject(rentRoute, fault{status: 502, code: "upstream_unavailable", times: 2})
		_, err = p.Create(context.Background(), createReq(testKey))
		wantCode(t, err, provider.CodeOutcomeUnknown, false)
		eq(t, len(f.rents()), 4)
	})
}

func TestLiumIdempotencyInProgress(t *testing.T) {
	f, p := liveFake(t)
	f.inject(rentRoute, fault{status: 409, code: "idempotency_in_progress",
		header: map[string]string{"Retry-After": "0"}, times: 2})
	mustCreate(t, p, createReq(testKey))
	rents := f.rents()
	eq(t, len(rents), 3)
	for _, r := range rents {
		eq(t, string(r.body), string(rents[0].body))
		eq(t, r.header.Get("Idempotency-Key"), testKey+":node-a")
	}

	t.Run("past the create budget", func(t *testing.T) {
		f, p := liveFake(t)
		p.createBudget = 100 * time.Millisecond
		f.inject(rentRoute, fault{status: 409, code: "idempotency_in_progress",
			header: map[string]string{"Retry-After": "0"}})
		_, err := p.Create(context.Background(), createReq(testKey))
		wantCode(t, err, provider.CodeOutcomeUnknown, false)
		// The rental was kept before it left, so the key's retry resends it to node-a
		f.clearFaults()
		f.setNodes(euNode("node-b"))
		relist(t, p)
		ref := mustCreate(t, p, createReq(testKey))
		eq(t, ref.Meta["node"], "node-a")
	})
}

func TestLiumOutcomeUnknownListsByName(t *testing.T) {
	t.Run("the pod is listed", func(t *testing.T) {
		f, p := liveFake(t)
		f.inject(rentRoute, fault{status: 409, code: "idempotency_outcome_unknown",
			after: true, times: 1})
		ref := mustCreate(t, p, createReq(testKey))
		pods := f.podsNamed(ref.Meta["pod_name"])
		if len(pods) != 1 || pods[0].id != ref.ID {
			t.Fatalf("pods %+v, ref %+v", pods, ref)
		}
		eq(t, len(f.rents()), 1)
	})
	t.Run("nothing is listed", func(t *testing.T) {
		f, p := liveFake(t)
		f.inject(rentRoute, fault{status: 409, code: "idempotency_outcome_unknown", times: 1})
		_, err := p.Create(context.Background(), createReq(testKey))
		wantCode(t, err, provider.CodeOutcomeUnknown, false)
		eq(t, len(f.rents()), 1)
	})
	t.Run("the listing fails", func(t *testing.T) {
		f, p := liveFake(t)
		f.inject(rentRoute, fault{status: 409, code: "idempotency_outcome_unknown",
			after: true, times: 1})
		f.inject(podsRoute, fault{status: 500, code: "internal_error", skip: 1})
		_, err := p.Create(context.Background(), createReq(testKey))
		wantCode(t, err, provider.CodeOutcomeUnknown, false)
	})
}

func TestLiumOwnRentInProgress(t *testing.T) {
	ownRent := fault{status: 400, code: "own_rent_in_progress"}
	t.Run("adopts our pod when listed", func(t *testing.T) {
		f, p := liveFake(t)
		pod := f.addPod(fakePod{name: podName(testKey, placementLabels()), status: "PENDING",
			node: "node-a"})
		// The first listing ran before Lium showed the pod
		f.inject(podsRoute, fault{status: 200, raw: "[]", times: 1})
		f.rentFault("node-a", ownRent)
		ref := mustCreate(t, p, createReq(testKey))
		eq(t, ref.ID, pod.id)
		eq(t, len(f.rents()), 1)
	})
	t.Run("moves to the next node", func(t *testing.T) {
		f, p := liveFake(t)
		f.rentFault("node-a", ownRent)
		ref := mustCreate(t, p, createReq(testKey))
		eq(t, ref.Meta["node"], "node-b")
		rents := f.rents()
		eq(t, len(rents), 2)
		eq(t, rents[1].header.Get("Idempotency-Key"), testKey+":node-b")
	})
	t.Run("never fails over by itself", func(t *testing.T) {
		f, p := liveFake(t)
		for _, n := range []string{"node-a", "node-b", "node-c"} {
			f.rentFault(n, ownRent)
		}
		_, err := p.Create(context.Background(), createReq(testKey))
		wantCode(t, err, provider.CodeCapacity, false)
		eq(t, len(f.rents()), 3)
	})
	t.Run("with another refusal beside it", func(t *testing.T) {
		f, p := liveFake(t)
		f.rentFault("node-a", ownRent)
		f.rentFault("node-b", fault{status: 409, code: "price_changed"})
		f.rentFault("node-c", ownRent)
		_, err := p.Create(context.Background(), createReq(testKey))
		wantCode(t, err, provider.CodeCapacity, true)
	})
}

func TestLiumNextNodeOnRefusal(t *testing.T) {
	f, p := liveFake(t)
	f.rentFault("node-a", fault{status: 400, code: "node_unavailable"})
	f.rentFault("node-b", fault{status: 409, code: "conflict"})
	ref := mustCreate(t, p, createReq(testKey))
	eq(t, ref.Meta["node"], "node-c")
	eq(t, ref.Meta["hourly_micros"], "300000")

	t.Run("three nodes at most", func(t *testing.T) {
		f, p := liveFake(t)
		d := euNode("node-d")
		d.price = 0.33
		f.mu.Lock()
		f.nodes = append(f.nodes, d)
		f.mu.Unlock()
		if err := p.nodes.refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
		for i, n := range []string{"node-a", "node-b", "node-c"} {
			code := []string{"node_unavailable", "node_not_found", "node_rent_in_progress"}[i]
			f.rentFault(n, fault{status: 400, code: code})
		}
		_, err := p.Create(context.Background(), createReq(testKey))
		wantCode(t, err, provider.CodeCapacity, true)
		eq(t, len(f.rents()), 3)
	})
	t.Run("an account refusal ends the create", func(t *testing.T) {
		f, p := liveFake(t)
		f.rentFault("node-a", fault{status: 403, code: "insufficient_balance"})
		_, err := p.Create(context.Background(), createReq(testKey))
		wantCode(t, err, provider.CodeUnfunded, true)
		eq(t, len(f.rents()), 1)
	})
}

func TestLiumTemplateNotFoundRemadeOnce(t *testing.T) {
	f, p := liveFake(t)
	mustCreate(t, p, createReq(freshKey()))
	first := f.rents()[0]
	relist(t, p)
	// The template went away at Lium while this process kept its id
	f.dropTemplates()
	ref := mustCreate(t, p, createReq(testKey))
	rents := f.rents()[1:]
	eq(t, len(rents), 2)
	eq(t, len(f.requestsTo("POST", "/templates")), 2)
	pod, _ := f.pod(ref.ID)
	if pod.template == "" || strings.Contains(string(rents[0].body), pod.template) ||
		!strings.Contains(string(rents[1].body), pod.template) ||
		!strings.Contains(string(first.body), templateOf(t, rents[0])) {
		t.Fatalf("the retry did not carry the new template %q", pod.template)
	}
	eq(t, rents[1].header.Get("Idempotency-Key"), rents[0].header.Get("Idempotency-Key"))

	t.Run("gone twice", func(t *testing.T) {
		f, p := liveFake(t)
		f.inject(rentRoute, fault{status: 404, code: "template_not_found", times: 2})
		_, err := p.Create(context.Background(), createReq(testKey))
		wantCode(t, err, provider.CodeInternal, true)
		eq(t, len(f.rents()), 2)
	})
}

func templateOf(t *testing.T, r recorded) string {
	t.Helper()
	var body struct {
		Template string `json:"template_id"`
	}
	if err := json.Unmarshal(r.body, &body); err != nil {
		t.Fatal(err)
	}
	return body.Template
}

func TestLiumTemplateInvalidDropsTemplate(t *testing.T) {
	f, p := liveFake(t)
	f.inject(rentRoute, fault{status: 400, code: "template_invalid", times: 1})
	_, err := p.Create(context.Background(), createReq(testKey))
	wantCode(t, err, provider.CodeInvalid, true)
	lists := len(f.requestsTo("GET", "/templates"))
	mustCreate(t, p, createReq(freshKey()))
	eq(t, len(f.requestsTo("GET", "/templates")), lists+1)
}
