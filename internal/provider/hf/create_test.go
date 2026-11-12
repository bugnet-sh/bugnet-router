package hf

import (
	"bytes"
	"context"
	"encoding/json"
	"maps"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const (
	testKey = "0123456789abcdef0123456789abcdef"
	// testKeyToken is testKey's token under testSeed, computed outside Go
	testKeyToken = "78477f9ed1deae7165c07faa3ed9d988fbf0bee8c0deb337d20e99bf9b8d0b0a"
	createRoute  = "POST /api/jobs/{ns}"
)

// keyed is a create with testKey, a placement's labels and a 20 minute lifetime
func keyed() provider.CreateRequest {
	return provider.CreateRequest{
		IdempotencyKey: testKey, Size: provider.Size{Name: "t4-small"},
		Labels: map[string]string{
			"bugnet": "1", "bugnet_deployment": "main",
			"placement": "9f1c2d3e-0000-4000-8000-000000000001",
		},
		Lifetime: 20 * time.Minute,
	}
}

// posts are the creates the fake got, their bodies decoded
func posts(t *testing.T, f *fakeHF) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, r := range f.requestsTo("POST", "/api/jobs/"+testNamespace) {
		var body map[string]any
		if err := json.Unmarshal(r.body, &body); err != nil {
			t.Fatal(err)
		}
		out = append(out, body)
	}
	return out
}

// lists are the list calls the fake got
func lists(f *fakeHF) []recorded { return f.requestsTo("GET", "/api/jobs/"+testNamespace) }

// codeOf is an error's provider code and fail over, empty for an error with none
func codeOf(err error) (provider.Code, bool) {
	if e, ok := provider.AsError(err); ok {
		return e.Code, e.CanFailOver
	}
	return "", false
}

// design410 is design 4.10's create body for testKey's Job with keyed's labels
const design410 = `{
  "dockerImage": "` + testImage + `",
  "command": ["/bin/sh", "-c", "set -e; d=/tmp/.sbx-server; ` +
	`curl -fsSL -o \"$d\" \"$SBX_SERVER_URL\"; ` +
	`echo \"$SBX_SERVER_SHA256  $d\" | sha256sum -c -; chmod +x \"$d\"; ` +
	`unset SBX_SERVER_URL SBX_SERVER_SHA256; exec \"$d\""],
  "arguments": [],
  "environment": {
    "SBX_PORT": "49983",
    "SBX_SERVER_URL": "https://huggingface.co/buckets/huggingface/sbx-server/resolve/` +
	`sbx-server-eb2b04f79fbe765195300aaf1582be2a2a9ea698ff067d0cab428171d5584932",
    "SBX_SERVER_SHA256": "eb2b04f79fbe765195300aaf1582be2a2a9ea698ff067d0cab428171d5584932"
  },
  "secrets": {"SBX_TOKEN": "` + testKeyToken + `"},
  "flavor": "t4-small",
  "arch": "amd64",
  "timeoutSeconds": 5400,
  "attempts": 1,
  "labels": {"bugnet": "1", "bugnet_deployment": "main",
    "placement": "9f1c2d3e-0000-4000-8000-000000000001", "bugnet_key": "` + testKey + `"},
  "expose": {"ports": [49983, 29417], "portsPublic": [49983, 29417]}
}`

// TestHFSandboxToken pins design 4.4's derivation to answers computed outside Go
func TestHFSandboxToken(t *testing.T) {
	p, err := newProvider(testOptions())
	if err != nil {
		t.Fatal(err)
	}
	eq(t, p.sandboxToken("0123456789abcdef0123456789abcdef"),
		"78477f9ed1deae7165c07faa3ed9d988fbf0bee8c0deb337d20e99bf9b8d0b0a")
	eq(t, p.sandboxToken("fedcba9876543210fedcba9876543210"),
		"cfe2b5bbc7b1cf36506190d955972588c081901d1577b2e58999fa855401f0e7")
	opts := testOptions()
	opts.SandboxSeed = bytes.Repeat([]byte{7}, 32)
	other, err := newProvider(opts)
	if err != nil {
		t.Fatal(err)
	}
	if other.sandboxToken("0123456789abcdef0123456789abcdef") ==
		p.sandboxToken("0123456789abcdef0123456789abcdef") {
		t.Fatal("two seeds gave one token")
	}
	// The provider keeps its own copy, so a caller clearing the seed changes nothing
	clear(opts.SandboxSeed)
	if other.sandboxToken("k") == p.sandboxToken("k") {
		t.Fatal("the provider shares the caller's seed")
	}
}

func TestHFJobKey(t *testing.T) {
	eq(t, jobKey("0123456789abcdef0123456789abcdef"), "0123456789abcdef0123456789abcdef")
	// The saga's keys are 32 hex already, any other key is hashed to that form
	eq(t, jobKey("order-1|1|hf"), "81e213554744e814d911849108067244")
	eq(t, jobKey("0123456789ABCDEF0123456789ABCDEF") == "0123456789ABCDEF0123456789ABCDEF",
		false)
	a, b := freshKey(), freshKey()
	if a == b || !keyHex.MatchString(a) || !keyHex.MatchString(b) {
		t.Fatalf("fresh keys %q and %q", a, b)
	}
}

func TestHFCreateSendsTheJobSpec(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	req := keyed()
	asked := maps.Clone(req.Labels)
	before := time.Now().UTC().Truncate(time.Second)
	ref, err := p.Create(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	var want map[string]any
	if err := json.Unmarshal([]byte(design410), &want); err != nil {
		t.Fatal(err)
	}
	got := posts(t, f)
	eq(t, len(got), 1)
	eq(t, got[0], want)
	// The key's Jobs are listed before the create, which comes last
	all := f.all()
	eq(t, len(all), 2)
	eq(t, all[0].method+" "+all[0].path, "GET /api/jobs/"+testNamespace)
	eq(t, all[0].query["label"], []string{"bugnet_key=" + testKey})
	eq(t, all[0].query["stage"], []string{"SCHEDULING", "RUNNING"})
	eq(t, all[1].method, "POST")
	eq(t, req.Labels, asked)

	f.mu.Lock()
	id := f.jobs[0].id
	f.mu.Unlock()
	created, err := time.Parse(time.RFC3339, ref.Meta["created_at"])
	if err != nil || created.Before(before) || created.After(time.Now()) {
		t.Fatalf("created_at %q is not the create's time", ref.Meta["created_at"])
	}
	eq(t, ref, provider.Ref{Provider: "hf", ID: id, Meta: map[string]string{
		"job_id": id, "created_at": ref.Meta["created_at"], "timeout": "5400",
		"secret": testKeyToken, "arch": "amd64",
	}})
}

func TestHFCreateTimeout(t *testing.T) {
	cases := []struct {
		name         string
		lifetime     time.Duration
		room, margin time.Duration
		maxLifetime  time.Duration
		timeout      int
		ref          string
	}{
		{"a 20 minute run", 20 * time.Minute, time.Hour, 10 * time.Minute, 2 * time.Hour,
			5400, "5400"},
		{"a 10 minute package with the slack", 15 * time.Minute, time.Hour, 10 * time.Minute,
			2 * time.Hour, 5100, "5100"},
		{"capped at the most a run may last", 2 * time.Hour, time.Hour, 10 * time.Minute,
			2 * time.Hour, 7800, "7800"},
		{"capped with part of the room", 61 * time.Minute, time.Hour, 10 * time.Minute,
			2 * time.Hour, 7800, "7800"},
		{"no room and no margin", 15 * time.Minute, 0, 0, 2 * time.Hour, 900, "900"},
		{"a part of a second rounds up", 20*time.Minute + time.Millisecond, time.Hour,
			10 * time.Minute, 2 * time.Hour, 5401, "5401"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t)
			opts := testOptions()
			opts.ExtendRoom, opts.TimeoutMargin, opts.MaxLifetime = tc.room, tc.margin,
				tc.maxLifetime
			p := f.provider(t, opts)
			req := keyed()
			req.Lifetime = tc.lifetime
			ref, err := p.Create(t.Context(), req)
			if err != nil {
				t.Fatal(err)
			}
			eq(t, posts(t, f)[0]["timeoutSeconds"], any(float64(tc.timeout)))
			eq(t, ref.Meta["timeout"], tc.ref)
		})
	}
}

// TestHFRetrySameJob retries a create whose Job HF does not list yet, in the same process
func TestHFRetrySameJob(t *testing.T) {
	f := newFake(t)
	f.unlistNew = true
	p := f.provider(t, testOptions())
	first, err := p.Create(t.Context(), keyed())
	if err != nil {
		t.Fatal(err)
	}
	made := len(f.all())
	again, err := p.Create(t.Context(), keyed())
	if err != nil {
		t.Fatal(err)
	}
	eq(t, again, first)
	// The key's Job comes from memory, with no list and no second create
	eq(t, len(f.all()), made)
	eq(t, len(posts(t, f)), 1)
	eq(t, f.jobCount(), 1)
	// A caller changing its ref leaves the kept one as it was
	again.Meta["secret"] = "changed"
	third, err := p.Create(t.Context(), keyed())
	if err != nil {
		t.Fatal(err)
	}
	eq(t, third.Meta["secret"], testKeyToken)
}

func TestHFCreateForgetsAKeyAfterAnHour(t *testing.T) {
	f := newFake(t)
	f.unlistNew = true
	p := f.provider(t, testOptions())
	now := time.Now()
	p.now = func() time.Time { return now }
	first, err := p.Create(t.Context(), keyed())
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(59 * time.Minute)
	if again, err := p.Create(t.Context(), keyed()); err != nil || again.ID != first.ID {
		t.Fatalf("a retry within the hour gave %+v, %v", again, err)
	}
	now = now.Add(2 * time.Minute)
	other := keyed()
	other.IdempotencyKey = "fedcba9876543210fedcba9876543210"
	if _, err := p.Create(t.Context(), other); err != nil {
		t.Fatal(err)
	}
	// Keeping a new key dropped the one past the hour
	p.kmu.Lock()
	_, kept := p.kept[testKey]
	p.kmu.Unlock()
	if kept {
		t.Fatal("a key past the hour is still kept")
	}
	again, err := p.Create(t.Context(), keyed())
	if err != nil {
		t.Fatal(err)
	}
	if again.ID == first.ID {
		t.Fatal("a key past the hour still gave its Job from memory")
	}
	eq(t, len(posts(t, f)), 3)
}

func TestHFCreateAdoptsALiveJob(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	req := keyed()
	ours := maps.Clone(req.Labels)
	ours["bugnet_key"] = testKey
	other := maps.Clone(ours)
	other["placement"] = "another-placement"
	// Ended Jobs and a Job of the key with other labels are never adopted
	for _, stage := range []string{"CANCELED", "ERROR", "COMPLETED", "DELETED"} {
		f.addJob(stage, ours)
	}
	// The oldest Job, so only its labels keep it from being adopted
	f.set(f.addJob("RUNNING", other), func(j *fakeJob) {
		j.createdAt = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	})
	live := f.addJob("SCHEDULING", ours)
	newer := f.addJob("RUNNING", ours)
	f.set(live, func(j *fakeJob) {
		j.createdAt = time.Date(2026, 11, 12, 10, 0, 0, 500e6, time.UTC)
		j.timeout = 5000
	})
	f.set(newer, func(j *fakeJob) { j.createdAt = time.Date(2026, 11, 12, 10, 1, 0, 0, time.UTC) })
	ref, err := p.Create(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, ref, provider.Ref{Provider: "hf", ID: live.id, Meta: map[string]string{
		"job_id": live.id, "created_at": "2026-11-12T10:00:00Z", "timeout": "5000",
		"secret": testKeyToken, "arch": "amd64",
	}})
	eq(t, len(posts(t, f)), 0)
	// The adopted Job is kept, so a retry makes no call
	calls := len(f.all())
	if again, err := p.Create(t.Context(), req); err != nil || again.ID != live.id {
		t.Fatalf("a retry gave %+v, %v", again, err)
	}
	eq(t, len(f.all()), calls)
}

// TestHFCreateChecksWhatItAdopts lists against a fake that ignores every filter
func TestHFCreateChecksWhatItAdopts(t *testing.T) {
	f := newFake(t)
	f.unfiltered = true
	p := f.provider(t, testOptions())
	req := keyed()
	ours := maps.Clone(req.Labels)
	ours["bugnet_key"] = testKey
	old := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, stage := range []string{"CANCELED", "ERROR", "COMPLETED", "DELETED", "RUNNING"} {
		f.set(f.addJob(stage, ours), func(j *fakeJob) { j.createdAt = old })
	}
	// The live one's id is not one plain path segment
	f.mu.Lock()
	f.jobs[len(f.jobs)-1].id = "6911.not-a-segment"
	f.mu.Unlock()
	f.addJob("RUNNING", map[string]string{"bugnet": "1", "bugnet_key": testKey})
	ref, err := p.Create(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(posts(t, f)), 1)
	f.mu.Lock()
	id := f.jobs[len(f.jobs)-1].id
	f.mu.Unlock()
	eq(t, ref.ID, id)
}

// TestHFCreateAdoptsAcrossProcesses has a second router adopt the Job a first one started
func TestHFCreateAdoptsAcrossProcesses(t *testing.T) {
	f := newFake(t)
	first, err := f.provider(t, testOptions()).Create(t.Context(), keyed())
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.provider(t, testOptions()).Create(t.Context(), keyed())
	if err != nil {
		t.Fatal(err)
	}
	eq(t, second.ID, first.ID)
	eq(t, second.Meta["secret"], first.Meta["secret"])
	eq(t, len(posts(t, f)), 1)
}

// TestEmptyKeyIsFresh gives two creates without a key two Jobs, two keys and two tokens
func TestEmptyKeyIsFresh(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	req := keyed()
	req.IdempotencyKey = ""
	a, err := p.Create(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.Create(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	if a.ID == b.ID || a.Meta["secret"] == b.Meta["secret"] {
		t.Fatalf("two creates without a key gave %+v and %+v", a, b)
	}
	got := posts(t, f)
	eq(t, len(got), 2)
	// No list runs first, so no Job of another keyless create is ever adopted
	eq(t, len(lists(f)), 0)
	var keys []string
	for i, body := range got {
		key := body["labels"].(map[string]any)["bugnet_key"].(string)
		token := body["secrets"].(map[string]any)["SBX_TOKEN"].(string)
		if !keyHex.MatchString(key) || token != p.sandboxToken(key) {
			t.Fatalf("create %d carried key %q and token %q", i, key, token)
		}
		keys = append(keys, key)
	}
	if keys[0] == keys[1] {
		t.Fatalf("both creates carried key %s", keys[0])
	}
	eq(t, a.Meta["secret"], p.sandboxToken(keys[0]))
	for _, r := range f.all() {
		if r.header.Get("Idempotency-Key") != "" {
			t.Fatalf("%s %s sent an Idempotency-Key header", r.method, r.path)
		}
	}
	p.kmu.Lock()
	defer p.kmu.Unlock()
	eq(t, len(p.kept), 0)
}

func TestHFCreateUsesTheRequestImage(t *testing.T) {
	other := "ghcr.io/bugnet-sh/image-check:2026.11.4@sha256:" + strings.Repeat("ab", 32)
	f := newFake(t)
	p := f.provider(t, testOptions())
	req := keyed()
	req.Image = other
	if _, err := p.Create(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	eq(t, posts(t, f)[0]["dockerImage"], any(other))

	f = newFake(t)
	opts := testOptions()
	opts.Image = ""
	p = f.provider(t, opts)
	for _, image := range []string{"", "ghcr.io/bugnet-sh/bugnet-base:2026.11.3",
		"ghcr.io/bugnet-sh/bugnet-base@sha256:2a47", "-v@sha256:" + strings.Repeat("ab", 32),
		"ghcr.io/x@sha256:" + strings.Repeat("AB", 32),
	} {
		req.Image = image
		_, err := p.Create(t.Context(), req)
		if code, failOver := codeOf(err); code != provider.CodeInvalid || !failOver {
			t.Errorf("image %q gave %v, want invalid with fail over", image, err)
		}
	}
	eq(t, len(f.all()), 0)
}

func TestHFCreateRefuses(t *testing.T) {
	long := strings.Repeat("a", 101)
	cases := map[string]struct {
		change   func(*provider.CreateRequest)
		failOver bool
	}{
		"a builder":       {func(r *provider.CreateRequest) { r.Builder = true }, false},
		"a CPU flavor":    {func(r *provider.CreateRequest) { r.Size.Name = "cpu-basic" }, false},
		"a flavor unsold": {func(r *provider.CreateRequest) { r.Size.Name = "l4x1" }, false},
		"no lifetime":     {func(r *provider.CreateRequest) { r.Lifetime = 0 }, false},
		"a lifetime past the most": {func(r *provider.CreateRequest) {
			r.Lifetime = 2*time.Hour + time.Second
		}, false},
		"a label key with a dot": {func(r *provider.CreateRequest) { r.Labels["a.b"] = "1" },
			true},
		"a label value with a space": {func(r *provider.CreateRequest) {
			r.Labels["placement"] = "a b"
		}, true},
		"a long label key":   {func(r *provider.CreateRequest) { r.Labels[long] = "1" }, true},
		"a long label value": {func(r *provider.CreateRequest) { r.Labels["x"] = long }, true},
		"an empty label key": {func(r *provider.CreateRequest) { r.Labels[""] = "1" }, true},
		"the adapter's own label": {func(r *provider.CreateRequest) {
			r.Labels["bugnet_key"] = testKey
		}, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := newFake(t)
			opts := testOptions()
			opts.Flavors = []string{"t4-small"}
			p := f.provider(t, opts)
			req := keyed()
			tc.change(&req)
			_, err := p.Create(t.Context(), req)
			if code, failOver := codeOf(err); code != provider.CodeInvalid ||
				failOver != tc.failOver {
				t.Fatalf("got %v, want invalid with fail over %v", err, tc.failOver)
			}
			eq(t, len(f.all()), 0)
		})
	}
	// A label at HF's longest is taken
	f := newFake(t)
	p := f.provider(t, testOptions())
	req := keyed()
	req.Labels[long[:100]] = long[:100]
	if _, err := p.Create(t.Context(), req); err != nil {
		t.Fatal(err)
	}
}

func TestHFCreateLostAnswer(t *testing.T) {
	cases := []struct {
		name string
		// fresh sends no key
		fresh, unlisted bool
		fault           fault
		// relist faults the listing after the create
		relist   *fault
		code     provider.Code
		failOver bool
	}{
		{name: "sent and dropped, then listed", fault: fault{drop: true, served: true}},
		{name: "a server error that started the Job",
			fault: fault{status: 500, message: "Internal Error", served: true}},
		{name: "an unreadable answer to a Job started",
			fault: fault{status: 200, raw: "{not json", served: true}},
		{name: "a fresh create sent and dropped, then listed", fresh: true,
			fault: fault{drop: true, served: true}},
		{name: "a server error that started nothing",
			fault: fault{status: 503, message: "Service Unavailable"},
			code:  provider.CodeInternal, failOver: true},
		{name: "sent and dropped, never listed", unlisted: true,
			fault: fault{drop: true, served: true}, code: provider.CodeOutcomeUnknown},
		{name: "dropped before it started anything", fault: fault{drop: true},
			code: provider.CodeOutcomeUnknown},
		{name: "a server error, then the listing failed", fresh: true,
			fault:  fault{status: 500, message: "Internal Error", served: true},
			relist: &fault{status: 500, message: "Internal Error"},
			code:   provider.CodeOutcomeUnknown},
		{name: "rate limited", fault: fault{status: 429, message: "slow down"},
			code: provider.CodeRateLimited, failOver: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t)
			f.unlistNew = tc.unlisted
			p := f.provider(t, testOptions())
			req := keyed()
			if tc.fresh {
				req.IdempotencyKey = ""
			}
			tc.fault.times = 1
			f.inject(createRoute, tc.fault)
			if tc.relist != nil {
				f.inject(listRoute, *tc.relist)
			}
			ref, err := p.Create(t.Context(), req)
			if code, failOver := codeOf(err); code != tc.code || failOver != tc.failOver {
				t.Fatalf("got %+v, %v, want code %q with fail over %v", ref, err, tc.code,
					tc.failOver)
			}
			if tc.code == provider.CodeRateLimited {
				eq(t, len(lists(f)), 1)
				return
			}
			if tc.code != "" {
				return
			}
			f.mu.Lock()
			id, token := f.jobs[0].id, f.jobs[0].token
			f.mu.Unlock()
			eq(t, ref.ID, id)
			eq(t, ref.Meta["secret"], token)
			eq(t, ref.Meta["timeout"], "5400")
			if tc.fresh {
				return
			}
			// The Job found is kept for the key's retries
			calls := len(f.all())
			if again, err := p.Create(t.Context(), req); err != nil || again.ID != id {
				t.Fatalf("a retry gave %+v, %v", again, err)
			}
			eq(t, len(f.all()), calls)
		})
	}
}

// TestHFCreateLostAnswerPastTheDeadline lists for the Job once its caller's time has run out
func TestHFCreateLostAnswerPastTheDeadline(t *testing.T) {
	f := newFake(t)
	f.createDelay = 300 * time.Millisecond
	p := f.provider(t, testOptions())
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	ref, err := p.Create(ctx, keyed())
	if err != nil {
		t.Fatalf("a create whose caller gave up before HF answered gave %v", err)
	}
	f.mu.Lock()
	id := f.jobs[0].id
	f.mu.Unlock()
	eq(t, ref.ID, id)
	eq(t, ref.Meta["secret"], testKeyToken)
}

// TestHFCreateLeansEarlyAndShort keeps a found Job's end no later than its create could make it
func TestHFCreateLeansEarlyAndShort(t *testing.T) {
	later := time.Now().Add(time.Hour).UTC()
	for _, c := range []struct {
		name string
		show func(j map[string]any)
	}{
		{"a later start and a longer timeout", func(j map[string]any) {
			j["createdAt"], j["timeout"] = later.Format(time.RFC3339Nano), 99999
		}},
		{"no timeout", func(j map[string]any) { delete(j, "timeout") }},
	} {
		t.Run(c.name, func(t *testing.T) {
			// A create whose answer was lost, found again by its key
			f := newFake(t)
			f.show = c.show
			p := f.provider(t, testOptions())
			f.inject(createRoute, fault{drop: true, served: true, times: 1})
			before := time.Now().UTC().Truncate(time.Second)
			ref, err := p.Create(t.Context(), keyed())
			if err != nil {
				t.Fatal(err)
			}
			created, err := time.Parse(time.RFC3339, ref.Meta["created_at"])
			if err != nil || created.Before(before) || created.After(time.Now()) {
				t.Fatalf("created_at %q is not the create's send time", ref.Meta["created_at"])
			}
			eq(t, ref.Meta["timeout"], "5400")
			end, ok := p.MaxUntil(ref)
			eq(t, ok, true)
			eq(t, end, created.Add(80*time.Minute))

			// A live Job of the key another router started
			f = newFake(t)
			f.show = c.show
			ours := maps.Clone(keyed().Labels)
			ours["bugnet_key"] = testKey
			j := f.addJob("RUNNING", ours)
			ref, err = f.provider(t, testOptions()).Create(t.Context(), keyed())
			if err != nil {
				t.Fatal(err)
			}
			eq(t, ref.ID, j.id)
			eq(t, ref.Meta["timeout"], "5400")
		})
	}
}

func TestHFCreateSameKeyAtOnce(t *testing.T) {
	f := newFake(t)
	f.unlistNew = true
	f.createDelay = 200 * time.Millisecond
	p := f.provider(t, testOptions())
	refs := make(chan provider.Ref, 5)
	var wg sync.WaitGroup
	for range 5 {
		wg.Go(func() {
			ref, err := p.Create(t.Context(), keyed())
			if err != nil {
				t.Error(err)
			}
			refs <- ref
		})
	}
	wg.Wait()
	close(refs)
	ids := map[string]bool{}
	for ref := range refs {
		ids[ref.ID] = true
	}
	eq(t, len(ids), 1)
	eq(t, len(posts(t, f)), 1)

	// A caller waiting on the key's create gives up with its context
	f.createDelay = time.Second
	other := keyed()
	other.IdempotencyKey = "fedcba9876543210fedcba9876543210"
	started := make(chan struct{})
	wg.Go(func() {
		close(started)
		if _, err := p.Create(t.Context(), other); err != nil {
			t.Error(err)
		}
	})
	<-started
	time.Sleep(100 * time.Millisecond)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	_, err := p.Create(ctx, other)
	if code, failOver := codeOf(err); code != provider.CodeOutcomeUnknown || failOver {
		t.Fatalf("a caller that gave up waiting gave %v, want outcome_unknown", err)
	}
	wg.Wait()
	eq(t, len(posts(t, f)), 2)
}
