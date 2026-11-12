package hf

import (
	"context"
	"errors"
	"maps"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// jobReads are the reads of one Job the fake got
func jobReads(f *fakeHF, id string) []recorded {
	return f.requestsTo("GET", "/api/jobs/"+testNamespace+"/"+id)
}

// created starts a Job through the adapter and returns its ref and the fake's Job
func created(t *testing.T, f *fakeHF, p *Provider) (provider.Ref, *fakeJob) {
	t.Helper()
	ref, err := p.Create(t.Context(), keyed())
	if err != nil {
		t.Fatal(err)
	}
	return ref, f.job(ref.ID)
}

func TestHFWaitReady(t *testing.T) {
	f := newFake(t)
	f.runAfter = 3
	p := f.provider(t, testOptions())
	ref, j := created(t, f, p)
	if err := p.WaitReady(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	eq(t, len(jobReads(f, j.id)), 3)
	got := sbxRequests(f)
	eq(t, len(got), 1)
	eq(t, got[0].host, j.id+"--49983.hf.jobs")
	eq(t, got[0].header.Get("X-Sandbox-Token"), testKeyToken)
	eq(t, got[0].header.Get("Authorization"), "")
	if dials := f.dialed(); dials[len(dials)-1] != j.id+"--49983.hf.jobs:443" {
		t.Fatalf("the health read dialled %q", dials)
	}
	// A ref this adapter never made is not found, with no call
	calls := len(f.all())
	err := p.WaitReady(t.Context(), provider.Ref{Provider: "lium", ID: j.id})
	if code, _ := codeOf(err); code != provider.CodeNotFound {
		t.Fatalf("a lium ref gave %v", err)
	}
	eq(t, len(f.all()), calls)
}

func TestHFWaitReadyPollsEvery(t *testing.T) {
	f := newFake(t)
	f.runAfter = 3
	opts := testOptions()
	opts.PollEvery = 100 * time.Millisecond
	p := f.provider(t, opts)
	ref, j := created(t, f, p)
	if err := p.WaitReady(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	at := arrivals(jobReads(f, j.id))
	for i := 1; i < len(at); i++ {
		if gap := at[i].Sub(at[i-1]); gap < 95*time.Millisecond {
			t.Fatalf("read %d came %s after the one before", i+1, gap)
		}
	}
	opts.PollEvery = 0
	if p, err := newProvider(opts); err != nil || p.pollEvery != 3*time.Second {
		t.Fatalf("no poll interval gave %v, %v, want 3 s", p.pollEvery, err)
	}
}

func TestHFWaitReadyHealth(t *testing.T) {
	type tc struct {
		name string
		// fault answers the first health reads
		fault *fault
		// body is every health answer of the Job's sbx-server
		body string
		// token replaces the ref's token
		token string
		// code is the wait's error code, empty when it ends ready
		code   provider.Code
		reads  int
		reason string
	}
	cases := []tc{
		{name: "the proxy before the server listens",
			fault: &fault{status: 502, raw: "<html>502</html>", times: 2}, reads: 3},
		{name: "a page that is not the server's",
			fault: &fault{status: 200, raw: "<html>starting</html>", times: 2}, reads: 3},
		{name: "an answer with no protocol",
			fault: &fault{status: 200, raw: `{"status":"ok"}`, times: 1}, reads: 2},
		{name: "a redirect", fault: &fault{status: 307, raw: "moved", times: 1,
			header: map[string]string{"Location": "https://evil.example/x"}}, reads: 2},
		{name: "a server error", fault: &fault{status: 500, raw: "oops", times: 1}, reads: 2},
		{name: "an old protocol",
			body: `{"status":"ok","protocol":2,"mode":"dedicated","auth":"required"}`,
			code: provider.CodeInternal, reads: 1, reason: "protocol 2"},
		{name: "a token the server does not take", token: strings.Repeat("0", 64),
			code: provider.CodeInternal, reads: 1, reason: "hf_token_refused"},
		{name: "pool mode",
			body: `{"status":"ok","protocol":3,"mode":"pool","auth":"required"}`,
			code: provider.CodeInternal, reads: 1, reason: `mode "pool"`},
		{name: "no auth",
			body: `{"status":"ok","protocol":3,"mode":"dedicated","auth":"none"}`,
			code: provider.CodeInternal, reads: 1, reason: `auth "none"`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFake(t)
			p := f.provider(t, testOptions())
			ref, j := created(t, f, p)
			if c.fault != nil {
				f.inject(healthRoute, *c.fault)
			}
			if c.body != "" {
				f.set(j, func(j *fakeJob) { j.health = c.body })
			}
			if c.token != "" {
				ref.Meta["secret"] = c.token
			}
			err := p.WaitReady(t.Context(), ref)
			code, failOver := codeOf(err)
			if code != c.code || c.code != "" && !failOver {
				t.Fatalf("got %v, want code %q", err, c.code)
			}
			if c.reason != "" && !strings.Contains(err.Error(), c.reason) {
				t.Fatalf("the error %q does not name %q", err, c.reason)
			}
			eq(t, len(sbxRequests(f)), c.reads)
			for _, r := range sbxRequests(f) {
				if r.host != j.id+"--49983.hf.jobs" {
					t.Fatalf("a health read went to %s", r.host)
				}
			}
			if err != nil && strings.Contains(err.Error(), ref.Meta["secret"]) {
				t.Fatal("the error carries the sandbox token")
			}
		})
	}
}

func TestHFWaitReadyFailsAtOnceOnAnEnd(t *testing.T) {
	cases := []struct {
		stage, cancelReason, message string
		code                         provider.Code
		failOver, named              bool
	}{
		{"ERROR", "", "exit code 1", provider.CodeCapacity, true, false},
		{"ERROR", "NO_CREDITS", "", provider.CodeUnfunded, true, true},
		{"CANCELED", "NO_CREDITS", "", provider.CodeUnfunded, true, true},
		{"CANCELED", "NO_SUBSCRIPTION", "", provider.CodeUnfunded, true, true},
		{"CANCELED", "SPEND_LIMIT", "Resource group spend limit reached", provider.CodeQuota,
			true, false},
		{"ERROR", "RESOURCE_GROUP_LIMIT", "", provider.CodeQuota, true, false},
		{"CANCELED", "", "", provider.CodeNotFound, false, false},
		{"CANCELED", "", "Job timeout", provider.CodeNotFound, false, false},
		{"COMPLETED", "", "", provider.CodeNotFound, false, false},
		{"DELETED", "", "", provider.CodeNotFound, false, false},
		{"SUSPENDED", "", "", provider.CodeInternal, false, false},
	}
	for _, c := range cases {
		t.Run(c.stage+" "+c.cancelReason, func(t *testing.T) {
			f := newFake(t)
			p := f.provider(t, testOptions())
			ref, j := created(t, f, p)
			f.set(j, func(j *fakeJob) {
				j.stage, j.cancelReason, j.message = c.stage, c.cancelReason, c.message
			})
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			err := p.WaitReady(ctx, ref)
			e, ok := provider.AsError(err)
			if !ok || e.Code != c.code || e.CanFailOver != c.failOver ||
				(e.Provider == "hf") != c.named {
				t.Fatalf("got %+v, want code %q fail over %v named %v", err, c.code,
					c.failOver, c.named)
			}
			if !strings.Contains(err.Error(), c.stage) {
				t.Fatalf("the error %q does not name the stage", err)
			}
			eq(t, len(jobReads(f, j.id)), 1)
			eq(t, len(sbxRequests(f)), 0)
		})
	}
}

func TestHFWaitReadyStopsOnARefusedRead(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	ref, j := created(t, f, p)
	for _, c := range []struct {
		fault fault
		code  provider.Code
	}{
		{fault{status: 401, message: "Invalid credentials"}, provider.CodeAuth},
		{fault{status: 404, message: "Job not found"}, provider.CodeNotFound},
		{fault{status: 403, message: "Your account has been suspended"},
			provider.CodeAbuseLock},
	} {
		before := len(jobReads(f, j.id))
		c.fault.times = 1
		f.inject(jobRoute, c.fault)
		err := p.WaitReady(t.Context(), ref)
		if code, _ := codeOf(err); code != c.code {
			t.Fatalf("a %d gave %v, want %s", c.fault.status, err, c.code)
		}
		eq(t, len(jobReads(f, j.id)), before+1)
	}
	// A read HF failed or held back is read again
	for _, ft := range []fault{
		{status: 500, message: "Internal Error", times: 2},
		{status: 429, message: "slow down", times: 1,
			header: map[string]string{"Retry-After": "1"}},
	} {
		f.inject(jobRoute, ft)
		if err := p.WaitReady(t.Context(), ref); err != nil {
			t.Fatalf("a %d on a read gave %v", ft.status, err)
		}
	}
}

func TestHFWaitReadyTimesOut(t *testing.T) {
	wait := func(p *Provider, ref provider.Ref) error {
		ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
		defer cancel()
		return p.WaitReady(ctx, ref)
	}
	f := newFake(t)
	f.runAfter = 0
	p := f.provider(t, testOptions())
	ref, _ := created(t, f, p)
	// HF never scheduled the Job, so the order may go elsewhere
	err := wait(p, ref)
	if code, failOver := codeOf(err); code != provider.CodeCapacity || !failOver ||
		!errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "SCHEDULING") {
		t.Fatalf("a Job scheduling past the deadline gave %v, want capacity", err)
	}

	f = newFake(t)
	p = f.provider(t, testOptions())
	ref, _ = created(t, f, p)
	f.inject(healthRoute, fault{status: 502, raw: "<html>502 Bad Gateway</html>"})
	err = wait(p, ref)
	if code, failOver := codeOf(err); code != provider.CodeInternal || !failOver ||
		!errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "502") {
		t.Fatalf("a server that never answered gave %v, want internal naming the 502", err)
	}

	f = newFake(t)
	p = f.provider(t, testOptions())
	ref, _ = created(t, f, p)
	f.inject(jobRoute, fault{status: 503, message: "Unavailable"})
	err = wait(p, ref)
	if code, failOver := codeOf(err); code != provider.CodeInternal || !failOver ||
		!strings.Contains(err.Error(), "Unavailable") {
		t.Fatalf("reads that never worked gave %v, want internal naming the last", err)
	}
}

func TestHFWaitReadyDerivesAMissingToken(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	ref, j := created(t, f, p)
	// The store drops a sealed token it cannot open, the Job's key gives it again
	delete(ref.Meta, "secret")
	if err := p.WaitReady(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	eq(t, sbxRequests(f)[0].header.Get("X-Sandbox-Token"), testKeyToken)

	f.set(j, func(j *fakeJob) { delete(j.labels, "bugnet_key") })
	err := p.WaitReady(t.Context(), ref)
	if code, _ := codeOf(err); code != provider.CodeInternal ||
		!strings.Contains(err.Error(), "no token") {
		t.Fatalf("a Job with no token anywhere gave %v", err)
	}
	eq(t, len(sbxRequests(f)), 1)
}

// TestHFWaitReadyKeepsToItsJob ends a wait whose read names another Job, which never gets a token
func TestHFWaitReadyKeepsToItsJob(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	ref, _ := created(t, f, p)
	other := keyed()
	other.IdempotencyKey = "fedcba9876543210fedcba9876543210"
	oref, err := p.Create(t.Context(), other)
	if err != nil {
		t.Fatal(err)
	}
	var labels map[string]string
	f.set(f.job(oref.ID), func(j *fakeJob) {
		j.stage, labels = "RUNNING", maps.Clone(j.labels)
	})
	f.mu.Lock()
	f.show = func(j map[string]any) {
		if j["id"] == ref.ID {
			j["id"], j["labels"] = oref.ID, labels
		}
	}
	f.mu.Unlock()
	bare := cloneRef(ref)
	delete(bare.Meta, "secret")
	for _, r := range []provider.Ref{ref, bare} {
		err := p.WaitReady(t.Context(), r)
		if code, _ := codeOf(err); code != provider.CodeInternal {
			t.Fatalf("a read naming another Job gave %v, want internal", err)
		}
	}
	eq(t, len(sbxRequests(f)), 0)
}

// stallFirst answers the first request with a 200 whose body never comes, the rest through next
type stallFirst struct {
	next    http.RoundTripper
	stalled atomic.Bool
}

func (s *stallFirst) RoundTrip(r *http.Request) (*http.Response, error) {
	if s.stalled.Swap(true) {
		return s.next.RoundTrip(r)
	}
	return &http.Response{
		StatusCode: http.StatusOK, Header: http.Header{}, Body: stalledBody{r}, Request: r,
	}, nil
}

// stalledBody sends nothing until its request ends
type stalledBody struct{ r *http.Request }

func (b stalledBody) Read([]byte) (int, error) {
	<-b.r.Context().Done()
	return 0, b.r.Context().Err()
}

func (stalledBody) Close() error { return nil }

func TestHFWaitReadyBoundsAHealthRead(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions())
	eq(t, p.healthWait, 10*time.Second)
	p.healthWait = 100 * time.Millisecond
	p.sbx.client.Transport = &stallFirst{next: p.sbx.client.Transport}
	ref, _ := created(t, f, p)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	start := time.Now()
	if err := p.WaitReady(ctx, ref); err != nil {
		t.Fatalf("a wait whose first health read stalled gave %v", err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("a stalled health read held the wait for %s", took)
	}
	eq(t, len(sbxRequests(f)), 1)
}
