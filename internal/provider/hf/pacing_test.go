package hf

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

func TestHFRetryAfter(t *testing.T) {
	date := time.Now().Add(30 * time.Second).UTC().Format(http.TimeFormat)
	cases := []struct {
		name                  string
		retryAfter, rateLimit string
		low, high             time.Duration
	}{
		{"neither", "", "", time.Second, time.Second},
		{"seconds", "7", "", 7 * time.Second, 7 * time.Second},
		{"a date", date, "", 28 * time.Second, 30 * time.Second},
		{"the window's reset", "", `"api";r=0;t=129`, 129 * time.Second, 129 * time.Second},
		{"the longer of both", "3", `"api";r=0;t=10`, 10 * time.Second, 10 * time.Second},
		{"the spent bucket's reset", "", `"api";r=0;t=12, "resolvers";r=5;t=40`,
			12 * time.Second, 12 * time.Second},
		{"no bucket spent", "", `"api";r=4;t=40`, 40 * time.Second, 40 * time.Second},
		{"zero", "0", `"api";r=0;t=0`, time.Second, time.Second},
		{"past the window", "", `"api";r=0;t=100000`, 5 * time.Minute, 5 * time.Minute},
		{"huge seconds", "9223372036854775807", "", 5 * time.Minute, 5 * time.Minute},
		{"unreadable", "soon", `"api";r=x;t=y`, time.Second, time.Second},
		{"negative", "-5", `"api";r=0;t=-5`, time.Second, time.Second},
	}
	for _, tc := range cases {
		h := http.Header{}
		if tc.retryAfter != "" {
			h.Set("Retry-After", tc.retryAfter)
		}
		if tc.rateLimit != "" {
			h.Set("RateLimit", tc.rateLimit)
		}
		if got := retryAfter(h); got < tc.low || got > tc.high {
			t.Errorf("%s: retryAfter = %s, want %s to %s", tc.name, got, tc.low, tc.high)
		}
	}
}

// arrivals are the times the fake got each request, in order
func arrivals(rs []recorded) []time.Time {
	out := make([]time.Time, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.at)
	}
	slices.SortFunc(out, func(a, b time.Time) int { return a.Compare(b) })
	return out
}

func TestHFRateLimitHoldsEveryCall(t *testing.T) {
	ctx := context.Background()
	f := newFake(t)
	p := f.provider(t, testOptions())
	f.inject(listRoute, fault{status: 429, message: "slow down", times: 1,
		header: map[string]string{
			"RateLimit":        `"api";r=0;t=1`,
			"RateLimit-Policy": `"fixed window";"api";q=1000;w=300`,
		}})
	err := listOnce(ctx, p)
	e, ok := provider.AsError(err)
	if !ok || e.Code != provider.CodeRateLimited || !e.CanFailOver ||
		e.RetryAfter != time.Second || e.Provider != "" {
		t.Fatalf("a 429 gave %v, want rate_limited with fail over after a second", err)
	}
	limited := f.all()[0].at
	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() {
			if err := listOnce(ctx, p); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	got := arrivals(f.all())
	eq(t, len(got), 4)
	if gap := got[1].Sub(limited); gap < time.Second-50*time.Millisecond {
		t.Fatalf("a call went %s after the 429 that asked for a second", gap)
	}

	// A caller that cannot wait out the hold gives up before sending
	f.inject(listRoute, fault{status: 429, message: "slow down", times: 1,
		header: map[string]string{"Retry-After": "3"}})
	if err := listOnce(ctx, p); err == nil {
		t.Fatal("the second 429 was taken as an answer")
	}
	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	err = listOnce(short, p)
	e, ok = provider.AsError(err)
	if !ok || e.Code != provider.CodeInternal || !e.CanFailOver ||
		!errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a call held past its caller's deadline gave %v", err)
	}
	eq(t, len(f.all()), 5)
}

func TestHFRateLimitKeepsTheLongerHold(t *testing.T) {
	p, err := newProvider(testOptions())
	if err != nil {
		t.Fatal(err)
	}
	limited := func(retryAfter string) {
		h := http.Header{"Retry-After": {retryAfter}}
		_ = p.jobs.classify(t.Context(), http.StatusTooManyRequests, h, nil, call{})
	}
	limited("30")
	limited("1")
	if held := p.jobs.held(); held < 29*time.Second {
		t.Fatalf("a 1 s 429 after a 30 s one left a hold of %s", held)
	}
}

// TestHFRateLimitHoldsQueuedCalls has calls waiting for their token when a 429 arrives
func TestHFRateLimitHoldsQueuedCalls(t *testing.T) {
	ctx := context.Background()
	f := newFake(t)
	opts := testOptions()
	opts.OpsPerSecond = 2
	p := f.provider(t, opts)
	f.inject(listRoute, fault{status: 429, message: "slow down", times: 1,
		header: map[string]string{"Retry-After": "2"}})
	errs := make(chan error, 3)
	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() { errs <- listOnce(ctx, p) })
	}
	wg.Wait()
	close(errs)
	var limited int
	for err := range errs {
		if e, ok := provider.AsError(err); ok && e.Code == provider.CodeRateLimited {
			limited++
		} else if err != nil {
			t.Error(err)
		}
	}
	got := arrivals(f.all())
	eq(t, len(got), 3)
	eq(t, limited, 1)
	// The first call got the 429, the two queued behind it at 0.5 s and 1 s wait out its hold
	for _, at := range got[1:] {
		if gap := at.Sub(got[0]); gap < 2*time.Second-50*time.Millisecond {
			t.Fatalf("a queued call went %s after the 429 that asked for 2 s", gap)
		}
	}
}

// TestHFPacing starts ten WaitReady calls at once against a fake that answers 429 past the pace
func TestHFPacing(t *testing.T) {
	const ops = 10
	f := newFake(t)
	f.pace = ops
	opts := testOptions()
	opts.OpsPerSecond = ops
	p := f.provider(t, opts)
	var refs []provider.Ref
	for range 10 {
		j := f.addJob("SCHEDULING", map[string]string{"bugnet": "1"})
		f.set(j, func(j *fakeJob) { j.runAfter, j.token = 2, freshKey() })
		refs = append(refs, refTo(j))
	}
	var wg sync.WaitGroup
	for _, ref := range refs {
		wg.Go(func() {
			if err := p.WaitReady(t.Context(), ref); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	// The health reads go to HF's proxy, which the Hub's rate limit does not count
	var hub []recorded
	for _, r := range f.all() {
		if r.host == jobsHost {
			hub = append(hub, r)
		}
	}
	eq(t, len(hub), 20)
	eq(t, len(sbxRequests(f)), 10)
	if f.pacedCount() != 0 {
		t.Fatalf("the fake answered %d 429s", f.pacedCount())
	}
	got := arrivals(hub)
	span := got[len(got)-1].Sub(got[0])
	if rate := float64(len(got)-1) / span.Seconds(); rate > ops*1.05 {
		t.Fatalf("%d calls in %s is %.1f a second, over %d", len(got), span, rate, ops)
	}

	// Without the pace the same waits run into the fake's limit at once
	f = newFake(t)
	f.pace = ops
	opts.OpsPerSecond = 0
	p = f.provider(t, opts)
	j := f.addJob("RUNNING", nil)
	errs := make(chan error, 10)
	for range 10 {
		wg.Go(func() {
			_, err := p.jobs.getJob(t.Context(), j.id)
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	var refused int
	for err := range errs {
		if e, ok := provider.AsError(err); ok && e.Code == provider.CodeRateLimited {
			refused++
		}
	}
	if refused == 0 || f.pacedCount() != refused {
		t.Fatalf("unpaced calls got %d 429s and the fake answered %d, want some", refused,
			f.pacedCount())
	}
}
