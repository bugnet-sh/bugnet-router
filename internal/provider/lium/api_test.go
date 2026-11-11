package lium

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const (
	podsRoute = "GET /pods"
	rentRoute = "POST /executors/{executor_uuid}/rent"
)

// readPods makes one keyed read the way every route but a rent is sent
func readPods(ctx context.Context, p *Provider) error {
	_, err := p.api.do(ctx, call{method: http.MethodGet, route: "/pods"}, nil)
	return err
}

// rentOne sends one rent of node-1 with a fixed body
func rentOne(ctx context.Context, p *Provider) error {
	_, err := p.api.do(ctx, call{
		method: http.MethodPost, route: "/executors/node-1/rent", body: []byte(`{"pod_name":"x"}`),
		rent: true,
	}, nil)
	return err
}

func TestLiumErrorMapping(t *testing.T) {
	type row struct {
		name       string
		rent       bool
		fault      fault
		code       provider.Code
		failOver   bool
		retryAfter time.Duration
		// liumCode is the code a caller reads back, the fault's code when empty
		liumCode string
	}
	ft := func(status int, code string) fault {
		return fault{status: status, code: code, message: "refused: " + code}
	}
	rows := []row{
		{name: "unauthorized", fault: ft(401, "unauthorized"), code: provider.CodeAuth,
			failOver: true},
		{name: "inactive key", fault: ft(401, "api_key_inactive"), code: provider.CodeAuth,
			failOver: true},
		{name: "insufficient balance", rent: true, fault: ft(403, "insufficient_balance"),
			code: provider.CodeUnfunded, failOver: true},
		{name: "key budget", rent: true, fault: ft(402, "API_KEY_BUDGET_EXCEEDED"),
			code: provider.CodeUnfunded, failOver: true},
		{name: "balance required", fault: ft(402, "BALANCE_REQUIRED"),
			code: provider.CodeUnfunded, failOver: true},
		{name: "forbidden", fault: ft(403, "forbidden"), code: provider.CodeAuth, failOver: true},
		{name: "email not verified", rent: true, fault: ft(403, "email_not_verified"),
			code: provider.CodeAuth, failOver: true},
		{name: "balance invalid", rent: true, fault: ft(403, "account_balance_invalid"),
			code: provider.CodeAuth, failOver: true},
		{name: "pod not found", fault: ft(404, "pod_not_found"), code: provider.CodeNotFound},
		{name: "own rent in progress", rent: true, fault: ft(400, "own_rent_in_progress"),
			code: provider.CodeCapacity},
		{name: "template not found", rent: true, fault: ft(404, "template_not_found"),
			code: provider.CodeInternal, failOver: true},
		{name: "key invalid", rent: true, fault: ft(422, "idempotency_key_invalid"),
			code: provider.CodeInternal},
		{name: "key reused", rent: true, fault: ft(422, "idempotency_key_reused"),
			code: provider.CodeInternal},
		{name: "rent in progress", rent: true, fault: fault{
			status: 409, code: "idempotency_in_progress", message: "still running",
			header: map[string]string{"Retry-After": "1"},
		}, code: provider.CodeOutcomeUnknown, retryAfter: time.Second},
		{name: "outcome unknown", rent: true, fault: ft(409, "idempotency_outcome_unknown"),
			code: provider.CodeOutcomeUnknown},
		{name: "pod state conflict", fault: ft(400, "pod_state_conflict"),
			code: provider.CodeInternal},
		{name: "conflict off the rent route", fault: ft(409, "conflict"),
			code: provider.CodeInternal},
		{name: "a code the table lacks", rent: true, fault: ft(400, "brand_new_refusal"),
			code: provider.CodeInvalid, failOver: true},
		{name: "rate limited", fault: fault{
			status: 429, code: "rate_limited", message: "slow down",
			header: map[string]string{"Retry-After": "7"},
		}, code: provider.CodeRateLimited, failOver: true, retryAfter: 7 * time.Second},
		{name: "rate limited without retry after", rent: true, fault: ft(429, "rate_limited"),
			code: provider.CodeRateLimited, failOver: true, retryAfter: time.Second},
		{name: "internal error", rent: true, fault: ft(500, "internal_error"),
			code: provider.CodeInternal, failOver: true},
		{name: "a redirect", fault: fault{
			status: 302, raw: "moved", header: map[string]string{"Location": "/elsewhere"},
		}, code: provider.CodeInternal, failOver: true},
		// Answers without an envelope fall back on their status
		{name: "bare 401", fault: fault{status: 401, raw: "Unauthorized"},
			code: provider.CodeAuth, failOver: true},
		{name: "older envelope 403", fault: fault{
			status: 403, raw: `{"success": false, "message": "Not allowed"}`,
		}, code: provider.CodeAuth, failOver: true},
		{name: "403 with a code the table lacks", fault: ft(403, "workspace_locked"),
			code: provider.CodeAuth, failOver: true},
		{name: "bare 402", rent: true, fault: fault{status: 402, raw: "Payment Required"},
			code: provider.CodeUnfunded, failOver: true},
		{name: "budget code under detail", rent: true, fault: fault{
			status: 402, raw: `{"detail": {"code": "API_KEY_BUDGET_EXCEEDED", "window": "daily"}}`,
		}, code: provider.CodeUnfunded, failOver: true, liumCode: "API_KEY_BUDGET_EXCEEDED"},
		{name: "budget code as the detail", rent: true, fault: fault{
			status: 402, raw: `{"detail": "API_KEY_BUDGET_EXCEEDED"}`,
		}, code: provider.CodeUnfunded, failOver: true, liumCode: "API_KEY_BUDGET_EXCEEDED"},
		{name: "validation detail list", rent: true, fault: fault{
			status: 422, raw: `{"detail": [{"loc": ["body", "pod_name"], "msg": "required"}]}`,
		}, code: provider.CodeInvalid, failOver: true},
		{name: "bare 404", fault: fault{status: 404, raw: "Not Found"},
			code: provider.CodeInvalid, failOver: true},
	}
	for _, code := range []string{
		"node_unavailable", "node_rent_in_progress", "node_paused_by_provider", "provider_banned",
		"node_below_bandwidth_floor", "price_changed", "conflict", "node_not_found",
		"node_not_verified",
	} {
		for _, status := range []int{400, 404, 409} {
			rows = append(rows, row{name: "rent refused " + code, rent: true,
				fault: ft(status, code), code: provider.CodeCapacity, failOver: true})
		}
	}
	for _, code := range []string{
		"template_invalid", "image_reference_invalid", "image_not_found",
		"image_needs_registry_credentials", "image_registry_credentials_refused",
		"image_no_linux_amd64", "gpu_count_invalid", "gpu_split_not_allowed", "ttl_invalid",
	} {
		rows = append(rows, row{name: "invalid " + code, rent: true, fault: ft(400, code),
			code: provider.CodeInvalid, failOver: true})
	}
	rows = append(rows, row{name: "validation error", rent: true,
		fault: ft(422, "validation_error"), code: provider.CodeInvalid, failOver: true})
	for _, status := range []int{502, 503, 504} {
		rows = append(rows, row{name: "upstream unavailable", rent: true,
			fault: ft(status, "upstream_unavailable"), code: provider.CodeInternal,
			failOver: true})
	}

	f := newFake(t)
	p := f.provider(t, testOptions(), time.Now)
	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			op, route := readPods, podsRoute
			if tc.rent {
				op, route = rentOne, rentRoute
			}
			f.inject(route, tc.fault)
			err := op(context.Background(), p)
			e, ok := provider.AsError(err)
			if !ok {
				t.Fatalf("got %v, want a provider error", err)
			}
			if e.Code != tc.code || e.CanFailOver != tc.failOver || e.RetryAfter != tc.retryAfter {
				t.Fatalf("got code %s, fail over %v, retry after %s, want %s, %v, %s: %v",
					e.Code, e.CanFailOver, e.RetryAfter, tc.code, tc.failOver, tc.retryAfter, err)
			}
			named := provider.AccountBlock(e.Code) != provider.BlockNone
			if named != (e.Provider == "lium") {
				t.Fatalf("an error of code %s names the provider %q", e.Code, e.Provider)
			}
			want := tc.liumCode
			if want == "" && tc.fault.raw == "" {
				want = tc.fault.code
			}
			eq(t, codeOf(err), want)
			if strings.Contains(err.Error(), fakeKey) {
				t.Fatalf("the error carries the API key: %v", err)
			}
		})
	}
	eq(t, codeOf(errors.New("plain")), "")
}

func TestLiumErrorNamesStatusCodeAndRequest(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions(), time.Now)
	f.inject(rentRoute, fault{status: 409, code: "node_unavailable", message: "Node is gone"})
	err := rentOne(context.Background(), p)
	for _, part := range []string{"409", "node_unavailable", "Node is gone", "req-fake"} {
		if err == nil || !strings.Contains(err.Error(), part) {
			t.Fatalf("error %v does not name %q", err, part)
		}
	}
}

func TestLiumUnknownCodeIsLoggedAtErrorLevel(t *testing.T) {
	f := newFake(t)
	var logs bytes.Buffer
	opts := testOptions()
	opts.Log = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelError}))
	p := f.provider(t, opts, time.Now)
	f.inject(rentRoute, fault{status: 400, code: "brand_new_refusal", message: "new"})
	_ = rentOne(context.Background(), p)
	if !strings.Contains(logs.String(), "level=ERROR") ||
		!strings.Contains(logs.String(), "brand_new_refusal") {
		t.Fatalf("an unknown code was not logged at error level: %q", logs.String())
	}
	logs.Reset()
	f.inject(rentRoute, fault{status: 409, code: "node_unavailable", message: "gone"})
	_ = rentOne(context.Background(), p)
	if logs.Len() != 0 {
		t.Fatalf("a known code was logged at error level: %q", logs.String())
	}
}

func TestLiumTransportErrors(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions(), time.Now)
	ctx := context.Background()
	check := func(t *testing.T, err error, code provider.Code, failOver bool) {
		t.Helper()
		e, ok := provider.AsError(err)
		if !ok || e.Code != code || e.CanFailOver != failOver {
			t.Fatalf("got %v, want code %s with fail over %v", err, code, failOver)
		}
	}
	t.Run("a dropped rent may have rented", func(t *testing.T) {
		f.inject(rentRoute, fault{drop: true, times: 1})
		check(t, rentOne(ctx, p), provider.CodeOutcomeUnknown, false)
	})
	t.Run("a dropped read", func(t *testing.T) {
		f.inject(podsRoute, fault{drop: true, times: 1})
		check(t, readPods(ctx, p), provider.CodeInternal, false)
	})
	t.Run("an unreadable rent answer may have rented", func(t *testing.T) {
		f.inject(rentRoute, fault{status: 200, raw: `{"id": `, times: 1})
		_, err := p.api.do(ctx, call{
			method: http.MethodPost, route: "/executors/node-1/rent", body: []byte(`{}`),
			rent: true,
		}, &map[string]any{})
		check(t, err, provider.CodeOutcomeUnknown, false)
	})
	t.Run("an unreadable read", func(t *testing.T) {
		f.inject(podsRoute, fault{status: 200, raw: `[{`, times: 1})
		_, err := p.api.do(ctx, call{method: http.MethodGet, route: "/pods"}, &[]any{})
		check(t, err, provider.CodeInternal, false)
	})
	t.Run("a rent never sent", func(t *testing.T) {
		gone := httptest.NewServer(http.NotFoundHandler())
		gone.Close()
		opts := testOptions()
		opts.BaseURL = gone.URL
		q, err := newProvider(opts, time.Now)
		if err != nil {
			t.Fatal(err)
		}
		check(t, rentOne(ctx, q), provider.CodeInternal, true)
	})
}

func TestLiumSendsTheKeyAsLiumAsks(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions(), time.Now)
	if err := readPods(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	if err := rentOne(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	for _, r := range append(f.requestsTo("GET", "/pods"), f.requestsTo("POST",
		"/executors/node-1/rent")...) {
		eq(t, r.header.Values("X-API-Key"), []string{fakeKey})
		eq(t, r.header.Get("Authorization"), "")
		eq(t, r.header.Get("User-Agent"), "bugnet-router")
		eq(t, r.header.Get("Accept"), "application/json")
	}
	rent := f.requestsTo("POST", "/executors/node-1/rent")[0]
	eq(t, rent.header.Get("Content-Type"), "application/json")
	eq(t, string(rent.body), `{"pod_name":"x"}`)

	// A wrong key reads as auth, named so the account check runs
	opts := testOptions()
	opts.APIKey = "another-key"
	q := f.provider(t, opts, time.Now)
	e, ok := provider.AsError(readPods(context.Background(), q))
	if !ok || e.Code != provider.CodeAuth || e.Provider != "lium" {
		t.Fatalf("a wrong key gave %v", e)
	}
}

func TestLiumRefusesRedirects(t *testing.T) {
	var followed sync.WaitGroup
	followed.Add(1)
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		followed.Done()
	}))
	t.Cleanup(target.Close)
	f := newFake(t)
	p := f.provider(t, testOptions(), time.Now)
	f.inject(podsRoute, fault{status: 307, raw: "moved",
		header: map[string]string{"Location": target.URL + "/pods"}})
	if err := readPods(context.Background(), p); err == nil {
		t.Fatal("a redirect was taken as an answer")
	}
	done := make(chan struct{})
	go func() { followed.Wait(); close(done) }()
	select {
	case <-done:
		t.Fatal("the client followed a redirect")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestLiumPacesCallsAndRents(t *testing.T) {
	span := func(rs []recorded) time.Duration { return rs[len(rs)-1].at.Sub(rs[0].at) }
	burst := func(n int, op func(context.Context, *Provider) error, p *Provider) {
		var wg sync.WaitGroup
		for range n {
			wg.Go(func() {
				if err := op(context.Background(), p); err != nil {
					t.Error(err)
				}
			})
		}
		wg.Wait()
	}

	f := newFake(t)
	opts := testOptions()
	opts.OpsPerSecond, opts.CreatesPerSecond = 20, 1000
	burst(6, readPods, f.provider(t, opts, time.Now))
	if got := span(f.requestsTo("GET", "/pods")); got < 200*time.Millisecond {
		t.Errorf("6 calls at 20 a second took %s, want at least 250ms less a margin", got)
	}

	f = newFake(t)
	opts.OpsPerSecond, opts.CreatesPerSecond = 1000, 10
	p := f.provider(t, opts, time.Now)
	burst(4, rentOne, p)
	if got := span(f.requestsTo("POST", "/executors/node-1/rent")); got < 240*time.Millisecond {
		t.Errorf("4 rents at 10 a second took %s, want at least 300ms less a margin", got)
	}
	// Reads never wait for the rents' pace
	burst(4, readPods, p)
	if got := span(f.requestsTo("GET", "/pods")); got > 200*time.Millisecond {
		t.Errorf("4 reads at 1000 a second took %s while rents are paced", got)
	}
}

func TestLiumPacingGivesUpWithTheCaller(t *testing.T) {
	f := newFake(t)
	opts := testOptions()
	opts.OpsPerSecond = 1
	p := f.provider(t, opts, time.Now)
	if err := readPods(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := rentOne(ctx, p)
	e, ok := provider.AsError(err)
	if !ok || e.Code != provider.CodeInternal || !e.CanFailOver ||
		!errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a call its caller gave up on while paced gave %v", err)
	}
	eq(t, len(f.requestsTo("POST", "/executors/node-1/rent")), 0)
}
