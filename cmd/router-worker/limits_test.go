package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/provider/boat"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

func askLimits(st *store.Store, vk valkey.Client, lc limitsCheck) (int, string) {
	rec := httptest.NewRecorder()
	monitor(discard, st, vk, testPrefix, healthy, testPools, feedCheck{}, nil,
		lc, nil, canaryCheck{}, lossCheck{})(rec,
		httptest.NewRequest(http.MethodGet, "/monitor", nil))
	return rec.Code, rec.Body.String()
}

// rateOf answers a RateLimit window of limit requests with remaining left
func rateOf(remaining, limit int) rateReader {
	return func(context.Context) (boat.RateLeft, bool, error) {
		return boat.RateLeft{Remaining: remaining, Limit: limit}, true, nil
	}
}

func TestMonitorShowsBoatRateLimitUnderTwentyPercent(t *testing.T) {
	st := store.New(migrated(t))
	vk, _ := testenv.Valkey(t)
	for _, tc := range []struct {
		remaining int
		shown     bool
	}{{19, true}, {20, false}, {21, false}, {0, true}} {
		code, body := askLimits(st, vk, limitsCheck{rate: rateOf(tc.remaining, 100)})
		line := "provider boat has " + strconv.Itoa(tc.remaining) +
			" of 100 requests left in its window"
		if tc.shown && (code != http.StatusServiceUnavailable || !strings.Contains(body, line)) {
			t.Errorf("%d of 100 left answered %d %q, want the line %q", tc.remaining, code,
				body, line)
		}
		if !tc.shown && (code != http.StatusOK || body != "ok\n") {
			t.Errorf("%d of 100 left answered %d %q, want ok", tc.remaining, code, body)
		}
	}
	// A header Boat never sent shows nothing
	none := func(context.Context) (boat.RateLeft, bool, error) {
		return boat.RateLeft{}, false, nil
	}
	if code, body := askLimits(st, vk, limitsCheck{rate: none}); code != http.StatusOK {
		t.Errorf("no header answered %d %q", code, body)
	}
}

func TestMonitorShowsStaleLimits(t *testing.T) {
	st := store.New(migrated(t))
	vk, _ := testenv.Valkey(t)
	ctx := t.Context()
	lc := limitsCheck{
		every:      map[string]time.Duration{"e2b": time.Minute, "boat": 15 * time.Second},
		staleAfter: 5,
	}
	now := time.Now()
	for name, age := range map[string]time.Duration{
		"e2b": 4 * time.Minute, "boat": 2 * time.Minute, "local": time.Hour,
	} {
		err := st.WriteLimits(ctx, name, 10, store.LimitFromProvider, 0, now.Add(-age))
		if err != nil {
			t.Fatal(err)
		}
	}
	// e2b is 4 of 5 intervals old, boat 8 intervals, and local is not polled at all
	code, body := askLimits(st, vk, lc)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("answered %d %q, want 503", code, body)
	}
	lines := strings.Split(strings.TrimSpace(body), "\n")
	if len(lines) != 1 || !strings.HasPrefix(lines[0], "provider boat limits not polled for 12") {
		t.Fatalf("lines = %q, want boat's old limits alone", lines)
	}
	// A poll brings it back
	if err := st.WriteLimits(ctx, "boat", 10, store.LimitFromProvider, 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	if code, body := askLimits(st, vk, lc); code != http.StatusOK {
		t.Fatalf("after a poll: %d %q", code, body)
	}
}

func TestMonitorIgnoresARowNoPollWrote(t *testing.T) {
	st := store.New(migrated(t))
	vk, _ := testenv.Valkey(t)
	// A reserve creates the row before any poll, and it has no poll time to judge
	if ok, err := st.ReserveCapacity(t.Context(), "e2b", 0); err != nil || !ok {
		t.Fatal(ok, err)
	}
	lc := limitsCheck{every: map[string]time.Duration{"e2b": time.Minute}, staleAfter: 5}
	if code, body := askLimits(st, vk, lc); code != http.StatusOK {
		t.Fatalf("answered %d %q", code, body)
	}
}

func TestMonitorShowsABlockedProvider(t *testing.T) {
	st := store.New(migrated(t))
	vk, _ := testenv.Valkey(t)
	block := func(block, detail string) {
		t.Helper()
		if _, err := st.RecordAccount(t.Context(), "boat", block, detail, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	block("unfunded", "wallet below zero")
	code, body := askLimits(st, vk, limitsCheck{})
	if code != http.StatusServiceUnavailable ||
		strings.TrimSpace(body) != "provider boat is blocked: unfunded" {
		t.Fatalf("answered %d %q", code, body)
	}
	// The reason is a word, never the provider's text
	if strings.Contains(body, "wallet") {
		t.Fatalf("the detail reached /monitor: %q", body)
	}
	block("", "")
	if code, body := askLimits(st, vk, limitsCheck{}); code != http.StatusOK {
		t.Fatalf("after the clear: %d %q", code, body)
	}
}
