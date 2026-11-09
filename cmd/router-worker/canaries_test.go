package main

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

func askCanaries(st *store.Store, vk valkey.Client, c canaryCheck) (int, string) {
	rec := httptest.NewRecorder()
	monitor(discard, st, vk, testPrefix, healthy, testPools, feedCheck{}, nil, limitsCheck{},
		c)(rec, httptest.NewRequest(http.MethodGet, "/monitor", nil))
	return rec.Code, rec.Body.String()
}

func always(string) bool { return true }

func TestMonitorShowsAProviderDownForLong(t *testing.T) {
	st := store.New(migrated(t))
	vk, _ := testenv.Valkey(t)
	now := time.Now()
	write := func(name, state string, ago time.Duration) {
		t.Helper()
		_, err := st.WriteHealth(t.Context(), name, store.HealthState{State: state}, now.Add(-ago))
		if err != nil {
			t.Fatal(err)
		}
	}
	write("e2b", store.HealthDown, 11*time.Minute)
	write("boxd", store.HealthDown, 4*time.Minute)
	write("koyeb", store.HealthSlow, time.Hour)
	code, body := askCanaries(st, vk, canaryCheck{downAfter: 10 * time.Minute})
	if code != http.StatusServiceUnavailable || body != "provider e2b is down for 11 minutes\n" {
		t.Fatalf("answered %d %q, want only e2b, down past 10 minutes", code, body)
	}
}

func TestMonitorShowsACanaryThreeIntervalsLate(t *testing.T) {
	st := store.New(migrated(t))
	vk, _ := testenv.Valkey(t)
	now := time.Now()
	started := now.Add(-2 * time.Hour)
	add := func(name string, kind store.SampleKind, ago time.Duration) {
		t.Helper()
		err := st.InsertStartSample(t.Context(), store.StartSample{
			Kind: kind, Provider: name, StartedAt: now.Add(-ago), Outcome: store.OutcomeOK,
			CreateMS: 1, ReadyMS: 1, RunnerMS: store.NoStep, GateMS: store.NoStep,
			FirstCmdMS: store.NoStep, WarmCmdMS: store.NoStep, DestroyMS: store.NoStep,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	every := map[string]time.Duration{
		"e2b": 5 * time.Minute, "koyeb": 5 * time.Minute, "boxd": 5 * time.Minute,
		"boat": 30 * time.Minute, "local": 5 * time.Minute,
	}
	add("e2b", store.SampleCanary, 16*time.Minute)   // late
	add("koyeb", store.SampleCanary, 14*time.Minute) // not yet
	add("boxd", store.SampleCanary, time.Hour)       // late, but a placement stood in
	add("boat", store.SampleCanary, 100*time.Minute) // late
	c := canaryCheck{every: every, since: started, due: always}
	p, _, err := st.CreatePlacement(t.Context(), store.NewPlacement{
		OrderID: "o", HandleSHA256: []byte("h"), PackageID: "p",
	})
	if err != nil {
		t.Fatal(err)
	}
	ref := provider.Ref{Provider: "boxd", ID: "sb"}
	readyAt := now.Add(-time.Minute)
	err = st.MarkPlacementReady(t.Context(), p.ID, ref, "s", readyAt, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	code, body := askCanaries(st, vk, c)
	lines := strings.Split(strings.TrimSpace(body), "\n")
	want := []string{
		"provider boat has had no canary for 100 minutes",
		"provider e2b has had no canary for 16 minutes",
		// local has never had a canary since the worker started two hours ago
		"provider local has had no canary for 120 minutes",
	}
	if code != http.StatusServiceUnavailable || !slices.Equal(lines, want) {
		t.Fatalf("answered %d %q, want %q", code, lines, want)
	}

	// A restart of the worker starts the count again, and a provider that pays for starts or is
	// blocked owes no canary
	c.since = now.Add(-time.Minute)
	c.due = func(name string) bool { return name != "boat" }
	if _, err := st.RecordAccount(t.Context(), "e2b", "banned", "x", now); err != nil {
		t.Fatal(err)
	}
	// Only the block itself is reported, by the limits check
	code, body = askCanaries(st, vk, c)
	if code != http.StatusServiceUnavailable || body != "provider e2b is blocked: banned\n" {
		t.Fatalf("answered %d %q, want only e2b's block", code, body)
	}
}

func TestMonitorShowsAProbeSandboxOverFifteenMinutesOld(t *testing.T) {
	st := store.New(migrated(t))
	vk, _ := testenv.Valkey(t)
	c := canaryCheck{oldProbes: func() map[string]time.Duration {
		return map[string]time.Duration{"koyeb": 17 * time.Minute}
	}}
	code, body := askCanaries(st, vk, c)
	if code != http.StatusServiceUnavailable ||
		body != "provider koyeb has a probe sandbox 17 minutes old\n" {
		t.Fatalf("answered %d %q", code, body)
	}
}
