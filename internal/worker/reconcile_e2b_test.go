package worker_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/e2b"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// fakeE2BKill serves the three calls Inspect and the end make on a sandbox E2B killed for a reason
func fakeE2BKill(t *testing.T, reason string) (*e2b.Provider, *atomic.Int32) {
	t.Helper()
	return fakeE2B(t, reason, false)
}

// fakeE2B is fakeE2BKill whose kill events can fail with a 500
func fakeE2B(t *testing.T, reason string, eventsFail bool) (*e2b.Provider, *atomic.Int32) {
	t.Helper()
	var asked atomic.Int32
	rest := http.NewServeMux()
	notFound := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"code":404,"message":"sandbox not found"}`))
	}
	rest.HandleFunc("GET /sandboxes/{id}", notFound)
	rest.HandleFunc("DELETE /sandboxes/{id}", notFound)
	rest.HandleFunc("GET /events/sandboxes/{id}", func(w http.ResponseWriter, r *http.Request) {
		asked.Add(1)
		if eventsFail {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if got := r.URL.Query()["types"]; len(got) != 1 || got[0] != "sandbox.lifecycle.killed" {
			t.Errorf("events asked for types %v, want the killed event alone", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{{
			"id": uuid.NewString(), "sandboxId": r.PathValue("id"),
			"type":      "sandbox.lifecycle.killed",
			"timestamp": "2026-11-09T10:00:00Z", "version": "v1", "sandboxBuildId": "b",
			"sandboxExecutionId": "x", "sandboxTeamId": uuid.NewString(),
			"sandboxTemplateId": "t", "eventData": map[string]any{"kill_reason": reason},
		}})
	})
	srv := httptest.NewServer(rest)
	t.Cleanup(srv.Close)
	// envd answers 502 to everything, as a proxy does for a sandbox that is gone
	envd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	t.Cleanup(envd.Close)
	p, err := e2b.New(e2b.Options{
		APIKey: "e2b_fake00000000000000000000000000000000", BaseURL: srv.URL, EnvdURL: envd.URL,
		Templates: []e2b.Template{{
			Name: "2x2", TemplateID: "bugnet-2x2", VCPU: 2, MemoryMiB: 2048, DiskGiB: 10,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return p, &asked
}

// A webhook for a killed sandbox reaches Inspect, which reads E2B's kill event, and the end is
// abuse with the payer blocked
func TestE2EAdminKillEndsAsAbuse(t *testing.T) {
	prov, asked := fakeE2BKill(t, "admin")
	e := newEnv(t, options{providers: []provider.Provider{prov}})
	p := e.paidPlacement(t, payerA, false)
	e.start(t)

	e.run(t, jobargs.HookHint{
		Provider: "e2b", SandboxID: p.ProviderRef.ID, EventID: "evt-1",
		EventType: "sandbox.lifecycle.killed", PlacementLabel: p.ID.String(),
		DeploymentLabel: e.deployment,
	})

	got := e.reload(t, p.ID)
	eq(t, got.State, store.PlacementTerminated)
	eq(t, got.TerminationReason, saga.ReasonAbuse)
	eq(t, got.Refundable, false)
	eq(t, e.blocked(t, payerA), true)
	eq(t, asked.Load(), int32(1))
	sg := e.signals(t, p.ID)
	eq(t, []string{sg[0].Class, sg[0].Action, sg[0].Source},
		[]string{"abuse", "blocked", "hook"})
	var evidence map[string]any
	if err := json.Unmarshal(sg[0].Observed, &evidence); err != nil {
		t.Fatal(err)
	}
	eq(t, evidence["kill_reason"], any("admin"))
	eq(t, evidence["event_id"], any("evt-1"))
	// The abuse verdict asks for an account check at once
	var checks int
	err := e.pool.QueryRow(t.Context(),
		`select count(*) from river_job
		where kind = 'account_check' and args->>'provider' = 'e2b'`).Scan(&checks)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, checks, 1)
}

// A kill event E2B fails to serve leaves the placement ready, since the reason is unknown
func TestFailedKillEventsReadLeavesThePlacementReady(t *testing.T) {
	prov, _ := fakeE2B(t, "admin", true)
	e := newEnv(t, options{providers: []provider.Provider{prov}})
	p := e.paidPlacement(t, payerA, false)
	e.start(t)
	e.run(t, jobargs.HookHint{Provider: "e2b", SandboxID: p.ProviderRef.ID, EventID: "evt-3"})

	eq(t, e.reload(t, p.ID).State, store.PlacementReady)
	eq(t, e.blocked(t, payerA), false)
	eq(t, len(e.signals(t, p.ID)), 0)
}

// A kill E2B says a request made, on a placement no stop is ending, is the provider's doing and
// raises the alert /monitor reads
func TestE2BRequestKillOnAReadyPlacementIsRefundableAndAlerts(t *testing.T) {
	prov, _ := fakeE2BKill(t, "request")
	e := newEnv(t, options{providers: []provider.Provider{prov}})
	p := e.paidPlacement(t, payerA, false)
	e.start(t)
	e.run(t, jobargs.HookHint{Provider: "e2b", SandboxID: p.ProviderRef.ID, EventID: "evt-2"})

	got := e.reload(t, p.ID)
	eq(t, got.TerminationReason, saga.ReasonProviderFailed)
	eq(t, got.Refundable, true)
	eq(t, e.blocked(t, payerA), false)
	kills, err := e.store.UnaskedKills(t.Context(), got.CreatedAt.Add(-1))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, kills, map[string]int{"e2b": 1})
}
