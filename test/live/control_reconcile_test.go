package live

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/reconcile"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

func init() {
	controlRows = append(controlRows,
		row{id: "G1", run: rowGuestStops},
		row{id: "R1", run: rowInspectAfterDestroy},
		row{id: "H1", providers: []string{"e2b"}, vps: true, run: rowE2BHooks},
		row{id: "H2", providers: []string{"e2b"}, run: rowE2BKillEvents},
		row{id: "H3", providers: []string{"boat"}, vps: true, run: rowBoatDegraded},
	)
}

const (
	// inspectEvery is how often a row reads a sandbox's state
	inspectEvery = 5 * time.Second
	// stateWithin is how long a row waits for a state to show
	stateWithin = 3 * time.Minute
)

// hookEnv names the variable that holds each provider's webhook secret
var hookEnv = map[string]string{"e2b": "E2B_WEBHOOK_SECRET", "boat": "BOAT_WEBHOOK_SECRET"}

// owned finds the placement's sandbox through the stack's deployment label
func (s *stack) owned(t *testing.T, p provider.Provider, placement string) provider.Summary {
	t.Helper()
	cfg, err := config.LoadFile(s.settings)
	if err != nil {
		t.Fatal(err)
	}
	labels := map[string]string{
		"bugnet": "1", "bugnet_deployment": cfg.DeploymentID, "placement": placement,
	}
	owned, err := p.ListOwned(t.Context(), labels)
	if err != nil || len(owned) != 1 {
		t.Fatalf("live: %d sandboxes carry the labels of placement %s (%v), want 1",
			len(owned), placement, err)
	}
	return owned[0]
}

// describe renders what Inspect read and the class Classify gives it on a ready placement
func describe(prov string, st provider.Status) string {
	live := reconcile.Live{State: store.PlacementReady}
	v := reconcile.Classify(prov, st, live, time.Now(), config.DefaultAbuse())
	return fmt.Sprintf("state=%q reason=%q kill_reason=%q class=%q recheck=%v", st.State,
		st.Reason, st.KillReason, v.Class, v.Recheck)
}

// watchStates logs each change of a sandbox's Inspect reading until the reading is gone or
// within passes, and answers every distinct reading it saw
func watchStates(
	t *testing.T, label string, p provider.Provider, ref provider.Ref, within time.Duration,
) []provider.Status {
	t.Helper()
	var seen []provider.Status
	start := time.Now()
	for time.Since(start) < within {
		st, err := p.Inspect(t.Context(), ref)
		if err != nil {
			t.Logf("%s: Inspect after %s: %v", label, time.Since(start).Round(time.Second), err)
		} else if n := len(seen); n == 0 || seen[n-1].State != st.State ||
			seen[n-1].Reason != st.Reason {
			seen = append(seen, st)
			t.Logf("%s: after %s %s", label, time.Since(start).Round(time.Second),
				describe(ref.Provider, st))
			if st.State == provider.StateGone {
				return seen
			}
		}
		time.Sleep(inspectEvery)
	}
	return seen
}

// rowGuestStops is CP G1. It forces the two stops a user with root can cause, one in each of
// two sandboxes, and records what Inspect reads and the class CP 6.3 gives it.
func rowGuestStops(t *testing.T, s *stack) {
	p := liveAdapter(t, s.provider)
	for _, stop := range []string{"sudo poweroff", "sudo kill -9 1"} {
		h := s.sandbox(t, xferPackage)
		placement := kv(s.dev(t, "get", "--handle", h))["placement_id"]
		sum := s.owned(t, p, placement)
		// The stop kills the shell that ran it, so its error is the sign it worked
		_, _, _ = s.devTry(t, "exec", "--handle", h, "--", "sh", "-c", stop)
		seen := watchStates(t, "G1 "+s.provider+" "+stop, p, sum.Ref, stateWithin)
		if len(seen) == 0 || seen[len(seen)-1].State == provider.StateRunning {
			t.Errorf("G1: %s left Inspect at running for %s", stop, stateWithin)
		}
	}
}

// rowInspectAfterDestroy is CP R1. A probe-size sandbox gets a lifetime, which Inspect must
// read back as ExpiresAt, then it is destroyed and Inspect must say gone.
func rowInspectAfterDestroy(t *testing.T, s *stack) {
	p := liveAdapter(t, s.provider)
	cfg, err := config.LoadFile(s.settings)
	if err != nil {
		t.Fatal(err)
	}
	size, ok := p.SizeFor(provider.Resources{
		VCPU: cfg.Health.CanaryVCPU, MemoryMiB: cfg.Health.CanaryMemoryMiB,
	})
	if !ok {
		t.Fatalf("live: %s has no size for the canary", s.provider)
	}
	run := fmt.Sprintf("r1-%d", time.Now().UnixNano())
	ctx := t.Context()
	ref, err := p.Create(ctx, provider.CreateRequest{
		IdempotencyKey: "probe-" + run, Size: size, Lifetime: 10 * time.Minute,
		Labels: map[string]string{
			"bugnet": "1", "bugnet_deployment": cfg.DeploymentID, "bugnet_probe": "canary",
			"probe": run,
		},
	})
	if err != nil {
		t.Fatalf("live: Create: %v", err)
	}
	t.Cleanup(func() { _ = p.Destroy(context.WithoutCancel(ctx), ref) })
	if err := p.WaitReady(ctx, ref); err != nil {
		t.Fatalf("live: WaitReady: %v", err)
	}
	until := time.Now().Add(20 * time.Minute)
	if err := p.SetLifetime(ctx, ref, until); err != nil {
		t.Fatalf("live: SetLifetime: %v", err)
	}
	st, err := p.Inspect(ctx, ref)
	if err != nil {
		t.Fatalf("live: Inspect: %v", err)
	}
	t.Logf("R1 %s: %s", s.provider, describe(s.provider, st))
	if st.State != provider.StateRunning {
		t.Errorf("R1: a ready sandbox reads %q, want running", st.State)
	}
	// Koyeb keeps its 24 hour backstop and cannot move it, so only the others must follow
	off := st.ExpiresAt.Sub(until)
	t.Logf("R1 %s: ExpiresAt is %v from the lifetime just set (zero ExpiresAt: %v)", s.provider,
		off.Round(time.Second), st.ExpiresAt.IsZero())
	if s.provider != "koyeb" && (st.ExpiresAt.IsZero() || off < -time.Minute || off > time.Minute) {
		t.Errorf("R1: ExpiresAt %v is %v off the lifetime set", st.ExpiresAt, off)
	}
	if err := p.Destroy(ctx, ref); err != nil {
		t.Fatalf("live: Destroy: %v", err)
	}
	seen := watchStates(t, "R1 "+s.provider+" after destroy", p, ref, time.Minute)
	if len(seen) == 0 || seen[len(seen)-1].State != provider.StateGone {
		t.Errorf("R1: Inspect after a destroy never said gone, saw %+v", seen)
	}
}

// rowE2BKillEvents is CP H2. E2B's kill event gives the reason after the sandbox answers 404.
func rowE2BKillEvents(t *testing.T, s *stack) {
	p := liveAdapter(t, "e2b")
	cfg, err := config.LoadFile(s.settings)
	if err != nil {
		t.Fatal(err)
	}
	size, _ := p.SizeFor(provider.Resources{
		VCPU: cfg.Health.CanaryVCPU, MemoryMiB: cfg.Health.CanaryMemoryMiB,
	})
	run := fmt.Sprintf("h2-%d", time.Now().UnixNano())
	ctx := t.Context()
	ref, err := p.Create(ctx, provider.CreateRequest{
		IdempotencyKey: "probe-" + run, Size: size, Lifetime: 10 * time.Minute,
		Labels: map[string]string{"bugnet": "1", "bugnet_deployment": cfg.DeploymentID,
			"bugnet_probe": "canary", "probe": run},
	})
	if err != nil {
		t.Fatalf("live: Create: %v", err)
	}
	t.Cleanup(func() { _ = p.Destroy(context.WithoutCancel(ctx), ref) })
	if err := p.Destroy(ctx, ref); err != nil {
		t.Fatalf("live: Destroy: %v", err)
	}
	// The event may lag the kill, so the reason is read until it shows
	var st provider.Status
	for start := time.Now(); time.Since(start) < 2*time.Minute; time.Sleep(inspectEvery) {
		if st, err = p.Inspect(ctx, ref); err == nil && st.KillReason != "" {
			break
		}
	}
	t.Logf("H2: our own kill reads %s", describe("e2b", st))
	eq(t, st.State, provider.StateGone)
	if st.KillReason == "" {
		t.Fatal("H2: the kill event gave no reason, so CP 6.3's kill rows cannot be told apart")
	}
	if st.KillReason != "request" {
		t.Errorf("H2: our own kill reads %q, CP 6.3 expects request", st.KillReason)
	}
}

// hookHints counts the completed hook_hint jobs for provider that the stack's worker ran
func (s *stack) hookHints(t *testing.T, prov string) int {
	t.Helper()
	var n int
	err := s.pg(t).QueryRow(t.Context(), `select count(*) from river_job
		where kind = 'hook_hint' and args ->> 'provider' = $1 and state = 'completed'`,
		prov).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// requireHooks skips a hook row unless the provider's webhook is registered and its secret set
func requireHooks(t *testing.T, prov string) {
	t.Helper()
	if os.Getenv(hookEnv[prov]) == "" {
		t.Skipf("%s is not set, so the %s hooks are off (DP question 5 or CP question 2)",
			hookEnv[prov], prov)
	}
}

// rowE2BHooks is CP H1. A stop of ours and an expiry each make E2B deliver a killed event to the
// dev hooks host, and the hint job runs for each.
func rowE2BHooks(t *testing.T, s *stack) {
	requireHooks(t, "e2b")
	p := liveAdapter(t, "e2b")
	h := s.sandbox(t, xferPackage)
	placement := kv(s.dev(t, "get", "--handle", h))["placement_id"]
	sum := s.owned(t, p, placement)
	before := s.hookHints(t, "e2b")
	s.dev(t, "stop", "--handle", h)
	waitHints(t, s, "e2b", before+1, "a stop of ours")
	st, err := p.Inspect(t.Context(), sum.Ref)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("H1: after our stop %s", describe("e2b", st))
	eq(t, st.KillReason, "request")

	// A sandbox the deployment owns that nobody ends reaches its provider-side lifetime
	cfg, err := config.LoadFile(s.settings)
	if err != nil {
		t.Fatal(err)
	}
	size, _ := p.SizeFor(provider.Resources{
		VCPU: cfg.Health.CanaryVCPU, MemoryMiB: cfg.Health.CanaryMemoryMiB,
	})
	run := fmt.Sprintf("h1-%d", time.Now().UnixNano())
	ref, err := p.Create(t.Context(), provider.CreateRequest{
		IdempotencyKey: "probe-" + run, Size: size, Lifetime: time.Minute,
		Labels: map[string]string{"bugnet": "1", "bugnet_deployment": cfg.DeploymentID,
			"bugnet_probe": "canary", "probe": run},
	})
	if err != nil {
		t.Fatalf("live: Create: %v", err)
	}
	t.Cleanup(func() { _ = p.Destroy(context.WithoutCancel(t.Context()), ref) })
	before = s.hookHints(t, "e2b")
	waitHints(t, s, "e2b", before+1, "an expiry")
	st, err = p.Inspect(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("H1: after the expiry %s", describe("e2b", st))
	eq(t, st.KillReason, "timeout")
}

// waitHints waits until the worker has completed n hook_hint jobs for the provider
func waitHints(t *testing.T, s *stack, prov string, n int, what string) {
	t.Helper()
	for start := time.Now(); time.Since(start) < 3*time.Minute; time.Sleep(inspectEvery) {
		if s.hookHints(t, prov) >= n {
			t.Logf("H1: a hook_hint job ran %s after %s", what,
				time.Since(start).Round(time.Second))
			return
		}
	}
	t.Fatalf("live: no hook_hint job ran for %s, so no delivery reached the hooks host", what)
}

// rowBoatDegraded is CP H3. Blocking the machine's outbound traffic inside makes Boat's tunnel
// fail, and the row times the degraded health and the recovery that follows.
func rowBoatDegraded(t *testing.T, s *stack) {
	requireHooks(t, "boat")
	p := liveAdapter(t, "boat")
	h := s.sandbox(t, xferPackage)
	placement := kv(s.dev(t, "get", "--handle", h))["placement_id"]
	sum := s.owned(t, p, placement)
	before := s.hookHints(t, "boat")

	// The rule goes in for 90 s and out again on its own, so the sandbox recovers
	blocked := time.Now()
	_, _, _ = s.devTry(t, "exec", "--handle", h, "--", "sh", "-c",
		`sudo sh -c 'nohup sh -c "iptables -I OUTPUT 1 -j DROP; sleep 90; iptables -D OUTPUT 1" `+
			`> /dev/null 2>&1 &'`)
	var degraded, recovered time.Duration
	for time.Since(blocked) < 6*time.Minute {
		st, err := p.Inspect(t.Context(), sum.Ref)
		switch {
		case err != nil:
			t.Logf("H3: Inspect: %v", err)
		case st.State == provider.StateDegraded && degraded == 0:
			degraded = time.Since(blocked)
			t.Logf("H3: degraded after %s, %s", degraded.Round(time.Second),
				describe("boat", st))
		case st.State == provider.StateRunning && degraded > 0:
			recovered = time.Since(blocked)
			t.Logf("H3: recovered after %s", recovered.Round(time.Second))
		}
		if recovered > 0 {
			break
		}
		time.Sleep(inspectEvery)
	}
	if degraded == 0 || recovered == 0 {
		t.Fatalf("H3: degraded after %v and recovered after %v, want both", degraded, recovered)
	}
	if got := s.hookHints(t, "boat"); got <= before {
		t.Errorf("H3: no hook_hint job ran for Boat's events, so the HMAC or the URL failed")
	}
	var events string
	rows, err := s.pg(t).Query(t.Context(), `select args ->> 'event_type' from river_job
		where kind = 'hook_hint' and args ->> 'provider' = 'boat' order by id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var typ string
		if err := rows.Scan(&typ); err != nil {
			t.Fatal(err)
		}
		events += typ + " "
	}
	t.Logf("H3: Boat's event types: %s", strings.TrimSpace(events))
}

func TestReconcileRowsAndTheirProviders(t *testing.T) {
	want := map[string]struct {
		providers []string
		vps       bool
	}{
		"G1": {nil, false}, "R1": {nil, false}, "H1": {[]string{"e2b"}, true},
		"H2": {[]string{"e2b"}, false}, "H3": {[]string{"boat"}, true},
	}
	found := 0
	for _, r := range controlRows {
		if w, ok := want[r.id]; ok {
			found++
			eq(t, r.providers, w.providers)
			eq(t, r.vps, w.vps)
		}
	}
	eq(t, found, len(want))
}
