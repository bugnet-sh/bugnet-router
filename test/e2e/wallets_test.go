package e2e

import (
	"strings"
	"testing"
	"time"
)

const (
	e2ePayer       = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
	e2eFacilitator = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
)

// awaitReason returns the first event of type typ with this reason, failing after within
func (r *receiver) awaitReason(t *testing.T, typ, reason string, within time.Duration) event {
	t.Helper()
	timer := time.NewTimer(within)
	defer timer.Stop()
	for {
		r.mu.Lock()
		changed := r.changed
		for _, ev := range r.events {
			if ev.Type == typ && ev.Payload.Reason == reason {
				r.mu.Unlock()
				return ev
			}
		}
		r.mu.Unlock()
		select {
		case <-changed:
		case <-timer.C:
			t.Fatalf("no %s event with reason %s within %s", typ, reason, within)
		}
	}
}

func TestE2ESixCreatesWithAPayer(t *testing.T) {
	t.Parallel()
	s := startStackWith(t, stackOptions{extra: `
[wallets]
use_payer = true
emit_events = true
max_concurrent = 5
facilitator_addresses = ["` + e2eFacilitator + `"]
`})
	create := []string{"create", "--package", smallPkg, "--payer", e2ePayer, "--fee-payer",
		e2eFacilitator}
	var ready []string
	for i := range 5 {
		p := s.a
		if i%2 == 1 {
			p = s.b
		}
		out := parseKV(s.devOK(t, p, create...))
		if out["state"] != "ready" {
			t.Fatalf("create %d printed %v", i+1, out)
		}
		ready = append(ready, out["placement_id"])
	}
	_, stderr, code := s.dev(t, s.b, create...)
	if code != 1 || !strings.Contains(stderr, "resource_exhausted") ||
		!strings.Contains(stderr, "wallet") {
		t.Fatalf("the sixth create exited %d with %q, want resource_exhausted", code, stderr)
	}
	failed := s.events.awaitReason(t, "placement.failed", "wallet_busy", eventWait)
	if !failed.Payload.Refundable || failed.Payload.State != "failed" {
		t.Fatalf("failed event %+v, want refundable and failed", failed.Payload)
	}
	for _, id := range ready {
		s.events.await(t, "placement.ready", id, eventWait)
	}

	// Without the payer the same order counts against the anonymous cap and goes through
	out := parseKV(s.devOK(t, s.a, "create", "--package", smallPkg))
	if out["state"] != "ready" {
		t.Fatalf("an unpaid create printed %v", out)
	}
}
