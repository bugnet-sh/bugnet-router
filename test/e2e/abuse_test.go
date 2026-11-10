package e2e

import (
	"strings"
	"testing"

	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
)

func TestE2EAbuseEndCutsEdgesAndBlocks(t *testing.T) {
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
	out := parseKV(s.devOK(t, s.a, create...))
	id := out["placement_id"]
	if out["state"] != "ready" || id == "" {
		t.Fatalf("create printed %v", out)
	}
	found := parseKV(s.devOK(t, s.b, "abuse", "find", id))
	if found["placement_id"] != id || found["payer"] != e2ePayer {
		t.Fatalf("abuse find printed %v, want the placement and its payer", found)
	}
	ended := parseKV(s.devOK(t, s.b, "abuse", "end", id, "--note", "e2e", "--actor", "tester"))
	if ended["termination_reason"] != "abuse" || ended["blocked_payers"] != e2ePayer {
		t.Fatalf("abuse end printed %v, want abuse and the payer blocked", ended)
	}
	s.events.awaitReason(t, "placement.terminated", "abuse", eventWait)
	s.events.await(t, "wallet.blocked", id, eventWait)
	evs := s.edgeEvents(t)
	if len(evs) != 1 || evs[0].GetPlacementEnded().GetPlacementId() != id ||
		evs[0].GetPlacementEnded().GetReason() != routerv1.EndReason_END_REASON_ABUSE {
		t.Errorf("edge events %v, want one PlacementEnded ABUSE for %s", evs, id)
	}
	if n := sandboxes(t, s.root); n != 0 {
		t.Errorf("%d sandboxes left after the abuse end, want 0", n)
	}
	_, stderr, code := s.dev(t, s.a, create...)
	if code != 1 || !strings.Contains(stderr, "permission_denied") ||
		!strings.Contains(stderr, "wallet is blocked") {
		t.Fatalf("a create by the blocked payer exited %d with %q, want permission_denied",
			code, stderr)
	}
}
