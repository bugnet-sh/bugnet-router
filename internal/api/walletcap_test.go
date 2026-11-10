package api_test

import (
	"fmt"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/sshkeys/sshkeystest"
)

const capBytes = 1 << 20

// capWallets turns the payer path on with a wallet cap of capBytes
func capWallets() config.Wallets {
	return payerWallets(func(w *config.Wallets) { w.BytesPerDay = capBytes })
}

// walletDay is today's UTC day as the wallet keys name it
func walletDay() string { return time.Now().UTC().Format("20060102") }

// guardSet reports whether a key of the test's prefix exists
func (e *env) guardSet(t *testing.T, key string) bool {
	t.Helper()
	n, err := e.vk.Do(t.Context(), e.vk.B().Exists().Key(e.prefix+key).Build()).AsInt64()
	if err != nil {
		t.Fatal(err)
	}
	return n == 1
}

// paidPlacement starts a placement for payer and answers its handle and message
func (e *env) paidPlacement(t *testing.T, payer string) (string, *routerv1.Placement) {
	t.Helper()
	h := e.readyPaid(t, payer)
	return h, e.waitReady(t, h)
}

func TestWalletCapSpendsOnce(t *testing.T) {
	e := newEnv(t, options{wallets: capWallets()})
	_, p1 := e.paidPlacement(t, payerA)
	_, p2 := e.paidPlacement(t, payerA)
	_, other := e.paidPlacement(t, payerB)
	id1, id2 := p1.GetPlacementId(), p2.GetPlacementId()
	e.reportEdge(t, usage(id1, 0, capBytes-1))
	eq(t, len(e.edgeEvents(t)), 0)
	// Both reports cross the cap, and only the first one acts
	a := e.reportEgress(t, usage(id2, 1, 0))
	eq(t, a.GetBytesRemaining()[id2], uint64(0))
	e.reportEdge(t, usage(id1, 0, 10))
	evs := e.edgeEvents(t)
	eq(t, len(evs), 4)
	got := map[string]bool{}
	for _, ev := range evs {
		s := ev.GetBudgetSpent()
		got[fmt.Sprint(s.GetPlacementId(), s.GetKind())] = true
	}
	for _, id := range []string{id1, id2} {
		for _, k := range []routerv1.BudgetKind{
			routerv1.BudgetKind_BUDGET_KIND_EDGE, routerv1.BudgetKind_BUDGET_KIND_EGRESS,
		} {
			eq(t, got[fmt.Sprint(id, k)], true)
		}
	}
	eq(t, got[fmt.Sprint(other.GetPlacementId(), routerv1.BudgetKind_BUDGET_KIND_EDGE)], false)
	for _, id := range []string{id1, id2} {
		eq(t, e.guardSet(t, "edge:spent:"+id) || e.guardSet(t, "egress:spent:"+id), false)
	}
	eq(t, e.guardSet(t, "wallet:spent:"+payerA+":"+walletDay()), true)
	// The other payer has its own wallet
	eq(t, e.reportEdge(t, usage(other.GetPlacementId(), 0, 1)).
		GetBytesRemaining()[other.GetPlacementId()], uint64(capBytes-1))
}

func TestWalletCapIsAnsweredInLeaseChecks(t *testing.T) {
	e := newEnv(t, options{wallets: capWallets()})
	_, p := e.paidPlacement(t, payerA)
	id := p.GetPlacementId()
	e.reportEdge(t, usage(id, 0, 100))
	check := func() uint64 {
		resp, err := e.edge(t).CheckEdgeLeases(t.Context(), connect.NewRequest(
			&routerv1.CheckEdgeLeasesRequest{PlacementIds: []string{id}}))
		if err != nil {
			t.Fatal(err)
		}
		return resp.Msg.GetLeases()[id].GetBytesRemaining()
	}
	eq(t, check(), uint64(capBytes-100))
	e.reportEdge(t, usage(id, 0, capBytes))
	eq(t, check(), uint64(0))
	// The next UTC day's key is empty, so the lease carries the placement's own figure again
	yesterday := time.Now().UTC().Add(-24 * time.Hour).Format("20060102")
	keys := []string{"wallet:bytes:", "wallet:spent:"}
	for _, k := range keys {
		err := e.vk.Do(t.Context(), e.vk.B().Rename().Key(e.prefix+k+payerA+":"+walletDay()).
			Newkey(e.prefix+k+payerA+":"+yesterday).Build()).Error()
		if err != nil {
			t.Fatal(err)
		}
	}
	eq(t, check(), uint64(capBytes))
}

func TestNoPayerOnlyThePlacementBudgetThroughTheAPI(t *testing.T) {
	e := newEnv(t, options{wallets: capWallets()})
	_, p := e.readyPlacement(t)
	id := p.GetPlacementId()
	a := e.reportEdge(t, usage(id, 0, 3*capBytes))
	eq(t, a.GetBytesRemaining()[id], uint64(2<<30-3*capBytes))
	eq(t, len(e.edgeEvents(t)), 0)
}

func TestResolveReadsTheWalletGuard(t *testing.T) {
	e := newEnv(t, options{wallets: capWallets(), withSSH: true, secrets: true})
	h1, p1 := e.paidPlacement(t, payerA)
	h2, p2 := e.paidPlacement(t, payerA)
	prev := e.mustCreatePreview(t, h1, 5173)
	e.put(t, h1, "/root/d", "x")
	m := e.mintOK(t, download(h1, "/root/d"))
	grant := e.grantFor(t, h2, sshkeystest.ED25519(t))
	e.armed(t, p2, 1)
	e.reportEdge(t, usage(p1.GetPlacementId(), 0, 2*capBytes))

	_, err := e.resolve(t, prev.GetPreviewId(), prev.GetToken())
	mustCode(t, err, connect.CodeResourceExhausted, "BUDGET_SPENT")
	_, err = e.openWith(t, tokenOf(t, m), fromSandbox, false)
	mustCode(t, err, connect.CodeResourceExhausted, "BUDGET_SPENT")
	_, err = e.acquire(t, grant)
	mustCode(t, err, connect.CodeResourceExhausted, "BUDGET_SPENT")
	_, err = e.tunnel(t, byToken(sha(tunnelToken)))
	mustCode(t, err, connect.CodeResourceExhausted, "BUDGET_SPENT")
}

func TestPrivateFetchCountsWalletBytes(t *testing.T) {
	pe := newPrivate(t, func(o *options) { o.wallets = capWallets() })
	if _, err := pe.pool.Exec(t.Context(),
		`update placements set payer = $1 where id = $2`, payerA, pe.id); err != nil {
		t.Fatal(err)
	}
	pe.mustFetch(t, pe.h)
	pe.bootstrapIs(t, pe.h, routerv1.BootstrapState_BOOTSTRAP_STATE_DONE)
	n, err := pe.vk.Do(t.Context(), pe.vk.B().Get().Key(pe.prefix+"wallet:bytes:"+payerA+":"+
		walletDay()).Build()).AsInt64()
	if err != nil || n <= 0 {
		t.Fatalf("the fetch left %d wallet bytes (%v)", n, err)
	}
	used, err := pe.vk.Do(t.Context(), pe.vk.B().Get().Key(pe.prefix+"egress:used:"+pe.id).
		Build()).AsInt64()
	eq(t, err, nil)
	eq(t, n, used)
}
