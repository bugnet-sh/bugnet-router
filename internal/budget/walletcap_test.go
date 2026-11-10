package budget_test

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/budget"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

const walletCap = 1 << 20

// walletMeter is a meter with a wallet cap whose UTC day the test moves
func walletMeter(t *testing.T) (meter, *time.Time) {
	vk, prefix := testenv.Valkey(t)
	day := time.Date(2026, 11, 9, 23, 0, 0, 0, time.UTC)
	m := budget.New(vk, prefix, defaults).WithWalletCap(walletCap).
		WithClock(func() time.Time { return day })
	return meter{Meter: m, t: t, vk: vk, prefix: prefix}, &day
}

func paid(p uuid.UUID, payer string) budget.Limit {
	l := limit(p, 1<<30)
	l.Payer = payer
	return l
}

func (m meter) walletSpentKey(payer, day string) bool {
	m.t.Helper()
	n, err := m.vk.Do(m.t.Context(),
		m.vk.B().Exists().Key(m.prefix+"wallet:spent:"+payer+":"+day).Build()).AsInt64()
	if err != nil {
		m.t.Fatal(err)
	}
	return n == 1
}

func TestWalletCountsEveryPlacementOfThePayer(t *testing.T) {
	m, _ := walletMeter(t)
	a, b := uuid.New(), uuid.New()
	ans, _ := m.report(budget.Edge, budget.ByEdge, paid(a, "A"), 300<<10)
	eq(t, ans[a].Remaining, uint64(walletCap-300<<10))
	ans, _ = m.report(budget.Egress, budget.ByEdge, paid(b, "A"), 200<<10)
	eq(t, ans[b].Remaining, uint64(walletCap-500<<10))
	// The placement's own remainder wins when it is the smaller one
	l := paid(a, "A")
	l.Budget = 400 << 10
	left, err := m.Remaining(t.Context(), budget.Edge, l)
	eq(t, err, nil)
	eq(t, left, uint64(100<<10))
	left, err = m.Remaining(t.Context(), budget.Egress, paid(b, "A"))
	eq(t, err, nil)
	eq(t, left, uint64(walletCap-500<<10))
	ttl := m.pttl("wallet:bytes:A:20261109")
	if ttl <= 47*time.Hour || ttl > 48*time.Hour {
		t.Fatalf("wallet:bytes ttl is %v, want about 48h", ttl)
	}
}

func TestWalletCapIsReachedOnceAndSetsNoPlacementGuard(t *testing.T) {
	m, _ := walletMeter(t)
	a, b := uuid.New(), uuid.New()
	ans, _ := m.report(budget.Edge, budget.ByEdge, paid(a, "A"), walletCap-1)
	eq(t, ans[a].WalletCrossed, false)
	ans, spent := m.report(budget.Egress, budget.ByEdge, paid(b, "A"), 1)
	eq(t, ans[b].WalletCrossed, true)
	eq(t, ans[b].Remaining, uint64(0))
	eq(t, len(spent), 0)
	ans, _ = m.report(budget.Edge, budget.ByEdge, paid(a, "A"), 10)
	eq(t, ans[a].WalletCrossed, false)
	eq(t, ans[a].Remaining, uint64(0))
	eq(t, m.walletSpentKey("A", "20261109"), true)
	for _, kind := range []budget.Kind{budget.Edge, budget.Egress} {
		for _, p := range []uuid.UUID{a, b} {
			spent, err := m.Spent(t.Context(), kind, p)
			eq(t, err, nil)
			eq(t, spent, false)
		}
	}
	// Another payer is unaffected
	other := uuid.New()
	ans, _ = m.report(budget.Edge, budget.ByEdge, paid(other, "B"), 1)
	eq(t, ans[other].Remaining, uint64(walletCap-1))
}

func TestSpentForReadsTheWalletKey(t *testing.T) {
	m, _ := walletMeter(t)
	a := uuid.New()
	m.report(budget.Edge, budget.ByEdge, paid(a, "A"), walletCap)
	for _, kind := range []budget.Kind{budget.Edge, budget.Egress} {
		spent, err := m.SpentFor(t.Context(), kind, paid(a, "A"))
		eq(t, err, nil)
		eq(t, spent, true)
		spent, err = m.SpentFor(t.Context(), kind, paid(uuid.New(), "B"))
		eq(t, err, nil)
		eq(t, spent, false)
	}
}

func TestRolloverRestoresThroughLeaseCheck(t *testing.T) {
	m, day := walletMeter(t)
	a := uuid.New()
	m.report(budget.Edge, budget.ByEdge, paid(a, "A"), walletCap)
	left, err := m.Remaining(t.Context(), budget.Edge, paid(a, "A"))
	eq(t, err, nil)
	eq(t, left, uint64(0))
	*day = day.Add(2 * time.Hour)
	left, err = m.Remaining(t.Context(), budget.Edge, paid(a, "A"))
	eq(t, err, nil)
	eq(t, left, uint64(walletCap))
	spent, err := m.SpentFor(t.Context(), budget.Edge, paid(a, "A"))
	eq(t, err, nil)
	eq(t, spent, false)
	// The new day counts afresh and can be spent again
	ans, _ := m.report(budget.Edge, budget.ByEdge, paid(a, "A"), walletCap)
	eq(t, ans[a].WalletCrossed, true)
	eq(t, m.walletSpentKey("A", "20261110"), true)
	eq(t, m.walletSpentKey("A", "20261109"), true)
}

func TestNoPayerOnlyThePlacementBudget(t *testing.T) {
	m, _ := walletMeter(t)
	a := uuid.New()
	ans, _ := m.report(budget.Edge, budget.ByEdge, paid(a, ""), 2*walletCap)
	eq(t, ans[a].Remaining, uint64(1<<30-2*walletCap))
	eq(t, ans[a].WalletCrossed, false)
	pattern := m.vk.B().Keys().Pattern(m.prefix + "wallet:*").Build()
	keys, err := m.vk.Do(t.Context(), pattern).AsStrSlice()
	eq(t, err, nil)
	eq(t, len(keys), 0)
	// A meter without a cap ignores a payer too
	plain := newMeter(t)
	ans, _ = plain.report(budget.Edge, budget.ByEdge, paid(a, "A"), 2*walletCap)
	eq(t, ans[a].Remaining, uint64(1<<30-2*walletCap))
}
