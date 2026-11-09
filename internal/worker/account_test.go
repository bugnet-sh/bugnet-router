package worker_test

import (
	"errors"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

func healthOf(t *testing.T, e *env, name string) (store.ProviderHealth, bool) {
	t.Helper()
	rows, err := e.store.ProviderHealths(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if r.Provider == name {
			return r, true
		}
	}
	return store.ProviderHealth{}, false
}

func signalActions(t *testing.T, e *env, name string) []string {
	t.Helper()
	rows, err := e.pool.Query(t.Context(), `
		select action from provider_signals
		where class = 'account' and provider = $1 order by created_at, id`, name)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			t.Fatal(err)
		}
		out = append(out, a)
	}
	return out
}

func TestAccountCheckBlocksAndClears(t *testing.T) {
	m := newMetered(t, "m")
	e := newEnv(t, options{providers: []provider.Provider{m}})
	e.start(t)
	m.set(func(m *metered) {
		m.account = provider.Account{Block: provider.BlockBanned, Detail: "team is banned"}
	})
	e.run(t, jobargs.AccountCheck{Provider: "m", Now: true})
	h, ok := healthOf(t, e, "m")
	if !ok || h.Blocked != "banned" || h.BlockedDetail != "team is banned" ||
		h.BlockedSince.IsZero() || h.AccountCheckedAt.IsZero() {
		t.Fatalf("health row = %+v, %v, want a banned provider", h, ok)
	}
	// A second check of the same block changes nothing and signals nothing
	e.run(t, jobargs.AccountCheck{Provider: "m", Now: true})
	m.set(func(m *metered) { m.account = provider.Account{} })
	e.run(t, jobargs.AccountCheck{Provider: "m", Now: true})
	if h, _ := healthOf(t, e, "m"); h.Blocked != "" || !h.BlockedSince.IsZero() {
		t.Fatalf("health row = %+v, want a cleared provider", h)
	}
	eq(t, signalActions(t, e, "m"), []string{"provider_blocked", "provider_cleared"})
}

func TestAccountCheckThatCannotReadChangesNothing(t *testing.T) {
	m := newMetered(t, "m")
	e := newEnv(t, options{providers: []provider.Provider{m}})
	e.start(t)
	m.set(func(m *metered) {
		m.account = provider.Account{Block: provider.BlockAuth, Detail: "401"}
	})
	e.run(t, jobargs.AccountCheck{Provider: "m", Now: true})
	// Three failed checks in a row only log, the block they could not read stands
	m.set(func(m *metered) { m.checkErr = errors.New("network") })
	for range 3 {
		e.run(t, jobargs.AccountCheck{Provider: "m", Now: true})
	}
	if h, _ := healthOf(t, e, "m"); h.Blocked != "auth" {
		t.Fatalf("health row = %+v, want the auth block to stand", h)
	}
	eq(t, signalActions(t, e, "m"), []string{"provider_blocked"})
}

func TestAccountCheckRunsOnlyWhenDue(t *testing.T) {
	m := newMetered(t, "m")
	clk := &clock{}
	e := newEnv(t, options{
		providers: []provider.Provider{m}, workerClock: clk,
		limitsPoll: config.LimitsPoll{
			Every: time.Minute, BoatEvery: time.Minute, AccountEvery: 5 * time.Minute,
			AccountEveryBlocked: time.Minute, ReserveSlots: 1, StaleAfter: 5,
		},
	})
	e.start(t)
	run := func(now bool) int {
		e.run(t, jobargs.AccountCheck{Provider: "m", Now: now})
		_, checks := m.counts()
		return checks
	}
	eq(t, run(false), 1)
	// Four minutes on, a periodic run finds the last check recent
	clk.Advance(4 * time.Minute)
	eq(t, run(false), 1)
	// A call that saw an account code asks at once
	eq(t, run(true), 2)
	clk.Advance(4 * time.Minute)
	eq(t, run(false), 2)
	clk.Advance(2 * time.Minute)
	eq(t, run(false), 3)

	// While the provider is blocked the interval is a minute
	m.set(func(m *metered) { m.account = provider.Account{Block: provider.BlockUnfunded} })
	eq(t, run(true), 4)
	clk.Advance(30 * time.Second)
	eq(t, run(false), 4)
	clk.Advance(31 * time.Second)
	eq(t, run(false), 5)
}

func TestAccountCheckSkipsProvidersWithoutOne(t *testing.T) {
	plain := newLocal(t, "plain")
	e := newEnv(t, options{providers: []provider.Provider{plain}})
	e.start(t)
	e.run(t, jobargs.AccountCheck{Provider: "plain", Now: true})
	e.run(t, jobargs.AccountCheck{Provider: "nobody", Now: true})
	if _, ok := healthOf(t, e, "plain"); ok {
		t.Fatal("a provider with no account check got a health row")
	}
}
