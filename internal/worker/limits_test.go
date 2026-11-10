package worker_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/worker"
)

// metered is a local provider that reads limits and its account from what the test sets
type metered struct {
	*local.Provider
	mu       sync.Mutex
	limits   provider.Limits
	limitErr error
	account  provider.Account
	checkErr error
	reads    int
	checks   int
}

func (m *metered) ReadLimits(context.Context) (provider.Limits, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reads++
	return m.limits, m.limitErr
}

func (m *metered) CheckAccount(context.Context) (provider.Account, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checks++
	return m.account, m.checkErr
}

func (m *metered) set(change func(*metered)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	change(m)
}

func (m *metered) counts() (reads, checks int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.reads, m.checks
}

func newMetered(t *testing.T, name string) *metered {
	t.Helper()
	return &metered{Provider: newLocal(t, name)}
}

func capacityOf(t *testing.T, e *env, name string) store.Capacity {
	t.Helper()
	rows, err := e.store.Capacities(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if r.Provider == name {
			return r
		}
	}
	t.Fatalf("no capacity row for %s in %+v", name, rows)
	return store.Capacity{}
}

func TestLimitsPollWritesTheRow(t *testing.T) {
	for _, tc := range []struct {
		name    string
		got     provider.Limits
		setting int
		max     int
		source  string
	}{
		{"the provider's cap alone", provider.Limits{MaxConcurrent: 100, Active: 7}, 0, 100,
			store.LimitFromProvider},
		{"a lower setting wins", provider.Limits{MaxConcurrent: 100, Active: 7}, 40, 40,
			store.LimitFromSettings},
		{"a higher setting does not raise the cap",
			provider.Limits{MaxConcurrent: 30, Active: 7}, 40, 30, store.LimitFromProvider},
		{"the setting alone", provider.Limits{Active: 7}, 20, 20, store.LimitFromSettings},
		{"neither", provider.Limits{Active: 7}, 0, store.DefaultMaxConcurrent,
			store.LimitFromDefault},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newMetered(t, "m")
			m.set(func(m *metered) { m.limits = tc.got })
			e := newEnv(t, options{
				providers: []provider.Provider{m}, maxConcurrent: map[string]int{"m": tc.setting},
			})
			e.start(t)
			e.run(t, jobargs.LimitsPoll{Provider: "m"})
			got := capacityOf(t, e, "m")
			if got.MaxConcurrent != tc.max || got.LimitSource != tc.source ||
				got.ProviderActive != 7 || got.ExternalInUse != 7 || got.PolledAt.IsZero() {
				t.Fatalf("row = %+v, want max %d from %s with 7 active", got, tc.max, tc.source)
			}
		})
	}
}

func TestLimitsPollCountsOnlyTheRestAsExternal(t *testing.T) {
	m := newMetered(t, "m")
	e := newEnv(t, options{providers: []provider.Provider{m}})
	e.start(t)
	e.placeReady(t, "order-1")
	m.set(func(m *metered) { m.limits = provider.Limits{MaxConcurrent: 10, Active: 4} })
	e.run(t, jobargs.LimitsPoll{Provider: "m"})
	got := capacityOf(t, e, "m")
	if got.InUse != 1 || got.ExternalInUse != 3 || got.ProviderActive != 4 {
		t.Fatalf("row = %+v, want 1 held here and 3 elsewhere", got)
	}
}

func TestFailedPollKeepsTheRow(t *testing.T) {
	m := newMetered(t, "m")
	m.set(func(m *metered) { m.limits = provider.Limits{MaxConcurrent: 10, Active: 2} })
	e := newEnv(t, options{providers: []provider.Provider{m}})
	e.start(t)
	e.run(t, jobargs.LimitsPoll{Provider: "m"})
	before := capacityOf(t, e, "m")
	m.set(func(m *metered) { m.limitErr = errors.New("down") })
	e.run(t, jobargs.LimitsPoll{Provider: "m"})
	if after := capacityOf(t, e, "m"); after != before {
		t.Fatalf("row = %+v after a failed poll, want %+v", after, before)
	}
}

func TestPollsRunAtStartForProvidersThatReadLimits(t *testing.T) {
	m := newMetered(t, "m")
	plain := newLocal(t, "plain")
	e := newEnv(t, options{periodic: true, providers: []provider.Provider{m, plain}})
	e.start(t)
	eventually(t, "the first limits poll", func() bool {
		reads, checks := m.counts()
		return reads > 0 && checks > 0
	})
	rows, err := e.store.Capacities(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if r.Provider == "plain" {
			t.Fatalf("a provider that reads no limits got a row: %+v", r)
		}
	}
}

func TestPeriodicPollIntervals(t *testing.T) {
	boat := newMetered(t, "boat")
	other := newMetered(t, "other")
	got := worker.PollEvery(worker.Deps{LimitsPoll: config.DefaultLimitsPoll()}, boat, other)
	eq(t, got, map[string]time.Duration{
		"limits_poll/boat": 15 * time.Second, "account_check/boat": time.Minute,
		"limits_poll/other": time.Minute, "account_check/other": time.Minute,
		"foreign_count": time.Hour,
	})
}
