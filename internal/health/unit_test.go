package health_test

import (
	"math"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/health"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

func eq[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func round3(f float64) float64 { return math.Round(f*1000) / 1000 }

func TestWilson(t *testing.T) {
	eq(t, round3(health.LowerBound(29, 30)), 0.833)
	eq(t, round3(health.LowerBound(0, 5)), 0.0)
	// No samples say nothing against a provider
	eq(t, health.LowerBound(0, 0), 1.0)
}

var rules = health.Rules{MinSamples: 5, DownBelow: 0.5, SlowReadyP95: 10 * time.Second}

func counts(ok, failed int, p95 time.Duration, canaries ...string) health.Counts {
	return health.Counts{OK: ok, Failed: failed, ReadyP95: p95, LastCanaries: canaries}
}

// two failed canaries -> down, low bound under 0.5 -> down, slow
func TestStateRules(t *testing.T) {
	failed, ok := store.OutcomeFailed, store.OutcomeOK
	for _, tc := range []struct {
		name string
		c    health.Counts
		want string
	}{
		{"no samples", counts(0, 0, 0), "ok"},
		{"all ok", counts(10, 0, 3*time.Second, ok, ok), "ok"},
		{"two failed canaries", counts(10, 2, time.Second, failed, failed), "down"},
		{"two failed canaries with one sample", counts(0, 2, 0, failed, failed), "down"},
		{"a failed canary then an ok one", counts(10, 1, time.Second, failed, ok), "ok"},
		{"one canary", counts(10, 1, time.Second, failed), "ok"},
		{"low bound", counts(1, 5, time.Second, ok), "down"},
		{"low bound under min samples", counts(0, 4, 0, ok), "ok"},
		{"slow", counts(10, 0, 11*time.Second, ok, ok), "slow"},
		{"slow under min samples", counts(4, 0, 30*time.Second), "ok"},
		{"slow at the threshold", counts(10, 0, 10*time.Second), "ok"},
		{"down beats slow", counts(1, 5, 30*time.Second, failed, failed), "down"},
	} {
		got := rules.State(tc.c)
		if got.State != tc.want {
			t.Errorf("%s: state %s, want %s", tc.name, got.State, tc.want)
		}
	}
	got := rules.State(counts(29, 1, 4*time.Second))
	eq(t, got.Samples, 30)
	eq(t, round3(got.SuccessLB), 0.833)
	eq(t, got.ReadyP95, 4*time.Second)
}

func TestAllDownGuardKeepsStates(t *testing.T) {
	failed := store.OutcomeFailed
	all := map[string]health.Counts{
		"a": counts(10, 2, time.Second, failed, failed),
		"b": counts(3, 3, time.Second, failed, failed),
	}
	got, guarded := rules.Rollup(all, map[string]string{"a": "ok", "b": "slow"})
	eq(t, guarded, true)
	eq(t, got["a"].State, "ok")
	eq(t, got["b"].State, "slow")
	// The window's figures are still written
	eq(t, got["b"].Samples, 6)
	// A provider with no row yet is ok
	got, guarded = rules.Rollup(all, nil)
	eq(t, guarded, true)
	eq(t, got["a"].State, "ok")

	// One provider whose canaries are fine lets the others go down
	all["c"] = counts(10, 0, time.Second, store.OutcomeOK, store.OutcomeOK)
	got, guarded = rules.Rollup(all, nil)
	eq(t, guarded, false)
	eq(t, got["a"].State, "down")
	eq(t, got["c"].State, "ok")

	// A provider that is down by its bound alone, not by canaries, is not a canary fault
	got, guarded = rules.Rollup(map[string]health.Counts{
		"a": counts(10, 2, time.Second, failed, failed),
		"b": counts(1, 6, time.Second, store.OutcomeOK),
	}, nil)
	eq(t, guarded, false)
	eq(t, got["b"].State, "down")

	// A lone provider has nothing to fall back on, so its canaries decide
	got, guarded = rules.Rollup(map[string]health.Counts{
		"a": counts(10, 2, time.Second, failed, failed),
	}, map[string]string{"a": "ok"})
	eq(t, guarded, false)
	eq(t, got["a"].State, "down")
}

var sched = health.Schedule{
	Every: 5 * time.Minute, EveryDown: time.Minute, BoatEvery: 30 * time.Minute,
	PassiveWindow: 5 * time.Minute,
}

var epoch = time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)

func facts(sinceCanary, sinceReady time.Duration, state string) store.CanaryFacts {
	f := store.CanaryFacts{State: state}
	if sinceCanary > 0 {
		f.LastCanary = epoch.Add(-sinceCanary)
	}
	if sinceReady > 0 {
		f.LastReady = epoch.Add(-sinceReady)
	}
	return f
}

// 20 percent jitter, 1 minute while down, Boat 30 minutes and free
func TestSchedule(t *testing.T) {
	for i := range 200 {
		last := epoch.Add(-time.Duration(i) * time.Hour)
		gap := health.Gap(5*time.Minute, "e2b", last)
		if gap < 4*time.Minute || gap > 6*time.Minute {
			t.Fatalf("a 5 minute gap came out as %s", gap)
		}
		eq(t, gap, health.Gap(5*time.Minute, "e2b", last))
	}
	seen := map[time.Duration]bool{}
	for i := range 50 {
		seen[health.Gap(5*time.Minute, "e2b", epoch.Add(-time.Duration(i)*time.Minute))] = true
	}
	if len(seen) < 25 {
		t.Fatalf("only %d different gaps in 50, the jitter does not spread", len(seen))
	}

	due := func(f store.CanaryFacts, costly, free bool) bool {
		return sched.Due(health.Inputs{Name: "p", Facts: f, Costly: costly, StartFree: free}, epoch)
	}
	// Never sampled, and long ago, are due
	eq(t, due(facts(0, 0, "ok"), false, true), true)
	eq(t, due(facts(7*time.Minute, 0, "ok"), false, true), true)
	eq(t, due(facts(3*time.Minute, 0, "ok"), false, true), false)
	// Down providers use the short gap
	eq(t, due(facts(61*time.Second, 0, "down"), false, true), true)
	eq(t, due(facts(50*time.Second, 0, "down"), false, true), false)
	// Boat waits 30 minutes, and only when its next start is free
	eq(t, due(facts(20*time.Minute, 0, "ok"), true, true), false)
	eq(t, due(facts(37*time.Minute, 0, "ok"), true, true), true)
	eq(t, due(facts(37*time.Minute, 0, "ok"), true, false), false)
	eq(t, due(facts(0, 0, "ok"), true, false), false)
	// An account that is blocked is the account check's to judge
	f := facts(time.Hour, 0, "ok")
	f.Blocked = "banned"
	eq(t, due(f, false, true), false)
}

func TestPassiveSkip(t *testing.T) {
	due := func(sinceReady time.Duration) bool {
		return sched.Due(health.Inputs{
			Name: "p", Facts: facts(time.Hour, sinceReady, "ok"), StartFree: true,
		}, epoch)
	}
	eq(t, due(0), true)
	eq(t, due(4*time.Minute), false)
	eq(t, due(6*time.Minute), true)
}

func TestOutcomeNamesWhatCounts(t *testing.T) {
	o, code := health.Outcome(nil)
	eq(t, o+code, "ok")
}
