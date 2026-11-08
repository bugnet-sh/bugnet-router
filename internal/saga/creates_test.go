package saga_test

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/health"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/ratelimit"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
)

// jobCount counts the River jobs of one kind and the args they carry
func (e *env) jobCount(t *testing.T, kind, args string) int {
	t.Helper()
	var n int
	err := e.pool.QueryRow(t.Context(), `
		select count(*) from river_job where kind = $1 and args @> $2::jsonb`, kind, args).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// failing is a local provider whose creates fail with err and are counted
func failing(t *testing.T, name string, calls *atomic.Int64, err error) *local.Provider {
	t.Helper()
	hook := func(int, provider.CreateRequest) error {
		calls.Add(1)
		return err
	}
	return newLocal(t, local.Options{Name: name, CreateHook: hook})
}

// drain takes up to n create tokens of name, as other creates on the account would
func (e *env) drain(t *testing.T, name string, rate, n int) {
	t.Helper()
	l := ratelimit.New(e.vk, e.prefix, 0, 0)
	b := ratelimit.Bucket{Key: "rl:create:" + name, PerSecond: float64(rate), Burst: rate}
	for range n {
		// A bucket already empty is what the test wants
		if _, _, err := l.TakeKey(t.Context(), b); err != nil {
			t.Fatalf("drain %s: %v", name, err)
		}
	}
}

func TestCreateBucketFailsOverAfterTheWait(t *testing.T) {
	saga.SetCreateWait(t, 100*time.Millisecond)
	var aCalls atomic.Int64
	a := newLocal(t, local.Options{Name: "a", CreateHook: func(int, provider.CreateRequest) error {
		aCalls.Add(1)
		return nil
	}})
	b := newLocal(t, local.Options{Name: "b"})
	e := newEnvWith(t, func(d *saga.Deps) { d.CreatesPerSecond = map[string]int{"a": 1} }, a, b)

	first, _ := e.placeReady(t, "order-1")
	// The bucket holds one token a second, so the next create finds none within the wait
	e.drain(t, "a", 1, 1)
	second, _ := e.placeReady(t, "order-2")
	if first.Provider != "a" || second.Provider != "b" || aCalls.Load() != 1 {
		t.Fatalf("placed on %q then %q after %d creates on a, want a then b after 1",
			first.Provider, second.Provider, aCalls.Load())
	}
	// The skipped attempt left no row and gave its slot back
	if got := e.attempts(t, second.ID); len(got) != 1 || got[0].provider != "b" {
		t.Fatalf("attempts %+v", got)
	}
	if n := e.inUse(t, "a"); n != 1 {
		t.Fatalf("a holds %d slots, want only the first placement's", n)
	}
}

func TestCreateBucketWaitsForATokenWithinTheWait(t *testing.T) {
	var aCalls atomic.Int64
	a := newLocal(t, local.Options{Name: "a", CreateHook: func(int, provider.CreateRequest) error {
		aCalls.Add(1)
		return nil
	}})
	e := newEnvWith(t, func(d *saga.Deps) { d.CreatesPerSecond = map[string]int{"a": 5} }, a)
	// Five creates a second with a burst of five, all spent, so the next create waits 200 ms
	e.drain(t, "a", 5, 5)
	start := time.Now()
	e.placeReady(t, "order-1")
	if aCalls.Load() != 1 {
		t.Fatalf("a created %d sandboxes, want the one that waited", aCalls.Load())
	}
	if waited := time.Since(start); waited < 150*time.Millisecond {
		t.Fatalf("the create went ahead after %v, before a token was there", waited)
	}
}

func TestCreateBucketWithNoOtherProviderFailsRateLimited(t *testing.T) {
	saga.SetCreateWait(t, 50*time.Millisecond)
	a := newLocal(t, local.Options{Name: "a"})
	e := newEnvWith(t, func(d *saga.Deps) { d.CreatesPerSecond = map[string]int{"a": 1} }, a)
	e.placeReady(t, "order-1")
	e.drain(t, "a", 1, 1)
	p, _ := e.place(t, "order-2")
	if p.State != "failed" || p.FailureReason != "rate_limited" || !p.Refundable {
		t.Fatalf("placement %+v, want a refundable rate_limited failure", p)
	}
	if n := e.inUse(t, "a"); n != 1 {
		t.Fatalf("a holds %d slots, want 1", n)
	}
}

func TestRefusalsAreNotFailures(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  *provider.Error
	}{
		{"quota", &provider.Error{
			Code: provider.CodeQuota, CanFailOver: true, Err: errors.New("cap"),
		}},
		{"rate limit", &provider.Error{
			Code: provider.CodeRateLimited, CanFailOver: true, Err: errors.New("slow"),
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var aCalls atomic.Int64
			a := failing(t, "a", &aCalls, tc.err)
			b := newLocal(t, local.Options{Name: "b"})
			var tracker *placement.Tracker
			e := newEnvWith(t, func(d *saga.Deps) { tracker = d.Tracker }, a, b)
			// A burst far past the three failures in a minute that close a breaker
			for i := range 6 {
				p, _ := e.placeReady(t, "order-"+string(rune('a'+i)))
				if p.Provider != "b" {
					t.Fatalf("order %d placed on %q, want the next provider", i, p.Provider)
				}
			}
			if !tracker.Healthy("a") {
				t.Fatal("refusals closed the provider's breaker")
			}
			if aCalls.Load() != 6 {
				t.Fatalf("a was asked %d times, want every order to try it", aCalls.Load())
			}
		})
	}
}

func TestOtherCreateErrorsStillCountAsFailures(t *testing.T) {
	var aCalls atomic.Int64
	a := failing(t, "a", &aCalls, &provider.Error{
		Code: provider.CodeInternal, CanFailOver: true, Err: errors.New("down"),
	})
	b := newLocal(t, local.Options{Name: "b"})
	var tracker *placement.Tracker
	e := newEnvWith(t, func(d *saga.Deps) { tracker = d.Tracker }, a, b)
	for i := range 3 {
		e.placeReady(t, "order-"+string(rune('a'+i)))
	}
	if tracker.Healthy("a") {
		t.Fatal("three internal failures left the breaker open")
	}
}

func TestRateLimitPausesCreatesOnTheProvider(t *testing.T) {
	var aCalls atomic.Int64
	a := failing(t, "a", &aCalls, &provider.Error{
		Code: provider.CodeRateLimited, CanFailOver: true, RetryAfter: 30 * time.Minute,
		Err: errors.New("slow"),
	})
	b := newLocal(t, local.Options{Name: "b"})
	e := newEnv(t, a, b)
	for i := range 4 {
		p, _ := e.placeReady(t, "order-"+string(rune('a'+i)))
		if p.Provider != "b" {
			t.Fatalf("order %d placed on %q", i, p.Provider)
		}
	}
	// Only the first order reached the wall, the rest read the pause before reserving a slot
	if aCalls.Load() != 1 {
		t.Fatalf("a was asked %d times, want 1", aCalls.Load())
	}
	key := e.prefix + "pause:a:create"
	ttl, err := e.vk.Do(t.Context(), e.vk.B().Pttl().Key(key).Build()).AsInt64()
	if err != nil || ttl <= 0 || ttl > (5*time.Minute).Milliseconds() {
		t.Fatalf("the pause expires in %d ms, %v, want at most 5 minutes", ttl, err)
	}
}

func TestQuotaTriggersAPoll(t *testing.T) {
	var aCalls atomic.Int64
	a := failing(t, "a", &aCalls, &provider.Error{
		Code: provider.CodeQuota, CanFailOver: true, Err: errors.New("cap"),
	})
	b := newLocal(t, local.Options{Name: "b"})
	e := newEnv(t, a, b)
	e.placeReady(t, "order-1")
	e.placeReady(t, "order-2")
	// One poll waits for its run however many creates met the cap, and none is asked for b
	if n := e.jobCount(t, "limits_poll", `{"provider":"a"}`); n != 1 {
		t.Fatalf("%d limits_poll jobs for a, want 1", n)
	}
	if n := e.jobCount(t, "limits_poll", `{"provider":"b"}`); n != 0 {
		t.Fatalf("%d limits_poll jobs for b, want 0", n)
	}
}

func TestAccountErrorsAskForACheck(t *testing.T) {
	for _, code := range []provider.Code{
		provider.CodeAbuseLock, provider.CodeUnfunded, provider.CodeAuth,
	} {
		t.Run(string(code), func(t *testing.T) {
			var aCalls atomic.Int64
			a := failing(t, "a", &aCalls, &provider.Error{
				Code: code, CanFailOver: true, Err: errors.New("no"),
			})
			b := newLocal(t, local.Options{Name: "b"})
			e := newEnv(t, a, b)
			e.placeReady(t, "order-1")
			if n := e.jobCount(t, "account_check", `{"provider":"a","now":true}`); n < 1 {
				t.Fatalf("%d account_check jobs for a, want a request", n)
			}
			// Requests within 30 s collapse into one
			before := e.jobCount(t, "account_check", `{"provider":"b","now":true}`)
			for range 3 {
				health.NoteAccount(t.Context(), e.river, "b", code)
			}
			after := e.jobCount(t, "account_check", `{"provider":"b","now":true}`)
			if after-before != 1 {
				t.Fatalf("3 requests made %d jobs, want 1", after-before)
			}
		})
	}
}

func TestOrdinaryErrorsAskForNothing(t *testing.T) {
	var aCalls atomic.Int64
	a := failing(t, "a", &aCalls, &provider.Error{
		Code: provider.CodeCapacity, CanFailOver: true, Err: errors.New("full"),
	})
	b := newLocal(t, local.Options{Name: "b"})
	e := newEnv(t, a, b)
	e.placeReady(t, "order-1")
	for _, kind := range []string{"account_check", "limits_poll"} {
		if n := e.jobCount(t, kind, `{}`); n != 0 {
			t.Fatalf("%d %s jobs after a capacity error", n, kind)
		}
	}
}
