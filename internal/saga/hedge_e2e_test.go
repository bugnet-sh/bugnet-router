package saga_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// slowReady is a local provider whose sandboxes take delay to become ready, which a cancelled
// context cuts short, and then fail when fail is set
type slowReady struct {
	*local.Provider
	delay     time.Duration
	fail      bool
	destroyed *atomic.Int64
}

func newSlow(t *testing.T, name string, delay time.Duration, fail bool) slowReady {
	t.Helper()
	return slowReady{
		Provider: newLocal(t, local.Options{Name: name}), delay: delay, fail: fail,
		destroyed: &atomic.Int64{},
	}
}

func (s slowReady) WaitReady(ctx context.Context, ref provider.Ref) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(s.delay):
	}
	if s.fail {
		return &provider.Error{Code: provider.CodeInternal, Err: errors.New("boot failed")}
	}
	return s.Provider.WaitReady(ctx, ref)
}

func (s slowReady) Destroy(ctx context.Context, ref provider.Ref) error {
	s.destroyed.Add(1)
	return s.Provider.Destroy(ctx, ref)
}

// hedgingEnv is an env whose saga hedges after 200 ms, on providers listed in cost order
func hedgingEnv(t *testing.T, ps ...provider.Provider) *env {
	t.Helper()
	return newEnvWith(t, func(d *saga.Deps) {
		h := hedgeSettings()
		h.DefaultAfter, h.MinAfter = 200*time.Millisecond, 50*time.Millisecond
		d.Hedger = saga.NewHedger(saga.HedgerDeps{
			Settings: h, Valkey: d.Valkey, Prefix: d.Prefix, MarginFloor: d.MarginFloor,
		})
	}, ps...)
}

func (e *env) hedged(t *testing.T, id uuid.UUID) bool {
	t.Helper()
	var hedged bool
	err := e.pool.QueryRow(t.Context(), `select hedged from placements where id = $1`, id).
		Scan(&hedged)
	if err != nil {
		t.Fatal(err)
	}
	return hedged
}

// sampleOutcomes lists the outcome and hedge flag of every placement sample of a provider
func (e *env) sampleOutcomes(t *testing.T, name string) [][2]any {
	t.Helper()
	rows, err := e.pool.Query(t.Context(), `
		select outcome, hedge from start_samples where provider = $1 and kind = 'placement'
		order by started_at`, name)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out [][2]any
	for rows.Next() {
		var o string
		var h bool
		if err := rows.Scan(&o, &h); err != nil {
			t.Fatal(err)
		}
		out = append(out, [2]any{o, h})
	}
	return out
}

func TestHedgeWinnerKeepsOneSlot(t *testing.T) {
	slow, fast := newSlow(t, "slow", 30*time.Second, false), newSlow(t, "fast", 0, false)
	e := hedgingEnv(t, slow, fast)
	p, _ := e.placeReady(t, "order-1")
	eq(t, p.Provider, "fast")
	eq(t, e.attempts(t, p.ID), []attemptRow{
		{1, "slow", "failed", saga.ErrorHedgeLost}, {2, "fast", "ready", ""},
	})
	eq(t, e.hedged(t, p.ID), true)
	eq(t, e.inUse(t, "slow"), 0)
	eq(t, e.inUse(t, "fast"), 1)
	eq(t, e.recountOf(t, "slow")+e.recountOf(t, "fast"), 1)
	// The loser's sandbox was destroyed, and only the winner's is left
	eq(t, slow.destroyed.Load(), int64(1))
	eq(t, len(owned(t, slow)), 0)
	eq(t, len(owned(t, fast)), 1)
	eq(t, e.reload(t, p.ID).CapacityProvider, "fast")
}

func TestHedgeLostLeftOutOfHealth(t *testing.T) {
	slow, fast := newSlow(t, "slow", 30*time.Second, false), newSlow(t, "fast", 0, false)
	e := hedgingEnv(t, slow, fast)
	e.placeReady(t, "order-1")
	eq(t, e.sampleOutcomes(t, "slow"), [][2]any{{store.OutcomeHedgeLost, true}})
	eq(t, e.sampleOutcomes(t, "fast"), [][2]any{{store.OutcomeOK, true}})
	counts, err := e.store.HealthWindow(t.Context(), "slow", time.Now().Add(-time.Hour))
	eq(t, err, nil)
	eq(t, counts.OK+counts.Failed, 0)
	counts, err = e.store.HealthWindow(t.Context(), "fast", time.Now().Add(-time.Hour))
	eq(t, err, nil)
	eq(t, counts.OK, 1)
}

func TestHedgeAtMostOncePerPlacement(t *testing.T) {
	a := newSlow(t, "a", 1500*time.Millisecond, false)
	b := newSlow(t, "b", 30*time.Second, false)
	c := newSlow(t, "c", 0, false)
	e := hedgingEnv(t, a, b, c)
	p, _ := e.placeReady(t, "order-1")
	eq(t, p.Provider, "a")
	// The one hedge went to b, and c, which would have won at 400 ms, never ran
	eq(t, e.attempts(t, p.ID), []attemptRow{
		{1, "a", "ready", ""}, {2, "b", "failed", saga.ErrorHedgeLost},
	})
	eq(t, e.hedged(t, p.ID), true)
	eq(t, e.inUse(t, "a"), 1)
	eq(t, e.inUse(t, "b")+e.inUse(t, "c"), 0)
}

func TestBothHedgedAttemptsFailGoesOn(t *testing.T) {
	a := newSlow(t, "a", 400*time.Millisecond, true)
	b := newSlow(t, "b", 600*time.Millisecond, true)
	c := newSlow(t, "c", 0, false)
	e := hedgingEnv(t, a, b, c)
	p, _ := e.placeReady(t, "order-1")
	eq(t, p.Provider, "c")
	got := e.attempts(t, p.ID)
	eq(t, len(got), 3)
	// A known failure of one attempt while the other runs does not fail over
	eq(t, got[0].state, "failed")
	eq(t, got[1].state, "failed")
	eq(t, got[2], attemptRow{3, "c", "ready", ""})
	eq(t, e.hedged(t, p.ID), true)
	eq(t, e.inUse(t, "a")+e.inUse(t, "b"), 0)
	eq(t, e.inUse(t, "c"), 1)
	eq(t, e.recountOf(t, "c"), 1)
}

// lateCreate holds a create until its caller leaves, then makes the sandbox anyway and answers with
// an error, as boxd and Koyeb settle a create after the caller left
type lateCreate struct {
	*local.Provider
	destroyed *atomic.Int64
}

func (l lateCreate) Create(ctx context.Context, req provider.CreateRequest) (provider.Ref, error) {
	<-ctx.Done()
	if _, err := l.Provider.Create(context.WithoutCancel(ctx), req); err != nil {
		return provider.Ref{}, err
	}
	return provider.Ref{}, &provider.Error{Code: provider.CodeInternal, Err: ctx.Err()}
}

func (l lateCreate) Destroy(ctx context.Context, ref provider.Ref) error {
	l.destroyed.Add(1)
	return l.Provider.Destroy(ctx, ref)
}

func TestLateLoserCreateIsSwept(t *testing.T) {
	late := lateCreate{
		Provider: newLocal(t, local.Options{Name: "late"}), destroyed: &atomic.Int64{},
	}
	fast := newSlow(t, "fast", 0, false)
	e := hedgingEnv(t, late, fast)
	p, _ := e.placeReady(t, "order-1")
	eq(t, p.Provider, "fast")
	eq(t, e.attempts(t, p.ID)[0], attemptRow{1, "late", "failed", saga.ErrorHedgeLost})
	// The loser's create landed and the placement's sweep destroyed what it made
	eq(t, len(owned(t, late)), 0)
	eq(t, late.destroyed.Load(), int64(1))
	eq(t, e.inUse(t, "late"), 0)
}

func TestNoHedgeAfterAnUnknownOutcome(t *testing.T) {
	var a *local.Provider
	var calls atomic.Int64
	var inner atomic.Bool
	// The first create reaches the provider but its answer is lost
	a = newLocal(t, local.Options{Name: "a", CreateHook: func(
		_ int, req provider.CreateRequest,
	) error {
		if inner.Load() || calls.Add(1) > 1 {
			return nil
		}
		inner.Store(true)
		defer inner.Store(false)
		if _, err := a.Create(context.Background(), req); err != nil {
			return err
		}
		return &provider.Error{Code: provider.CodeOutcomeUnknown, Err: errors.New("timeout")}
	}})
	slowA := slowReady{Provider: a, delay: 700 * time.Millisecond, destroyed: &atomic.Int64{}}
	b := newSlow(t, "b", 0, false)
	e := hedgingEnv(t, slowA, b)
	p, _ := e.placeReady(t, "order-1")
	// A second box beside a create that may still boot and bill never starts
	eq(t, p.Provider, "a")
	eq(t, len(e.attempts(t, p.ID)), 1)
	eq(t, e.hedged(t, p.ID), false)
	eq(t, len(owned(t, b)), 0)
}

func TestNoHedgeWhenTheOtherProviderIsFull(t *testing.T) {
	slow, fast := newSlow(t, "slow", 800*time.Millisecond, false), newSlow(t, "fast", 0, false)
	e := hedgingEnv(t, slow, fast)
	eq(t, e.store.SetCapacity(t.Context(), "fast", 1), nil)
	if ok, err := e.store.ReserveCapacity(t.Context(), "fast", 0); err != nil || !ok {
		t.Fatalf("fill fast = %v, %v", ok, err)
	}
	p, _ := e.placeReady(t, "order-1")
	eq(t, p.Provider, "slow")
	eq(t, len(e.attempts(t, p.ID)), 1)
	eq(t, e.hedged(t, p.ID), false)
}

func TestNoHedgeWithoutAHedger(t *testing.T) {
	slow, fast := newSlow(t, "slow", 500*time.Millisecond, false), newSlow(t, "fast", 0, false)
	e := newEnv(t, slow, fast)
	p, _ := e.placeReady(t, "order-1")
	eq(t, p.Provider, "slow")
	eq(t, len(e.attempts(t, p.ID)), 1)
	eq(t, e.hedged(t, p.ID), false)
}

func TestHedgeWinReleasesTheAwaitedReservation(t *testing.T) {
	h := newHeldReady(newLocal(t, local.Options{Name: "a"}))
	b := newSlow(t, "b", 0, false)
	e := newPoolEnv(t, func(d *saga.Deps) {
		d.Reserve = reserveSettings("a")
		d.HeartbeatInterval = 50 * time.Millisecond
		hs := hedgeSettings()
		hs.DefaultAfter, hs.MinAfter = 200*time.Millisecond, 50*time.Millisecond
		d.Hedger = saga.NewHedger(saga.HedgerDeps{
			Settings: hs, Valkey: d.Valkey, Prefix: d.Prefix, MarginFloor: d.MarginFloor,
		})
	}, h, b)
	p := startAwaiting(t, e.env, h, "q2.h", "order-1", placement.Requirements{})
	res := e.reservation(t, "q2.h")
	// The reservation still prepares, so the hedge starts beside the wait and wins
	start := time.Now()
	for e.reload(t, p.ID).State == store.PlacementPlacing && time.Since(start) < 10*time.Second {
		time.Sleep(20 * time.Millisecond)
	}
	got := e.reload(t, p.ID)
	eq(t, got.State, store.PlacementReady)
	eq(t, got.Provider, "b")
	eq(t, e.hedged(t, p.ID), true)
	eq(t, e.reservation(t, "q2.h").State, store.ReservationReleased)
	eq(t, e.member(t, res.MemberID).State, store.MemberRetiring)
	eq(t, e.retireJobs(t), 1)
	close(h.hold)
	e.saga.Wait()
	eq(t, e.mgr.Retire(t.Context(), res.MemberID, store.EndUnused), nil)
	eq(t, e.member(t, res.MemberID).State, store.MemberGone)
	eq(t, e.inUse(t, "a"), 0)
	eq(t, e.inUse(t, "b"), 1)
	eq(t, e.recountOf(t, "a")+e.recountOf(t, "b"), 1)
}
