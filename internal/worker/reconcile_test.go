package worker_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/reconcile"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/worker"
)

// inspecting is a local provider whose readings and listing states the test sets, named as the
// provider whose rows are under test
type inspecting struct {
	*local.Provider
	mu        sync.Mutex
	status    map[string]provider.Status // by sandbox id, the real reading when absent
	states    map[string]provider.State  // by sandbox id, the state a listing shows
	err       error
	inspects  int
	lifetimes []time.Time
}

func newInspecting(t *testing.T, name string) *inspecting {
	t.Helper()
	return &inspecting{
		Provider: newLocal(t, name), status: map[string]provider.Status{},
		states: map[string]provider.State{},
	}
}

func (i *inspecting) Inspect(ctx context.Context, ref provider.Ref) (provider.Status, error) {
	i.mu.Lock()
	st, ok := i.status[ref.ID]
	err := i.err
	i.inspects++
	i.mu.Unlock()
	if err != nil {
		return provider.Status{}, err
	}
	if ok {
		return st, nil
	}
	return i.Provider.Inspect(ctx, ref)
}

func (i *inspecting) ListOwned(
	ctx context.Context, l map[string]string,
) ([]provider.Summary, error) {
	owned, err := i.Provider.ListOwned(ctx, l)
	i.mu.Lock()
	defer i.mu.Unlock()
	for n := range owned {
		owned[n].State = i.states[owned[n].Ref.ID]
	}
	return owned, err
}

func (i *inspecting) SetLifetime(ctx context.Context, ref provider.Ref, until time.Time) error {
	i.mu.Lock()
	i.lifetimes = append(i.lifetimes, until)
	i.mu.Unlock()
	return i.Provider.SetLifetime(ctx, ref, until)
}

func (i *inspecting) set(id string, st provider.Status) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.status[id] = st
}

func (i *inspecting) setErr(err error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.err = err
}

func (i *inspecting) counts() (int, []time.Time) {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.inspects, append([]time.Time(nil), i.lifetimes...)
}

// reconcileJobs counts the reconcile jobs inserted so far
func (e *env) reconcileJobs(t *testing.T) int {
	t.Helper()
	var n int
	err := e.pool.QueryRow(t.Context(),
		`select count(*) from river_job where kind = 'reconcile'`).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// signals returns the placement's provider signals
func (e *env) signals(t *testing.T, id uuid.UUID) []store.Signal {
	t.Helper()
	sg, err := e.store.Signals(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	return sg
}

func (e *env) blocked(t *testing.T, payer string) bool {
	t.Helper()
	w, err := e.store.Wallet(t.Context(), payer, time.Now())
	if errors.Is(err, store.ErrNotFound) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	return w.Blocked(time.Now())
}

// aMinuteLater lets the sweep's ready grace pass for placements made a moment ago
func aMinuteLater() *clock {
	c := &clock{}
	c.Advance(time.Minute)
	return c
}

func TestLostPlacementEndsProviderFailed(t *testing.T) {
	e := newEnv(t, options{workerClock: aMinuteLater()})
	p := e.paidPlacement(t, payerA, false)
	e.start(t)

	// The listing lacks the sandbox, so the sweep inserts a job and Inspect decides there
	e.run(t, jobargs.Sweep{})
	e.finished(t, "reconcile")

	got := e.reload(t, p.ID)
	eq(t, got.State, store.PlacementTerminated)
	eq(t, got.TerminationReason, saga.ReasonProviderFailed)
	eq(t, got.Refundable, true)
	evs := e.pendingEvents(t, saga.EventTerminated)
	eq(t, len(evs), 1)
	if evs[0].UnusedSeconds == nil || *evs[0].UnusedSeconds < 500 || *evs[0].UnusedSeconds > 600 {
		t.Fatalf("terminated event %+v, want about 600 unused seconds", evs[0])
	}
	if len(evs[0].Refunds) != 1 || evs[0].Refunds[0].RefundMicros <= 0 {
		t.Fatalf("refunds %+v, want one priced entry", evs[0].Refunds)
	}
	sg := e.signals(t, p.ID)
	eq(t, len(sg), 1)
	eq(t, []string{sg[0].Class, sg[0].Action, sg[0].Source},
		[]string{"provider_failed", "ended", "sweep"})
}

func TestFiveHundredMissingInsertTwoHundred(t *testing.T) {
	e := newEnv(t, options{workerClock: aMinuteLater()})
	for range 500 {
		e.paidPlacement(t, payerA, false)
	}
	start := time.Now()
	e.worker.SweepAll(t.Context())
	eq(t, e.reconcileJobs(t), 200)
	if took := time.Since(start); took > 30*time.Second {
		t.Fatalf("the sweep took %v with 500 lost placements", took)
	}
}

func TestIncompleteListingDoesNothing(t *testing.T) {
	prov := &flaky{Provider: newLocal(t, "local"), listFails: true}
	e := newEnv(t, options{
		workerClock: aMinuteLater(), providers: []provider.Provider{prov},
	})
	e.paidPlacement(t, payerA, false)
	e.worker.SweepAll(t.Context())
	eq(t, e.reconcileJobs(t), 0)
}

func TestFreshPlacementIsNotLost(t *testing.T) {
	e := newEnv(t, options{})
	e.paidPlacement(t, payerA, false)
	e.worker.SweepAll(t.Context())
	eq(t, e.reconcileJobs(t), 0)
}

func TestListedUnwellStateInsertsAJob(t *testing.T) {
	prov := newInspecting(t, "local")
	e := newEnv(t, options{workerClock: aMinuteLater(), providers: []provider.Provider{prov}})
	now := time.Now()
	_, ref := e.readyPlacement(t, prov, now, now.Add(time.Hour))
	e.readyPlacement(t, prov, now, now.Add(time.Hour))
	prov.states[ref.ID] = provider.StateDegraded
	e.worker.SweepAll(t.Context())
	eq(t, e.reconcileJobs(t), 1)
}

func TestInspectErrorNeverEndsAPlacement(t *testing.T) {
	prov := newInspecting(t, "local")
	e := newEnv(t, options{providers: []provider.Provider{prov}})
	now := time.Now()
	p, _ := e.readyPlacement(t, prov, now, now.Add(time.Hour))
	prov.setErr(&provider.Error{Code: provider.CodeInternal, Err: errors.New("api down")})
	if err := e.worker.Reconciler().Reconcile(t.Context(), p, nil); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	eq(t, e.reload(t, p.ID).State, store.PlacementReady)
	eq(t, len(e.signals(t, p.ID)), 0)
}

func TestDegradedSpellNeedsTheGraceAndAHealthyReadingClearsIt(t *testing.T) {
	prov := newInspecting(t, "boat")
	e := newEnv(t, options{providers: []provider.Provider{prov}})
	now := time.Now()
	p, ref := e.readyPlacement(t, prov, now, now.Add(time.Hour))
	rc := e.worker.Reconciler()
	prov.set(ref.ID, provider.Status{State: provider.StateDegraded, Reason: "tunnel"})

	if err := rc.Reconcile(t.Context(), p, nil); err != nil {
		t.Fatal(err)
	}
	got := e.reload(t, p.ID)
	eq(t, got.State, store.PlacementReady)
	if got.DegradedSince == nil {
		t.Fatal("degraded_since stayed empty")
	}
	// A re-check waits for the end of the grace
	eq(t, e.reconcileJobs(t), 1)

	// A healthy reading clears the spell
	prov.set(ref.ID, provider.Status{State: provider.StateRunning})
	if err := rc.Reconcile(t.Context(), got, nil); err != nil {
		t.Fatal(err)
	}
	if e.reload(t, p.ID).DegradedSince != nil {
		t.Fatal("a healthy reading left degraded_since set")
	}

	// A spell past the grace gives the setting's class, a strike for control_lost
	prov.set(ref.ID, provider.Status{State: provider.StateDegraded, Reason: "tunnel"})
	spell := time.Now().Add(-10 * time.Minute)
	if _, err := e.pool.Exec(t.Context(),
		`update placements set degraded_since = $2 where id = $1`, p.ID, spell); err != nil {
		t.Fatal(err)
	}
	if err := rc.Reconcile(t.Context(), e.reload(t, p.ID), nil); err != nil {
		t.Fatal(err)
	}
	eq(t, e.reload(t, p.ID).TerminationReason, saga.ReasonControlLost)
}

func TestHintForAnotherDeploymentDropped(t *testing.T) {
	prov := newInspecting(t, "e2b")
	e := newEnv(t, options{providers: []provider.Provider{prov}})
	now := time.Now()
	p, ref := e.readyPlacement(t, prov, now, now.Add(time.Hour))
	prov.set(ref.ID, provider.Status{State: provider.StateGone, KillReason: "admin"})

	other := jobargs.HookHint{
		Provider: "e2b", SandboxID: ref.ID, EventID: "evt-1", PlacementLabel: p.ID.String(),
		DeploymentLabel: "dev",
	}
	if err := e.worker.HookHintWork(t.Context(), other); err != nil {
		t.Fatal(err)
	}
	n, _ := prov.counts()
	eq(t, n, 0)
	eq(t, e.reload(t, p.ID).State, store.PlacementReady)

	// No placement for the sandbox drops the hint too
	unknown := jobargs.HookHint{Provider: "e2b", SandboxID: "nobody", EventID: "evt-2"}
	if err := e.worker.HookHintWork(t.Context(), unknown); err != nil {
		t.Fatal(err)
	}
	n, _ = prov.counts()
	eq(t, n, 0)

	// The same hint for this deployment acts, finding the placement by its label
	mine := other
	mine.DeploymentLabel = e.deployment
	if err := e.worker.HookHintWork(t.Context(), mine); err != nil {
		t.Fatal(err)
	}
	eq(t, e.reload(t, p.ID).TerminationReason, saga.ReasonAbuse)
}

func TestHintFindsThePlacementBySandbox(t *testing.T) {
	prov := newInspecting(t, "boat")
	e := newEnv(t, options{providers: []provider.Provider{prov}})
	now := time.Now()
	p, ref := e.readyPlacement(t, prov, now, now.Add(time.Hour))
	prov.set(ref.ID, provider.Status{State: provider.StateGone})
	hint := jobargs.HookHint{Provider: "boat", SandboxID: ref.ID, EventID: "evt-9"}
	if err := e.worker.HookHintWork(t.Context(), hint); err != nil {
		t.Fatal(err)
	}
	got := e.reload(t, p.ID)
	eq(t, got.TerminationReason, saga.ReasonProviderFailed)
	sg := e.signals(t, p.ID)
	eq(t, []string{sg[0].Source}, []string{"hook"})
}

func TestLateAbuseHintBlocksThePayer(t *testing.T) {
	prov := newInspecting(t, "e2b")
	e := newEnv(t, options{providers: []provider.Provider{prov}})
	ctx := t.Context()
	p := e.paidPlacement(t, payerA, false)
	hourAgo := time.Now().Add(-time.Hour)
	if _, err := e.store.MarkPlacementTerminating(ctx, p.ID, "stop", hourAgo); err != nil {
		t.Fatal(err)
	}
	if err := e.store.MarkPlacementTerminated(ctx, p.ID, hourAgo); err != nil {
		t.Fatal(err)
	}
	prov.set(p.ProviderRef.ID, provider.Status{State: provider.StateGone, KillReason: "admin"})

	hint := jobargs.HookHint{Provider: "e2b", SandboxID: p.ProviderRef.ID, EventID: "evt-3"}
	if err := e.worker.HookHintWork(ctx, hint); err != nil {
		t.Fatal(err)
	}
	got := e.reload(t, p.ID)
	eq(t, got.TerminationReason, "stop")
	eq(t, got.Refundable, false)
	eq(t, e.blocked(t, payerA), true)
	sg := e.signals(t, p.ID)
	eq(t, []string{sg[len(sg)-1].Class, sg[len(sg)-1].Action}, []string{"abuse", "flagged"})

	// Any other verdict on an ended placement is dropped, and so is an older placement
	prov.set(p.ProviderRef.ID, provider.Status{State: provider.StateGone})
	q := e.paidPlacement(t, payerB, false)
	if _, err := e.store.MarkPlacementTerminating(ctx, q.ID, "stop", hourAgo); err != nil {
		t.Fatal(err)
	}
	if err := e.store.MarkPlacementTerminated(ctx, q.ID, hourAgo); err != nil {
		t.Fatal(err)
	}
	prov.set(q.ProviderRef.ID, provider.Status{State: provider.StateGone})
	if err := e.worker.HookHintWork(ctx,
		jobargs.HookHint{Provider: "e2b", SandboxID: q.ProviderRef.ID}); err != nil {
		t.Fatal(err)
	}
	eq(t, e.reload(t, q.ID).TerminationReason, "stop")
	eq(t, e.blocked(t, payerB), false)
}

func TestExtensionAndDriftRace(t *testing.T) {
	prov := newInspecting(t, "e2b")
	e := newEnv(t, options{providers: []provider.Provider{prov}})
	ctx := t.Context()
	now := time.Now()
	p, ref := e.readyPlacement(t, prov, now, now.Add(10*time.Minute))
	rc := e.worker.Reconciler()

	// The listing was taken after an extension's SetLifetime, before its transaction committed
	later := p.Deadline.Add(10*time.Minute + saga.LifetimeSlack)
	reading := &reconcile.Observation{
		Status: provider.Status{State: provider.StateRunning, ExpiresAt: later}, Source: "sweep",
	}
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.Tx(tx).PlacementForUpdate(ctx, p.ID); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- rc.Reconcile(ctx, p, reading) }()
	select {
	case err := <-done:
		t.Fatalf("Reconcile finished (%v) while an extension held the row", err)
	case <-time.After(300 * time.Millisecond):
	}
	if _, _, err := e.store.Tx(tx).ExtendDeadline(ctx, uuid.NewString(), p.ID,
		10*time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// The lifetime stays where the extension put it
	_, lifetimes := prov.counts()
	eq(t, len(lifetimes), 0)
	grown := e.reload(t, p.ID)
	if got := *grown.Deadline; got.Sub(*p.Deadline) < 9*time.Minute {
		t.Fatalf("deadline %v, want the extended one", got)
	}

	// A lifetime ending before the paid time is set to the deadline plus the slack
	early := &reconcile.Observation{
		Status: provider.Status{
			State: provider.StateRunning, ExpiresAt: grown.Deadline.Add(-5 * time.Minute),
		},
		Source: "sweep",
	}
	if err := rc.Reconcile(ctx, grown, early); err != nil {
		t.Fatal(err)
	}
	_, lifetimes = prov.counts()
	eq(t, len(lifetimes), 1)
	if d := lifetimes[0].Sub(grown.Deadline.Add(saga.LifetimeSlack)); d != 0 {
		t.Fatalf("lifetime set %v off the deadline plus slack", d)
	}

	// At most one fix every ten minutes
	if err := rc.Reconcile(ctx, grown, early); err != nil {
		t.Fatal(err)
	}
	_, lifetimes = prov.counts()
	eq(t, len(lifetimes), 1)
	_ = ref
}

func TestDriftWithinTheBoundChangesNothing(t *testing.T) {
	prov := newInspecting(t, "e2b")
	e := newEnv(t, options{
		providers: []provider.Provider{prov},
		reconcile: config.Reconcile{ReadyGrace: 30 * time.Second, MaxPerSweep: 200,
			LifetimeDrift: 2 * time.Minute},
	})
	now := time.Now()
	p, _ := e.readyPlacement(t, prov, now, now.Add(10*time.Minute))
	for _, off := range []time.Duration{-90 * time.Second, 90 * time.Second, 0} {
		o := &reconcile.Observation{Status: provider.Status{
			State: provider.StateRunning, ExpiresAt: p.Deadline.Add(saga.LifetimeSlack + off),
		}}
		if err := e.worker.Reconciler().Reconcile(t.Context(), p, o); err != nil {
			t.Fatal(err)
		}
	}
	_, lifetimes := prov.counts()
	eq(t, len(lifetimes), 0)

	// Later than expected by more than the bound is brought down
	o := &reconcile.Observation{Status: provider.Status{
		State: provider.StateRunning, ExpiresAt: p.Deadline.Add(saga.LifetimeSlack + time.Hour),
	}}
	if err := e.worker.Reconciler().Reconcile(t.Context(), p, o); err != nil {
		t.Fatal(err)
	}
	_, lifetimes = prov.counts()
	eq(t, len(lifetimes), 1)
}

func TestReconcileJobTimeoutIsNinetySeconds(t *testing.T) {
	eq(t, worker.ReconcileTimeout(), 90*time.Second)
}
