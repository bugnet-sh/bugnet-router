package worker_test

import (
	"crypto/rand"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/wallets"
	"github.com/bugnet-sh/bugnet-router/internal/worker"
)

const (
	payerA      = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
	payerB      = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
	payerC      = "DevFaci1itatorAddress1111111111111111111111"
	facilitator = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
)

func payerWallets() config.Wallets {
	return config.Wallets{UsePayer: true, FacilitatorAddresses: []string{facilitator}}
}

// paidPlacement records a placement of payer, ready on a sandbox the provider no longer has
// unless placing is set
func (e *env) paidPlacement(t *testing.T, payer string, placing bool) store.Placement {
	t.Helper()
	ctx := t.Context()
	sha := make([]byte, 32)
	if _, err := rand.Read(sha); err != nil {
		t.Fatal(err)
	}
	p, _, err := e.store.CreatePlacement(ctx, store.NewPlacement{
		HandleSHA256: sha, OrderID: uuid.NewString(), PackageID: smallPkg, Payer: payer,
		FeePayer: facilitator, AmountMicros: smallPrice, SecondsBought: 600,
	})
	if err != nil {
		t.Fatal(err)
	}
	if placing {
		return p
	}
	now := time.Now()
	ref := provider.Ref{Provider: e.local.Name(), ID: "gone-" + p.ID.String()}
	if err := e.store.MarkPlacementReady(ctx, p.ID, ref, small.Name, now,
		now.Add(10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	return e.reload(t, p.ID)
}

type stopLiveJob struct {
	args  jobargs.WalletStopLive
	state rivertype.JobState
	at    time.Time
}

func (e *env) stopLiveJobs(t *testing.T) []stopLiveJob {
	t.Helper()
	res, err := e.river.JobList(t.Context(), river.NewJobListParams().Kinds("wallet_stop_live"))
	if err != nil {
		t.Fatal(err)
	}
	var out []stopLiveJob
	for _, j := range res.Jobs {
		var args jobargs.WalletStopLive
		if err := json.Unmarshal(j.EncodedArgs, &args); err != nil {
			t.Fatal(err)
		}
		out = append(out, stopLiveJob{args: args, state: j.State, at: j.ScheduledAt})
	}
	return out
}

func (e *env) endedFor(t *testing.T, reason string) int {
	t.Helper()
	var n int
	err := e.pool.QueryRow(t.Context(), `select count(*) from placements
		where state = 'terminated' and termination_reason = $1`, reason).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestWalletStopLive(t *testing.T) {
	var logs syncBuffer
	e := newEnv(t, options{wallets: payerWallets(), log: &logs})
	ctx := t.Context()
	var ready []store.Placement
	for range 7 {
		ready = append(ready, e.paidPlacement(t, payerA, false))
	}
	placing := e.paidPlacement(t, payerA, true)
	// Payer A paid an extension of payer B's placement, so it ends too
	other := e.paidPlacement(t, payerB, false)
	if _, _, err := e.store.ExtendWithOrder(ctx, store.Order{
		ID: "ext-1", PlacementID: other.ID, Payer: payerA, FeePayer: facilitator,
		AmountMicros: morePrice, SecondsBought: 600,
	}, 10*time.Minute); err != nil {
		t.Fatal(err)
	}
	untouched := e.paidPlacement(t, payerB, false)

	// One run ends five and inserts itself again at once
	eq(t, e.worker.StopLive(ctx, jobargs.WalletStopLive{Payer: payerA}), nil)
	eq(t, e.endedFor(t, saga.ReasonWalletBlocked), 5)
	jobs := e.stopLiveJobs(t)
	eq(t, len(jobs), 1)
	eq(t, jobs[0].args, jobargs.WalletStopLive{Payer: payerA})
	eq(t, jobs[0].state, rivertype.JobStateAvailable)

	// The next ends the rest and waits 15 s for the one still placing
	eq(t, e.worker.StopLive(ctx, jobs[0].args), nil)
	eq(t, e.endedFor(t, saga.ReasonWalletBlocked), 8)
	for _, p := range append(ready, other) {
		got := e.reload(t, p.ID)
		eq(t, []string{got.State, got.TerminationReason, got.EndActor},
			[]string{store.PlacementTerminated, saga.ReasonWalletBlocked, wallets.ActorRouter})
		eq(t, got.Refundable, false)
	}
	eq(t, e.reload(t, placing.ID).State, store.PlacementPlacing)
	eq(t, e.reload(t, untouched.ID).State, store.PlacementReady)
	jobs = e.stopLiveJobs(t)
	eq(t, len(jobs), 2)
	eq(t, jobs[1].args, jobargs.WalletStopLive{Payer: payerA, Waits: 1})
	eq(t, jobs[1].state, rivertype.JobStateScheduled)
	wait := time.Until(jobs[1].at)
	eq(t, wait > 10*time.Second && wait <= 15*time.Second, true)

	// After twenty waits the placing one is left, with an error in the log
	eq(t, e.worker.StopLive(ctx, jobargs.WalletStopLive{Payer: payerA, Waits: 20}), nil)
	eq(t, len(e.stopLiveJobs(t)), 2)
	eq(t, strings.Contains(logs.String(), "still placing"), true)
	eq(t, worker.StopLiveTimeout(), 150*time.Second)
}

func TestWalletStopLiveRefundsUnderTheSetting(t *testing.T) {
	w := payerWallets()
	w.RefundStoppedLive = true
	e := newEnv(t, options{wallets: w})
	p := e.paidPlacement(t, payerA, false)
	eq(t, e.worker.StopLive(t.Context(), jobargs.WalletStopLive{Payer: payerA}), nil)
	got := e.reload(t, p.ID)
	eq(t, got.Refundable, true)
	evs := e.pendingEvents(t, saga.EventTerminated)
	eq(t, len(evs), 1)
	eq(t, len(evs[0].Refunds), 1)
}

func TestWalletStopLiveJobRuns(t *testing.T) {
	e := newEnv(t, options{wallets: payerWallets()})
	e.start(t)
	p := e.paidPlacement(t, payerA, false)
	e.run(t, jobargs.WalletStopLive{Payer: payerA})
	eq(t, e.reload(t, p.ID).TerminationReason, saga.ReasonWalletBlocked)
}

func TestWalletExpiryLiftsTimedBlock(t *testing.T) {
	w := payerWallets()
	w.EmitEvents = true
	e := newEnv(t, options{wallets: w})
	ctx := t.Context()
	past, future := time.Now().Add(-time.Minute), time.Now().Add(time.Hour)
	for payer, until := range map[string]*time.Time{payerA: &past, payerB: &future,
		payerC: nil} {
		if err := e.store.SetWalletBlock(ctx, payer, time.Now().Add(-time.Hour), until,
			wallets.ReasonStrikes, wallets.ActorRouter); err != nil {
			t.Fatal(err)
		}
	}
	e.start(t)
	e.run(t, jobargs.WalletExpiry{})

	a, err := e.store.Wallet(ctx, payerA, time.Now())
	eq(t, err, nil)
	eq(t, a.BlockedAt == nil && a.BlockedUntil == nil && a.BlockedReason == "", true)
	evs, err := e.store.WalletEvents(ctx, payerA, 10)
	eq(t, err, nil)
	eq(t, len(evs), 1)
	eq(t, []string{evs[0].Kind, evs[0].Reason, evs[0].Actor},
		[]string{store.WalletUnblock, wallets.ReasonExpired, wallets.ActorRouter})
	b, err := e.store.Wallet(ctx, payerB, time.Now())
	eq(t, err, nil)
	eq(t, b.Blocked(time.Now()), true)
	permanent, err := e.store.Wallet(ctx, payerC, time.Now())
	eq(t, err, nil)
	eq(t, permanent.Blocked(time.Now()), true)

	var unblocked []string
	pending, err := e.store.PendingOutbox(ctx, time.Now(), 100)
	eq(t, err, nil)
	for _, ev := range pending {
		if ev.EventType == wallets.EventUnblocked {
			unblocked = append(unblocked, string(ev.Payload))
		}
	}
	eq(t, len(unblocked), 1)
	eq(t, strings.Contains(unblocked[0], payerA), true)

	// Each later run finds nothing to lift
	e.run(t, jobargs.WalletExpiry{})
	evs, err = e.store.WalletEvents(ctx, payerA, 10)
	eq(t, err, nil)
	eq(t, len(evs), 1)
	eq(t, worker.ExpiryTimeout(), 30*time.Second)
}

func TestWalletExpiryRunsEveryTenMinutes(t *testing.T) {
	eq(t, worker.Periodic(), map[string]time.Duration{
		"sweep": time.Minute, "watch": 30 * time.Second, "outbox": time.Second,
		"wallet_expiry": 10 * time.Minute,
	})
}

func TestDefaultQueueHasTwentyWorkers(t *testing.T) {
	q := worker.Queues(worker.Deps{})
	eq(t, q[river.QueueDefault].MaxWorkers, 20)
	eq(t, worker.Queues(worker.Deps{DefaultWorkers: 3})[river.QueueDefault].MaxWorkers, 3)
	e := newEnv(t, options{})
	eq(t, e.worker.Workers(), 28)
	e = newEnv(t, options{defaultWorkers: 5, dataplaneWorkers: 2})
	eq(t, e.worker.Workers(), 7)
}
