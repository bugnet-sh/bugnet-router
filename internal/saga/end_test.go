package saga_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/google/uuid"
	"github.com/riverqueue/river"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/handle"
	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/ordertoken"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/wallets"
)

// endEnv is walletEnv on the clock given, nil for wall time, on prov or a fresh local provider
func endEnv(
	t *testing.T, adjust func(*config.Wallets), clk *clock, prov provider.Provider,
) *env {
	t.Helper()
	w := config.Wallets{UsePayer: true, FacilitatorAddresses: []string{facilitator}}
	if adjust != nil {
		adjust(&w)
	}
	if prov == nil {
		prov = newLocal(t, local.Options{Name: "a"})
	}
	return newEnvWith(t, func(d *saga.Deps) {
		d.Wallets = w
		if clk != nil {
			d.Now = clk.Now
		}
	}, prov)
}

// extendPaid extends a placement by pkg, paid by payer
func (e *env) extendPaid(t *testing.T, h, order, payer, pkg string, price int64) {
	t.Helper()
	sha, err := handle.Hash(h)
	if err != nil {
		t.Fatal(err)
	}
	tok := e.token(t, ordertoken.Claims{
		OrderID: order, Kind: ordertoken.KindExtend, PackageID: pkg, AmountMicros: price,
		HandleSHA256: hex.EncodeToString(sha), Payer: payer, FeePayer: facilitator,
	})
	if _, err := e.saga.Extend(t.Context(), h, tok); err != nil {
		t.Fatalf("extend %s: %v", order, err)
	}
}

func (e *env) end(t *testing.T, id uuid.UUID, en saga.End) store.Placement {
	t.Helper()
	p, err := e.saga.End(t.Context(), id, en)
	if err != nil {
		t.Fatalf("end %s for %s: %v", id, en.Reason, err)
	}
	return p
}

// walletEvents counts a payer's wallet events of one kind
func (e *env) walletEvents(t *testing.T, payer, kind string) int {
	t.Helper()
	var n int
	err := e.pool.QueryRow(t.Context(),
		`select count(*) from wallet_events where payer = $1 and kind = $2`, payer, kind).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func (e *env) blocked(t *testing.T, payer string) store.Wallet {
	t.Helper()
	w, err := e.store.Wallet(t.Context(), payer, time.Now().Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("wallet %s: %v", payer, err)
	}
	return w
}

// stopLiveJobs returns the payers of the wallet_stop_live jobs inserted
func (e *env) stopLiveJobs(t *testing.T) []string {
	t.Helper()
	res, err := e.river.JobList(t.Context(), river.NewJobListParams().Kinds("wallet_stop_live"))
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, j := range res.Jobs {
		var args jobargs.WalletStopLive
		if err := json.Unmarshal(j.EncodedArgs, &args); err != nil {
			t.Fatal(err)
		}
		out = append(out, args.Payer)
	}
	slices.Sort(out)
	return out
}

// terminated returns the placement.terminated event of a placement and its raw payload
func (e *env) terminated(t *testing.T, id uuid.UUID) (saga.Event, string) {
	t.Helper()
	pending, err := e.store.PendingOutbox(t.Context(), time.Now().Add(48*time.Hour), 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range pending {
		var x saga.Event
		if err := json.Unmarshal(ev.Payload, &x); err != nil {
			t.Fatal(err)
		}
		if ev.EventType == saga.EventTerminated && x.PlacementID == id {
			return x, string(ev.Payload)
		}
	}
	t.Fatalf("no placement.terminated for %s", id)
	return saga.Event{}, ""
}

// fields are the top-level fields of a JSON object, each as compact JSON
func fields(t *testing.T, raw string) map[string]string {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for k, v := range m {
		out[k] = strings.ReplaceAll(string(v), " ", "")
	}
	return out
}

func (e *env) eventCount(t *testing.T, eventType string) int {
	t.Helper()
	var n int
	err := e.pool.QueryRow(t.Context(), `select count(*) from outbox where event_type = $1`,
		eventType).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestEndBlocksEveryPayerOnce(t *testing.T) {
	e := endEnv(t, func(w *config.Wallets) {
		w.EmitEvents, w.StopLiveOnAbuse = true, true
	}, nil, nil)
	p, h := e.placePaid(t, "order-1", payerA)
	e.extendPaid(t, h, "ext-1", payerB, morePkg, morePrice)
	e.extendPaid(t, h, "ext-2", payerB, shortPkg, shortPrice)
	e.extendPaid(t, h, "ext-3", payerA, shortPkg, shortPrice)

	ended := e.end(t, p.ID, saga.End{
		Reason: saga.ReasonAbuse, Actor: wallets.ActorRouter,
		Evidence: json.RawMessage(`{"source":"hook","event_id":"ev-1"}`),
	})
	eq(t, ended.State, store.PlacementTerminated)
	eq(t, ended.TerminationReason, saga.ReasonAbuse)
	eq(t, ended.Refundable, false)
	eq(t, ended.EndActor, wallets.ActorRouter)
	for _, payer := range []string{payerA, payerB} {
		eq(t, e.walletEvents(t, payer, store.WalletBlock), 1)
		w := e.blocked(t, payer)
		eq(t, w.Blocked(time.Now()), true)
		eq(t, w.BlockedReason, wallets.ReasonAbuse)
		eq(t, w.BlockedUntil == nil, true)
	}
	eq(t, e.eventCount(t, wallets.EventBlocked), 2)
	eq(t, e.stopLiveJobs(t), []string{payerB, payerA})
	sigs, err := e.store.Signals(t.Context(), p.ID)
	eq(t, err, nil)
	eq(t, len(sigs), 1)
	eq(t, []string{sigs[0].Class, sigs[0].Source, sigs[0].Action, sigs[0].Provider},
		[]string{"abuse", "hook", "blocked", "a"})

	ev, _ := e.terminated(t, p.ID)
	eq(t, ev.Refundable, false)
	eq(t, *ev.UnusedSeconds, int64(0))
	eq(t, ev.Refunds, []saga.Refund{})
	var ends []routerv1.EndReason
	for _, ee := range e.edgeEvents(t) {
		if pe := ee.GetPlacementEnded(); pe != nil && pe.GetPlacementId() == p.ID.String() {
			ends = append(ends, pe.GetReason())
		}
	}
	eq(t, ends, []routerv1.EndReason{routerv1.EndReason_END_REASON_ABUSE})
}

func TestEndAbuseBlockForAndNoStopLive(t *testing.T) {
	e := endEnv(t, func(w *config.Wallets) { w.AbuseBlockFor = 720 * time.Hour }, nil, nil)
	p, _ := e.placePaid(t, "order-1", payerA)
	e.end(t, p.ID, saga.End{Reason: saga.ReasonAbuse, Actor: "operator:ann"})
	w := e.blocked(t, payerA)
	eq(t, w.BlockedUntil != nil && near(*w.BlockedUntil, time.Now().Add(720*time.Hour)), true)
	eq(t, len(e.stopLiveJobs(t)), 0)
	sigs, err := e.store.Signals(t.Context(), p.ID)
	eq(t, err, nil)
	eq(t, sigs[0].Source, "operator")
	// The next order of the payer is refused
	_, err = e.createPaid(t, "order-2", payerA)
	eq(t, errors.Is(err, saga.ErrWalletBlocked), true)
}

func TestAbuseReplacesAStopInFlight(t *testing.T) {
	e := endEnv(t, nil, nil, nil)
	ctx := t.Context()
	stopped, _ := e.placePaid(t, "order-1", payerA)
	if _, err := e.store.MarkPlacementTerminating(ctx, stopped.ID, saga.ReasonStop,
		time.Now()); err != nil {
		t.Fatal(err)
	}
	p := e.end(t, stopped.ID, saga.End{Reason: saga.ReasonAbuse, Actor: wallets.ActorRouter})
	eq(t, p.State, store.PlacementTerminated)
	eq(t, p.TerminationReason, saga.ReasonAbuse)
	eq(t, e.blocked(t, payerA).Blocked(time.Now()), true)

	// Any other reason leaves a stop in flight as it is, says so, and takes no wallet action
	other, _ := e.placePaid(t, "order-2", payerB)
	if _, err := e.store.MarkPlacementTerminating(ctx, other.ID, saga.ReasonStop,
		time.Now()); err != nil {
		t.Fatal(err)
	}
	p, err := e.saga.End(ctx, other.ID, saga.End{
		Reason: saga.ReasonControlLost, Actor: wallets.ActorRouter,
	})
	eq(t, errors.Is(err, saga.ErrAlreadyEnded), true)
	eq(t, p.State, store.PlacementTerminated)
	eq(t, p.TerminationReason, saga.ReasonStop)
	eq(t, e.walletEvents(t, payerB, store.WalletStrike), 0)
}

func TestFlagEndedKeepsReasonAndMoney(t *testing.T) {
	e := endEnv(t, func(w *config.Wallets) { w.StopLiveOnAbuse = true }, nil, nil)
	ctx := t.Context()
	p, h := e.placePaid(t, "order-1", payerA)
	e.extendPaid(t, h, "ext-1", payerB, morePkg, morePrice)
	if _, err := e.saga.Stop(ctx, h); err != nil {
		t.Fatal(err)
	}
	before, raw := e.terminated(t, p.ID)

	ev := json.RawMessage(`{"source":"hook","event_id":"late"}`)
	flagged, blocked, err := e.saga.FlagEnded(ctx, p.ID, saga.End{
		Reason: saga.ReasonAbuse, Actor: wallets.ActorRouter, Evidence: ev,
	})
	eq(t, err, nil)
	eq(t, blocked, []string{payerB, payerA})
	eq(t, flagged.TerminationReason, saga.ReasonStop)
	eq(t, flagged.Refundable, false)
	eq(t, e.eventCount(t, saga.EventTerminated), 1)
	rec, err := e.store.SessionRecordByPlacement(ctx, p.ID)
	eq(t, err, nil)
	eq(t, rec.ClosedBy, store.ClosedByAbuse)
	eq(t, rec.ClosedAt != nil, true)
	eq(t, rec.HoldUntil != nil && rec.HoldUntil.After(time.Now().Add(24*time.Hour)), true)
	after, rawAfter := e.terminated(t, p.ID)
	eq(t, after, before)
	eq(t, rawAfter, raw)
	eq(t, e.blocked(t, payerA).Blocked(time.Now()), true)
	eq(t, e.stopLiveJobs(t), []string{payerB, payerA})

	// A second report adds evidence and blocks nothing new
	_, blocked, err = e.saga.FlagEnded(ctx, p.ID, saga.End{
		Reason: saga.ReasonAbuse, Actor: wallets.ActorRouter, Evidence: ev,
	})
	eq(t, err, nil)
	eq(t, len(blocked), 0)
	eq(t, e.walletEvents(t, payerA, store.WalletBlock), 1)
	eq(t, e.walletEvents(t, payerA, store.WalletNote), 1)
	sigs, err := e.store.Signals(ctx, p.ID)
	eq(t, err, nil)
	eq(t, len(sigs), 2)
	eq(t, sigs[0].Action, "flagged")

	live, _ := e.placePaid(t, "order-2", payerC)
	_, _, err = e.saga.FlagEnded(ctx, live.ID, saga.End{Reason: saga.ReasonAbuse})
	eq(t, errors.Is(err, saga.ErrNotEnded), true)
	_, _, err = e.saga.FlagEnded(ctx, p.ID, saga.End{Reason: saga.ReasonControlLost})
	eq(t, errors.Is(err, saga.ErrBadReason), true)
}

func TestStrikeThreshold(t *testing.T) {
	clk := &clock{}
	e := endEnv(t, nil, clk, nil)
	var ps [3]store.Placement
	for i := range ps {
		ps[i], _ = e.placePaid(t, "order-"+string(rune('a'+i)), payerA)
	}
	lost := saga.End{Reason: saga.ReasonControlLost, Actor: wallets.ActorRouter}
	e.end(t, ps[0].ID, lost)
	eq(t, e.walletEvents(t, payerA, store.WalletStrike), 1)
	eq(t, e.blocked(t, payerA).Blocked(clk.Now()), false)

	// The first strike is outside the window by now
	clk.Advance(25 * time.Hour)
	e.end(t, ps[1].ID, lost)
	eq(t, e.blocked(t, payerA).Blocked(clk.Now()), false)

	// The second strike inside 24 h blocks
	e.end(t, ps[2].ID, lost)
	w := e.blocked(t, payerA)
	eq(t, w.Blocked(clk.Now()), true)
	eq(t, w.BlockedReason, wallets.ReasonStrikes)
	sigs, err := e.store.Signals(t.Context(), ps[2].ID)
	eq(t, err, nil)
	eq(t, sigs[0].Action, "struck")
}

func TestDailyRefundCap(t *testing.T) {
	e := endEnv(t, nil, nil, nil)
	var ps [4]store.Placement
	for i := range ps {
		ps[i], _ = e.placePaid(t, "order-"+string(rune('a'+i)), payerA)
	}
	failed := saga.End{Reason: saga.ReasonProviderFailed, Actor: wallets.ActorRouter}
	for i := range 2 {
		p := e.end(t, ps[i].ID, failed)
		eq(t, p.Refundable, true)
		ev, _ := e.terminated(t, p.ID)
		eq(t, len(ev.Refunds), 1)
	}
	eq(t, e.walletEvents(t, payerA, store.WalletStrike), 0)

	// The third in a day refunds nothing and strikes the payer
	p := e.end(t, ps[2].ID, failed)
	eq(t, p.Refundable, false)
	ev, _ := e.terminated(t, p.ID)
	eq(t, *ev.UnusedSeconds, int64(0))
	eq(t, ev.Refunds, []saga.Refund{})
	eq(t, e.walletEvents(t, payerA, store.WalletStrike), 1)

	// An end the correlation guard made refunds outside the cap and takes no wallet action
	held := failed
	held.Held = true
	held.Evidence = json.RawMessage(`{"source":"sweep","held_class":"control_lost"}`)
	p = e.end(t, ps[3].ID, held)
	eq(t, p.Refundable, true)
	eq(t, strings.Contains(string(p.EndEvidence), `"correlated": true`) ||
		strings.Contains(string(p.EndEvidence), `"correlated":true`), true)
	eq(t, e.walletEvents(t, payerA, store.WalletStrike), 1)
	sigs, err := e.store.Signals(t.Context(), p.ID)
	eq(t, err, nil)
	eq(t, []string{sigs[0].Action, sigs[0].HeldClass}, []string{"held", "control_lost"})
	n, err := e.store.FailedRefunds(t.Context(), payerA, time.Now().Add(-time.Hour))
	eq(t, err, nil)
	eq(t, n, 2)
}

func TestAnonymousRefundCapCountsTogether(t *testing.T) {
	e := endEnv(t, func(w *config.Wallets) { w.AnonymousFailedRefundsPerDay = 1 }, nil, nil)
	first, _ := e.placeReady(t, "order-1")
	second, _ := e.placeReady(t, "order-2")
	failed := saga.End{Reason: saga.ReasonProviderFailed, Actor: wallets.ActorRouter}
	eq(t, e.end(t, first.ID, failed).Refundable, true)
	eq(t, e.end(t, second.ID, failed).Refundable, false)
}

func TestTerminatedCarriesUnusedSeconds(t *testing.T) {
	clk := &clock{}
	e := endEnv(t, nil, clk, advancingDestroy{newLocal(t, local.Options{Name: "a"}), clk})
	p, _ := e.placePaid(t, "order-1", payerA)
	ended := e.end(t, p.ID, saga.End{
		Reason: saga.ReasonProviderFailed, Actor: wallets.ActorRouter,
	})
	ev, raw := e.terminated(t, p.ID)
	// The destroy took two minutes, and the user is not charged for them
	eq(t, ended.TerminatedAt.Sub(*ended.TerminatingAt) >= 2*time.Minute, true)
	want := int64(ended.Deadline.Sub(*ended.TerminatingAt) / time.Second)
	eq(t, *ev.UnusedSeconds, want)
	eq(t, want > 590 && want <= 600, true)
	eq(t, ev.Refundable, true)
	eq(t, ev.Reason, saga.ReasonProviderFailed)
	// 50000 over 600 s is 83 a second, rounded down
	eq(t, ev.Refunds, []saga.Refund{{OrderID: "order-1", RefundMicros: 83 * want}})
	eq(t, fields(t, raw)["unused_seconds"], strconv.FormatInt(want, 10))

	// A stop refunds nothing, and says so
	_, h := e.placePaid(t, "order-2", payerB)
	stopped, err := e.saga.Stop(t.Context(), h)
	eq(t, err, nil)
	ev, raw = e.terminated(t, stopped.ID)
	eq(t, *ev.UnusedSeconds, int64(0))
	f := fields(t, raw)
	eq(t, []string{f["unused_seconds"], f["refunds"]}, []string{"0", "[]"})
	// Other events carry neither field
	for _, ready := range e.events(t, saga.EventReady) {
		eq(t, ready.UnusedSeconds == nil && ready.Refunds == nil, true)
	}
}

func TestRefundsExcludeAllowance(t *testing.T) {
	e := endEnv(t, nil, nil, nil)
	tok := e.token(t, ordertoken.Claims{
		OrderID: "paid", Kind: ordertoken.KindStart, PackageID: smallPkg, AmountMicros: 1053000,
		AllowanceMicros: 1000000,
	})
	p, _, err := e.saga.Create(t.Context(), tok, placement.Requirements{})
	if err != nil {
		t.Fatal(err)
	}
	e.saga.Wait()
	o, err := e.store.OrderByID(t.Context(), "paid")
	eq(t, err, nil)
	eq(t, []int64{o.AmountMicros, o.SecondsBought}, []int64{53000, 600})
	e.end(t, p.ID, saga.End{Reason: saga.ReasonProviderFailed, Actor: wallets.ActorRouter})
	ev, _ := e.terminated(t, p.ID)
	// 53000 over 600 s is 88 a second, the allowance subtracted once only
	eq(t, ev.Refunds, []saga.Refund{{OrderID: "paid", RefundMicros: 88 * *ev.UnusedSeconds}})
}

func TestRefundsTakeTheNewestOrderFirst(t *testing.T) {
	clk := &clock{}
	e := endEnv(t, nil, clk, nil)
	p, h := e.placePaid(t, "order-1", payerA)
	e.extendPaid(t, h, "ext-1", payerB, morePkg, morePrice)
	e.extendPaid(t, h, "ext-2", payerC, shortPkg, shortPrice)
	// 600 s of the start and 600 s of the first extension are used, so it ends inside the second
	clk.Advance(1300 * time.Second)
	e.end(t, p.ID, saga.End{Reason: saga.ReasonProviderFailed, Actor: wallets.ActorRouter})
	ev, _ := e.terminated(t, p.ID)
	unused := *ev.UnusedSeconds
	eq(t, unused > 190 && unused <= 200, true)
	// 25000 over 300 s is 83 a second, paid back to the second extension's order alone
	eq(t, ev.Refunds, []saga.Refund{{OrderID: "ext-2", RefundMicros: 83 * unused}})
}

func rf(order string, micros int64) saga.Refund {
	return saga.Refund{OrderID: order, RefundMicros: micros}
}

func TestRefundsPriceEachOrder(t *testing.T) {
	start := store.Order{ID: "s", Kind: store.OrderStart, AmountMicros: 50000, SecondsBought: 600}
	ext1 := store.Order{ID: "e1", Kind: store.OrderExtend, AmountMicros: 45000, SecondsBought: 600}
	ext2 := store.Order{ID: "e2", Kind: store.OrderExtend, AmountMicros: 25000, SecondsBought: 300}
	newest := []store.Order{ext2, ext1, start}
	cases := []struct {
		name   string
		orders []store.Order
		unused int64
		want   []saga.Refund
	}{
		{"start alone", []store.Order{start}, 300, []saga.Refund{rf("s", 83*300)}},
		{"inside the second extension", newest, 120, []saga.Refund{rf("e2", 83*120)}},
		{"through both extensions into the start", newest, 1000, []saga.Refund{
			rf("e2", 83*300), rf("e1", 75*600), rf("s", 83*100),
		}},
		{"more than was bought", []store.Order{start}, 700, []saga.Refund{rf("s", 83*600)}},
		{"nothing unused", newest, 0, []saga.Refund{}},
		{"an order from before seconds were kept", []store.Order{
			ext2, {ID: "old", AmountMicros: 50000},
		}, 400, []saga.Refund{rf("e2", 83*300)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			eq(t, saga.Refunds(c.orders, c.unused), c.want)
		})
	}
}

func TestNonRefundableEndsListNoRefunds(t *testing.T) {
	e := endEnv(t, nil, nil, nil)
	// Abuse comes last, since it blocks the payer
	for i, reason := range []string{saga.ReasonSandboxExited, saga.ReasonWalletBlocked,
		saga.ReasonOperator, saga.ReasonControlLost, saga.ReasonAbuse} {
		p, _ := e.placePaid(t, "order-"+string(rune('a'+i)), payerB)
		e.end(t, p.ID, saga.End{Reason: reason, Actor: "operator:ann"})
		ev, _ := e.terminated(t, p.ID)
		eq(t, ev.Refundable, false)
		eq(t, ev.Refunds, []saga.Refund{})
	}
	// An operator's refundable end refunds, as does wallet_blocked under refund_stopped_live
	p, _ := e.placeReady(t, "order-op")
	e.end(t, p.ID, saga.End{Reason: saga.ReasonOperator, Refundable: true, Actor: "operator:ann"})
	ev, _ := e.terminated(t, p.ID)
	eq(t, ev.Refundable, true)
	eq(t, len(ev.Refunds), 1)
}

func TestEndRefusesWhatItCannotEnd(t *testing.T) {
	e := endEnv(t, nil, nil, nil)
	_, err := e.saga.End(t.Context(), uuid.New(), saga.End{Reason: saga.ReasonStop})
	eq(t, errors.Is(err, saga.ErrBadReason), true)
	_, err = e.saga.End(t.Context(), uuid.New(), saga.End{Reason: saga.ReasonAbuse})
	eq(t, errors.Is(err, store.ErrNotFound), true)
	p, _ := e.placePaid(t, "order-1", payerA)
	first := e.end(t, p.ID, saga.End{Reason: saga.ReasonSandboxExited})
	// An end of an ended placement answers it as it is, saying its wallet action did not run
	again, err := e.saga.End(t.Context(), p.ID, saga.End{Reason: saga.ReasonControlLost})
	eq(t, errors.Is(err, saga.ErrAlreadyEnded), true)
	eq(t, again.TerminationReason, first.TerminationReason)
	eq(t, e.walletEvents(t, payerA, store.WalletStrike), 0)

	// A retry of the end in force is done, not refused, and runs its wallet action only once
	q, _ := e.placePaid(t, "order-2", payerB)
	lost := saga.End{Reason: saga.ReasonControlLost, Actor: wallets.ActorRouter}
	e.end(t, q.ID, lost)
	strikes := e.walletEvents(t, payerB, store.WalletStrike)
	retried, err := e.saga.End(t.Context(), q.ID, lost)
	eq(t, err, nil)
	eq(t, retried.TerminationReason, saga.ReasonControlLost)
	eq(t, e.walletEvents(t, payerB, store.WalletStrike), strikes)
}

func TestARetriedEndDuringItsOwnDestroyIsDone(t *testing.T) {
	gate := make(chan struct{})
	e := endEnv(t, nil, nil, gatedDestroy{newLocal(t, local.Options{Name: "a"}), gate})
	ctx := t.Context()
	p, _ := e.placePaid(t, "order-1", payerA)
	lost := saga.End{Reason: saga.ReasonControlLost, Actor: wallets.ActorRouter}
	first := make(chan error, 1)
	go func() {
		_, err := e.saga.End(ctx, p.ID, lost)
		first <- err
	}()
	for {
		cur, err := e.store.PlacementByID(ctx, p.ID)
		eq(t, err, nil)
		if cur.State == store.PlacementTerminating {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		close(gate)
	}()
	got, err := e.saga.End(ctx, p.ID, lost)
	eq(t, err, nil)
	eq(t, got.State, store.PlacementTerminated)
	eq(t, <-first, nil)
	eq(t, e.walletEvents(t, payerA, store.WalletStrike), 1)
}

func TestAProviderFailedEndPastItsDeadlineUsesNoDailyRefund(t *testing.T) {
	clk := &clock{}
	e := endEnv(t, func(w *config.Wallets) { w.FailedRefundsPerDay = 1 }, clk, nil)
	late, _ := e.placePaid(t, "order-1", payerA)
	failed := saga.End{Reason: saga.ReasonProviderFailed, Actor: wallets.ActorRouter}
	// The deadline passed before the sweep ended it, so nothing is left to refund
	clk.Advance(late.Deadline.Sub(clk.Now()) + 5*time.Second)
	p := e.end(t, late.ID, failed)
	eq(t, p.Refundable, false)
	ev, _ := e.terminated(t, p.ID)
	eq(t, ev.Refunds, []saga.Refund{})
	eq(t, e.walletEvents(t, payerA, store.WalletStrike), 0)
	n, err := e.store.FailedRefunds(t.Context(), payerA, clk.Now().Add(-time.Hour))
	eq(t, err, nil)
	eq(t, n, 0)
}

func TestAbuseEndAfterAStopFlagsIt(t *testing.T) {
	e := endEnv(t, func(w *config.Wallets) { w.StopLiveOnAbuse = true }, nil, nil)
	ctx := t.Context()
	p, h := e.placePaid(t, "order-1", payerA)
	// The owner's stop commits before the abuse end reads the placement
	if _, err := e.saga.Stop(ctx, h); err != nil {
		t.Fatal(err)
	}
	before, raw := e.terminated(t, p.ID)
	got := e.end(t, p.ID, saga.End{Reason: saga.ReasonAbuse, Actor: wallets.ActorRouter})
	eq(t, got.TerminationReason, saga.ReasonStop)
	eq(t, e.blocked(t, payerA).Blocked(time.Now()), true)
	eq(t, e.walletEvents(t, payerA, store.WalletBlock), 1)
	eq(t, e.stopLiveJobs(t), []string{payerA})
	sigs, err := e.store.Signals(ctx, p.ID)
	eq(t, err, nil)
	eq(t, len(sigs), 1)
	eq(t, sigs[0].Action, "flagged")
	after, rawAfter := e.terminated(t, p.ID)
	eq(t, after, before)
	eq(t, rawAfter, raw)
}

func TestAnEndDuringAnotherEndIsAlreadyEnded(t *testing.T) {
	for _, reason := range []string{saga.ReasonControlLost, saga.ReasonProviderFailed} {
		t.Run(reason, func(t *testing.T) {
			gate := make(chan struct{})
			e := endEnv(t, nil, nil, gatedDestroy{newLocal(t, local.Options{Name: "a"}), gate})
			ctx := t.Context()
			p, h := e.placePaid(t, "order-1", payerA)
			stopped := make(chan error, 1)
			go func() {
				_, err := e.saga.Stop(ctx, h)
				stopped <- err
			}()
			// The stop holds the placement in terminating while its destroy waits on the gate
			for {
				cur, err := e.store.PlacementByID(ctx, p.ID)
				eq(t, err, nil)
				if cur.State == store.PlacementTerminating {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			go func() {
				time.Sleep(100 * time.Millisecond)
				close(gate)
			}()
			got, err := e.saga.End(ctx, p.ID, saga.End{Reason: reason, Actor: wallets.ActorRouter})
			eq(t, errors.Is(err, saga.ErrAlreadyEnded), true)
			eq(t, got.TerminationReason, saga.ReasonStop)
			eq(t, <-stopped, nil)
			eq(t, e.walletEvents(t, payerA, store.WalletStrike), 0)
		})
	}
}

func TestAStopBetweenEndsReadAndLockWinsTheRace(t *testing.T) {
	e := endEnv(t, func(w *config.Wallets) { w.StopLiveOnAbuse = true }, nil, nil)
	ctx := t.Context()
	stopAt := func(h string) func() {
		return func() {
			if _, err := e.saga.Stop(ctx, h); err != nil {
				t.Errorf("stop: %v", err)
			}
		}
	}
	// control_lost read a ready placement, then the owner's stop ended it first
	p, h := e.placePaid(t, "order-1", payerA)
	saga.SetBeforeMark(e.saga, stopAt(h))
	got, err := e.saga.End(ctx, p.ID, saga.End{Reason: saga.ReasonControlLost})
	eq(t, errors.Is(err, saga.ErrAlreadyEnded), true)
	eq(t, got.TerminationReason, saga.ReasonStop)
	eq(t, e.walletEvents(t, payerA, store.WalletStrike), 0)
	// abuse in the same race still blocks the payer, through FlagEnded
	q, hq := e.placePaid(t, "order-2", payerB)
	saga.SetBeforeMark(e.saga, stopAt(hq))
	got, err = e.saga.End(ctx, q.ID, saga.End{Reason: saga.ReasonAbuse, Actor: wallets.ActorRouter})
	eq(t, err, nil)
	eq(t, got.TerminationReason, saga.ReasonStop)
	eq(t, e.blocked(t, payerB).Blocked(time.Now()), true)
	eq(t, e.walletEvents(t, payerB, store.WalletBlock), 1)
	saga.SetBeforeMark(e.saga, nil)
}

func TestEndWithoutPayerLeavesTheSharedLock(t *testing.T) {
	e := endEnv(t, nil, nil, nil)
	ctx := t.Context()
	p, _ := e.placeReady(t, "order-1")
	// A create without a payer holds "-" while it records its order
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := e.store.Tx(tx).LockWallets(ctx, ""); err != nil {
		t.Fatal(err)
	}
	ended := make(chan error, 1)
	go func() {
		_, err := e.saga.End(ctx, p.ID, saga.End{
			Reason: saga.ReasonOperator, Actor: "operator:ann",
		})
		ended <- err
	}()
	select {
	case err := <-ended:
		eq(t, err, nil)
	case <-time.After(10 * time.Second):
		_ = tx.Rollback(ctx)
		<-ended
		t.Fatal("an operator end of a placement without a payer waited on the shared lock")
	}
	eq(t, e.reload(t, p.ID).State, store.PlacementTerminated)
}

func TestFlagEndedLeavesALongerBlock(t *testing.T) {
	e := endEnv(t, func(w *config.Wallets) {
		w.EmitEvents, w.StopLiveOnAbuse, w.AbuseBlockFor = true, true, time.Hour
	}, nil, nil)
	ctx := t.Context()
	p, h := e.placePaid(t, "order-1", payerA)
	e.extendPaid(t, h, "ext-1", payerB, morePkg, morePrice)
	e.extendPaid(t, h, "ext-2", payerC, shortPkg, shortPrice)
	if _, err := e.saga.Stop(ctx, h); err != nil {
		t.Fatal(err)
	}
	// A blocked until lifted, B for longer than an abuse block, C for less
	long, short := time.Now().Add(48*time.Hour), time.Now().Add(10*time.Minute)
	for payer, until := range map[string]*time.Time{payerA: nil, payerB: &long, payerC: &short} {
		if err := e.store.SetWalletBlock(ctx, payer, time.Now(), until, wallets.ReasonOperator,
			"operator:ann"); err != nil {
			t.Fatal(err)
		}
	}
	var got [][]string
	for range 2 {
		_, blocked, err := e.saga.FlagEnded(ctx, p.ID, saga.End{
			Reason: saga.ReasonAbuse, Actor: wallets.ActorRouter,
		})
		eq(t, err, nil)
		got = append(got, blocked)
	}
	eq(t, got, [][]string{{payerC}, nil})
	for _, payer := range []string{payerA, payerB} {
		eq(t, e.walletEvents(t, payer, store.WalletBlock), 0)
		eq(t, e.walletEvents(t, payer, store.WalletNote), 2)
		eq(t, e.blocked(t, payer).BlockedReason, wallets.ReasonOperator)
	}
	// C's shorter block gave way to the abuse block once, and the second flag only noted it
	eq(t, e.walletEvents(t, payerC, store.WalletBlock), 1)
	eq(t, e.walletEvents(t, payerC, store.WalletNote), 1)
	eq(t, e.blocked(t, payerC).BlockedReason, wallets.ReasonAbuse)
	eq(t, e.stopLiveJobs(t), []string{payerC})
	eq(t, e.eventCount(t, wallets.EventBlocked), 1)
}

func TestEndTakesThePayerLocksBeforeTheRow(t *testing.T) {
	prov := newCapped(t, t.TempDir(), 0)
	e := endEnv(t, nil, nil, prov)
	ctx := t.Context()
	p, h := e.placePaid(t, "order-1", payerA)
	prov.hold.Store(true)
	extended := make(chan error, 1)
	go func() {
		_, err := e.saga.Extend(ctx, h, e.paidExtend(t, "ext-1", payerB, h))
		extended <- err
	}()
	<-prov.held
	// The extension holds payer B's lock and the placement row inside SetLifetime
	ended := make(chan error, 1)
	go func() {
		_, err := e.saga.End(ctx, p.ID, saga.End{Reason: saga.ReasonAbuse, Actor: "router"})
		ended <- err
	}()
	time.Sleep(300 * time.Millisecond)
	close(prov.release)
	for _, ch := range []chan error{extended, ended} {
		select {
		case err := <-ch:
			eq(t, err, nil)
		case <-time.After(20 * time.Second):
			t.Fatal("the extension and the end waited on each other")
		}
	}
	// The end saw the extension's payer once it held the row
	eq(t, e.blocked(t, payerA).Blocked(time.Now()), true)
	eq(t, e.blocked(t, payerB).Blocked(time.Now()), true)
	eq(t, e.reload(t, p.ID).TerminationReason, saga.ReasonAbuse)
}

// advancingDestroy moves the clock two minutes on during each destroy, like a slow provider
type advancingDestroy struct {
	*local.Provider
	clk *clock
}

func (a advancingDestroy) Destroy(ctx context.Context, ref provider.Ref) error {
	a.clk.Advance(2 * time.Minute)
	return a.Provider.Destroy(ctx, ref)
}

// gatedDestroy holds each destroy until the gate closes, keeping its placement in terminating
type gatedDestroy struct {
	*local.Provider
	gate chan struct{}
}

func (g gatedDestroy) Destroy(ctx context.Context, ref provider.Ref) error {
	<-g.gate
	return g.Provider.Destroy(ctx, ref)
}
