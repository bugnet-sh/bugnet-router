package saga

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/wallet"
	"github.com/bugnet-sh/bugnet-router/internal/wallets"
)

// Reasons a placement ends for beside a stop or its deadline (CP 3.3)
const (
	ReasonAbuse          = "abuse"
	ReasonControlLost    = "control_lost"
	ReasonProviderFailed = "provider_failed"
	ReasonSandboxExited  = "sandbox_exited"
	ReasonWalletBlocked  = "wallet_blocked"
	ReasonOperator       = "operator"
)

var (
	// ErrBadReason refuses an End for a reason CP 3.3 does not list, or a flag other than abuse
	ErrBadReason = errors.New("saga: not a reason a placement ends for")
	// ErrNotEnded refuses to flag a placement that has not ended yet
	ErrNotEnded = errors.New("saga: placement has not ended")
	// ErrAlreadyEnded answers an End other than abuse whose placement had ended before it, so its
	// wallet action did not run; End for abuse flags such a placement instead
	ErrAlreadyEnded = errors.New("saga: placement had already ended")
	// ErrPayersMoved restarts an end whose placement gained a payer before its row was locked,
	// and reaches the caller when that keeps happening
	ErrPayersMoved = errors.New("saga: the placement gained a payer")
)

// endAttempts bounds the restarts of an end whose placement keeps gaining payers
const endAttempts = 5

// failedRefundWindow is the window wallets.failed_refunds_per_day counts in
const failedRefundWindow = 24 * time.Hour

// Signal sources and actions of provider_signals (CP 6.11)
const (
	sourceOperator = "operator"
	sourceSweep    = "sweep"
	actionEnded    = "ended"
	actionStruck   = "struck"
	actionBlocked  = "blocked"
	actionFlagged  = "flagged"
	actionHeld     = "held"
)

var signalSources = []string{"hook", "sweep", "poll", "account_check", sourceOperator}

// End is why the router ends a live placement other than by stop or deadline
type End struct {
	// Reason is abuse, control_lost, provider_failed, sandbox_exited, wallet_blocked or operator
	Reason string
	// Refundable is honoured for operator, set by the saga for provider_failed (6.4) and
	// wallet_blocked (wallets.refund_stopped_live), and false otherwise
	Refundable bool
	// Evidence is the signal as JSON: provider, source, observed state, event id, never secrets
	Evidence json.RawMessage
	// Actor is router or operator:<name>, router when empty
	Actor string
	// Held skips the wallet actions and records them for review (the correlation guard, 6.4)
	Held bool
	// Guard decides Held under the placement's locks, in the transaction that marks the end, so
	// the guard's lock is released with it and never waits on the destroy. A held end other than
	// an abuse flag ends as provider_failed and keeps its reason as held_class in the evidence.
	Guard func(ctx context.Context, tx pgx.Tx) (bool, error)
	// blocked collects the payers an abuse end blocked
	blocked []string
}

// hold turns e into a held end, an end of a live placement becoming provider_failed
func (e *End) hold(live bool) error {
	m := map[string]any{}
	if len(e.Evidence) > 0 {
		if err := json.Unmarshal(e.Evidence, &m); err != nil {
			return fmt.Errorf("held end evidence must be a JSON object: %w", err)
		}
	}
	m["held_class"] = e.Reason
	raw, err := json.Marshal(m)
	if err != nil {
		return err
	}
	e.Evidence, e.Held, e.Guard = raw, true, nil
	if live {
		e.Reason = ReasonProviderFailed
	}
	return nil
}

// End ends a ready or terminating placement for e, a sandbox already gone counting as destroyed.
// A placement that ended first, even while End ran, gets FlagEnded for abuse and answers
// ErrAlreadyEnded for any other reason.
func (s *Saga) End(ctx context.Context, id uuid.UUID, e End) (store.Placement, error) {
	p, _, err := s.EndBlocked(ctx, id, e)
	return p, err
}

// EndBlocked is End that also returns the payers an abuse end blocked
func (s *Saga) EndBlocked(
	ctx context.Context, id uuid.UUID, e End,
) (store.Placement, []string, error) {
	switch e.Reason {
	case ReasonAbuse, ReasonControlLost, ReasonProviderFailed, ReasonSandboxExited,
		ReasonWalletBlocked, ReasonOperator:
	default:
		return store.Placement{}, nil, fmt.Errorf("%w: %q", ErrBadReason, e.Reason)
	}
	e.Actor = cmp.Or(e.Actor, wallets.ActorRouter)
	p, err := s.d.Store.PlacementByID(ctx, id)
	if err != nil {
		return store.Placement{}, nil, err
	}
	ended := p.State == store.PlacementTerminated
	if !ended {
		if s.beforeMark != nil {
			s.beforeMark()
		}
		taken := false
		reason := func() string { return e.Reason }
		p, err = s.terminate(ctx, p, reason, func(ctx context.Context) (bool, error) {
			moved, done, other, err := s.markEnd(ctx, id, &e)
			ended, taken = done, other
			return moved, err
		})
		if err == nil && taken {
			return p, nil, fmt.Errorf("%w: ending for %s", ErrAlreadyEnded, p.TerminationReason)
		}
		if err != nil || !ended {
			return p, e.blocked, err
		}
	}
	if e.Reason != ReasonAbuse {
		// A retry of the end in force did its wallet action the first time, so it is done
		if p.TerminationReason == e.Reason {
			return p, nil, nil
		}
		return p, nil, fmt.Errorf("%w: ended for %s", ErrAlreadyEnded, p.TerminationReason)
	}
	return s.FlagEnded(ctx, id, e)
}

// FlagEnded records abuse on a terminated or failed placement and runs its wallet actions,
// changing neither its reason nor its money, and returns the payers it blocked
func (s *Saga) FlagEnded(
	ctx context.Context, id uuid.UUID, e End,
) (store.Placement, []string, error) {
	if e.Reason != ReasonAbuse {
		return store.Placement{}, nil, fmt.Errorf("%w: flag %q", ErrBadReason, e.Reason)
	}
	e.Actor = cmp.Or(e.Actor, wallets.ActorRouter)
	var p store.Placement
	var blocked []string
	err := s.underPayers(ctx, id, false, func(tx pgx.Tx, cur store.Placement,
		payers []string,
	) error {
		if cur.State != store.PlacementTerminated && cur.State != store.PlacementFailed {
			return fmt.Errorf("%w: placement is %s", ErrNotEnded, cur.State)
		}
		e := e
		if e.Guard != nil {
			held, err := e.Guard(ctx, tx)
			if err != nil {
				return err
			}
			if held {
				if err := e.hold(false); err != nil {
					return err
				}
			}
		}
		p, blocked = cur, nil
		action := actionFlagged
		if e.Held {
			action = actionHeld
		} else {
			var err error
			if blocked, err = s.flagPayers(ctx, tx, cur, payers, e); err != nil {
				return err
			}
		}
		// History ignores the observation of a placement flagged after its end
		if !e.Held {
			if err := s.d.Store.Tx(tx).FlagObservation(ctx, cur.ID); err != nil {
				return err
			}
		}
		// An abuse flag closes the public page and holds the record for operators
		err := s.d.Store.Tx(tx).CloseSessionRecord(ctx, cur.ID, store.ClosedByAbuse,
			s.now().Add(s.d.SessionAbuseHold))
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return err
		}
		return s.signal(ctx, s.d.Store.Tx(tx), cur, e, action)
	})
	if err != nil {
		return store.Placement{}, nil, err
	}
	return p, blocked, nil
}

// flagPayers blocks every payer of an ended placement, a payer whose block in force already
// covers an abuse block getting only a note with the new evidence
func (s *Saga) flagPayers(
	ctx context.Context, tx pgx.Tx, p store.Placement, payers []string, e End,
) ([]string, error) {
	ts := s.d.Store.Tx(tx)
	var out []string
	for _, payer := range payers {
		w, err := ts.Wallet(ctx, payer, s.now())
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
		if err == nil && s.covered(w) {
			err := ts.InsertWalletEvent(ctx, store.WalletEvent{
				Payer: payer, Kind: store.WalletNote, Reason: ReasonAbuse, PlacementID: p.ID,
				Provider: p.Provider, Evidence: e.Evidence, Actor: e.Actor, CreatedAt: s.now(),
			})
			if err != nil {
				return nil, err
			}
			continue
		}
		ok, err := s.block(ctx, tx, p, payer, e)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, payer)
		}
	}
	return out, nil
}

// covered reports whether the block in force on w is one Block keeps over an abuse block: an
// abuse block, or one lasting at least as long, so a new block would block nothing new
func (s *Saga) covered(w store.Wallet) bool {
	now := s.now()
	switch {
	case !w.Blocked(now):
		return false
	case w.BlockedReason == wallets.ReasonAbuse || w.BlockedUntil == nil:
		return true
	}
	return s.w.AbuseBlockFor > 0 && !w.BlockedUntil.Before(now.Add(s.w.AbuseBlockFor))
}

// markEnd runs step 1 of End under the payers' locks, then the placement row's, the order
// Extend takes them in, and reports whether it moved the placement or replaced its reason, and
// whether the placement had already terminated
// markEnd also reports whether another end already holds the placement, so End's reason lost
func (s *Saga) markEnd(ctx context.Context, id uuid.UUID, ep *End) (bool, bool, bool, error) {
	moved, ended, taken := false, false, false
	var decided End
	// Only provider_failed reads the start payer's daily count, so only it waits on "-"; a guard
	// may turn the end into one, so it waits too
	startLock := ep.Reason == ReasonProviderFailed || ep.Guard != nil
	err := s.underPayers(ctx, id, startLock, func(tx pgx.Tx, cur store.Placement,
		payers []string,
	) error {
		moved, ended, taken = false, cur.State == store.PlacementTerminated, false
		e := *ep
		switch cur.State {
		case store.PlacementReady:
		case store.PlacementTerminating:
			// Abuse replaces a stop in flight, so its wallet action still happens
			if e.Reason != ReasonAbuse || cur.TerminationReason == ReasonAbuse {
				taken = e.Reason != ReasonAbuse && cur.TerminationReason != e.Reason
				return nil
			}
		default:
			return nil
		}
		ts := s.d.Store.Tx(tx)
		if e.Guard != nil {
			held, err := e.Guard(ctx, tx)
			if err != nil {
				return err
			}
			if held {
				if err := e.hold(true); err != nil {
					return err
				}
			}
			e.Guard = nil
		}
		refundable, action, err := s.endActions(ctx, tx, cur, payers, &e)
		if err != nil {
			return err
		}
		evidence := e.Evidence
		if e.Held {
			if evidence, err = correlated(evidence); err != nil {
				return err
			}
		}
		err = ts.MarkPlacementEnding(ctx, id, store.Ending{
			Reason: e.Reason, Refundable: refundable, Evidence: evidence, Actor: e.Actor,
			At: s.now(),
		})
		if err != nil {
			return err
		}
		if action != "" {
			if err := s.signal(ctx, ts, cur, e, action); err != nil {
				return err
			}
		}
		moved, decided = true, e
		return nil
	})
	if moved {
		*ep = decided
	}
	return moved, ended, taken, err
}

// endActions decides the end's refund and runs the wallet actions of its reason (CP 6.4),
// naming the provider_signals action, empty for an end that is no provider signal
func (s *Saga) endActions(
	ctx context.Context, tx pgx.Tx, p store.Placement, payers []string, e *End,
) (bool, string, error) {
	switch e.Reason {
	case ReasonWalletBlocked:
		return s.w.RefundStoppedLive, "", nil
	case ReasonOperator:
		return e.Refundable, "", nil
	case ReasonSandboxExited:
		return false, signalAction(*e, actionEnded), nil
	}
	if e.Held {
		return e.Reason == ReasonProviderFailed, actionHeld, nil
	}
	switch e.Reason {
	case ReasonAbuse:
		n := 0
		for _, payer := range payers {
			ok, err := s.block(ctx, tx, p, payer, *e)
			if err != nil {
				return false, "", err
			}
			if ok {
				n++
				e.blocked = append(e.blocked, payer)
			}
		}
		return false, cond(n > 0, actionBlocked, actionEnded), nil
	case ReasonControlLost:
		n, err := s.strike(ctx, tx, p, payers, *e)
		return false, cond(n > 0, actionStruck, actionEnded), err
	}
	// With no whole second left there is nothing to refund, so no daily refund is used up
	if p.Deadline == nil || p.Deadline.Sub(s.now()) < time.Second {
		return false, actionEnded, nil
	}
	// More provider_failed ends on one provider than the incident maximum inside its window are
	// an outage: refundable outside the daily cap, no strike, and left out of the count
	outage, err := s.outage(ctx, tx, p.Provider)
	if err != nil {
		return false, "", err
	}
	if outage {
		ev, err := correlated(e.Evidence)
		if err != nil {
			return false, "", err
		}
		e.Evidence = ev
		return true, actionEnded, nil
	}
	// provider_failed refunds its unused time while the start payer is under the daily cap
	limit := s.w.FailedRefundsPerDay
	if p.Payer == "" {
		limit = s.w.AnonymousFailedRefundsPerDay
	}
	n, err := s.d.Store.Tx(tx).FailedRefunds(ctx, p.Payer, s.now().Add(-failedRefundWindow))
	if err != nil {
		return false, "", err
	}
	if n < limit {
		return true, actionEnded, nil
	}
	struck, err := s.strike(ctx, tx, p, payers, *e)
	return false, cond(struck > 0, actionStruck, actionEnded), err
}

// outage reports whether this provider_failed end is one more than abuse.correlated_max of the
// provider's inside abuse.correlated_window, counted under the provider's signal lock
func (s *Saga) outage(ctx context.Context, tx pgx.Tx, prov string) (bool, error) {
	if err := store.LockProviderSignals(ctx, tx, prov); err != nil {
		return false, err
	}
	n, err := store.LostCount(ctx, tx, prov, []string{ReasonProviderFailed},
		s.now().Add(-s.d.Abuse.CorrelatedWindow))
	if err != nil {
		return false, err
	}
	return n+1 > s.d.Abuse.CorrelatedMax, nil
}

// block blocks one payer for abuse and queues the end of its other live placements, reporting
// false for a facilitator address, which is never blocked
func (s *Saga) block(
	ctx context.Context, tx pgx.Tx, p store.Placement, payer string, e End,
) (bool, error) {
	var until *time.Time
	if s.w.AbuseBlockFor > 0 {
		t := s.now().Add(s.w.AbuseBlockFor)
		until = &t
	}
	err := s.wallets.Block(ctx, tx, wallets.Block{
		Payer: payer, Reason: wallets.ReasonAbuse, Until: until, Actor: e.Actor,
		PlacementID: p.ID, Provider: p.Provider, Evidence: e.Evidence,
	})
	if errors.Is(err, wallets.ErrFacilitator) {
		s.d.Log.Error("a payer of an abuse end is a facilitator address, not blocked",
			"placement", p.ID)
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !s.w.StopLiveOnAbuse {
		return true, nil
	}
	// The job carries the block's start, so it ends nothing once this block is lifted
	w, err := s.d.Store.Tx(tx).Wallet(ctx, payer, s.now())
	if err != nil {
		return false, err
	}
	args := jobargs.WalletStopLive{Payer: payer, BlockedAt: w.BlockedAt}
	_, err = s.d.River.InsertTx(ctx, tx, args, nil)
	if err != nil {
		return false, fmt.Errorf("insert wallet_stop_live job: %w", err)
	}
	return true, nil
}

// strike gives every payer one strike, skipping a facilitator address, and counts the strikes
func (s *Saga) strike(
	ctx context.Context, tx pgx.Tx, p store.Placement, payers []string, e End,
) (int, error) {
	n := 0
	for _, payer := range payers {
		_, err := s.wallets.Strike(ctx, tx, wallets.Strike{
			Payer: payer, Reason: e.Reason, Actor: e.Actor, PlacementID: p.ID,
			Provider: p.Provider, Evidence: e.Evidence,
		})
		if errors.Is(err, wallets.ErrFacilitator) {
			s.d.Log.Error("a payer of an end is a facilitator address, not struck",
				"placement", p.ID, "reason", e.Reason)
			continue
		}
		if err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// underPayers runs fn in a transaction that holds every payer's wallet lock and then the
// placement row, starting again when the placement gained a payer before its row was locked.
// startLock also takes the start payer's lock, "-" for a placement without one.
func (s *Saga) underPayers(
	ctx context.Context, id uuid.UUID, startLock bool,
	fn func(tx pgx.Tx, cur store.Placement, payers []string) error,
) error {
	for attempt := 1; ; attempt++ {
		err := s.d.Store.WithTx(ctx, func(tx pgx.Tx) error {
			ts := s.d.Store.Tx(tx)
			seen, err := ts.PlacementByID(ctx, id)
			if err != nil {
				return err
			}
			payers, err := s.payers(ctx, ts, seen)
			if err != nil {
				return err
			}
			locks := payers
			if startLock {
				// The start payer's lock guards the daily refund count, "-" for every
				// placement without one
				locks = slices.Concat(payers, []string{seen.Payer})
			}
			if err := ts.LockWallets(ctx, locks...); err != nil {
				return err
			}
			cur, err := ts.PlacementForUpdate(ctx, id)
			if err != nil {
				return err
			}
			now, err := s.payers(ctx, ts, cur)
			if err != nil {
				return err
			}
			if cur.Payer != seen.Payer || !subset(now, payers) {
				return ErrPayersMoved
			}
			return fn(tx, cur, payers)
		})
		if !errors.Is(err, ErrPayersMoved) || attempt == endAttempts {
			return err
		}
	}
}

// payers returns every payer of a placement in sorted order: its start payer and each
// extension's claim decided again, as ruling 28R says, without the logs Create writes
func (s *Saga) payers(ctx context.Context, ts *store.Store, p store.Placement) ([]string, error) {
	var out []string
	if p.Payer != "" {
		out = append(out, p.Payer)
	}
	claims, err := ts.ExtendClaims(ctx, p.ID)
	if err != nil {
		return nil, err
	}
	for _, o := range claims {
		payer, _, err := wallet.Decide(s.guard,
			wallet.Claim{Payer: o.Payer, FeePayer: o.FeePayer},
			func(addr string) (bool, error) { return ts.FeePayerSeen(ctx, addr) })
		if err != nil {
			return nil, err
		}
		if payer != "" && !slices.Contains(out, payer) {
			out = append(out, payer)
		}
	}
	slices.Sort(out)
	return out, nil
}

// LivePlacementsOf returns the placements placing or ready that payer paid a start or an
// extension of, as decided
func (s *Saga) LivePlacementsOf(ctx context.Context, payer string) ([]store.Placement, error) {
	ps, err := s.d.Store.LivePlacementsOf(ctx, payer)
	if err != nil {
		return nil, err
	}
	var out []store.Placement
	for _, p := range ps {
		if p.State == store.PlacementTerminating {
			continue
		}
		if p.Payer != payer {
			payers, err := s.payers(ctx, s.d.Store, p)
			if err != nil {
				return nil, err
			}
			if !slices.Contains(payers, payer) {
				continue
			}
		}
		out = append(out, p)
	}
	return out, nil
}

// signal records the end's provider signal and what was done about it
func (s *Saga) signal(
	ctx context.Context, ts *store.Store, p store.Placement, e End, action string,
) error {
	var ev struct {
		Source    string `json:"source"`
		HeldClass string `json:"held_class"`
	}
	// Evidence that is no JSON object still records, with the source from the actor
	_ = json.Unmarshal(e.Evidence, &ev)
	source := ev.Source
	if !slices.Contains(signalSources, source) {
		source = cond(strings.HasPrefix(e.Actor, sourceOperator+":"), sourceOperator, sourceSweep)
	}
	sg := store.Signal{
		Class: e.Reason, Provider: p.Provider, PlacementID: p.ID, Source: source,
		Observed: e.Evidence, Action: action, CreatedAt: s.now(),
	}
	if action == actionHeld {
		sg.HeldClass = ev.HeldClass
	}
	return ts.InsertSignal(ctx, sg)
}

// signalAction is the action of an end with no wallet action, held when the guard held it
func signalAction(e End, action string) string {
	return cond(e.Held, actionHeld, action)
}

// correlated marks a held end's evidence, so the daily refund count leaves it out
func correlated(evidence json.RawMessage) (json.RawMessage, error) {
	m := map[string]any{}
	if len(evidence) > 0 {
		if err := json.Unmarshal(evidence, &m); err != nil {
			return nil, fmt.Errorf("held end evidence must be a JSON object: %w", err)
		}
	}
	m["correlated"] = true
	return json.Marshal(m)
}

// subset reports whether every payer of a is in b
func subset(a, b []string) bool {
	for _, x := range a {
		if !slices.Contains(b, x) {
			return false
		}
	}
	return true
}

func cond[T any](ok bool, yes, no T) T {
	if ok {
		return yes
	}
	return no
}

// Refund is one order's share of a refundable end's unused time
type Refund struct {
	OrderID      string `json:"order_id"`
	RefundMicros int64  `json:"refund_micros"`
}

// refunds prices unused seconds from the newest order back to the start, each order's seconds
// at its own price per second, amount_micros / seconds_bought rounded down (CP 3.3)
func refunds(newestFirst []store.Order, unused int64) []Refund {
	out := []Refund{}
	for _, o := range newestFirst {
		// An order from before seconds_bought was kept has no price per second
		if unused <= 0 || o.SecondsBought <= 0 {
			break
		}
		take := min(unused, o.SecondsBought)
		price := o.AmountMicros / o.SecondsBought
		out = append(out, Refund{OrderID: o.ID, RefundMicros: price * take})
		unused -= take
	}
	return out
}

// terminatedEvent is placement.terminated with its unused seconds and refunds, both empty
// unless the end is refundable
func (s *Saga) terminatedEvent(
	ctx context.Context, ts *store.Store, p store.Placement, reason string,
) (Event, error) {
	ev := newEvent(p, reason)
	unused := int64(0)
	ev.Refunds = []Refund{}
	// Counted from when the end began, so a slow or failing destroy never shrinks the refund
	if began := cmp.Or(p.TerminatingAt, p.TerminatedAt); p.Refundable && p.Deadline != nil &&
		began != nil {
		unused = max(int64(p.Deadline.Sub(*began)/time.Second), 0)
		orders, err := ts.OrdersNewestFirst(ctx, p.ID)
		if err != nil {
			return Event{}, err
		}
		ev.Refunds = refunds(orders, unused)
	}
	ev.UnusedSeconds = &unused
	ev.CostMicros, ev.EdgeBytes, ev.EgressBytes = &p.CostMicros, &p.EdgeBytes, &p.EgressBytes
	return ev, nil
}
