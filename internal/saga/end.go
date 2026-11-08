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
	// errPayersMoved restarts an end whose placement gained a payer before its row was locked
	errPayersMoved = errors.New("saga: the placement gained a payer")
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
}

// End ends a ready or terminating placement for e, a sandbox already gone counting as destroyed
func (s *Saga) End(ctx context.Context, id uuid.UUID, e End) (store.Placement, error) {
	switch e.Reason {
	case ReasonAbuse, ReasonControlLost, ReasonProviderFailed, ReasonSandboxExited,
		ReasonWalletBlocked, ReasonOperator:
	default:
		return store.Placement{}, fmt.Errorf("%w: %q", ErrBadReason, e.Reason)
	}
	e.Actor = cmp.Or(e.Actor, wallets.ActorRouter)
	p, err := s.d.Store.PlacementByID(ctx, id)
	if err != nil {
		return store.Placement{}, err
	}
	return s.terminate(ctx, p, e.Reason, func(ctx context.Context) (bool, error) {
		return s.markEnd(ctx, id, e)
	})
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
	err := s.underPayers(ctx, id, func(tx pgx.Tx, cur store.Placement, payers []string) error {
		if cur.State != store.PlacementTerminated && cur.State != store.PlacementFailed {
			return fmt.Errorf("%w: placement is %s", ErrNotEnded, cur.State)
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
		return s.signal(ctx, s.d.Store.Tx(tx), cur, e, action)
	})
	if err != nil {
		return store.Placement{}, nil, err
	}
	return p, blocked, nil
}

// flagPayers blocks every payer of an ended placement, a payer already blocked for abuse
// getting only a note with the new evidence
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
		if err == nil && w.Blocked(s.now()) && w.BlockedReason == wallets.ReasonAbuse {
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

// markEnd runs step 1 of End under the payers' locks, then the placement row's, the order
// Extend takes them in, and reports whether it moved the placement or replaced its reason
func (s *Saga) markEnd(ctx context.Context, id uuid.UUID, e End) (bool, error) {
	moved := false
	err := s.underPayers(ctx, id, func(tx pgx.Tx, cur store.Placement, payers []string) error {
		moved = false
		switch cur.State {
		case store.PlacementReady:
		case store.PlacementTerminating:
			// Abuse replaces a stop in flight, so its wallet action still happens
			if e.Reason != ReasonAbuse || cur.TerminationReason == ReasonAbuse {
				return nil
			}
		default:
			return nil
		}
		ts := s.d.Store.Tx(tx)
		refundable, action, err := s.endActions(ctx, tx, cur, payers, e)
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
		moved = true
		return nil
	})
	return moved, err
}

// endActions decides the end's refund and runs the wallet actions of its reason (CP 6.4),
// naming the provider_signals action, empty for an end that is no provider signal
func (s *Saga) endActions(
	ctx context.Context, tx pgx.Tx, p store.Placement, payers []string, e End,
) (bool, string, error) {
	switch e.Reason {
	case ReasonWalletBlocked:
		return s.w.RefundStoppedLive, "", nil
	case ReasonOperator:
		return e.Refundable, "", nil
	case ReasonSandboxExited:
		return false, signalAction(e, actionEnded), nil
	}
	if e.Held {
		return e.Reason == ReasonProviderFailed, actionHeld, nil
	}
	switch e.Reason {
	case ReasonAbuse:
		n := 0
		for _, payer := range payers {
			ok, err := s.block(ctx, tx, p, payer, e)
			if err != nil {
				return false, "", err
			}
			if ok {
				n++
			}
		}
		return false, cond(n > 0, actionBlocked, actionEnded), nil
	case ReasonControlLost:
		n, err := s.strike(ctx, tx, p, payers, e)
		return false, cond(n > 0, actionStruck, actionEnded), err
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
	struck, err := s.strike(ctx, tx, p, payers, e)
	return false, cond(struck > 0, actionStruck, actionEnded), err
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
	_, err = s.d.River.InsertTx(ctx, tx, jobargs.WalletStopLive{Payer: payer}, nil)
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
// placement row, starting again when the placement gained a payer before its row was locked
func (s *Saga) underPayers(
	ctx context.Context, id uuid.UUID,
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
			// The start payer's lock guards the daily refund count, "-" for every placement
			// without one
			locks := slices.Concat(payers, []string{seen.Payer})
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
				return errPayersMoved
			}
			return fn(tx, cur, payers)
		})
		if !errors.Is(err, errPayersMoved) || attempt == endAttempts {
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
	if p.Refundable && p.Deadline != nil && p.TerminatedAt != nil {
		unused = max(int64(p.Deadline.Sub(*p.TerminatedAt)/time.Second), 0)
		orders, err := ts.OrdersNewestFirst(ctx, p.ID)
		if err != nil {
			return Event{}, err
		}
		ev.Refunds = refunds(orders, unused)
	}
	ev.UnusedSeconds = &unused
	return ev, nil
}
