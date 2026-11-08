// Package wallets writes wallet blocks and strikes inside the caller's transaction.
//
// Every write first takes the wallet's transaction lock, the one Saga.Create takes while it
// records an order, so a block waits for any order of that payer in flight. A facilitator
// address, listed or seen as an order's fee payer, is never blocked or struck, so one bad address
// cannot block everyone. A block never shortens a block already in force. With events on, each
// block and unblock also writes wallet.blocked or wallet.unblocked to the outbox.
package wallets

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// ErrFacilitator refuses a block or strike of a facilitator address
var ErrFacilitator = errors.New("wallets: a facilitator address is never blocked or struck")

// Block reasons
const (
	ReasonAbuse    = "abuse"
	ReasonStrikes  = "strikes"
	ReasonRefusals = "refusals"
	ReasonOperator = "operator"
)

// ActorRouter is the actor of every block and strike the router makes itself
const ActorRouter = "router"

// Outbox event types
const (
	EventBlocked   = "wallet.blocked"
	EventUnblocked = "wallet.unblocked"
)

// Block is one block to write
type Block struct {
	Payer string
	// Reason is abuse, strikes, refusals or operator
	Reason string
	// Until is nil for a block an operator must lift
	Until *time.Time
	// Actor is router or operator:<name>
	Actor       string
	PlacementID uuid.UUID
	Provider    string
	Evidence    json.RawMessage
}

// Strike is one strike to record
type Strike struct {
	Payer       string
	Reason      string
	Actor       string
	PlacementID uuid.UUID
	Provider    string
	Evidence    json.RawMessage
}

// Wallets writes blocks and strikes inside the caller's transaction, under each wallet's lock
type Wallets interface {
	// Block refuses a facilitator address with ErrFacilitator
	Block(ctx context.Context, tx pgx.Tx, b Block) error
	// Strike records one strike and reports whether it blocked the wallet
	Strike(ctx context.Context, tx pgx.Tx, s Strike) (bool, error)
	Unblock(ctx context.Context, tx pgx.Tx, payer, actor, note string) error
	Blocked(ctx context.Context, q store.Querier, payer string, now time.Time) (bool, error)
}

// Settings are the wallet settings blocks and strikes follow
type Settings struct {
	// Facilitators are wallets.facilitator_addresses
	Facilitators    []string
	StrikeThreshold int
	StrikeWindow    time.Duration
	StrikeBlockFor  time.Duration
	// EmitEvents writes wallet.blocked and wallet.unblocked for Payments
	EmitEvents bool
	// Now is time.Now when nil
	Now func() time.Time
}

// Store is Wallets over the router's store
type Store struct {
	st *store.Store
	s  Settings
}

var _ Wallets = (*Store)(nil)

// New returns Wallets writing through st
func New(st *store.Store, s Settings) *Store {
	if s.Now == nil {
		s.Now = time.Now
	}
	return &Store{st: st, s: s}
}

func (w *Store) now() time.Time { return w.s.Now().UTC() }

// Facilitator reports whether addr is listed or came as an order's fee payer
func (w *Store) Facilitator(ctx context.Context, q *store.Store, addr string) (bool, error) {
	if slices.Contains(w.s.Facilitators, addr) {
		return true, nil
	}
	return q.FeePayerSeen(ctx, addr)
}

// Block writes a block, keeping the longer of it and one already in force
func (w *Store) Block(ctx context.Context, tx pgx.Tx, b Block) error {
	ts := w.st.Tx(tx)
	if err := w.guard(ctx, ts, b.Payer); err != nil {
		return err
	}
	now := w.now()
	at, until, reason, by := now, b.Until, b.Reason, b.Actor
	cur, err := ts.Wallet(ctx, b.Payer, now)
	switch {
	case errors.Is(err, store.ErrNotFound):
	case err != nil:
		return err
	case cur.Blocked(now):
		at = *cur.BlockedAt
		// The block in force stays whole, the new one is only an event
		if cur.BlockedUntil == nil || until != nil && cur.BlockedUntil.After(*until) {
			until, reason, by = cur.BlockedUntil, cur.BlockedReason, cur.BlockedBy
		}
	}
	if err := ts.SetWalletBlock(ctx, b.Payer, at, until, reason, by); err != nil {
		return err
	}
	err = ts.InsertWalletEvent(ctx, store.WalletEvent{
		Payer: b.Payer, Kind: store.WalletBlock, Reason: b.Reason, PlacementID: b.PlacementID,
		Provider: b.Provider, Evidence: b.Evidence, Actor: b.Actor, CreatedAt: now,
	})
	if err != nil || !w.s.EmitEvents {
		return err
	}
	ev := blocked{Payer: b.Payer, Until: until, Reason: reason}
	if b.PlacementID != uuid.Nil {
		ev.PlacementID = &b.PlacementID
	}
	return ts.InsertOutbox(ctx, tx, EventBlocked, ev)
}

// blocked is wallet.blocked's payload
type blocked struct {
	Payer       string     `json:"payer"`
	Until       *time.Time `json:"until"`
	Reason      string     `json:"reason"`
	PlacementID *uuid.UUID `json:"placement_id"`
}

// unblocked is wallet.unblocked's payload
type unblocked struct {
	Payer string `json:"payer"`
}

// Strike records a strike, blocking the wallet once its strikes inside the window reach the limit,
// even under a shorter block in force
func (w *Store) Strike(ctx context.Context, tx pgx.Tx, s Strike) (bool, error) {
	ts := w.st.Tx(tx)
	if err := w.guard(ctx, ts, s.Payer); err != nil {
		return false, err
	}
	now := w.now()
	if err := ts.SeeWallet(ctx, s.Payer, now); err != nil {
		return false, err
	}
	err := ts.InsertWalletEvent(ctx, store.WalletEvent{
		Payer: s.Payer, Kind: store.WalletStrike, Reason: s.Reason, PlacementID: s.PlacementID,
		Provider: s.Provider, Evidence: s.Evidence, Actor: s.Actor, CreatedAt: now,
	})
	if err != nil {
		return false, err
	}
	n, err := ts.CountWalletEvents(ctx, s.Payer, store.WalletStrike, now.Add(-w.s.StrikeWindow))
	if err != nil || n < w.s.StrikeThreshold {
		return false, err
	}
	// A shorter block in force gives way, Block keeps a longer one
	until := now.Add(w.s.StrikeBlockFor)
	err = w.Block(ctx, tx, Block{
		Payer: s.Payer, Reason: ReasonStrikes, Until: &until, Actor: s.Actor,
		PlacementID: s.PlacementID, Provider: s.Provider, Evidence: s.Evidence,
	})
	return err == nil, err
}

// Unblock lifts a block, store.ErrNotFound when the router never saw the payer
func (w *Store) Unblock(ctx context.Context, tx pgx.Tx, payer, actor, note string) error {
	ts := w.st.Tx(tx)
	if err := ts.LockWallets(ctx, payer); err != nil {
		return err
	}
	if err := ts.ClearWalletBlock(ctx, payer); err != nil {
		return err
	}
	err := ts.InsertWalletEvent(ctx, store.WalletEvent{
		Payer: payer, Kind: store.WalletUnblock, Reason: note, Actor: actor,
		CreatedAt: w.now(),
	})
	if err != nil || !w.s.EmitEvents {
		return err
	}
	return ts.InsertOutbox(ctx, tx, EventUnblocked, unblocked{Payer: payer})
}

// Blocked reports whether the payer's block holds at now, reading by primary key with no cache
func (w *Store) Blocked(
	ctx context.Context, q store.Querier, payer string, now time.Time,
) (bool, error) {
	var at, until *time.Time
	err := q.QueryRow(ctx, `select blocked_at, blocked_until from wallets where payer = $1`,
		payer).Scan(&at, &until)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("select wallet block: %w", err)
	}
	return store.Wallet{BlockedAt: at, BlockedUntil: until}.Blocked(now), nil
}

// guard takes the payer's lock and refuses a facilitator address
func (w *Store) guard(ctx context.Context, ts *store.Store, payer string) error {
	if err := ts.LockWallets(ctx, payer); err != nil {
		return err
	}
	facilitator, err := w.Facilitator(ctx, ts, payer)
	if err != nil {
		return err
	}
	if facilitator {
		return ErrFacilitator
	}
	return nil
}
