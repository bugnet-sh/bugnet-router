package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// NoPayer is the wallet lock's name for orders without a payer
const NoPayer = "-"

// Wallet event kinds
const (
	WalletBlock   = "block"
	WalletUnblock = "unblock"
	WalletStrike  = "strike"
	WalletNote    = "note"
)

// Wallet is one payer's row with its block, and counts read beside it
type Wallet struct {
	Payer     string
	FirstSeen time.Time
	LastSeen  time.Time
	// BlockedAt is nil when the wallet was never blocked or its block was lifted
	BlockedAt *time.Time
	// BlockedUntil is nil with BlockedAt set while the block lasts until an operator lifts it
	BlockedUntil  *time.Time
	BlockedReason string
	BlockedBy     string
	// Strikes counts the strikes since the time the read was given
	Strikes int
	// Live counts the placements placing, ready or terminating whose start order it paid
	Live int
}

// Blocked reports whether the wallet's block holds at now
func (w Wallet) Blocked(now time.Time) bool {
	return w.BlockedAt != nil && (w.BlockedUntil == nil || w.BlockedUntil.After(now))
}

// WalletEvent is one block, unblock, strike or note of a wallet
type WalletEvent struct {
	ID    uuid.UUID
	Payer string
	Kind  string
	// Reason is the block's reason, or the operator's note
	Reason string
	// PlacementID is uuid.Nil when the event names no placement
	PlacementID uuid.UUID
	Provider    string
	// Evidence is JSON, never secrets, an empty object when nil
	Evidence  json.RawMessage
	Actor     string
	CreatedAt time.Time
}

// WalletPage asks for one page of wallets in payer order
type WalletPage struct {
	BlockedOnly bool
	// After is the last payer of the previous page, empty for the first
	After string
	Limit int
	Now   time.Time
	// StrikesSince starts the window strikes are counted in
	StrikesSince time.Time
}

const liveStates = `('placing', 'ready', 'terminating')`

const walletColumns = `w.payer, w.first_seen, w.last_seen, w.blocked_at, w.blocked_until,
	coalesce(w.blocked_reason, ''), coalesce(w.blocked_by, ''),
	(select count(*) from wallet_events e
		where e.payer = w.payer and e.kind = 'strike' and e.created_at > $1),
	(select count(*) from placements p where p.payer = w.payer and p.state in ` + liveStates + `)`

func scanWallet(row pgx.Row) (Wallet, error) {
	var w Wallet
	err := row.Scan(&w.Payer, &w.FirstSeen, &w.LastSeen, &w.BlockedAt, &w.BlockedUntil,
		&w.BlockedReason, &w.BlockedBy, &w.Strikes, &w.Live)
	if errors.Is(err, pgx.ErrNoRows) {
		return Wallet{}, ErrNotFound
	}
	if err != nil {
		return Wallet{}, err
	}
	w.FirstSeen, w.LastSeen = utc(w.FirstSeen), utc(w.LastSeen)
	w.BlockedAt, w.BlockedUntil = utcPtr(w.BlockedAt), utcPtr(w.BlockedUntil)
	return w, nil
}

// LockWallets takes each payer's transaction lock in sorted order, NoPayer standing for none
func (s *Store) LockWallets(ctx context.Context, payers ...string) error {
	names := make([]string, 0, len(payers))
	for _, p := range payers {
		if p == "" {
			p = NoPayer
		}
		names = append(names, p)
	}
	slices.Sort(names)
	for _, name := range slices.Compact(names) {
		_, err := s.q.Exec(ctx,
			`select pg_advisory_xact_lock(hashtextextended('bugnet:wallet:' || $1, 0))`, name)
		if err != nil {
			return fmt.Errorf("lock wallet: %w", err)
		}
	}
	return nil
}

// Wallet returns a payer's row with its strikes since strikesSince, or ErrNotFound
func (s *Store) Wallet(ctx context.Context, payer string, strikesSince time.Time) (Wallet, error) {
	w, err := scanWallet(s.q.QueryRow(ctx, `select `+walletColumns+
		` from wallets w where w.payer = $2`, utc(strikesSince), payer))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return Wallet{}, fmt.Errorf("select wallet: %w", err)
	}
	return w, err
}

// ListWallets returns one page of wallets in payer order
func (s *Store) ListWallets(ctx context.Context, pg WalletPage) ([]Wallet, error) {
	rows, err := s.q.Query(ctx, `select `+walletColumns+` from wallets w
		where w.payer > $2 and (not $3 or (w.blocked_at is not null
			and (w.blocked_until is null or w.blocked_until > $4)))
		order by w.payer limit $5`,
		utc(pg.StrikesSince), pg.After, pg.BlockedOnly, utc(pg.Now), pg.Limit)
	if err != nil {
		return nil, fmt.Errorf("list wallets: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Wallet, error) {
		return scanWallet(r)
	})
	if err != nil {
		return nil, fmt.Errorf("list wallets: %w", err)
	}
	return out, nil
}

// SeeWallet records a payer's new order, moving last_seen at most once an hour
func (s *Store) SeeWallet(ctx context.Context, payer string, at time.Time) error {
	_, err := s.q.Exec(ctx, `
		insert into wallets (payer, first_seen, last_seen) values ($1, $2, $2)
		on conflict (payer) do update set last_seen = excluded.last_seen
		where wallets.last_seen < excluded.last_seen - interval '1 hour'`, payer, utc(at))
	if err != nil {
		return fmt.Errorf("see wallet: %w", err)
	}
	return nil
}

// SetWalletBlock writes a block, creating the wallet's row when it has none
func (s *Store) SetWalletBlock(
	ctx context.Context, payer string, at time.Time, until *time.Time, reason, by string,
) error {
	_, err := s.q.Exec(ctx, `
		insert into wallets (payer, first_seen, last_seen, blocked_at, blocked_until,
			blocked_reason, blocked_by)
		values ($1, $2, $2, $2, $3, $4, $5)
		on conflict (payer) do update set blocked_at = excluded.blocked_at,
			blocked_until = excluded.blocked_until, blocked_reason = excluded.blocked_reason,
			blocked_by = excluded.blocked_by`,
		payer, utc(at), utcPtr(until), reason, by)
	if err != nil {
		return fmt.Errorf("block wallet: %w", err)
	}
	return nil
}

// ClearWalletBlock lifts a wallet's block, ErrNotFound when the router never saw the payer
func (s *Store) ClearWalletBlock(ctx context.Context, payer string) error {
	tag, err := s.q.Exec(ctx, `
		update wallets set blocked_at = null, blocked_until = null, blocked_reason = null,
			blocked_by = null
		where payer = $1`, payer)
	if err != nil {
		return fmt.Errorf("unblock wallet: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("unblock wallet: %w", ErrNotFound)
	}
	return nil
}

// InsertWalletEvent records one event, with a new ID when it has none
func (s *Store) InsertWalletEvent(ctx context.Context, e WalletEvent) error {
	if e.ID == uuid.Nil {
		e.ID = uuid.New()
	}
	evidence := e.Evidence
	if len(evidence) == 0 {
		evidence = json.RawMessage(`{}`)
	}
	var placement *uuid.UUID
	if e.PlacementID != uuid.Nil {
		placement = &e.PlacementID
	}
	_, err := s.q.Exec(ctx, `
		insert into wallet_events (id, payer, kind, reason, placement_id, provider, evidence,
			actor, created_at)
		values ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		e.ID, e.Payer, e.Kind, e.Reason, placement, nullIfEmpty(e.Provider), evidence, e.Actor,
		utc(e.CreatedAt))
	if err != nil {
		return fmt.Errorf("insert wallet event: %w", err)
	}
	return nil
}

// WalletEvents returns a payer's newest events first, at most limit
func (s *Store) WalletEvents(ctx context.Context, payer string, limit int) ([]WalletEvent, error) {
	rows, err := s.q.Query(ctx, `
		select id, payer, kind, reason, placement_id, coalesce(provider, ''), evidence, actor,
			created_at
		from wallet_events where payer = $1 order by created_at desc, id limit $2`,
		payer, limit)
	if err != nil {
		return nil, fmt.Errorf("select wallet events: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (WalletEvent, error) {
		var e WalletEvent
		var placement *uuid.UUID
		err := r.Scan(&e.ID, &e.Payer, &e.Kind, &e.Reason, &placement, &e.Provider,
			&e.Evidence, &e.Actor, &e.CreatedAt)
		if placement != nil {
			e.PlacementID = *placement
		}
		e.CreatedAt = utc(e.CreatedAt)
		return e, err
	})
	if err != nil {
		return nil, fmt.Errorf("select wallet events: %w", err)
	}
	return out, nil
}

// CountWalletEvents counts a payer's events of one kind after since
func (s *Store) CountWalletEvents(
	ctx context.Context, payer, kind string, since time.Time,
) (int, error) {
	var n int
	err := s.q.QueryRow(ctx, `
		select count(*) from wallet_events where payer = $1 and kind = $2 and created_at > $3`,
		payer, kind, utc(since)).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count wallet events: %w", err)
	}
	return n, nil
}

// LiveCount counts the live placements whose start order payer paid, every unpaid one for ""
func (s *Store) LiveCount(ctx context.Context, payer string) (int, error) {
	var n int
	err := s.q.QueryRow(ctx, `
		select count(*) from placements
		where state in `+liveStates+` and payer is not distinct from $1`,
		nullIfEmpty(payer)).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count live placements: %w", err)
	}
	return n, nil
}

// LivePlacementsOf returns the live placements payer paid a start of, as decided, or claims to
// have paid an extension of. The caller decides each such extension's payer again.
func (s *Store) LivePlacementsOf(ctx context.Context, payer string) ([]Placement, error) {
	rows, err := s.q.Query(ctx, `select `+placementColumns+` from placements
		where state in `+liveStates+` and (payer = $1 or id in (
			select placement_id from orders where payer = $1 and kind = $2))
		order by created_at, id`, payer, OrderExtend)
	if err != nil {
		return nil, fmt.Errorf("select live placements of a payer: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Placement, error) {
		return s.scanPlacement(r)
	})
	if err != nil {
		return nil, fmt.Errorf("select live placements of a payer: %w", err)
	}
	return out, nil
}

// ExtendClaims returns each distinct payer and fee_payer claim of a placement's extension orders
// that claim a payer, the payer as the token named it
func (s *Store) ExtendClaims(ctx context.Context, id uuid.UUID) ([]Order, error) {
	rows, err := s.q.Query(ctx, `
		select distinct payer, coalesce(fee_payer, '') from orders
		where placement_id = $1 and kind = $2 and payer is not null
		order by 1, 2`, id, OrderExtend)
	if err != nil {
		return nil, fmt.Errorf("select extension claims: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Order, error) {
		o := Order{Kind: OrderExtend, PlacementID: id}
		return o, r.Scan(&o.Payer, &o.FeePayer)
	})
	if err != nil {
		return nil, fmt.Errorf("select extension claims: %w", err)
	}
	return out, nil
}

// FeePayerSeen reports whether any order carried addr as its fee_payer claim
func (s *Store) FeePayerSeen(ctx context.Context, addr string) (bool, error) {
	var seen bool
	err := s.q.QueryRow(ctx,
		`select exists (select 1 from orders where fee_payer = $1)`, addr).Scan(&seen)
	if err != nil {
		return false, fmt.Errorf("select fee payer: %w", err)
	}
	return seen, nil
}

// DayCount is one UTC day's count
type DayCount struct {
	Day   time.Time
	Count int64
}

// AnonymousRefusals counts per UTC day the starts without a payer refused at the anonymous cap
func (s *Store) AnonymousRefusals(ctx context.Context, since time.Time) ([]DayCount, error) {
	rows, err := s.q.Query(ctx, `select date_trunc('day', created_at at time zone 'UTC'),
			count(*)
		from placements
		where state = $1 and failure_reason = 'wallet_busy' and payer is null
			and created_at >= $2
		group by 1 order by 1`, PlacementFailed, utc(since))
	if err != nil {
		return nil, fmt.Errorf("anonymous refusals: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (DayCount, error) {
		var d DayCount
		err := r.Scan(&d.Day, &d.Count)
		d.Day = d.Day.UTC()
		return d, err
	})
	if err != nil {
		return nil, fmt.Errorf("anonymous refusals: %w", err)
	}
	return out, nil
}
