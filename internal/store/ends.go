package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Ending is why and by whom a placement ends, recorded as it moves to terminating
type Ending struct {
	Reason     string
	Refundable bool
	// Evidence is JSON, never secrets, null when nil
	Evidence json.RawMessage
	Actor    string
	At       time.Time
}

// MarkPlacementEnding moves a ready placement to terminating for e, or records e on a terminating
// one, keeping the time it started terminating
func (s *Store) MarkPlacementEnding(ctx context.Context, id uuid.UUID, e Ending) error {
	var evidence any
	if len(e.Evidence) > 0 {
		evidence = e.Evidence
	}
	return s.transition(ctx, id, "mark ending", `
		update placements set state = $2, termination_reason = $3, refundable = $4,
			end_evidence = $5, end_actor = $6, terminating_at = coalesce(terminating_at, $7)
		where id = $1 and state in ($2, $8)`,
		PlacementTerminating, e.Reason, e.Refundable, evidence, nullIfEmpty(e.Actor), utc(e.At),
		PlacementReady)
}

// OrdersNewestFirst returns a placement's orders, its extensions newest first and its start last
func (s *Store) OrdersNewestFirst(ctx context.Context, placementID uuid.UUID) ([]Order, error) {
	rows, err := s.q.Query(ctx, `
		select order_id, kind, placement_id, coalesce(payer, ''), coalesce(fee_payer, ''),
			amount_micros, seconds_bought
		from orders where placement_id = $1
		order by kind = $2, created_at desc, order_id desc`, placementID, OrderStart)
	if err != nil {
		return nil, fmt.Errorf("select orders: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Order, error) {
		var o Order
		err := r.Scan(&o.ID, &o.Kind, &o.PlacementID, &o.Payer, &o.FeePayer, &o.AmountMicros,
			&o.SecondsBought)
		return o, err
	})
	if err != nil {
		return nil, fmt.Errorf("select orders: %w", err)
	}
	return out, nil
}

// FailedRefunds counts the refundable provider_failed ends of a start payer since then, every
// placement without a payer together for "", an end the correlation guard made left out
func (s *Store) FailedRefunds(ctx context.Context, payer string, since time.Time) (int, error) {
	var n int
	err := s.q.QueryRow(ctx, `
		select count(*) from placements
		where termination_reason = 'provider_failed' and refundable and terminating_at > $2
			and payer is not distinct from $1
			and not coalesce(end_evidence @> '{"correlated": true}', false)`,
		nullIfEmpty(payer), utc(since)).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count refunded provider failures: %w", err)
	}
	return n, nil
}

// Signal is one classified provider signal and what was done about it
type Signal struct {
	ID       uuid.UUID
	Class    string
	Provider string
	// PlacementID is uuid.Nil for an account signal
	PlacementID uuid.UUID
	Source      string
	// Observed is JSON, never secrets, an empty object when nil
	Observed json.RawMessage
	Action   string
	// HeldClass is the class the correlation guard held back, empty otherwise
	HeldClass string
	CreatedAt time.Time
}

// InsertSignal records a provider signal, with a new ID when it has none
func (s *Store) InsertSignal(ctx context.Context, sg Signal) error {
	if sg.ID == uuid.Nil {
		sg.ID = uuid.New()
	}
	observed := sg.Observed
	if len(observed) == 0 {
		observed = json.RawMessage(`{}`)
	}
	var placement *uuid.UUID
	if sg.PlacementID != uuid.Nil {
		placement = &sg.PlacementID
	}
	_, err := s.q.Exec(ctx, `
		insert into provider_signals (id, class, provider, placement_id, source, observed, action,
			held_class, created_at)
		values ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		sg.ID, sg.Class, sg.Provider, placement, sg.Source, observed, sg.Action,
		nullIfEmpty(sg.HeldClass), utc(sg.CreatedAt))
	if err != nil {
		return fmt.Errorf("insert provider signal: %w", err)
	}
	return nil
}

// Signals returns a placement's signals, oldest first
func (s *Store) Signals(ctx context.Context, placementID uuid.UUID) ([]Signal, error) {
	rows, err := s.q.Query(ctx, `
		select id, class, provider, placement_id, source, observed, action,
			coalesce(held_class, ''), created_at
		from provider_signals where placement_id = $1 order by created_at, id`, placementID)
	if err != nil {
		return nil, fmt.Errorf("select provider signals: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Signal, error) {
		var sg Signal
		var placement *uuid.UUID
		err := r.Scan(&sg.ID, &sg.Class, &sg.Provider, &placement, &sg.Source, &sg.Observed,
			&sg.Action, &sg.HeldClass, &sg.CreatedAt)
		if placement != nil {
			sg.PlacementID = *placement
		}
		sg.CreatedAt = utc(sg.CreatedAt)
		return sg, err
	})
	if err != nil {
		return nil, fmt.Errorf("select provider signals: %w", err)
	}
	return out, nil
}

// ExpiredBlocks returns at most limit payers whose timed block ended by now, in payer order
func (s *Store) ExpiredBlocks(ctx context.Context, now time.Time, limit int) ([]string, error) {
	rows, err := s.q.Query(ctx, `
		select payer from wallets
		where blocked_at is not null and blocked_until is not null and blocked_until <= $1
		order by payer limit $2`, utc(now), limit)
	if err != nil {
		return nil, fmt.Errorf("select expired blocks: %w", err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("select expired blocks: %w", err)
	}
	return out, nil
}
