package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ReleaseAttemptSlot frees the slot one attempt holds, on its own row or the placement's
//
// A null capacity_attempt means attempt 1. An attempt that holds nothing frees nothing, so a
// loser's release never takes the winner's slot.
func (s *Store) ReleaseAttemptSlot(ctx context.Context, id uuid.UUID, attempt int) (bool, error) {
	released := false
	err := s.WithTx(ctx, func(tx pgx.Tx) error {
		var provider string
		// RETURNING sees the cleared column, so the provider comes from the locked row
		err := tx.QueryRow(ctx, `
			with held as (
				select id, capacity_provider from placements
				where id = $1 and capacity_provider is not null
					and coalesce(capacity_attempt, 1) = $2
				for update)
			update placements p set capacity_provider = null, capacity_attempt = null from held
			where p.id = held.id
			returning held.capacity_provider`, id, attempt).Scan(&provider)
		if errors.Is(err, pgx.ErrNoRows) {
			err = tx.QueryRow(ctx, `
				with held as (
					select placement_id, attempt, slot_provider from placement_attempts
					where placement_id = $1 and attempt = $2 and slot_provider is not null
					for update)
				update placement_attempts a set slot_provider = null from held
				where a.placement_id = held.placement_id and a.attempt = held.attempt
				returning held.slot_provider`, id, attempt).Scan(&provider)
		}
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("clear attempt slot: %w", err)
		}
		released = true
		return s.Tx(tx).ReleaseCapacity(ctx, provider)
	})
	if err != nil {
		return false, err
	}
	return released, nil
}

// ReleaseAttemptSlots frees every slot the placement's attempts hold on their own rows and
// returns how many it freed
func (s *Store) ReleaseAttemptSlots(ctx context.Context, id uuid.UUID) (int, error) {
	freed := 0
	err := s.WithTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			with held as (
				select placement_id, attempt, slot_provider from placement_attempts
				where placement_id = $1 and slot_provider is not null
				for update)
			update placement_attempts a set slot_provider = null from held
			where a.placement_id = held.placement_id and a.attempt = held.attempt
			returning held.slot_provider`, id)
		if err != nil {
			return fmt.Errorf("clear attempt slots: %w", err)
		}
		providers, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return fmt.Errorf("clear attempt slots: %w", err)
		}
		for _, provider := range providers {
			if err := s.Tx(tx).ReleaseCapacity(ctx, provider); err != nil {
				return err
			}
		}
		freed = len(providers)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return freed, nil
}

// ReserveHedgeCapacity takes a slot on provider for a hedge, held by the attempt's own row, which
// it creates, and reports false when the provider has none or the placement left placing
//
// A hedge never uses the placement row, so the first attempt's slot stays where it is. The attempt
// row is the hedge's from here on, and RecordAttempt keeps its slot.
func (s *Store) ReserveHedgeCapacity(
	ctx context.Context, id uuid.UUID, provider string, attempt int,
) (bool, error) {
	err := s.WithTx(ctx, func(tx pgx.Tx) error {
		ts := s.Tx(tx)
		cur, err := ts.PlacementForUpdate(ctx, id)
		if err != nil {
			return err
		}
		if cur.State != PlacementPlacing {
			return fmt.Errorf("hold hedge capacity %s: %w", id, ErrStateConflict)
		}
		ok, err := ts.ReserveCapacity(ctx, provider, s.reserveSlots)
		if err == nil && !ok {
			return errFull
		}
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
			insert into placement_attempts (placement_id, attempt, provider, hedge, slot_provider)
			values ($1, $2, $3, true, $3)
			on conflict (placement_id, attempt) do update
			set provider = excluded.provider, hedge = true, slot_provider = excluded.slot_provider`,
			id, attempt, provider)
		if err != nil {
			return fmt.Errorf("hold hedge capacity: %w", err)
		}
		return nil
	})
	if errors.Is(err, errFull) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// WinAttemptSlot makes the slot an attempt holds the placement row's, in the transaction that made
// that attempt's sandbox the placement's, and frees the slot the row held for another attempt
//
// A row that holds this attempt's slot already, or an attempt that holds none, changes nothing.
func (s *Store) WinAttemptSlot(ctx context.Context, id uuid.UUID, attempt int) error {
	var rowProvider *string
	var rowAttempt *int
	err := s.q.QueryRow(ctx, `
		select capacity_provider, capacity_attempt from placements where id = $1 for update`,
		id).Scan(&rowProvider, &rowAttempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("win attempt slot %s: %w", id, ErrNotFound)
	}
	if err != nil {
		return fmt.Errorf("win attempt slot: %w", err)
	}
	if rowProvider != nil && (rowAttempt == nil && attempt == 1 ||
		rowAttempt != nil && *rowAttempt == attempt) {
		return nil
	}
	var held *string
	err = s.q.QueryRow(ctx, `
		select slot_provider from placement_attempts
		where placement_id = $1 and attempt = $2 for update`, id, attempt).Scan(&held)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && held == nil {
		return nil
	}
	if err != nil {
		return fmt.Errorf("win attempt slot: %w", err)
	}
	// The attempt's slot moves to the placement row and the one the row held goes back
	if _, err := s.q.Exec(ctx, `
		update placements set capacity_provider = $2, capacity_attempt = $3 where id = $1`,
		id, *held, attempt); err != nil {
		return fmt.Errorf("move attempt slot to placement: %w", err)
	}
	if _, err := s.q.Exec(ctx, `
		update placement_attempts set slot_provider = null
		where placement_id = $1 and attempt = $2`, id, attempt); err != nil {
		return fmt.Errorf("clear attempt slot: %w", err)
	}
	if rowProvider != nil {
		return s.ReleaseCapacity(ctx, *rowProvider)
	}
	return nil
}

// SetPlacementHedged records that a second attempt ran beside the first
func (s *Store) SetPlacementHedged(ctx context.Context, id uuid.UUID) error {
	tag, err := s.q.Exec(ctx, `update placements set hedged = true where id = $1`, id)
	if err != nil {
		return fmt.Errorf("set hedged on %s: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("set hedged on %s: %w", id, ErrNotFound)
	}
	return nil
}
