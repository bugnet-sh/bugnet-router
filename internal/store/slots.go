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
