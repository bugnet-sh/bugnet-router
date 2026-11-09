package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ReserveCapacity takes one slot and leaves margin free, counting what others use on the account
func (s *Store) ReserveCapacity(ctx context.Context, provider string, margin int) (bool, error) {
	var inUse int
	err := s.q.QueryRow(ctx, `
		insert into provider_capacity (provider, max_concurrent, in_use) values ($1, $2, 1)
		on conflict (provider) do update set in_use = provider_capacity.in_use + 1
		where provider_capacity.in_use + provider_capacity.external_in_use + $3
			< provider_capacity.max_concurrent
		returning in_use`, provider, DefaultMaxConcurrent, margin).Scan(&inUse)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("reserve capacity: %w", err)
	}
	return true, nil
}

// ReleaseCapacity frees one slot on provider and never goes below zero
func (s *Store) ReleaseCapacity(ctx context.Context, provider string) error {
	_, err := s.q.Exec(ctx, `
		update provider_capacity set in_use = greatest(in_use - 1, 0) where provider = $1`,
		provider)
	if err != nil {
		return fmt.Errorf("release capacity: %w", err)
	}
	return nil
}

// SetCapacity sets the maximum concurrent sandboxes for provider
func (s *Store) SetCapacity(ctx context.Context, provider string, limit int) error {
	_, err := s.q.Exec(ctx, `
		insert into provider_capacity (provider, max_concurrent) values ($1, $2)
		on conflict (provider) do update set max_concurrent = excluded.max_concurrent`,
		provider, limit)
	if err != nil {
		return fmt.Errorf("set capacity: %w", err)
	}
	return nil
}

// errFull rolls back a placement's claim on a provider that has no free slot
var errFull = errors.New("store: provider is full")

// ReservePlacementCapacity takes a slot on provider for a placing placement that holds none
func (s *Store) ReservePlacementCapacity(
	ctx context.Context, id uuid.UUID, provider string,
) (bool, error) {
	err := s.WithTx(ctx, func(tx pgx.Tx) error {
		ts := s.Tx(tx)
		// The placement row is locked before the capacity row, in the order a release takes them
		err := ts.transition(ctx, id, "hold capacity", `
			update placements set capacity_provider = $2
			where id = $1 and state = $3 and capacity_provider is null`,
			provider, PlacementPlacing)
		if err != nil {
			return err
		}
		ok, err := ts.ReserveCapacity(ctx, provider, s.reserveSlots)
		if err == nil && !ok {
			return errFull
		}
		return err
	})
	if errors.Is(err, errFull) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// ReleasePlacementCapacity frees the slot a placement holds and reports whether it held one
func (s *Store) ReleasePlacementCapacity(ctx context.Context, id uuid.UUID) (bool, error) {
	released := false
	err := s.WithTx(ctx, func(tx pgx.Tx) error {
		var provider string
		// RETURNING sees the cleared column, so the provider comes from the locked row
		err := tx.QueryRow(ctx, `
			with held as (
				select id, capacity_provider from placements
				where id = $1 and capacity_provider is not null
				for update)
			update placements p set capacity_provider = null from held
			where p.id = held.id
			returning held.capacity_provider`, id).Scan(&provider)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("clear held capacity: %w", err)
		}
		released = true
		return s.Tx(tx).ReleaseCapacity(ctx, provider)
	})
	if err != nil {
		return false, err
	}
	return released, nil
}

// ReleaseEndedCapacity frees every slot still held by a failed or terminated placement
func (s *Store) ReleaseEndedCapacity(ctx context.Context) (int, error) {
	released := 0
	err := s.WithTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			with held as (
				select id, capacity_provider from placements
				where state in ($1, $2) and capacity_provider is not null
				for update skip locked)
			update placements p set capacity_provider = null from held
			where p.id = held.id
			returning held.capacity_provider`, PlacementFailed, PlacementTerminated)
		if err != nil {
			return fmt.Errorf("clear ended capacity: %w", err)
		}
		providers, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return fmt.Errorf("clear ended capacity: %w", err)
		}
		for _, provider := range providers {
			if err := s.Tx(tx).ReleaseCapacity(ctx, provider); err != nil {
				return err
			}
		}
		released = len(providers)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return released, nil
}
