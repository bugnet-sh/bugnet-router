package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// ReserveCapacity takes one slot on provider, reporting false when it is full
func (s *Store) ReserveCapacity(ctx context.Context, provider string) (bool, error) {
	var inUse int
	err := s.q.QueryRow(ctx, `
		insert into provider_capacity (provider, max_concurrent, in_use) values ($1, $2, 1)
		on conflict (provider) do update set in_use = provider_capacity.in_use + 1
		where provider_capacity.in_use < provider_capacity.max_concurrent
		returning in_use`, provider, DefaultMaxConcurrent).Scan(&inUse)
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
