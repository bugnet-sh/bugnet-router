package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// PlacementByPreview finds the placement of a preview by the preview's id, or ErrNotFound
func (s *Store) PlacementByPreview(ctx context.Context, previewID string) (Placement, error) {
	return s.placementWhere(ctx, "id = (select placement_id from previews where id = $1)",
		previewID)
}

// PlacementByTransfer finds the placement of a transfer by the SHA-256 of its token, through
// transfer_index, or ErrNotFound
func (s *Store) PlacementByTransfer(ctx context.Context, tokenSHA256 []byte) (Placement, error) {
	return s.placementWhere(ctx,
		"id = (select placement_id from transfer_index where token_sha256 = $1)", tokenSHA256)
}

// SignalFilter narrows the signals ListSignals returns
type SignalFilter struct {
	// Since is the oldest signal returned, zero for all
	Since time.Time
	// Provider and Class match exactly, empty for any
	Provider string
	Class    string
	// HeldOnly returns only the signals whose wallet actions the correlation guard held
	HeldOnly bool
	Limit    int
}

// ListSignals returns the signals f matches, newest first
func (s *Store) ListSignals(ctx context.Context, f SignalFilter) ([]Signal, error) {
	rows, err := s.q.Query(ctx, `
		select id, class, provider, placement_id, source, observed, action,
			coalesce(held_class, ''), created_at
		from provider_signals
		where created_at >= $1 and ($2 = '' or provider = $2) and ($3 = '' or class = $3)
			and (not $4 or action = 'held')
		order by created_at desc, id desc limit $5`,
		utc(f.Since), f.Provider, f.Class, f.HeldOnly, f.Limit)
	if err != nil {
		return nil, fmt.Errorf("select provider signals: %w", err)
	}
	out, err := pgx.CollectRows(rows, scanSignal)
	if err != nil {
		return nil, fmt.Errorf("select provider signals: %w", err)
	}
	return out, nil
}
