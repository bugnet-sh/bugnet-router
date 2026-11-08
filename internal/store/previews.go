package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

var (
	// ErrPreviewRevoked means an operator revoked the placement's preview of that port
	ErrPreviewRevoked = errors.New("store: the preview of that port was revoked")
	// ErrTooManyPreviews means the placement already has its most live previews
	ErrTooManyPreviews = errors.New("store: the placement has its most previews")
)

// Preview is one port of a placement served at its own preview host
type Preview struct {
	ID          string
	PlacementID uuid.UUID
	Port        int
	// Generation moves on revocation, so every token of an earlier one stops working
	Generation uint32
	CreatedAt  time.Time
	RevokedAt  *time.Time
}

const previewColumns = `id, placement_id, port, generation, created_at, revoked_at`

func scanPreview(row pgx.Row) (Preview, error) {
	var pv Preview
	var gen int64
	err := row.Scan(&pv.ID, &pv.PlacementID, &pv.Port, &gen, &pv.CreatedAt, &pv.RevokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Preview{}, ErrNotFound
	}
	if err != nil {
		return Preview{}, err
	}
	pv.Generation = uint32(gen)
	pv.CreatedAt = utc(pv.CreatedAt)
	pv.RevokedAt = utcPtr(pv.RevokedAt)
	return pv, nil
}

// WithLock runs fn in a transaction holding the advisory lock name, so one holder runs at a time
func (s *Store) WithLock(ctx context.Context, name string, fn func(*Store) error) error {
	return s.WithTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `select pg_advisory_xact_lock(hashtextextended($1, 0))`,
			"bugnet:"+name); err != nil {
			return fmt.Errorf("lock %s: %w", name, err)
		}
		return fn(s.Tx(tx))
	})
}

// UpsertPreview returns the placement's preview of port, making it as id while under most
func (s *Store) UpsertPreview(
	ctx context.Context, placement uuid.UUID, port, most int, id string,
) (Preview, error) {
	var pv Preview
	err := s.WithLock(ctx, "previews:"+placement.String(), func(ts *Store) error {
		var err error
		pv, err = scanPreview(ts.q.QueryRow(ctx, `select `+previewColumns+
			` from previews where placement_id = $1 and port = $2`, placement, port))
		switch {
		case err == nil && pv.RevokedAt != nil:
			return ErrPreviewRevoked
		case err == nil:
			return nil
		case !errors.Is(err, ErrNotFound):
			return err
		}
		var live int
		if err := ts.q.QueryRow(ctx, `select count(*) from previews
			where placement_id = $1 and revoked_at is null`, placement).Scan(&live); err != nil {
			return err
		}
		if live >= most {
			return ErrTooManyPreviews
		}
		pv, err = scanPreview(ts.q.QueryRow(ctx, `insert into previews (id, placement_id, port)
			values ($1, $2, $3) returning `+previewColumns, id, placement, port))
		return err
	})
	switch {
	case errors.Is(err, ErrPreviewRevoked):
		return pv, err
	case errors.Is(err, ErrTooManyPreviews):
		return Preview{}, err
	case err != nil:
		return Preview{}, fmt.Errorf("upsert preview: %w", err)
	}
	return pv, nil
}

// PreviewByID returns a preview by its id, or ErrNotFound
func (s *Store) PreviewByID(ctx context.Context, id string) (Preview, error) {
	pv, err := scanPreview(s.q.QueryRow(ctx, `select `+previewColumns+
		` from previews where id = $1`, id))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return Preview{}, fmt.Errorf("select preview: %w", err)
	}
	return pv, err
}

// PreviewsByIDs returns the previews of ids that exist
func (s *Store) PreviewsByIDs(ctx context.Context, ids []string) ([]Preview, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := s.q.Query(ctx, `select `+previewColumns+` from previews where id = any($1)`, ids)
	if err != nil {
		return nil, fmt.Errorf("select previews: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Preview, error) {
		return scanPreview(r)
	})
	if err != nil {
		return nil, fmt.Errorf("select previews: %w", err)
	}
	return out, nil
}

// RevokePreview moves a preview's generation and marks it revoked, once, returning its row
func (s *Store) RevokePreview(ctx context.Context, id string) (Preview, error) {
	pv, err := scanPreview(s.q.QueryRow(ctx, `update previews
		set generation = generation + 1, revoked_at = now()
		where id = $1 and revoked_at is null returning `+previewColumns, id))
	if errors.Is(err, ErrNotFound) {
		return s.PreviewByID(ctx, id)
	}
	if err != nil {
		return Preview{}, fmt.Errorf("revoke preview: %w", err)
	}
	return pv, nil
}

// SetPlacementRoute stores a ready placement's sandbox ref, and gateAt as its gate time when set
func (s *Store) SetPlacementRoute(
	ctx context.Context, id uuid.UUID, ref provider.Ref, gateAt *time.Time,
) error {
	ref, err := s.sealRef(id, ref)
	if err != nil {
		return fmt.Errorf("set route of %s: %w", id, err)
	}
	raw, err := json.Marshal(ref)
	if err != nil {
		return fmt.Errorf("encode provider ref: %w", err)
	}
	return s.transition(ctx, id, "set route", `
		update placements set provider_ref = $2, gate_ready_at = coalesce($3, gate_ready_at)
		where id = $1 and state = $4`, raw, utcPtr(gateAt), PlacementReady)
}

// MarkGateReady records at as a ready placement's gate time unless it has one
func (s *Store) MarkGateReady(ctx context.Context, id uuid.UUID, at time.Time) error {
	return s.transition(ctx, id, "mark gate ready", `
		update placements set gate_ready_at = coalesce(gate_ready_at, $2)
		where id = $1 and state = $3`, at.UTC(), PlacementReady)
}
