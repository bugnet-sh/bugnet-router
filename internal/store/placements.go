package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/oynozan/bugnet-router/internal/provider"
)

// Placement is a sandbox the router placed or is placing for an order
type Placement struct {
	ID            uuid.UUID
	HandleSHA256  []byte
	OrderID       string
	PackageID     string
	State         string
	Provider      string
	ProviderRef   *provider.Ref
	SizeName      string
	Deadline      *time.Time
	CreatedAt     time.Time
	ReadyAt       *time.Time
	TerminatedAt  *time.Time
	FailureReason string
	Refundable    bool
	SagaOwner     string
	SagaHeartbeat *time.Time
}

// NewPlacement describes a placement to create in the placing state
type NewPlacement struct {
	ID           uuid.UUID
	HandleSHA256 []byte
	OrderID      string
	PackageID    string
}

// Attempt is one try at creating a sandbox on a provider
type Attempt struct {
	PlacementID    uuid.UUID
	Attempt        int
	Provider       string
	IdempotencyKey string
	State          string
	ErrorCode      string
	Error          string
}

const placementColumns = `id, handle_sha256, order_id, package_id, state, coalesce(provider, ''),
	provider_ref, coalesce(size_name, ''), deadline, created_at, ready_at, terminated_at,
	coalesce(failure_reason, ''), refundable, coalesce(saga_owner, ''), saga_heartbeat`

func scanPlacement(row pgx.Row) (Placement, error) {
	var p Placement
	var ref []byte
	err := row.Scan(&p.ID, &p.HandleSHA256, &p.OrderID, &p.PackageID, &p.State, &p.Provider,
		&ref, &p.SizeName, &p.Deadline, &p.CreatedAt, &p.ReadyAt, &p.TerminatedAt,
		&p.FailureReason, &p.Refundable, &p.SagaOwner, &p.SagaHeartbeat)
	if errors.Is(err, pgx.ErrNoRows) {
		return Placement{}, ErrNotFound
	}
	if err != nil {
		return Placement{}, err
	}
	if ref != nil {
		p.ProviderRef = &provider.Ref{}
		if err := json.Unmarshal(ref, p.ProviderRef); err != nil {
			return Placement{}, fmt.Errorf("decode provider ref: %w", err)
		}
	}
	p.CreatedAt = utc(p.CreatedAt)
	p.Deadline = utcPtr(p.Deadline)
	p.ReadyAt = utcPtr(p.ReadyAt)
	p.TerminatedAt = utcPtr(p.TerminatedAt)
	p.SagaHeartbeat = utcPtr(p.SagaHeartbeat)
	return p, nil
}

func (s *Store) placementWhere(ctx context.Context, where string, arg any) (Placement, error) {
	q := `select ` + placementColumns + ` from placements where ` + where
	p, err := scanPlacement(s.q.QueryRow(ctx, q, arg))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return Placement{}, fmt.Errorf("select placement: %w", err)
	}
	return p, err
}

// PlacementByHandle returns the placement with this handle hash or ErrNotFound
func (s *Store) PlacementByHandle(ctx context.Context, sha []byte) (Placement, error) {
	return s.placementWhere(ctx, "handle_sha256 = $1", sha)
}

// PlacementByID returns the placement or ErrNotFound
func (s *Store) PlacementByID(ctx context.Context, id uuid.UUID) (Placement, error) {
	return s.placementWhere(ctx, "id = $1", id)
}

// PlacementForUpdate returns the placement and locks its row until the transaction ends
func (s *Store) PlacementForUpdate(ctx context.Context, id uuid.UUID) (Placement, error) {
	return s.placementWhere(ctx, "id = $1 for update", id)
}

// CreatePlacement records the start order and a placing placement in one transaction
func (s *Store) CreatePlacement(ctx context.Context, p NewPlacement) (Placement, bool, error) {
	if p.ID == uuid.Nil {
		p.ID = uuid.New()
	}
	var out Placement
	var created bool
	err := s.WithTx(ctx, func(tx pgx.Tx) error {
		ts := s.Tx(tx)
		tag, err := tx.Exec(ctx, `
			insert into orders (order_id, kind, placement_id) values ($1, $2, $3)
			on conflict (order_id) do nothing`, p.OrderID, orderStart, p.ID)
		if err != nil {
			return fmt.Errorf("insert order: %w", err)
		}
		if tag.RowsAffected() == 0 {
			var id uuid.UUID
			err := tx.QueryRow(ctx,
				`select placement_id from orders where order_id = $1`, p.OrderID).Scan(&id)
			if err != nil {
				return fmt.Errorf("select order: %w", err)
			}
			out, err = ts.PlacementByID(ctx, id)
			return err
		}
		_, err = tx.Exec(ctx, `
			insert into placements (id, handle_sha256, order_id, package_id, state)
			values ($1, $2, $3, $4, $5)`,
			p.ID, p.HandleSHA256, p.OrderID, p.PackageID, PlacementPlacing)
		if err != nil {
			return fmt.Errorf("insert placement: %w", err)
		}
		out, err = ts.PlacementByID(ctx, p.ID)
		created = true
		return err
	})
	if err != nil {
		return Placement{}, false, err
	}
	return out, created, nil
}

// RecordAttempt stores an attempt, replacing an earlier record of the same attempt number
func (s *Store) RecordAttempt(ctx context.Context, a Attempt) error {
	_, err := s.q.Exec(ctx, `
		insert into placement_attempts
			(placement_id, attempt, provider, idempotency_key, state, error_code, error)
		values ($1, $2, $3, $4, $5, $6, $7)
		on conflict (placement_id, attempt) do update set provider = excluded.provider,
			idempotency_key = excluded.idempotency_key, state = excluded.state,
			error_code = excluded.error_code, error = excluded.error`,
		a.PlacementID, a.Attempt, nullIfEmpty(a.Provider), nullIfEmpty(a.IdempotencyKey),
		nullIfEmpty(a.State), nullIfEmpty(a.ErrorCode), nullIfEmpty(a.Error))
	if err != nil {
		return fmt.Errorf("record attempt: %w", err)
	}
	return nil
}

func (s *Store) transition(ctx context.Context, id uuid.UUID, name, sql string, args ...any) error {
	tag, err := s.q.Exec(ctx, sql, append([]any{id}, args...)...)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%s %s: %w", name, id, ErrStateConflict)
	}
	return nil
}

// MarkPlacementReady moves a placing placement to ready with its sandbox and deadline
func (s *Store) MarkPlacementReady(
	ctx context.Context, id uuid.UUID, ref provider.Ref, size string, readyAt, deadline time.Time,
) error {
	raw, err := json.Marshal(ref)
	if err != nil {
		return fmt.Errorf("encode provider ref: %w", err)
	}
	return s.transition(ctx, id, "mark ready", `
		update placements set state = $2, provider = $3, provider_ref = $4, size_name = $5,
			ready_at = $6, deadline = $7
		where id = $1 and state = $8`,
		PlacementReady, ref.Provider, raw, size, utc(readyAt), utc(deadline), PlacementPlacing)
}

// MarkPlacementFailed moves a placing placement to failed
func (s *Store) MarkPlacementFailed(
	ctx context.Context, id uuid.UUID, reason string, refundable bool,
) error {
	return s.transition(ctx, id, "mark failed", `
		update placements set state = $2, failure_reason = $3, refundable = $4
		where id = $1 and state = $5`, PlacementFailed, reason, refundable, PlacementPlacing)
}

// MarkPlacementTerminating reports whether it moved a placing or ready placement to terminating
func (s *Store) MarkPlacementTerminating(ctx context.Context, id uuid.UUID) (bool, error) {
	tag, err := s.q.Exec(ctx, `
		update placements set state = $2 where id = $1 and state in ($3, $4)`,
		id, PlacementTerminating, PlacementPlacing, PlacementReady)
	if err != nil {
		return false, fmt.Errorf("mark terminating: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// MarkPlacementTerminated moves a terminating placement to terminated and stamps the time.
// A placement that is already terminated is left alone; any other state is ErrStateConflict.
func (s *Store) MarkPlacementTerminated(ctx context.Context, id uuid.UUID) error {
	tag, err := s.q.Exec(ctx, `
		update placements set state = $2, terminated_at = now()
		where id = $1 and state = $3`, id, PlacementTerminated, PlacementTerminating)
	if err != nil {
		return fmt.Errorf("mark terminated: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	cur, err := s.PlacementByID(ctx, id)
	if err != nil {
		return fmt.Errorf("mark terminated %s: %w", id, err)
	}
	if cur.State == PlacementTerminated {
		return nil
	}
	return fmt.Errorf("mark terminated %s in state %s: %w", id, cur.State, ErrStateConflict)
}

// ExtendDeadline pushes a ready placement's deadline out once per extend order
func (s *Store) ExtendDeadline(
	ctx context.Context, orderID string, placementID uuid.UUID, by time.Duration,
) (Placement, bool, error) {
	var out Placement
	var applied bool
	err := s.WithTx(ctx, func(tx pgx.Tx) error {
		ts := s.Tx(tx)
		cur, err := ts.PlacementByID(ctx, placementID)
		if err != nil {
			return err
		}
		out = cur
		tag, err := tx.Exec(ctx, `
			insert into orders (order_id, kind, placement_id) values ($1, $2, $3)
			on conflict (order_id) do nothing`, orderID, orderExtend, placementID)
		if err != nil {
			return fmt.Errorf("insert extend order: %w", err)
		}
		if tag.RowsAffected() == 0 {
			var kind string
			var owner uuid.UUID
			err := tx.QueryRow(ctx,
				`select kind, placement_id from orders where order_id = $1`,
				orderID).Scan(&kind, &owner)
			if err != nil {
				return fmt.Errorf("select order: %w", err)
			}
			if kind != orderExtend || owner != placementID {
				return fmt.Errorf("order %s already used as %s for %s: %w",
					orderID, kind, owner, ErrStateConflict)
			}
			out, err = ts.PlacementByID(ctx, placementID)
			return err
		}
		tag, err = tx.Exec(ctx, `
			update placements set deadline = deadline + make_interval(secs => $2)
			where id = $1 and state = $3`, placementID, by.Seconds(), PlacementReady)
		if err != nil {
			return fmt.Errorf("extend deadline: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("extend deadline %s in state %s: %w",
				placementID, cur.State, ErrStateConflict)
		}
		out, err = ts.PlacementByID(ctx, placementID)
		applied = true
		return err
	})
	if err != nil {
		return out, false, err
	}
	return out, applied, nil
}
