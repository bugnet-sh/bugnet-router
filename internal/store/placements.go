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
	ID           uuid.UUID
	HandleSHA256 []byte
	OrderID      string
	PackageID    string
	State        string
	Provider     string
	ProviderRef  *provider.Ref
	SizeName     string
	Deadline     *time.Time
	CreatedAt    time.Time
	ReadyAt      *time.Time
	// TerminatedAt is when the placement ended, as terminated or failed, nil before it did
	TerminatedAt  *time.Time
	FailureReason string
	Refundable    bool
	SagaOwner     string
	SagaHeartbeat *time.Time
	// CapacityProvider is the provider whose capacity slot the placement holds, empty for none
	CapacityProvider string
	// TerminationReason is why the placement moved to terminating, empty before it did
	TerminationReason string
	TerminatingAt     *time.Time
	// KeyID names the data-plane root of the placement's gate key and placeholders
	KeyID string
	// SSHAlias is the placement's SSH alias, empty until it has one
	SSHAlias    string
	SourceLabel string
	// EdgeBudgetBytes is the edge bytes the placement may use both ways, 0 before it is set
	EdgeBudgetBytes   int64
	EgressBudgetBytes int64
	// EgressCAVersion is the Bugnet CA version installed in the sandbox, 0 for none
	EgressCAVersion int
	// SecretsVersion moves on every change to the placement's secrets
	SecretsVersion int64
	GateReadyAt    *time.Time
	// AppliedHosts are the hosts the last apply_secrets run wrote into the sandbox
	AppliedHosts []string
	// NeedsSecrets says the order asked for secrets, so x402 may be enabled while placing
	NeedsSecrets bool
}

// NewPlacement describes a placement to create, placing unless it has a FailureReason
type NewPlacement struct {
	ID            uuid.UUID
	HandleSHA256  []byte
	OrderID       string
	PackageID     string
	FailureReason string // creates the placement already failed and refundable
	// KeyID names the data-plane root the placement's derived values come from
	KeyID string
	// NeedsSecrets records that the order's requirements asked for secrets
	NeedsSecrets bool
}

// Order is a start or extend order the router took, with the placement it is for
type Order struct {
	ID          string
	Kind        string // OrderStart or OrderExtend
	PlacementID uuid.UUID
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
	coalesce(failure_reason, ''), refundable, coalesce(saga_owner, ''), saga_heartbeat,
	coalesce(capacity_provider, ''), coalesce(termination_reason, ''), terminating_at,
	coalesce(key_id, ''), coalesce(ssh_alias, ''), coalesce(source_label, ''), edge_budget_bytes,
	egress_budget_bytes, coalesce(egress_ca_version, 0), secrets_version, gate_ready_at,
	applied_hosts, needs_secrets`

func (s *Store) scanPlacement(row pgx.Row) (Placement, error) {
	var p Placement
	var ref []byte
	err := row.Scan(&p.ID, &p.HandleSHA256, &p.OrderID, &p.PackageID, &p.State, &p.Provider,
		&ref, &p.SizeName, &p.Deadline, &p.CreatedAt, &p.ReadyAt, &p.TerminatedAt,
		&p.FailureReason, &p.Refundable, &p.SagaOwner, &p.SagaHeartbeat, &p.CapacityProvider,
		&p.TerminationReason, &p.TerminatingAt, &p.KeyID, &p.SSHAlias, &p.SourceLabel,
		&p.EdgeBudgetBytes, &p.EgressBudgetBytes, &p.EgressCAVersion, &p.SecretsVersion,
		&p.GateReadyAt, &p.AppliedHosts, &p.NeedsSecrets)
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
		s.openRef(&p)
	}
	p.CreatedAt = utc(p.CreatedAt)
	p.Deadline = utcPtr(p.Deadline)
	p.ReadyAt = utcPtr(p.ReadyAt)
	p.TerminatedAt = utcPtr(p.TerminatedAt)
	p.SagaHeartbeat = utcPtr(p.SagaHeartbeat)
	p.TerminatingAt = utcPtr(p.TerminatingAt)
	p.GateReadyAt = utcPtr(p.GateReadyAt)
	return p, nil
}

func (s *Store) placementWhere(ctx context.Context, where string, arg any) (Placement, error) {
	q := `select ` + placementColumns + ` from placements where ` + where
	p, err := s.scanPlacement(s.q.QueryRow(ctx, q, arg))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return Placement{}, fmt.Errorf("select placement: %w", err)
	}
	return p, err
}

// PlacementByHandle returns the placement with this handle hash or ErrUnknownHandle
func (s *Store) PlacementByHandle(ctx context.Context, sha []byte) (Placement, error) {
	p, err := s.placementWhere(ctx, "handle_sha256 = $1", sha)
	if errors.Is(err, ErrNotFound) {
		return Placement{}, ErrUnknownHandle
	}
	return p, err
}

// PlacementByID returns the placement or ErrNotFound
func (s *Store) PlacementByID(ctx context.Context, id uuid.UUID) (Placement, error) {
	return s.placementWhere(ctx, "id = $1", id)
}

// PlacementsByIDs returns the placements of ids that exist, ordered by id
func (s *Store) PlacementsByIDs(ctx context.Context, ids []uuid.UUID) ([]Placement, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := s.q.Query(ctx, `select `+placementColumns+
		` from placements where id = any($1) order by id`, ids)
	if err != nil {
		return nil, fmt.Errorf("select placements: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Placement, error) {
		return s.scanPlacement(r)
	})
	if err != nil {
		return nil, fmt.Errorf("select placements: %w", err)
	}
	return out, nil
}

// OrderByID returns the order the router took under this id, or ErrNotFound
func (s *Store) OrderByID(ctx context.Context, id string) (Order, error) {
	o := Order{ID: id}
	err := s.q.QueryRow(ctx, `select kind, placement_id from orders where order_id = $1`, id).
		Scan(&o.Kind, &o.PlacementID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Order{}, ErrNotFound
	}
	if err != nil {
		return Order{}, fmt.Errorf("select order: %w", err)
	}
	return o, nil
}

// AddPlacementBudgets grows the placement's edge and egress budgets by these bytes
func (s *Store) AddPlacementBudgets(ctx context.Context, id uuid.UUID, edge, egress int64) error {
	tag, err := s.q.Exec(ctx, `
		update placements set edge_budget_bytes = edge_budget_bytes + $2,
			egress_budget_bytes = egress_budget_bytes + $3
		where id = $1`, id, edge, egress)
	if err != nil {
		return fmt.Errorf("add budgets: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("add budgets of %s: %w", id, ErrNotFound)
	}
	return nil
}

// PlacementForUpdate returns the placement and locks its row until the transaction ends
func (s *Store) PlacementForUpdate(ctx context.Context, id uuid.UUID) (Placement, error) {
	return s.placementWhere(ctx, "id = $1 for update", id)
}

// CreatePlacement records the start order and its placement in one transaction
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
			on conflict (order_id) do nothing`, p.OrderID, OrderStart, p.ID)
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
		failed := p.FailureReason != ""
		state := PlacementPlacing
		if failed {
			state = PlacementFailed
		}
		// now() is the transaction's start, so a placement created failed ends at its created_at
		_, err = tx.Exec(ctx, `
			insert into placements (id, handle_sha256, order_id, package_id, state,
				failure_reason, refundable, terminated_at, key_id, needs_secrets)
			values ($1, $2, $3, $4, $5, $6, $7, case when $7 then now() end, $8, $9)`,
			p.ID, p.HandleSHA256, p.OrderID, p.PackageID, state, nullIfEmpty(p.FailureReason),
			failed, nullIfEmpty(p.KeyID), p.NeedsSecrets)
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

// Attempts returns a placement's attempts in attempt order
func (s *Store) Attempts(ctx context.Context, placementID uuid.UUID) ([]Attempt, error) {
	rows, err := s.q.Query(ctx, `
		select placement_id, attempt, coalesce(provider, ''), coalesce(idempotency_key, ''),
			coalesce(state, ''), coalesce(error_code, ''), coalesce(error, '')
		from placement_attempts where placement_id = $1 order by attempt`, placementID)
	if err != nil {
		return nil, fmt.Errorf("select attempts: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Attempt, error) {
		var a Attempt
		err := r.Scan(&a.PlacementID, &a.Attempt, &a.Provider, &a.IdempotencyKey, &a.State,
			&a.ErrorCode, &a.Error)
		return a, err
	})
	if err != nil {
		return nil, fmt.Errorf("select attempts: %w", err)
	}
	return out, nil
}

// ReadyOrTerminatingPastDeadline returns the placements still to end whose deadline is before now
func (s *Store) ReadyOrTerminatingPastDeadline(
	ctx context.Context, now time.Time,
) ([]uuid.UUID, error) {
	return s.placementIDs(ctx, "ready or terminating past deadline", `
		select id from placements where state in ($1, $2) and deadline < $3 order by deadline`,
		PlacementReady, PlacementTerminating, utc(now))
}

// PastDeadlineCount counts the placements still to end whose deadline is before cutoff
func (s *Store) PastDeadlineCount(ctx context.Context, cutoff time.Time) (int, error) {
	var n int
	err := s.q.QueryRow(ctx, `
		select count(*) from placements where state in ($1, $2) and deadline < $3`,
		PlacementReady, PlacementTerminating, utc(cutoff)).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count placements past deadline: %w", err)
	}
	return n, nil
}

// LongTerminatingCount counts placements terminating since before cutoff, not yet past deadline
func (s *Store) LongTerminatingCount(ctx context.Context, cutoff time.Time) (int, error) {
	var n int
	err := s.q.QueryRow(ctx, `
		select count(*) from placements
		where state = $1 and (terminating_at is null or terminating_at < $2)
			and (deadline is null or deadline >= $2)`,
		PlacementTerminating, utc(cutoff)).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count placements long terminating: %w", err)
	}
	return n, nil
}

// TerminatingSince returns terminating placements that moved there before, oldest first
func (s *Store) TerminatingSince(ctx context.Context, before time.Time) ([]uuid.UUID, error) {
	return s.placementIDs(ctx, "terminating since", `
		select id from placements
		where state = $1 and (terminating_at is null or terminating_at < $2)
		order by terminating_at nulls first`, PlacementTerminating, utc(before))
}

// StalledPlacing returns placing placements with no heartbeat since beat, or none since created
func (s *Store) StalledPlacing(
	ctx context.Context, beat, created time.Time,
) ([]uuid.UUID, error) {
	return s.placementIDs(ctx, "stalled placing", `
		select id from placements where state = $1 and (saga_heartbeat < $2
			or (saga_heartbeat is null and created_at < $3))
		order by created_at`, PlacementPlacing, utc(beat), utc(created))
}

func (s *Store) placementIDs(
	ctx context.Context, name, sql string, args ...any,
) ([]uuid.UUID, error) {
	rows, err := s.q.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	return ids, nil
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
	ref, err := s.sealRef(id, ref)
	if err != nil {
		return fmt.Errorf("mark ready %s: %w", id, err)
	}
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

// MarkPlacementFailed moves a placing placement to failed, ending it now
func (s *Store) MarkPlacementFailed(
	ctx context.Context, id uuid.UUID, reason string, refundable bool,
) error {
	return s.transition(ctx, id, "mark failed", `
		update placements set state = $2, failure_reason = $3, refundable = $4,
			terminated_at = now()
		where id = $1 and state = $5`, PlacementFailed, reason, refundable, PlacementPlacing)
}

// MarkPlacementTerminating reports whether it moved a placing or ready placement to terminating
func (s *Store) MarkPlacementTerminating(
	ctx context.Context, id uuid.UUID, reason string, at time.Time,
) (bool, error) {
	tag, err := s.q.Exec(ctx, `
		update placements set state = $2, termination_reason = $3, terminating_at = $4
		where id = $1 and state in ($5, $6)`,
		id, PlacementTerminating, nullIfEmpty(reason), utc(at), PlacementPlacing, PlacementReady)
	if err != nil {
		return false, fmt.Errorf("mark terminating: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// MarkPlacementDue reports whether it moved a ready placement whose deadline is not after due
func (s *Store) MarkPlacementDue(
	ctx context.Context, id uuid.UUID, reason string, due, at time.Time,
) (bool, error) {
	// The deadline is checked under the row lock, so an extension committed first is honoured
	tag, err := s.q.Exec(ctx, `
		update placements set state = $2, termination_reason = $3, terminating_at = $4
		where id = $1 and state = $5 and deadline <= $6`,
		id, PlacementTerminating, nullIfEmpty(reason), utc(at), PlacementReady, utc(due))
	if err != nil {
		return false, fmt.Errorf("mark due: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// MarkPlacementTerminated ends a terminating placement at the given time, a repeat changes nothing
func (s *Store) MarkPlacementTerminated(ctx context.Context, id uuid.UUID, at time.Time) error {
	tag, err := s.q.Exec(ctx, `
		update placements set state = $2, terminated_at = $4
		where id = $1 and state = $3`, id, PlacementTerminated, PlacementTerminating, utc(at))
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

// HeartbeatPlacement records which saga owns a placing placement and when it last showed life
func (s *Store) HeartbeatPlacement(
	ctx context.Context, id uuid.UUID, owner string, at time.Time,
) error {
	return s.transition(ctx, id, "heartbeat", `
		update placements set saga_owner = $2, saga_heartbeat = $3
		where id = $1 and state = $4`, nullIfEmpty(owner), utc(at), PlacementPlacing)
}

// HasExtendOrder reports whether orderID is recorded as an extend order for this placement
func (s *Store) HasExtendOrder(
	ctx context.Context, orderID string, placementID uuid.UUID,
) (bool, error) {
	var found bool
	err := s.q.QueryRow(ctx, `
		select exists (select 1 from orders
			where order_id = $1 and kind = $2 and placement_id = $3)`,
		orderID, OrderExtend, placementID).Scan(&found)
	if err != nil {
		return false, fmt.Errorf("select extend order: %w", err)
	}
	return found, nil
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
			on conflict (order_id) do nothing`, orderID, OrderExtend, placementID)
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
			if kind != OrderExtend || owner != placementID {
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
