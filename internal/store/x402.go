package store

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/bugnet-sh/bugnet-router/internal/x402"
)

// X402 service states, as the x402_services check names them
const (
	X402Applying = "applying"
	X402Active   = "active"
	X402Failed   = "failed"
	X402Disabled = "disabled"
)

// X402Service is one row of x402_services
type X402Service struct {
	PlacementID               uuid.UUID
	ServiceID                 string
	Spec                      x402.Spec
	State, FailureReason      string
	Requests, Paid, Uncertain int
	UpdatedAt                 time.Time
}

// X402Refusal is one reason's count for a placement
type X402Refusal struct {
	Reason string
	Count  int
	LastAt time.Time
}

// X402Services answers every row of the placement, disabled ones included, by service id
func (s *Store) X402Services(ctx context.Context, placement uuid.UUID) ([]X402Service, error) {
	rows, err := s.q.Query(ctx, `
		select placement_id, service_id, host, prefix, method, max_price_micros, pay_to, indexed,
			coalesce(paid_path, ''), state, coalesce(failure_reason, ''), requests, paid,
			uncertain, updated_at
		from x402_services where placement_id = $1 order by service_id`, placement)
	if err != nil {
		return nil, fmt.Errorf("select x402 services: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (X402Service, error) {
		var x X402Service
		err := r.Scan(&x.PlacementID, &x.ServiceID, &x.Spec.Host, &x.Spec.Prefix,
			&x.Spec.Method, &x.Spec.MaxPriceMicros, &x.Spec.PayTo, &x.Spec.Indexed,
			&x.Spec.PaidPath, &x.State, &x.FailureReason, &x.Requests, &x.Paid, &x.Uncertain,
			&x.UpdatedAt)
		x.UpdatedAt = utc(x.UpdatedAt)
		return x, err
	})
	if err != nil {
		return nil, fmt.Errorf("select x402 services: %w", err)
	}
	return out, nil
}

// PutX402Service inserts or updates one row, state applying and updated_at now()
func (s *Store) PutX402Service(ctx context.Context, row X402Service) error {
	// A failure reason belongs to the failed state alone, so a put clears it
	_, err := s.q.Exec(ctx, `
		insert into x402_services (placement_id, service_id, host, prefix, method,
			max_price_micros, pay_to, indexed, paid_path, state)
		values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		on conflict (placement_id, service_id) do update set
			max_price_micros = excluded.max_price_micros, pay_to = excluded.pay_to,
			indexed = excluded.indexed, paid_path = excluded.paid_path, state = excluded.state,
			failure_reason = null, updated_at = now()`,
		row.PlacementID, row.ServiceID, row.Spec.Host, row.Spec.Prefix, row.Spec.Method,
		row.Spec.MaxPriceMicros, row.Spec.PayTo, row.Spec.Indexed, nullIfEmpty(row.Spec.PaidPath),
		X402Applying)
	if err != nil {
		return fmt.Errorf("put x402 service: %w", err)
	}
	return nil
}

// DisableX402Service sets disabled, clears the failure reason, moves updated_at, keeps counters
func (s *Store) DisableX402Service(ctx context.Context, placement uuid.UUID, id string) error {
	tag, err := s.q.Exec(ctx, `
		update x402_services set state = $3, failure_reason = null, updated_at = now()
		where placement_id = $1 and service_id = $2`, placement, id, X402Disabled)
	if err != nil {
		return fmt.Errorf("disable x402 service: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("disable x402 service %s: %w", id, ErrNotFound)
	}
	return nil
}

// X402Refusals answers the placement's refusal counts by reason
func (s *Store) X402Refusals(ctx context.Context, placement uuid.UUID) ([]X402Refusal, error) {
	rows, err := s.q.Query(ctx, `
		select reason, count, last_at from x402_refusals
		where placement_id = $1 order by reason`, placement)
	if err != nil {
		return nil, fmt.Errorf("select x402 refusals: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (X402Refusal, error) {
		var x X402Refusal
		err := r.Scan(&x.Reason, &x.Count, &x.LastAt)
		x.LastAt = utc(x.LastAt)
		return x, err
	})
	if err != nil {
		return nil, fmt.Errorf("select x402 refusals: %w", err)
	}
	return out, nil
}

// SecretHosts answers every host of the placement's secrets, fetch_only included
func (s *Store) SecretHosts(ctx context.Context, placement uuid.UUID) ([]string, error) {
	rows, err := s.q.Query(ctx, `
		select distinct host from secrets, unnest(hosts) as host
		where placement_id = $1 order by host`, placement)
	if err != nil {
		return nil, fmt.Errorf("select secret hosts: %w", err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("select secret hosts: %w", err)
	}
	return out, nil
}

// LiveX402Hosts answers the hosts of the placement's x402 rows that are not disabled
func (s *Store) LiveX402Hosts(ctx context.Context, placement uuid.UUID) ([]string, error) {
	rows, err := s.q.Query(ctx, `
		select distinct host from x402_services
		where placement_id = $1 and state <> $2 order by host`, placement, X402Disabled)
	if err != nil {
		return nil, fmt.Errorf("select live x402 hosts: %w", err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("select live x402 hosts: %w", err)
	}
	return out, nil
}

// X402Loaded names a row as step 4 read it, so step 7 marks only that version
type X402Loaded struct {
	ServiceID string
	Host      string
	UpdatedAt time.Time
}

// LoadX402ForApply answers the rows not disabled, under the apply lock
func (s *Store) LoadX402ForApply(ctx context.Context, placement uuid.UUID) ([]X402Loaded, error) {
	rows, err := s.q.Query(ctx, `
		select service_id, host, updated_at from x402_services
		where placement_id = $1 and state <> $2 order by service_id`, placement, X402Disabled)
	if err != nil {
		return nil, fmt.Errorf("load x402 for apply: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (X402Loaded, error) {
		var x X402Loaded
		err := r.Scan(&x.ServiceID, &x.Host, &x.UpdatedAt)
		x.UpdatedAt = utc(x.UpdatedAt)
		return x, err
	})
	if err != nil {
		return nil, fmt.Errorf("load x402 for apply: %w", err)
	}
	return out, nil
}

// MarkX402Applied moves each loaded row still at its updated_at to state, with reason
func (s *Store) MarkX402Applied(
	ctx context.Context, placement uuid.UUID, rows []X402Loaded, state, reason string,
) error {
	ids := make([]string, len(rows))
	ats := make([]time.Time, len(rows))
	for i, r := range rows {
		ids[i], ats[i] = r.ServiceID, r.UpdatedAt
	}
	_, err := s.q.Exec(ctx, `
		update x402_services x set state = $2, failure_reason = $3
		from unnest($4::text[], $5::timestamptz[]) as l (id, at)
		where x.placement_id = $1 and x.service_id = l.id and x.updated_at = l.at`,
		placement, state, nullIfEmpty(reason), ids, ats)
	if err != nil {
		return fmt.Errorf("mark x402 applied: %w", err)
	}
	return nil
}

// SetAppliedHosts records the hosts the run wrote
func (s *Store) SetAppliedHosts(
	ctx context.Context, placement uuid.UUID, hosts []string,
) error {
	tag, err := s.q.Exec(ctx, `update placements set applied_hosts = $2 where id = $1`,
		placement, emptyIfNil(hosts))
	return rowChanged(tag.RowsAffected(), err, "set applied hosts", placement)
}
