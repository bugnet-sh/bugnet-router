package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// RecountCapacity sets the provider's in_use to what its placements, attempts, pool members and
// probe slots hold and returns the old and new values
//
// It is CP 10.5's transaction, the capacity row locked first so every reserve and release that
// committed before is counted and one that waits adds its own after.
func (s *Store) RecountCapacity(
	ctx context.Context, provider string, probeAge time.Duration, now time.Time,
) (oldValue, newValue int, err error) {
	err = s.WithTx(ctx, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			select in_use from provider_capacity where provider = $1 for update`,
			provider).Scan(&oldValue)
		if errors.Is(err, pgx.ErrNoRows) {
			newValue = 0
			return nil
		}
		if err != nil {
			return fmt.Errorf("lock capacity: %w", err)
		}
		// Each statement takes its own snapshot under read committed
		_, err = tx.Exec(ctx, `
			delete from probe_slots where provider = $1 and reserved_at < $2`,
			provider, utc(now.Add(-probeAge)))
		if err != nil {
			return fmt.Errorf("delete stale probe slots: %w", err)
		}
		err = tx.QueryRow(ctx, `
			select (select count(*) from placements where capacity_provider = $1)
				+ (select count(*) from placement_attempts where slot_provider = $1)
				+ (select count(*) from pool_members where provider = $1 and holds_slot)
				+ (select count(*) from probe_slots where provider = $1)`,
			provider).Scan(&newValue)
		if err != nil {
			return fmt.Errorf("count held slots: %w", err)
		}
		_, err = tx.Exec(ctx, `
			update provider_capacity set in_use = $2 where provider = $1 and in_use <> $2`,
			provider, newValue)
		if err != nil {
			return fmt.Errorf("write recount: %w", err)
		}
		return nil
	})
	return oldValue, newValue, err
}

// CapacityProviders lists the providers that have a capacity row
func (s *Store) CapacityProviders(ctx context.Context) ([]string, error) {
	rows, err := s.q.Query(ctx, `select provider from provider_capacity order by provider`)
	if err != nil {
		return nil, fmt.Errorf("select capacity providers: %w", err)
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// SetForeignSandboxes stores how many sandboxes no known deployment owns on a provider
func (s *Store) SetForeignSandboxes(
	ctx context.Context, provider string, n int, at time.Time,
) error {
	_, err := s.q.Exec(ctx, `
		insert into provider_health (provider, foreign_sandboxes, updated_at) values ($1, $2, $3)
		on conflict (provider) do update set foreign_sandboxes = excluded.foreign_sandboxes`,
		provider, n, utc(at))
	if err != nil {
		return fmt.Errorf("write foreign sandboxes: %w", err)
	}
	return nil
}

// ForeignSandboxes reads each provider's last count of sandboxes no known deployment owns
func (s *Store) ForeignSandboxes(ctx context.Context) (map[string]int, error) {
	rows, err := s.q.Query(ctx, `select provider, foreign_sandboxes from provider_health`)
	if err != nil {
		return nil, fmt.Errorf("select foreign sandboxes: %w", err)
	}
	return collectCounts(rows)
}

// SetPlacementCost writes what a placement cost and what its edges and egress counted
func (s *Store) SetPlacementCost(
	ctx context.Context, id uuid.UUID, cost, edgeBytes, egressBytes int64,
) error {
	_, err := s.q.Exec(ctx, `
		update placements set cost_micros = $2, edge_bytes = $3, egress_bytes = $4
		where id = $1`, id, cost, edgeBytes, egressBytes)
	if err != nil {
		return fmt.Errorf("write placement cost: %w", err)
	}
	return nil
}

// AttemptSpan is the provider time of one attempt that made a sandbox
type AttemptSpan struct {
	Provider, SandboxID          string
	HourlyMicros, StartFeeMicros int64
	CreatedAt                    time.Time
	// DestroyedAt is nil while the sandbox lives
	DestroyedAt *time.Time
}

const spanColumns = `coalesce(provider, ''), sandbox_id, hourly_micros, start_fee_micros,
	created_at, destroyed_at`

func collectSpans(rows pgx.Rows) ([]AttemptSpan, error) {
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (AttemptSpan, error) {
		var a AttemptSpan
		err := r.Scan(&a.Provider, &a.SandboxID, &a.HourlyMicros, &a.StartFeeMicros,
			&a.CreatedAt, &a.DestroyedAt)
		a.CreatedAt = utc(a.CreatedAt)
		a.DestroyedAt = utcPtr(a.DestroyedAt)
		return a, err
	})
	if err != nil {
		return nil, fmt.Errorf("select attempt spans: %w", err)
	}
	return out, nil
}

// PlacementSpans lists the attempts of a placement that made a sandbox
func (s *Store) PlacementSpans(ctx context.Context, id uuid.UUID) ([]AttemptSpan, error) {
	rows, err := s.q.Query(ctx, `select `+spanColumns+` from placement_attempts
		where placement_id = $1 and sandbox_id is not null order by attempt`, id)
	if err != nil {
		return nil, fmt.Errorf("select attempt spans: %w", err)
	}
	return collectSpans(rows)
}

// SpansCreatedBetween lists a provider's attempts that made a sandbox in [from, to)
func (s *Store) SpansCreatedBetween(
	ctx context.Context, provider string, from, to time.Time,
) ([]AttemptSpan, error) {
	rows, err := s.q.Query(ctx, `select `+spanColumns+` from placement_attempts
		where provider = $1 and sandbox_id is not null and created_at >= $2 and created_at < $3
		order by created_at, attempt`, provider, utc(from), utc(to))
	if err != nil {
		return nil, fmt.Errorf("select attempt spans: %w", err)
	}
	return collectSpans(rows)
}

// ProbeCostBetween sums the cost of a provider's canary and bench sandboxes started in [from, to)
func (s *Store) ProbeCostBetween(
	ctx context.Context, provider string, from, to time.Time,
) (int64, error) {
	var n int64
	err := s.q.QueryRow(ctx, `
		select coalesce(sum(cost_micros), 0)::bigint from start_samples
		where provider = $1 and kind in ($4, $5) and started_at >= $2 and started_at < $3`,
		provider, utc(from), utc(to), SampleCanary, SampleBench).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("sum probe cost: %w", err)
	}
	return n, nil
}

// SandboxIDsBetween lists the ids of the sandboxes an attempt or a probe of ours made on a
// provider in [from, to)
func (s *Store) SandboxIDsBetween(
	ctx context.Context, provider string, from, to time.Time,
) ([]string, error) {
	rows, err := s.q.Query(ctx, `
		select sandbox_id from placement_attempts
		where provider = $1 and sandbox_id is not null and created_at >= $2 and created_at < $3
		union
		select sandbox_id from start_samples
		where provider = $1 and sandbox_id is not null and sandbox_id <> ''
			and started_at >= $2 and started_at < $3`, provider, utc(from), utc(to))
	if err != nil {
		return nil, fmt.Errorf("select sandbox ids: %w", err)
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// OursAmong returns which of ids an attempt or a probe of ours made on the provider
func (s *Store) OursAmong(
	ctx context.Context, provider string, ids []string,
) (map[string]bool, error) {
	rows, err := s.q.Query(ctx, `
		select sandbox_id from placement_attempts where provider = $1 and sandbox_id = any($2)
		union
		select sandbox_id from start_samples where provider = $1 and sandbox_id = any($2)`,
		provider, ids)
	if err != nil {
		return nil, fmt.Errorf("select known sandboxes: %w", err)
	}
	found, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("select known sandboxes: %w", err)
	}
	out := make(map[string]bool, len(found))
	for _, id := range found {
		out[id] = true
	}
	return out, nil
}

// CostDay is one provider's row of provider_cost_daily
type CostDay struct {
	Provider string
	// Day is the UTC date at midnight
	Day                       time.Time
	OursMicros, TheirsMicros  int64
	TheirsComplete            bool
	UnknownIDs                int
	Cursor                    string
	CheckedAt, FirstCheckedAt time.Time
	SeenUnknown               []string
}

const costDayColumns = `provider, day, ours_micros, theirs_micros, theirs_complete, unknown_ids,
	coalesce(cursor, ''), checked_at, first_checked_at, seen_unknown`

func scanCostDay(r pgx.Row) (CostDay, error) {
	var c CostDay
	err := r.Scan(&c.Provider, &c.Day, &c.OursMicros, &c.TheirsMicros, &c.TheirsComplete,
		&c.UnknownIDs, &c.Cursor, &c.CheckedAt, &c.FirstCheckedAt, &c.SeenUnknown)
	c.Day, c.CheckedAt, c.FirstCheckedAt = utc(c.Day), utc(c.CheckedAt), utc(c.FirstCheckedAt)
	return c, err
}

// CostDayFor returns the row of a provider and UTC day, found false when none was started
func (s *Store) CostDayFor(
	ctx context.Context, provider string, day time.Time, forUpdate bool,
) (CostDay, bool, error) {
	q := `select ` + costDayColumns + ` from provider_cost_daily where provider = $1 and day = $2`
	if forUpdate {
		q += ` for update`
	}
	c, err := scanCostDay(s.q.QueryRow(ctx, q, provider, utc(day)))
	if errors.Is(err, pgx.ErrNoRows) {
		return CostDay{}, false, nil
	}
	if err != nil {
		return CostDay{}, false, fmt.Errorf("select cost day: %w", err)
	}
	return c, true, nil
}

// SaveCostDay writes a provider's day, replacing the row
func (s *Store) SaveCostDay(ctx context.Context, c CostDay) error {
	seen := c.SeenUnknown
	if seen == nil {
		seen = []string{}
	}
	_, err := s.q.Exec(ctx, `
		insert into provider_cost_daily (provider, day, ours_micros, theirs_micros,
			theirs_complete, unknown_ids, cursor, checked_at, first_checked_at, seen_unknown)
		values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		on conflict (provider, day) do update set ours_micros = excluded.ours_micros,
			theirs_micros = excluded.theirs_micros, theirs_complete = excluded.theirs_complete,
			unknown_ids = excluded.unknown_ids, cursor = excluded.cursor,
			checked_at = excluded.checked_at, first_checked_at = excluded.first_checked_at,
			seen_unknown = excluded.seen_unknown`,
		c.Provider, utc(c.Day), c.OursMicros, c.TheirsMicros, c.TheirsComplete, c.UnknownIDs,
		nullIfEmpty(c.Cursor), utc(c.CheckedAt), utc(c.FirstCheckedAt), seen)
	if err != nil {
		return fmt.Errorf("save cost day: %w", err)
	}
	return nil
}

// UnfinishedCostDays lists the days of a provider from since on that its daily check must read
// again, those read to the end but still incomplete and those whose page chain stopped
// advancing before staleBefore
func (s *Store) UnfinishedCostDays(
	ctx context.Context, provider string, since, staleBefore time.Time,
) ([]time.Time, error) {
	rows, err := s.q.Query(ctx, `
		select day from provider_cost_daily
		where provider = $1 and day >= $2
			and ((cursor is null and not theirs_complete)
				or (cursor is not null and checked_at < $3))
		order by day`, provider, utc(since), utc(staleBefore))
	if err != nil {
		return nil, fmt.Errorf("select unfinished cost days: %w", err)
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (time.Time, error) {
		var d time.Time
		err := r.Scan(&d)
		return utc(d), err
	})
}

// CostAlerts lists the days since a time whose reading is done and shows a mismatch, a provider
// billing more than our meter by more than fraction or naming sandboxes that are not ours
func (s *Store) CostAlerts(
	ctx context.Context, since time.Time, fraction float64,
) ([]CostDay, error) {
	rows, err := s.q.Query(ctx, `select `+costDayColumns+` from provider_cost_daily
		where day >= $1 and cursor is null
			and (unknown_ids > 0 or (theirs_micros >= 0 and theirs_micros > ours_micros * (1 + $2)))
		order by day desc, provider`, utc(since), fraction)
	if err != nil {
		return nil, fmt.Errorf("select cost alerts: %w", err)
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (CostDay, error) {
		return scanCostDay(r)
	})
}

// MarginCounts counts the placements that ended since a time costing more than (1 - floor) of
// what their start and extension orders paid, and those costing more than they paid
func (s *Store) MarginCounts(
	ctx context.Context, since time.Time, floor float64,
) (below, loss int, err error) {
	err = s.q.QueryRow(ctx, `
		select count(*) filter (where cost > (1 - $2::float8) * paid),
			count(*) filter (where cost > paid)
		from (select p.cost_micros::float8 as cost,
				coalesce((select sum(o.amount_micros) from orders o
					where o.placement_id = p.id), 0)::float8 as paid
			from placements p
			where p.state = $3 and p.terminated_at > $1 and p.cost_micros > 0) t`,
		utc(since), floor, PlacementTerminated).Scan(&below, &loss)
	if err != nil {
		return 0, 0, fmt.Errorf("count margins: %w", err)
	}
	return below, loss, nil
}
