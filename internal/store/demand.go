package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Pause reasons of pool_pauses
const (
	PauseDrained = "drained"
	PauseFailing = "failing"
	PauseSpend   = "spend"
)

// RecordMiss counts one order or reservation that wanted a pool and found it empty, in the
// minute it happened, in the caller's transaction
func (s *Store) RecordMiss(ctx context.Context, provider, size string, at time.Time) error {
	_, err := s.q.Exec(ctx, `
		insert into pool_misses (provider, size_name, minute, misses)
		values ($1, $2, date_trunc('minute', $3::timestamptz), 1)
		on conflict (provider, size_name, minute) do update set misses = pool_misses.misses + 1`,
		provider, size, utc(at))
	if err != nil {
		return fmt.Errorf("record pool miss: %w", err)
	}
	return nil
}

// Demand is what a pool saw over a window
type Demand struct {
	// Claims are the members claimed, Misses the orders and reservations that found none
	Claims, Misses int
}

// DemandSince counts a pool's claims and misses since a time
func (s *Store) DemandSince(
	ctx context.Context, provider, size string, since time.Time,
) (Demand, error) {
	var d Demand
	err := s.q.QueryRow(ctx, `
		select (select count(*) from pool_members
				where provider = $1 and size_name = $2 and claimed_at >= $3)::int,
			coalesce((select sum(misses) from pool_misses
				where provider = $1 and size_name = $2 and minute >= date_trunc('minute', $3::timestamptz)
				), 0)::int`, provider, size, utc(since)).Scan(&d.Claims, &d.Misses)
	if err != nil {
		return Demand{}, fmt.Errorf("count pool demand: %w", err)
	}
	return d, nil
}

// PoolTarget is the target pool_tick last stored for one pool
type PoolTarget struct {
	Provider, SizeName string
	Target             int
	ComputedAt         time.Time
}

// PutTarget stores a pool's target and when it was computed
func (s *Store) PutTarget(ctx context.Context, t PoolTarget) error {
	_, err := s.q.Exec(ctx, `
		insert into pool_targets (provider, size_name, target, computed_at)
		values ($1, $2, $3, $4)
		on conflict (provider, size_name) do update set target = excluded.target,
			computed_at = excluded.computed_at`,
		t.Provider, t.SizeName, t.Target, utc(t.ComputedAt))
	if err != nil {
		return fmt.Errorf("put pool target: %w", err)
	}
	return nil
}

// Targets returns every stored pool target
func (s *Store) Targets(ctx context.Context) ([]PoolTarget, error) {
	rows, err := s.q.Query(ctx, `
		select provider, size_name, target, computed_at from pool_targets
		order by provider, size_name`)
	if err != nil {
		return nil, fmt.Errorf("select pool targets: %w", err)
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (PoolTarget, error) {
		var t PoolTarget
		err := r.Scan(&t.Provider, &t.SizeName, &t.Target, &t.ComputedAt)
		t.ComputedAt = utc(t.ComputedAt)
		return t, err
	})
}

// DeleteTargetsExcept drops the targets of pools no longer configured, keep naming those that are
func (s *Store) DeleteTargetsExcept(ctx context.Context, keep [][2]string) error {
	providers, sizes := make([]string, len(keep)), make([]string, len(keep))
	for i, k := range keep {
		providers[i], sizes[i] = k[0], k[1]
	}
	_, err := s.q.Exec(ctx, `
		delete from pool_targets t where not exists (
			select 1 from unnest($1::text[], $2::text[]) k(provider, size_name)
			where k.provider = t.provider and k.size_name = t.size_name)`, providers, sizes)
	if err != nil {
		return fmt.Errorf("delete pool targets: %w", err)
	}
	return nil
}

// Pause is a pool whose refill is held off until a time
type Pause struct {
	Provider, SizeName string
	Until              time.Time
	Reason, Actor      string
}

// PutPause holds a pool's refill off until a time, replacing an earlier pause
func (s *Store) PutPause(ctx context.Context, p Pause) error {
	_, err := s.q.Exec(ctx, `
		insert into pool_pauses (provider, size_name, paused_until, reason, actor)
		values ($1, $2, $3, $4, nullif($5, ''))
		on conflict (provider, size_name) do update set paused_until = excluded.paused_until,
			reason = excluded.reason, actor = excluded.actor`,
		p.Provider, p.SizeName, utc(p.Until), p.Reason, p.Actor)
	if err != nil {
		return fmt.Errorf("put pool pause: %w", err)
	}
	return nil
}

// Pauses returns every pause row, an expired one included
func (s *Store) Pauses(ctx context.Context) ([]Pause, error) {
	rows, err := s.q.Query(ctx, `
		select provider, size_name, paused_until, reason, coalesce(actor, '') from pool_pauses
		order by provider, size_name`)
	if err != nil {
		return nil, fmt.Errorf("select pool pauses: %w", err)
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Pause, error) {
		var p Pause
		err := r.Scan(&p.Provider, &p.SizeName, &p.Until, &p.Reason, &p.Actor)
		p.Until = utc(p.Until)
		return p, err
	})
}

// RecentFillFailures counts the failed fills of a pool in a row, newest first, among the members
// made after since, up to limit
func (s *Store) RecentFillFailures(
	ctx context.Context, provider, size string, since time.Time, limit int,
) (int, error) {
	var n int
	err := s.q.QueryRow(ctx, `
		select count(*) filter (where warm_at is null and end_reason = 'failed')::int
		from (select warm_at, end_reason from pool_members
			where provider = $1 and size_name = $2 and origin = 'pool' and created_at > $3
				and state in ('retiring', 'gone', 'warm', 'held', 'bound')
			order by created_at desc limit $4) last`,
		provider, size, utc(since), limit).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count failed fills: %w", err)
	}
	return n, nil
}

// PoolStat is what Postgres says about one pool's members and demand
type PoolStat struct {
	Provider, SizeName string
	Warm, Filling      int
	Held               int
	ClaimsLastHour     int
	MissesLastHour     int
	// WastedLastDay counts members destroyed unused in the last day
	WastedLastDay    int
	FillP95, WakeP95 time.Duration
	// LastWarmChange is the last time a member became warm or left warm, zero when none did
	LastWarmChange time.Time
}

// PoolStats reads every pool's counts, claims, misses, waste and p95 times at now
func (s *Store) PoolStats(ctx context.Context, now time.Time) ([]PoolStat, error) {
	hour, day := utc(now.Add(-time.Hour)), utc(now.Add(-24*time.Hour))
	rows, err := s.q.Query(ctx, `
		with keys as (
			select provider, size_name from pool_members
			union select provider, size_name from pool_targets
			union select provider, size_name from pool_pauses)
		select k.provider, k.size_name,
			(select count(*) from pool_members m where m.provider = k.provider
				and m.size_name = k.size_name and m.state = 'warm')::int,
			(select count(*) from pool_members m where m.provider = k.provider
				and m.size_name = k.size_name and m.state = 'filling')::int,
			(select count(*) from pool_members m where m.provider = k.provider
				and m.size_name = k.size_name and m.state = 'held')::int,
			(select count(*) from pool_members m where m.provider = k.provider
				and m.size_name = k.size_name and m.claimed_at >= $1)::int,
			coalesce((select sum(x.misses) from pool_misses x where x.provider = k.provider
				and x.size_name = k.size_name and x.minute >= date_trunc('minute', $1::timestamptz)
				), 0)::int,
			(select count(*) from pool_members m where m.provider = k.provider
				and m.size_name = k.size_name and m.state = 'gone' and m.bound_at is null
				and m.ended_at >= $2)::int,
			coalesce((select percentile_disc(0.95) within group (order by s.ready_ms)
				from start_samples s where s.kind = 'pool' and s.outcome = 'ok'
					and s.provider = k.provider and s.size_name = k.size_name
					and s.started_at >= $2 and s.ready_ms is not null), 0)::int,
			coalesce((select percentile_disc(0.95) within group (order by s.wake_ms)
				from start_samples s where s.kind <> 'canary' and s.outcome = 'ok'
					and s.provider = k.provider and s.size_name = k.size_name
					and s.started_at >= $2 and s.wake_ms is not null), 0)::int,
			(select max(greatest(m.warm_at, m.claimed_at, m.ended_at)) from pool_members m
				where m.provider = k.provider and m.size_name = k.size_name)
		from keys k order by k.provider, k.size_name`, hour, day)
	if err != nil {
		return nil, fmt.Errorf("select pool stats: %w", err)
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (PoolStat, error) {
		var p PoolStat
		var fill, wake int
		var last *time.Time
		err := r.Scan(&p.Provider, &p.SizeName, &p.Warm, &p.Filling, &p.Held, &p.ClaimsLastHour,
			&p.MissesLastHour, &p.WastedLastDay, &fill, &wake, &last)
		if last != nil {
			p.LastWarmChange = utc(*last)
		}
		p.FillP95 = time.Duration(fill) * time.Millisecond
		p.WakeP95 = time.Duration(wake) * time.Millisecond
		return p, err
	})
}

// RefillP95 is the nearest-rank p95 of a pool's fills over the last day and how many there were
func (s *Store) RefillP95(
	ctx context.Context, provider, size string, since time.Time,
) (time.Duration, int, error) {
	var p95 *float64
	var n int
	err := s.q.QueryRow(ctx, `
		select percentile_disc(0.95) within group (order by ready_ms), count(*)::int
		from start_samples where kind = 'pool' and outcome = 'ok' and provider = $1
			and size_name = $2 and started_at >= $3 and ready_ms is not null`,
		provider, size, utc(since)).Scan(&p95, &n)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, fmt.Errorf("select refill p95: %w", err)
	}
	if p95 == nil {
		return 0, n, nil
	}
	return time.Duration(*p95 * float64(time.Millisecond)), n, nil
}
