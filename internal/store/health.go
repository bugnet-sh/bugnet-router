package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// SampleKind says what started the sandbox a start sample timed
type SampleKind string

// Sample kinds
const (
	SamplePlacement SampleKind = "placement"
	SampleCanary    SampleKind = "canary"
	SampleBench     SampleKind = "bench"
	// SamplePool times the fill of one pool member
	SamplePool SampleKind = "pool"
	// SampleReserve times the start of a sandbox for one reservation
	SampleReserve SampleKind = "reserve"
)

// Start paths a sample or a placement records
const (
	PathCold   = "cold"
	PathPooled = "pooled"
	PathPool   = "pool"
	// PathReserved is a placement that bound a reservation's sandbox
	PathReserved = "reserved"
	// PathSpeculative is the sample path of a reservation that started its own sandbox
	PathSpeculative = "speculative"
)

// Sample outcomes, of which only ok and failed count in a provider's health
const (
	OutcomeOK          = "ok"
	OutcomeFailed      = "failed"
	OutcomeCapacity    = "capacity"
	OutcomeRateLimited = "rate_limited"
	OutcomeQuota       = "quota"
)

// Provider health states
const (
	HealthOK   = "ok"
	HealthSlow = "slow"
	HealthDown = "down"
)

// StartSample is the timing of one start, a negative step meaning it did not run
type StartSample struct {
	ID          uuid.UUID
	Kind        SampleKind
	BenchID     string
	PlacementID uuid.UUID // zero for probes
	Provider    string
	SizeName    string
	SandboxID   string
	StartedAt   time.Time
	// CreateMS to DestroyMS are milliseconds, negative for a step that did not run
	CreateMS, ReadyMS, RunnerMS, GateMS int
	FirstCmdMS, WarmCmdMS, DestroyMS    int
	Outcome                             string
	ErrorCode, Error                    string
	CostMicros                          int64
	Replica                             string
	// StartPath is cold, pooled or pool for a fill, empty when the sample says none
	StartPath string
	// MemberID is the pool member a fill made or an attempt claimed, the zero id for none
	MemberID uuid.UUID
	// WakeMS is the time to wake a parked member, negative when none was woken
	WakeMS int
	// ReservationID is the reservation a reserve sample or a bound start belongs to
	ReservationID uuid.UUID
	// Hedge says a second attempt ran beside this one
	Hedge bool
	// BindMS is CreatePlacement to ready and PaidToReadyMS payment to ready, zero for none
	BindMS, PaidToReadyMS int
	// FirstFrameMS is the first frame time of the placement's first job, read with the sample
	// and zero for none
	FirstFrameMS int
}

// NoStep marks a StartSample timing that did not run
const NoStep = -1

// maxSampleError bounds the error text of a start sample
const maxSampleError = 1000

// InsertStartSample stores one sample in a transaction that does not wait for the disk
func (s *Store) InsertStartSample(ctx context.Context, m StartSample) error {
	if m.ID == uuid.Nil {
		m.ID = uuid.New()
	}
	if len(m.Error) > maxSampleError {
		m.Error = m.Error[:maxSampleError]
	}
	return s.WithTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `set local synchronous_commit = off`); err != nil {
			return fmt.Errorf("insert start sample: %w", err)
		}
		_, err := tx.Exec(ctx, `
			insert into start_samples (id, kind, bench_id, placement_id, provider, size_name,
				sandbox_id, started_at, create_ms, ready_ms, runner_ms, gate_ms, first_cmd_ms,
				warm_cmd_ms, destroy_ms, outcome, error_code, error, cost_micros, replica,
				start_path, member_id, wake_ms, reservation_id, hedge, bind_ms, paid_to_ready_ms)
			values ($1, $2, nullif($3, ''), $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
				$16, $17, $18, $19, $20, nullif($21, ''), $22, $23, $24, $25, $26, $27)`,
			m.ID, string(m.Kind), m.BenchID, nilUUID(m.PlacementID), m.Provider, m.SizeName,
			m.SandboxID, utc(m.StartedAt), step(m.CreateMS), step(m.ReadyMS), step(m.RunnerMS),
			step(m.GateMS), step(m.FirstCmdMS), step(m.WarmCmdMS), step(m.DestroyMS), m.Outcome,
			m.ErrorCode, m.Error, m.CostMicros, m.Replica, m.StartPath, nilUUID(m.MemberID),
			step(m.WakeMS), nilUUID(m.ReservationID), m.Hedge, nullIf0(m.BindMS),
			nullIf0(m.PaidToReadyMS))
		if err != nil {
			return fmt.Errorf("insert start sample: %w", err)
		}
		return nil
	})
}

// nilUUID is nil for the zero id, so the column holds null
func nilUUID(id uuid.UUID) any {
	if id == uuid.Nil {
		return nil
	}
	return id
}

// step is nil for a timing that did not run
func step(ms int) any {
	if ms < 0 {
		return nil
	}
	return ms
}

// startSampleColumns are the columns collectSamples reads
const startSampleColumns = `id, kind, coalesce(bench_id, ''), placement_id, provider, size_name,
	sandbox_id, started_at, create_ms, ready_ms, runner_ms, gate_ms, first_cmd_ms, warm_cmd_ms,
	destroy_ms, outcome, error_code, error, cost_micros, replica, coalesce(start_path, ''),
	member_id, wake_ms, reservation_id, hedge, bind_ms, paid_to_ready_ms,
	coalesce((select j.first_frame_ms from jobs j where j.placement_id = start_samples.placement_id
		order by j.created_at, j.id limit 1), 0)`

// StartSamples returns the samples of provider since a time, oldest first
func (s *Store) StartSamples(
	ctx context.Context, provider string, since time.Time,
) ([]StartSample, error) {
	return s.SampleQuery(ctx, SampleFilter{Provider: provider, Since: since})
}

// SampleFilter picks start samples, each empty field matching every sample
type SampleFilter struct {
	BenchID  string
	Provider string
	Kinds    []SampleKind
	// Paths are the start paths to keep, hedged naming the samples of a start with a second
	// attempt, every path when empty
	Paths []string
	Since time.Time
}

// SampleQuery returns the samples f picks, oldest first
func (s *Store) SampleQuery(ctx context.Context, f SampleFilter) ([]StartSample, error) {
	kinds := make([]string, len(f.Kinds))
	for i, k := range f.Kinds {
		kinds[i] = string(k)
	}
	paths := f.Paths
	if paths == nil {
		paths = []string{}
	}
	rows, err := s.q.Query(ctx, `select `+startSampleColumns+`
		from start_samples
		where ($1 = '' or bench_id = $1) and ($2 = '' or provider = $2)
			and (cardinality($3::text[]) = 0 or kind = any($3)) and started_at >= $4
			and (cardinality($5::text[]) = 0 or coalesce(start_path, '') = any($5)
				or 'hedged' = any($5) and hedge)
		order by started_at, id`, f.BenchID, f.Provider, kinds, utc(f.Since), paths)
	if err != nil {
		return nil, fmt.Errorf("select start samples: %w", err)
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (StartSample, error) {
		var m StartSample
		var placement *uuid.UUID
		var t [7]*int
		var member, reservation *uuid.UUID
		var wake, bind, paid *int
		err := r.Scan(&m.ID, &m.Kind, &m.BenchID, &placement, &m.Provider, &m.SizeName,
			&m.SandboxID, &m.StartedAt, &t[0], &t[1], &t[2], &t[3], &t[4], &t[5], &t[6],
			&m.Outcome, &m.ErrorCode, &m.Error, &m.CostMicros, &m.Replica, &m.StartPath, &member,
			&wake, &reservation, &m.Hedge, &bind, &paid, &m.FirstFrameMS)
		if reservation != nil {
			m.ReservationID = *reservation
		}
		if bind != nil {
			m.BindMS = *bind
		}
		if paid != nil {
			m.PaidToReadyMS = *paid
		}
		if placement != nil {
			m.PlacementID = *placement
		}
		if member != nil {
			m.MemberID = *member
		}
		m.WakeMS = NoStep
		if wake != nil {
			m.WakeMS = *wake
		}
		m.StartedAt = utc(m.StartedAt)
		for i, into := range []*int{&m.CreateMS, &m.ReadyMS, &m.RunnerMS, &m.GateMS,
			&m.FirstCmdMS, &m.WarmCmdMS, &m.DestroyMS} {
			*into = NoStep
			if t[i] != nil {
				*into = *t[i]
			}
		}
		return m, err
	})
}

// ReserveProbeSlot takes a slot with no margin for a probe and records it, false when none is free
func (s *Store) ReserveProbeSlot(
	ctx context.Context, provider string, id uuid.UUID, at time.Time,
) (bool, error) {
	reserved := false
	err := s.WithTx(ctx, func(tx pgx.Tx) error {
		ts := s.Tx(tx)
		ok, err := ts.ReserveCapacity(ctx, provider, 0)
		if err != nil || !ok {
			return err
		}
		_, err = tx.Exec(ctx, `
			insert into probe_slots (id, provider, reserved_at) values ($1, $2, $3)`,
			id, provider, utc(at))
		if err != nil {
			return fmt.Errorf("record probe slot: %w", err)
		}
		reserved = true
		return nil
	})
	return reserved, err
}

// ReleaseProbeSlot frees a probe's slot once, a repeat or a row the recount removed freeing none
func (s *Store) ReleaseProbeSlot(ctx context.Context, provider string, id uuid.UUID) error {
	return s.WithTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `delete from probe_slots where id = $1`, id)
		if err != nil {
			return fmt.Errorf("delete probe slot: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return nil
		}
		return s.Tx(tx).ReleaseCapacity(ctx, provider)
	})
}

// ProbeSlotCount counts the probe slots of provider
func (s *Store) ProbeSlotCount(ctx context.Context, provider string) (int, error) {
	var n int
	err := s.q.QueryRow(ctx, `select count(*) from probe_slots where provider = $1`,
		provider).Scan(&n)
	return n, err
}

// HealthCounts is what the samples of a provider's window say about it
type HealthCounts struct {
	// OK and Failed count the samples that count in health, the other outcomes being left out
	OK, Failed int
	// ReadyP95 is the nearest-rank 95th percentile of ready_ms over ok samples, 0 without any
	ReadyP95 time.Duration
	// ColdTotalP95 and PooledTotalP95 are the nearest-rank 95th percentiles of whole attempts, the
	// steps from the start to ready added, over the ok samples of the cold and pooled paths
	ColdTotalP95, PooledTotalP95 time.Duration
	// LastCanaries are the outcomes of the last two canary samples that count, newest first
	LastCanaries []string
}

// HealthWindow reads what the samples of provider since a time say
func (s *Store) HealthWindow(
	ctx context.Context, provider string, since time.Time,
) (HealthCounts, error) {
	var c HealthCounts
	var p95, cold, pooled *float64
	err := s.q.QueryRow(ctx, `
		select count(*) filter (where outcome = 'ok'), count(*) filter (where outcome = 'failed'),
			percentile_disc(0.95) within group (order by ready_ms)
				filter (where outcome = 'ok' and ready_ms is not null and kind <> 'pool'
					and coalesce(start_path, 'cold') = 'cold'),
			percentile_disc(0.95) within group (order by `+attemptTotal+`)
				filter (where outcome = 'ok' and kind <> 'pool'
					and coalesce(start_path, 'cold') = 'cold'),
			percentile_disc(0.95) within group (order by `+attemptTotal+`)
				filter (where outcome = 'ok' and kind <> 'pool' and start_path = 'pooled')
		from start_samples where provider = $1 and started_at >= $2`,
		provider, utc(since)).Scan(&c.OK, &c.Failed, &p95, &cold, &pooled)
	if err != nil {
		return HealthCounts{}, fmt.Errorf("count start samples: %w", err)
	}
	for _, v := range []struct {
		from *float64
		to   *time.Duration
	}{{p95, &c.ReadyP95}, {cold, &c.ColdTotalP95}, {pooled, &c.PooledTotalP95}} {
		if v.from != nil {
			*v.to = time.Duration(*v.from * float64(time.Millisecond))
		}
	}
	rows, err := s.q.Query(ctx, `
		select outcome from start_samples
		where provider = $1 and started_at >= $2 and kind = 'canary'
			and outcome in ('ok', 'failed')
		order by started_at desc limit 2`, provider, utc(since))
	if err != nil {
		return HealthCounts{}, fmt.Errorf("select canary samples: %w", err)
	}
	if c.LastCanaries, err = pgx.CollectRows(rows, pgx.RowTo[string]); err != nil {
		return HealthCounts{}, fmt.Errorf("select canary samples: %w", err)
	}
	return c, nil
}

// attemptTotal is a whole attempt's time, its steps from the start to ready added
const attemptTotal = `(coalesce(ready_ms, 0) + coalesce(runner_ms, 0) + coalesce(gate_ms, 0)
	+ coalesce(wake_ms, 0))`

// HealthState is what a rollup writes to one provider's health row
type HealthState struct {
	State string
	// Samples, SuccessLB and ReadyP95 are the window's count, Wilson bound and ready p95
	Samples   int
	SuccessLB float64
	ReadyP95  time.Duration
	// ColdTotalP95 and PooledTotalP95 are the whole-attempt p95 figures hedges read
	ColdTotalP95, PooledTotalP95 time.Duration
}

// WriteHealth stores a rollup, moving state_since only when the state changes, and returns the
// state the row held before, empty for a new row
func (s *Store) WriteHealth(
	ctx context.Context, provider string, h HealthState, at time.Time,
) (string, error) {
	var before string
	err := s.WithTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			insert into provider_health (provider, state, state_since, updated_at)
			values ($1, $2, $3, $3) on conflict do nothing`, provider, h.State, utc(at))
		if err != nil {
			return fmt.Errorf("write health: %w", err)
		}
		err = tx.QueryRow(ctx, `
			select state from provider_health where provider = $1 for update`,
			provider).Scan(&before)
		if err != nil {
			return fmt.Errorf("write health: %w", err)
		}
		if tag.RowsAffected() == 1 {
			before = ""
		}
		_, err = tx.Exec(ctx, `
			update provider_health set state = $2,
				state_since = case when state = $2 then state_since else $3::timestamptz end,
				samples = $4, success_lb = $5, ready_p95_ms = $6, updated_at = $3,
				cold_total_p95_ms = $7, pooled_total_p95_ms = $8
			where provider = $1`, provider, h.State, utc(at), h.Samples, h.SuccessLB,
			int(h.ReadyP95/time.Millisecond), nullIf0(int(h.ColdTotalP95/time.Millisecond)),
			nullIf0(int(h.PooledTotalP95/time.Millisecond)))
		if err != nil {
			return fmt.Errorf("write health: %w", err)
		}
		return nil
	})
	return before, err
}

// CanaryFacts are the rows the canary schedule judges one provider by
type CanaryFacts struct {
	// LastCanary is the start of the newest canary sample, zero for none
	LastCanary time.Time
	// LastReady is when a placement got ready or a pool fill reached warm on the provider last,
	// zero for none
	LastReady time.Time
	// State is the shared state, ok without a row
	State string
	// Blocked is the account block, empty while the account works
	Blocked string
}

// CanaryFactsOf reads the facts that decide whether provider is due a canary
func (s *Store) CanaryFactsOf(ctx context.Context, provider string) (CanaryFacts, error) {
	f := CanaryFacts{State: HealthOK}
	var canary, ready *time.Time
	err := s.q.QueryRow(ctx, `
		select (select max(started_at) from start_samples
				where provider = $1 and kind = 'canary'),
			(select max(t) from (
				select max(ready_at) as t from placements where provider = $1
				union all select max(warm_at) from pool_members where provider = $1) r)`,
		provider).Scan(&canary, &ready)
	if err != nil {
		return f, fmt.Errorf("select canary facts: %w", err)
	}
	if canary != nil {
		f.LastCanary = utc(*canary)
	}
	if ready != nil {
		f.LastReady = utc(*ready)
	}
	var blocked *string
	err = s.q.QueryRow(ctx, `select state, blocked from provider_health where provider = $1`,
		provider).Scan(&f.State, &blocked)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return f, fmt.Errorf("select canary facts: %w", err)
	}
	if blocked != nil {
		f.Blocked = *blocked
	}
	return f, nil
}
