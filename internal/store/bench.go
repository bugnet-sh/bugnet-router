package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Bench is one benchmark, whose samples carry its id
type Bench struct {
	ID                  string
	Provider            string
	SizeName            string
	Runs, Concurrency   int
	EstimatedCostMicros int64
	Actor               string
	CreatedAt           time.Time
	// FinishedAt is zero while the benchmark runs
	FinishedAt time.Time
}

// InsertBench stores a benchmark that has not finished
func (s *Store) InsertBench(ctx context.Context, b Bench) error {
	_, err := s.q.Exec(ctx, `
		insert into benches (id, provider, size_name, runs, concurrency, estimated_cost_micros,
			actor, created_at)
		values ($1, $2, $3, $4, $5, $6, $7, $8)`,
		b.ID, b.Provider, b.SizeName, b.Runs, b.Concurrency, b.EstimatedCostMicros, b.Actor,
		utc(b.CreatedAt))
	if err != nil {
		return fmt.Errorf("insert bench: %w", err)
	}
	return nil
}

// BenchByID reads a benchmark, ErrNotFound when there is none
func (s *Store) BenchByID(ctx context.Context, id string) (Bench, error) {
	var b Bench
	var finished *time.Time
	err := s.q.QueryRow(ctx, `
		select id, provider, size_name, runs, concurrency, estimated_cost_micros, actor,
			created_at, finished_at
		from benches where id = $1`, id).Scan(&b.ID, &b.Provider, &b.SizeName, &b.Runs,
		&b.Concurrency, &b.EstimatedCostMicros, &b.Actor, &b.CreatedAt, &finished)
	if errors.Is(err, pgx.ErrNoRows) {
		return Bench{}, ErrNotFound
	}
	if err != nil {
		return Bench{}, fmt.Errorf("select bench: %w", err)
	}
	b.CreatedAt = utc(b.CreatedAt)
	if finished != nil {
		b.FinishedAt = utc(*finished)
	}
	return b, nil
}

// FinishBench marks a benchmark finished at a time, keeping the first time it was given
func (s *Store) FinishBench(ctx context.Context, id string, at time.Time) error {
	_, err := s.q.Exec(ctx, `
		update benches set finished_at = $2 where id = $1 and finished_at is null`, id, utc(at))
	if err != nil {
		return fmt.Errorf("finish bench: %w", err)
	}
	return nil
}

// BenchSampleCount counts the samples stored under a benchmark
func (s *Store) BenchSampleCount(ctx context.Context, id string) (int, error) {
	var n int
	err := s.q.QueryRow(ctx, `select count(*) from start_samples where bench_id = $1`,
		id).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count bench samples: %w", err)
	}
	return n, nil
}

// BenchProbeJobs counts the probe jobs of runs from up to before to under a benchmark, and how
// many of them have not ended
func (s *Store) BenchProbeJobs(
	ctx context.Context, id string, from, to int,
) (total, live int, err error) {
	err = s.q.QueryRow(ctx, `
		select count(*), count(*) filter (where state in
			('available', 'pending', 'running', 'retryable', 'scheduled'))
		from river_job
		where kind = 'probe' and args->>'bench_id' = $1
			and coalesce((args->>'run')::int, 0) >= $2 and coalesce((args->>'run')::int, 0) < $3`,
		id, from, to).Scan(&total, &live)
	if err != nil {
		return 0, 0, fmt.Errorf("count bench probe jobs: %w", err)
	}
	return total, live, nil
}
