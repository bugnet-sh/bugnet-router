package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Job is one command run inside a placement
type Job struct {
	ID             string
	PlacementID    uuid.UUID
	IdempotencyKey string
	State          string
	ExitCode       *int
	StdoutBytes    int64
	StderrBytes    int64
	CreatedAt      time.Time
	FinishedAt     *time.Time
}

// NewJob describes a job to create in the running state
type NewJob struct {
	ID             string
	PlacementID    uuid.UUID
	IdempotencyKey string
}

const jobColumns = `id, placement_id, coalesce(idempotency_key, ''), state, exit_code,
	stdout_bytes, stderr_bytes, created_at, finished_at`

func scanJob(row pgx.Row) (Job, error) {
	var j Job
	err := row.Scan(&j.ID, &j.PlacementID, &j.IdempotencyKey, &j.State, &j.ExitCode,
		&j.StdoutBytes, &j.StderrBytes, &j.CreatedAt, &j.FinishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	if err != nil {
		return Job{}, err
	}
	j.CreatedAt = utc(j.CreatedAt)
	j.FinishedAt = utcPtr(j.FinishedAt)
	return j, nil
}

// CreateJob inserts j, or returns the existing job with created false when the ID is taken
func (s *Store) CreateJob(ctx context.Context, j NewJob) (Job, bool, error) {
	tag, err := s.q.Exec(ctx, `
		insert into jobs (id, placement_id, idempotency_key, state) values ($1, $2, $3, $4)
		on conflict (id) do nothing`,
		j.ID, j.PlacementID, nullIfEmpty(j.IdempotencyKey), JobRunning)
	if err != nil {
		return Job{}, false, fmt.Errorf("create job: %w", err)
	}
	job, err := s.JobByID(ctx, j.ID)
	if err != nil {
		return Job{}, false, err
	}
	return job, tag.RowsAffected() == 1, nil
}

// JobByID returns the job or ErrNotFound
func (s *Store) JobByID(ctx context.Context, id string) (Job, error) {
	j, err := scanJob(s.q.QueryRow(ctx, `select `+jobColumns+` from jobs where id = $1`, id))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return Job{}, fmt.Errorf("job by id: %w", err)
	}
	return j, err
}

// FinishJob records the outcome of a running job
func (s *Store) FinishJob(
	ctx context.Context, id, state string, code int, outBytes, errBytes int64,
) error {
	tag, err := s.q.Exec(ctx, `
		update jobs set state = $2, exit_code = $3, stdout_bytes = $4, stderr_bytes = $5,
			finished_at = now()
		where id = $1 and state = $6`, id, state, code, outBytes, errBytes, JobRunning)
	if err != nil {
		return fmt.Errorf("finish job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("finish job %s: %w", id, ErrStateConflict)
	}
	return nil
}

// RunningJobs returns every job still in the running state
func (s *Store) RunningJobs(ctx context.Context) ([]Job, error) {
	rows, err := s.q.Query(ctx,
		`select `+jobColumns+` from jobs where state = $1 order by created_at, id`, JobRunning)
	if err != nil {
		return nil, fmt.Errorf("running jobs: %w", err)
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("running jobs: %w", err)
	}
	return out, nil
}
