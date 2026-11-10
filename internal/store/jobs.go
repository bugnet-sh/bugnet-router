package store

import (
	"context"
	"errors"
	"fmt"
	"math"
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
	// OutputTruncated says the job's stream left output out, which its sandbox files still hold
	OutputTruncated bool
	// OOMKilled says the kernel's OOM killer ended a process of the job
	OOMKilled bool
	// PeakMemoryMiB is the job's peak memory, 0 when the sandbox could not measure it
	PeakMemoryMiB int
	// Install says the job's command is a known installer
	Install    bool
	CreatedAt  time.Time
	FinishedAt *time.Time
}

// NewJob describes a job to create in the running state
type NewJob struct {
	ID             string
	PlacementID    uuid.UUID
	IdempotencyKey string
	// Install marks a job whose command is a known installer
	Install bool
	Limit   int // refuses a new job while the placement runs this many, none when zero
}

// ErrJobLimit means the placement already runs as many jobs as the new job's Limit allows
var ErrJobLimit = errors.New("store: the placement runs as many jobs as it may")

const jobColumns = `id, placement_id, coalesce(idempotency_key, ''), state, exit_code,
	stdout_bytes, stderr_bytes, output_truncated, oom_killed, coalesce(peak_memory_mib, 0),
	install, created_at, finished_at`

func scanJob(row pgx.Row) (Job, error) {
	var j Job
	err := row.Scan(&j.ID, &j.PlacementID, &j.IdempotencyKey, &j.State, &j.ExitCode,
		&j.StdoutBytes, &j.StderrBytes, &j.OutputTruncated, &j.OOMKilled, &j.PeakMemoryMiB,
		&j.Install, &j.CreatedAt, &j.FinishedAt)
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
	if j.Limit > 0 {
		return s.createLimited(ctx, j)
	}
	tag, err := s.q.Exec(ctx, `
		insert into jobs (id, placement_id, idempotency_key, state, install)
		values ($1, $2, $3, $4, $5)
		on conflict (id) do nothing`,
		j.ID, j.PlacementID, nullIfEmpty(j.IdempotencyKey), JobRunning, j.Install)
	if err != nil {
		return Job{}, false, fmt.Errorf("create job: %w", err)
	}
	job, err := s.JobByID(ctx, j.ID)
	if err != nil {
		return Job{}, false, err
	}
	return job, tag.RowsAffected() == 1, nil
}

// createLimited counts and inserts under a lock per placement, so no two creates pass one limit
func (s *Store) createLimited(ctx context.Context, j NewJob) (Job, bool, error) {
	var job Job
	created := false
	err := s.WithTx(ctx, func(tx pgx.Tx) error {
		ts := s.Tx(tx)
		_, err := tx.Exec(ctx, `select pg_advisory_xact_lock(hashtextextended($1::text, 0))`,
			j.PlacementID)
		if err != nil {
			return fmt.Errorf("lock placement jobs: %w", err)
		}
		if job, err = ts.JobByID(ctx, j.ID); !errors.Is(err, ErrNotFound) {
			return err
		}
		var running int
		err = tx.QueryRow(ctx, `select count(*) from jobs where placement_id = $1 and state = $2`,
			j.PlacementID, JobRunning).Scan(&running)
		if err != nil {
			return fmt.Errorf("count running jobs: %w", err)
		}
		if running >= j.Limit {
			return fmt.Errorf("%w: %d of %d running", ErrJobLimit, running, j.Limit)
		}
		j.Limit = 0
		job, created, err = ts.CreateJob(ctx, j)
		return err
	})
	if err != nil {
		return Job{}, false, err
	}
	return job, created, nil
}

// JobByID returns the job or ErrNotFound
func (s *Store) JobByID(ctx context.Context, id string) (Job, error) {
	j, err := scanJob(s.q.QueryRow(ctx, `select `+jobColumns+` from jobs where id = $1`, id))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return Job{}, fmt.Errorf("job by id: %w", err)
	}
	return j, err
}

// PlacementJobIDs returns the ids of every job the placement ran, whatever its record holds
func (s *Store) PlacementJobIDs(ctx context.Context, placement uuid.UUID) ([]string, error) {
	rows, err := s.q.Query(ctx, `select id from jobs where placement_id = $1 order by id`,
		placement)
	if err != nil {
		return nil, fmt.Errorf("placement job ids: %w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("placement job ids: %w", err)
	}
	return ids, nil
}

// JobOutcome is how a job ended, a lost job keeping no exit code
type JobOutcome struct {
	State                    string
	Code                     int
	StdoutBytes, StderrBytes int64
	Truncated                bool // the job's stream left output out
	// OOMKilled and PeakMemoryMiB come from the exit frame
	OOMKilled     bool
	PeakMemoryMiB int
}

// FinishJob records the outcome of a running job
func (s *Store) FinishJob(ctx context.Context, id string, o JobOutcome) error {
	tag, err := s.q.Exec(ctx, `
		update jobs set state = $2, exit_code = case when $2 = $7 then null else $3::int end,
			stdout_bytes = $4, stderr_bytes = $5, output_truncated = $8, oom_killed = $9,
			peak_memory_mib = $10, finished_at = now()
		where id = $1 and state = $6`,
		id, o.State, o.Code, o.StdoutBytes, o.StderrBytes, JobRunning, JobLost, o.Truncated,
		o.OOMKilled, nullIf0(o.PeakMemoryMiB))
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

func nullIf0(n int) any {
	if n == 0 {
		return nil
	}
	return n
}

// InstallSeconds sums the seconds the placement's installer jobs ran, a job still running
// counting until now
func (s *Store) InstallSeconds(ctx context.Context, placement uuid.UUID) (int, error) {
	var n int64
	err := s.q.QueryRow(ctx, `
		select coalesce(sum(extract(epoch from coalesce(finished_at, now()) - created_at)),
			0)::bigint
		from jobs where placement_id = $1 and install`, placement).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("install seconds: %w", err)
	}
	return int(min(n, math.MaxInt32)), nil
}

// SetJobFirstFrame stores the milliseconds from Exec's arrival to the job's first frame, once, in
// a transaction that does not wait for the disk
func (s *Store) SetJobFirstFrame(ctx context.Context, id string, ms int) error {
	return s.WithTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `set local synchronous_commit = off`); err != nil {
			return fmt.Errorf("set job first frame: %w", err)
		}
		_, err := tx.Exec(ctx, `
			update jobs set first_frame_ms = $2 where id = $1 and first_frame_ms is null`,
			id, ms)
		if err != nil {
			return fmt.Errorf("set job first frame: %w", err)
		}
		return nil
	})
}

// JobFirstFrame returns the job's first frame time in milliseconds, false when none is stored
func (s *Store) JobFirstFrame(ctx context.Context, id string) (int, bool, error) {
	var ms *int
	err := s.q.QueryRow(ctx, `select first_frame_ms from jobs where id = $1`, id).Scan(&ms)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, ErrNotFound
	}
	if err != nil {
		return 0, false, fmt.Errorf("job first frame: %w", err)
	}
	if ms == nil {
		return 0, false, nil
	}
	return *ms, true, nil
}
