package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// The modes of a placement's FetchSource row, empty before the first fetch
const (
	// BootstrapPublic is a clone job started from the sandbox
	BootstrapPublic = "public"
	// BootstrapPrivate is a fetch through the egress proxy, then an unpack job
	BootstrapPrivate = "private"
	// BootstrapFailed is a private fetch or guard that failed, its reason in Reason
	BootstrapFailed = "failed"
	// BootstrapNeedsSecret is a private source waiting for a GitHub secret
	BootstrapNeedsSecret = "needs_secret"
)

// Bootstrap is the FetchSource state kept on a placement row
type Bootstrap struct {
	State, JobID, Path, Reason, Revision string
	Heartbeat                            *time.Time
	// Attempt counts public clone jobs, PrivateAttempts counts private fetches
	Attempt, PrivateAttempts int
}

const bootstrapColumns = `coalesce(bootstrap_state, ''), coalesce(bootstrap_job_id, ''),
	coalesce(bootstrap_path, ''), coalesce(bootstrap_reason, ''),
	coalesce(bootstrap_revision, ''), bootstrap_heartbeat, bootstrap_attempt,
	bootstrap_private_attempts`

// BootstrapOf reads the placement's FetchSource row
func (s *Store) BootstrapOf(ctx context.Context, id uuid.UUID) (Bootstrap, error) {
	var b Bootstrap
	err := s.q.QueryRow(ctx, `select `+bootstrapColumns+` from placements where id = $1`, id).
		Scan(&b.State, &b.JobID, &b.Path, &b.Reason, &b.Revision, &b.Heartbeat, &b.Attempt,
			&b.PrivateAttempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return Bootstrap{}, ErrNotFound
	}
	if err != nil {
		return Bootstrap{}, fmt.Errorf("read bootstrap of %s: %w", id, err)
	}
	b.Heartbeat = utcPtr(b.Heartbeat)
	return b, nil
}

// ClaimBootstrap replaces the row with to and a fresh heartbeat while it still equals from in
// state, job and both counters, and says whether it did
func (s *Store) ClaimBootstrap(
	ctx context.Context, id uuid.UUID, from, to Bootstrap,
) (bool, error) {
	tag, err := s.q.Exec(ctx, `
		update placements set bootstrap_state = $2, bootstrap_job_id = $3, bootstrap_path = $4,
			bootstrap_reason = $5, bootstrap_revision = $6, bootstrap_heartbeat = now(),
			bootstrap_attempt = $7, bootstrap_private_attempts = $8
		where id = $1 and bootstrap_state is not distinct from $9
			and bootstrap_job_id is not distinct from $10 and bootstrap_attempt = $11
			and bootstrap_private_attempts = $12`,
		id, nullIfEmpty(to.State), nullIfEmpty(to.JobID), nullIfEmpty(to.Path),
		nullIfEmpty(to.Reason), nullIfEmpty(to.Revision), to.Attempt, to.PrivateAttempts,
		nullIfEmpty(from.State), nullIfEmpty(from.JobID), from.Attempt, from.PrivateAttempts)
	if err != nil {
		return false, fmt.Errorf("claim bootstrap of %s: %w", id, err)
	}
	return tag.RowsAffected() == 1, nil
}

// UpdateBootstrap sets state, job, reason and revision of the row while its counters equal at's,
// an empty field clearing its column except revision, which empty keeps, and the heartbeat to
// to's or to now when it has none
func (s *Store) UpdateBootstrap(
	ctx context.Context, id uuid.UUID, at, to Bootstrap,
) (bool, error) {
	tag, err := s.q.Exec(ctx, `
		update placements set bootstrap_state = $2, bootstrap_job_id = $3, bootstrap_reason = $4,
			bootstrap_revision = coalesce($5, bootstrap_revision),
			bootstrap_heartbeat = coalesce($8, now())
		where id = $1 and bootstrap_attempt = $6 and bootstrap_private_attempts = $7`,
		id, nullIfEmpty(to.State), nullIfEmpty(to.JobID), nullIfEmpty(to.Reason),
		nullIfEmpty(to.Revision), at.Attempt, at.PrivateAttempts, to.Heartbeat)
	if err != nil {
		return false, fmt.Errorf("update bootstrap of %s: %w", id, err)
	}
	return tag.RowsAffected() == 1, nil
}

// BeatBootstrap renews the heartbeat of the private fetch of attempt n, and says whether that
// fetch still holds the row
func (s *Store) BeatBootstrap(ctx context.Context, id uuid.UUID, n int) (bool, error) {
	tag, err := s.q.Exec(ctx, `
		update placements set bootstrap_heartbeat = now()
		where id = $1 and bootstrap_state = $2 and bootstrap_job_id is null
			and bootstrap_private_attempts = $3`, id, BootstrapPrivate, n)
	if err != nil {
		return false, fmt.Errorf("beat bootstrap of %s: %w", id, err)
	}
	return tag.RowsAffected() == 1, nil
}
