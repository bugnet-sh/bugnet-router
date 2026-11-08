package store

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// DefaultSessionMaxEvents is the events one record keeps when the store is given no cap
const DefaultSessionMaxEvents = 5000

// Copy states of a session record's output
const (
	CopyPending  = "pending"
	CopyComplete = "complete"
	CopyPartial  = "partial"
	CopyExpired  = "expired"
)

// Who closed a session record's page early
const (
	ClosedByUser     = "user"
	ClosedByOperator = "operator"
	ClosedByAbuse    = "abuse"
)

// WithSessionMaxEvents caps the events one session record keeps, the default when n is 0
func WithSessionMaxEvents(n int) Option {
	return func(s *Store) {
		if n > 0 {
			s.maxEvents = n
		}
	}
}

// SessionRecord is a placement's session page record
type SessionRecord struct {
	ID          string
	PlacementID uuid.UUID
	CreatedAt   time.Time
	EndedAt     *time.Time
	ExpiresAt   *time.Time
	ClosedAt    *time.Time
	ClosedBy    string
	HoldUntil   *time.Time
	Generation  uint32
	CopyState   string
	// EventsDropped counts events the cap kept out
	EventsDropped int
}

// Open reports whether the record's public page may still be served at now
func (r SessionRecord) Open(now time.Time) bool {
	return r.ClosedAt == nil && (r.ExpiresAt == nil || now.Before(*r.ExpiresAt))
}

// Held reports whether an operator hold keeps the record at now
func (r SessionRecord) Held(now time.Time) bool {
	return r.HoldUntil != nil && now.Before(*r.HoldUntil)
}

// SessionEvent is one stored event of a record
type SessionEvent struct {
	ID   int64
	At   time.Time
	Kind string
	Data json.RawMessage
}

// SessionJob is one command a record shows, by its sequence number
type SessionJob struct {
	Seq        int
	JobID      string
	Command    string
	StartedAt  time.Time
	FinishedAt *time.Time
	ExitCode   *int
	// Frames is the copied output as JSON, nil until the copy ran
	Frames   json.RawMessage
	CutBytes int64
	// State is the job's state, lost when the jobs table has no row for it
	State       string
	StdoutBytes int64
	StderrBytes int64
}

// SessionEnd is how a placement's end changes its record
type SessionEnd struct {
	EndedAt time.Time
	Keep    time.Duration
	// ClosedBy closes the page at once with an operator hold until HoldUntil, empty for none
	ClosedBy  string
	HoldUntil time.Time
}

var sessionIDs = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").
	WithPadding(base32.NoPadding)

// newSessionID returns 16 random lower-case base32 characters, 80 bits
func newSessionID() string {
	b := make([]byte, 10)
	_, _ = rand.Read(b)
	return sessionIDs.EncodeToString(b)
}

const sessionColumns = `id, placement_id, created_at, ended_at, expires_at, closed_at,
	coalesce(closed_by, ''), hold_until, generation, copy_state, events_dropped`

func scanSessionRecord(row pgx.Row) (SessionRecord, error) {
	var r SessionRecord
	var gen int64
	err := row.Scan(&r.ID, &r.PlacementID, &r.CreatedAt, &r.EndedAt, &r.ExpiresAt, &r.ClosedAt,
		&r.ClosedBy, &r.HoldUntil, &gen, &r.CopyState, &r.EventsDropped)
	if errors.Is(err, pgx.ErrNoRows) {
		return SessionRecord{}, ErrNotFound
	}
	if err != nil {
		return SessionRecord{}, err
	}
	r.Generation = uint32(gen)
	r.CreatedAt = utc(r.CreatedAt)
	r.EndedAt, r.ExpiresAt = utcPtr(r.EndedAt), utcPtr(r.ExpiresAt)
	r.ClosedAt, r.HoldUntil = utcPtr(r.ClosedAt), utcPtr(r.HoldUntil)
	return r, nil
}

func (s *Store) sessionWhere(ctx context.Context, where string, arg any) (SessionRecord, error) {
	r, err := scanSessionRecord(s.q.QueryRow(ctx,
		`select `+sessionColumns+` from session_records where `+where, arg))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return SessionRecord{}, fmt.Errorf("select session record: %w", err)
	}
	return r, err
}

// CreateSessionRecord makes the placement's record, or returns the one it has
func (s *Store) CreateSessionRecord(
	ctx context.Context, placement uuid.UUID,
) (SessionRecord, error) {
	_, err := s.q.Exec(ctx, `
		insert into session_records (id, placement_id) values ($1, $2)
		on conflict (placement_id) do nothing`, newSessionID(), placement)
	if err != nil {
		return SessionRecord{}, fmt.Errorf("create session record: %w", err)
	}
	return s.SessionRecordByPlacement(ctx, placement)
}

// SessionRecordByID returns the record with this URL id or ErrNotFound
func (s *Store) SessionRecordByID(ctx context.Context, id string) (SessionRecord, error) {
	return s.sessionWhere(ctx, "id = $1", id)
}

// SessionRecordByPlacement returns the placement's record or ErrNotFound
func (s *Store) SessionRecordByPlacement(
	ctx context.Context, placement uuid.UUID,
) (SessionRecord, error) {
	return s.sessionWhere(ctx, "placement_id = $1", placement)
}

// lockSession locks the record row, a user-deleted one reading as absent so nothing lands again
func lockSession(ctx context.Context, tx pgx.Tx, placement uuid.UUID) (string, error) {
	var id string
	err := tx.QueryRow(ctx, `
		select id from session_records
		where placement_id = $1 and coalesce(closed_by, '') <> $2 for update`,
		placement, ClosedByUser).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("lock session record: %w", err)
	}
	return id, nil
}

// InsertSessionEvent stores one row on the placement's record under sessions.max_events
func (s *Store) InsertSessionEvent(ctx context.Context, placement uuid.UUID, kind string,
	data []byte, alwaysLand bool) (inserted, capDropped bool, err error) {
	err = s.WithTx(ctx, func(tx pgx.Tx) error {
		id, err := lockSession(ctx, tx, placement)
		if err != nil {
			return err
		}
		inserted, capDropped, err = s.insertEvent(ctx, tx, id, kind, data, alwaysLand)
		return err
	})
	return inserted, capDropped, err
}

// InsertSessionEvents stores rows of one kind on the placement's record in one transaction
func (s *Store) InsertSessionEvents(ctx context.Context, placement uuid.UUID, kind string,
	data [][]byte, alwaysLand bool) error {
	return s.WithTx(ctx, func(tx pgx.Tx) error {
		id, err := lockSession(ctx, tx, placement)
		if err != nil {
			return err
		}
		for _, d := range data {
			if _, _, err := s.insertEvent(ctx, tx, id, kind, d, alwaysLand); err != nil {
				return err
			}
		}
		return nil
	})
}

// insertEvent counts the record's rows and inserts under its lock, at taken from data when set
func (s *Store) insertEvent(ctx context.Context, tx pgx.Tx, record, kind string, data []byte,
	alwaysLand bool) (inserted, capDropped bool, err error) {
	if !alwaysLand {
		var n int
		err := tx.QueryRow(ctx,
			`select count(*) from session_events where record_id = $1`, record).Scan(&n)
		if err != nil {
			return false, false, fmt.Errorf("count session events: %w", err)
		}
		if n >= s.sessionCap() {
			return s.dropEvent(ctx, tx, record, kind, data)
		}
	}
	tag, err := tx.Exec(ctx, insertEventSQL, record, kind, data)
	if err != nil {
		return false, false, fmt.Errorf("insert session event: %w", err)
	}
	return tag.RowsAffected() == 1, false, nil
}

const insertEventSQL = `
	insert into session_events (record_id, at, kind, data)
	values ($1, coalesce(($3::jsonb ->> 'at')::timestamptz, now()), $2, $3)
	on conflict do nothing`

// dropEvent counts a row past the cap as dropped, unless it repeats a stored one
func (s *Store) dropEvent(ctx context.Context, tx pgx.Tx, record, kind string,
	data []byte) (inserted, capDropped bool, err error) {
	// The insert runs in a savepoint it never keeps, so any unique index decides a repeat
	sp, err := tx.Begin(ctx)
	if err != nil {
		return false, false, fmt.Errorf("probe a session event: %w", err)
	}
	tag, err := sp.Exec(ctx, insertEventSQL, record, kind, data)
	if rerr := sp.Rollback(ctx); err == nil {
		err = rerr
	}
	if err != nil {
		return false, false, fmt.Errorf("probe a session event: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return false, false, nil
	}
	_, err = tx.Exec(ctx, `
		update session_records set events_dropped = events_dropped + 1 where id = $1`, record)
	if err != nil {
		return false, false, fmt.Errorf("count a dropped session event: %w", err)
	}
	return false, true, nil
}

func (s *Store) sessionCap() int {
	if s.maxEvents > 0 {
		return s.maxEvents
	}
	return DefaultSessionMaxEvents
}

// InsertSessionEventOnce stores the row unless one of kind already holds match's fields
func (s *Store) InsertSessionEventOnce(ctx context.Context, placement uuid.UUID, kind string,
	match, data []byte, alwaysLand bool) (inserted bool, err error) {
	err = s.WithTx(ctx, func(tx pgx.Tx) error {
		id, err := lockSession(ctx, tx, placement)
		if err != nil {
			return err
		}
		var found bool
		err = tx.QueryRow(ctx, `
			select exists (select 1 from session_events
				where record_id = $1 and kind = $2 and data @> $3::jsonb)`,
			id, kind, match).Scan(&found)
		if err != nil || found {
			return err
		}
		inserted, _, err = s.insertEvent(ctx, tx, id, kind, data, alwaysLand)
		return err
	})
	return inserted, err
}

// FoldSessionEvent adds counts to the row of kind at minute whose data holds match, or makes it
func (s *Store) FoldSessionEvent(ctx context.Context, placement uuid.UUID, kind string,
	minute time.Time, match []byte, add map[string]int64) error {
	return s.WithTx(ctx, func(tx pgx.Tx) error {
		id, err := lockSession(ctx, tx, placement)
		if err != nil {
			return err
		}
		var row int64
		var data []byte
		err = tx.QueryRow(ctx, `
			select id, data from session_events
			where record_id = $1 and kind = $2 and at = $3 and data @> $4::jsonb
			order by id limit 1`, id, kind, utc(minute), match).Scan(&row, &data)
		if errors.Is(err, pgx.ErrNoRows) {
			fields := map[string]any{}
			if err := json.Unmarshal(match, &fields); err != nil {
				return fmt.Errorf("fold match: %w", err)
			}
			fields["at"] = utc(minute)
			for k, v := range add {
				fields[k] = v
			}
			raw, err := json.Marshal(fields)
			if err != nil {
				return err
			}
			_, _, err = s.insertEvent(ctx, tx, id, kind, raw, false)
			return err
		}
		if err != nil {
			return fmt.Errorf("select folded session event: %w", err)
		}
		fields := map[string]any{}
		dec := json.NewDecoder(strings.NewReader(string(data)))
		dec.UseNumber()
		if err := dec.Decode(&fields); err != nil {
			return fmt.Errorf("decode folded session event: %w", err)
		}
		for k, v := range add {
			var have int64
			if n, ok := fields[k].(json.Number); ok {
				have, _ = n.Int64()
			}
			fields[k] = have + v
		}
		raw, err := json.Marshal(fields)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `update session_events set data = $2 where id = $1`, row, raw)
		if err != nil {
			return fmt.Errorf("fold session event: %w", err)
		}
		return nil
	})
}

// SessionEvents returns a record's events in time order
func (s *Store) SessionEvents(ctx context.Context, record string) ([]SessionEvent, error) {
	rows, err := s.q.Query(ctx, `
		select id, at, kind, data from session_events where record_id = $1 order by at, id`,
		record)
	if err != nil {
		return nil, fmt.Errorf("select session events: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (SessionEvent, error) {
		var e SessionEvent
		err := r.Scan(&e.ID, &e.At, &e.Kind, &e.Data)
		e.At = utc(e.At)
		return e, err
	})
	if err != nil {
		return nil, fmt.Errorf("select session events: %w", err)
	}
	return out, nil
}

/* Jobs */

// StartSessionJob gives the job its sequence number on the placement's record, once per job
func (s *Store) StartSessionJob(ctx context.Context, placement uuid.UUID, jobID, command string,
	at time.Time) (seq int, created bool, err error) {
	err = s.WithTx(ctx, func(tx pgx.Tx) error {
		id, err := lockSession(ctx, tx, placement)
		if err != nil {
			return err
		}
		err = tx.QueryRow(ctx, `select seq from session_jobs where record_id = $1 and job_id = $2`,
			id, jobID).Scan(&seq)
		if err == nil || !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		err = tx.QueryRow(ctx, `
			insert into session_jobs (record_id, seq, job_id, command, started_at)
			select $1, coalesce(max(seq), 0) + 1, $2, $3, $4 from session_jobs
			where record_id = $1
			returning seq`, id, jobID, nullIfEmpty(command), utc(at)).Scan(&seq)
		if err != nil {
			return fmt.Errorf("insert session job: %w", err)
		}
		created = true
		return nil
	})
	return seq, created, err
}

// FinishSessionJob records a job's end on its record, answering its seq and start
func (s *Store) FinishSessionJob(ctx context.Context, placement uuid.UUID, jobID string,
	at time.Time, exitCode *int) (seq int, started time.Time, err error) {
	err = s.q.QueryRow(ctx, `
		update session_jobs sj set finished_at = coalesce(sj.finished_at, $3), exit_code = $4
		from session_records r
		where r.id = sj.record_id and r.placement_id = $1 and sj.job_id = $2
		returning sj.seq, sj.started_at`, placement, jobID, utc(at), exitCode).
		Scan(&seq, &started)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, time.Time{}, ErrNotFound
	}
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("finish session job: %w", err)
	}
	return seq, utc(started), nil
}

// SessionJobs returns a record's jobs by seq, with each job's state and byte counts
func (s *Store) SessionJobs(ctx context.Context, record string) ([]SessionJob, error) {
	rows, err := s.q.Query(ctx, `
		select sj.seq, sj.job_id, coalesce(sj.command, ''), sj.started_at, sj.finished_at,
			sj.exit_code, sj.frames, sj.cut_bytes, coalesce(j.state, $2),
			coalesce(j.stdout_bytes, 0), coalesce(j.stderr_bytes, 0)
		from session_jobs sj left join jobs j on j.id = sj.job_id
		where sj.record_id = $1 order by sj.seq`, record, JobLost)
	if err != nil {
		return nil, fmt.Errorf("select session jobs: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (SessionJob, error) {
		var j SessionJob
		err := r.Scan(&j.Seq, &j.JobID, &j.Command, &j.StartedAt, &j.FinishedAt, &j.ExitCode,
			&j.Frames, &j.CutBytes, &j.State, &j.StdoutBytes, &j.StderrBytes)
		j.StartedAt = utc(j.StartedAt)
		j.FinishedAt = utcPtr(j.FinishedAt)
		return j, err
	})
	if err != nil {
		return nil, fmt.Errorf("select session jobs: %w", err)
	}
	return out, nil
}

// SessionJobBySeq returns the job a record shows under seq or ErrNotFound
func (s *Store) SessionJobBySeq(ctx context.Context, record string, seq int) (SessionJob, error) {
	jobs, err := s.SessionJobs(ctx, record)
	if err != nil {
		return SessionJob{}, err
	}
	for _, j := range jobs {
		if j.Seq == seq {
			return j, nil
		}
	}
	return SessionJob{}, ErrNotFound
}

// SetSessionJobFrames stores a job's copied output
func (s *Store) SetSessionJobFrames(
	ctx context.Context, record string, seq int, frames []byte, cutBytes int64,
) error {
	_, err := s.q.Exec(ctx, `
		update session_jobs set frames = $3, cut_bytes = $4 where record_id = $1 and seq = $2`,
		record, seq, frames, cutBytes)
	if err != nil {
		return fmt.Errorf("store session job output: %w", err)
	}
	return nil
}

/* The record's life */

// EndSessionRecord marks the placement's record ended, and closes it with a hold when e says so
func (s *Store) EndSessionRecord(ctx context.Context, placement uuid.UUID, e SessionEnd) error {
	var closedBy, hold any
	if e.ClosedBy != "" {
		closedBy, hold = e.ClosedBy, utc(e.HoldUntil)
	}
	// A record its user deleted keeps that close, so no hold reopens it to events
	_, err := s.q.Exec(ctx, `
		update session_records set ended_at = coalesce(ended_at, $2),
			expires_at = coalesce(expires_at, $2 + make_interval(secs => $3)),
			closed_at = case when $4::text is null then closed_at else coalesce(closed_at, $2) end,
			closed_by = case when $4::text is null or closed_by = $6 then closed_by else $4 end,
			hold_until = case when $4::text is null or closed_by = $6 then hold_until
				else greatest(hold_until, $5) end
		where placement_id = $1`,
		placement, utc(e.EndedAt), e.Keep.Seconds(), closedBy, hold, ClosedByUser)
	if err != nil {
		return fmt.Errorf("end session record: %w", err)
	}
	return nil
}

// CloseSessionRecord ends the placement's public page now and holds the record until holdUntil
func (s *Store) CloseSessionRecord(
	ctx context.Context, placement uuid.UUID, by string, holdUntil time.Time,
) error {
	tag, err := s.q.Exec(ctx, `
		update session_records set closed_at = coalesce(closed_at, now()),
			closed_by = case when closed_by = $4 then closed_by else $2 end,
			hold_until = case when closed_by = $4 then hold_until else greatest(hold_until, $3) end
		where placement_id = $1`, placement, by, utc(holdUntil), ClosedByUser)
	if err != nil {
		return fmt.Errorf("close session record: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// DeleteSessionRecordNow closes the page as the user asked and deletes its rows unless held
func (s *Store) DeleteSessionRecordNow(ctx context.Context, id string, now time.Time) error {
	return s.WithTx(ctx, func(tx pgx.Tx) error {
		r, err := s.Tx(tx).sessionWhere(ctx, "id = $1 for update", id)
		if err != nil {
			return err
		}
		if r.ClosedAt == nil {
			_, err := tx.Exec(ctx, `
				update session_records set closed_at = $2, closed_by = $3 where id = $1`,
				id, utc(now), ClosedByUser)
			if err != nil {
				return fmt.Errorf("close session record: %w", err)
			}
		}
		if r.Held(now) {
			return nil
		}
		for _, table := range []string{"session_events", "session_jobs"} {
			_, err := tx.Exec(ctx, `delete from `+table+` where record_id = $1`, id)
			if err != nil {
				return fmt.Errorf("delete %s: %w", table, err)
			}
		}
		return nil
	})
}

// RotateSessionGeneration moves the record from generation from to the next, answering the current
func (s *Store) RotateSessionGeneration(
	ctx context.Context, id string, from uint32,
) (uint32, error) {
	var gen int64
	err := s.q.QueryRow(ctx, `
		update session_records set generation = generation + 1
		where id = $1 and generation = $2 returning generation`, id, int64(from)).Scan(&gen)
	if errors.Is(err, pgx.ErrNoRows) {
		r, err := s.SessionRecordByID(ctx, id)
		return r.Generation, err
	}
	if err != nil {
		return 0, fmt.Errorf("rotate session link: %w", err)
	}
	return uint32(gen), nil
}

// SetSessionCopyState records how the copy of the record's output ended
func (s *Store) SetSessionCopyState(ctx context.Context, id, state string) error {
	_, err := s.q.Exec(ctx, `update session_records set copy_state = $2 where id = $1`, id, state)
	if err != nil {
		return fmt.Errorf("set session copy state: %w", err)
	}
	return nil
}

// ExpireSessionRecords deletes up to limit records whose page ended and that no hold keeps
func (s *Store) ExpireSessionRecords(ctx context.Context, now time.Time, limit int) (int, error) {
	tag, err := s.q.Exec(ctx, `
		delete from session_records where id in (
			select id from session_records
			where (expires_at <= $1 or closed_at is not null)
				and (hold_until is null or hold_until <= $1)
			limit $2)`, utc(now), limit)
	if err != nil {
		return 0, fmt.Errorf("expire session records: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// SessionDiskBytes is what the three session tables take on disk, indexes and toast included
func (s *Store) SessionDiskBytes(ctx context.Context) (int64, error) {
	var n int64
	err := s.q.QueryRow(ctx, `
		select pg_total_relation_size('session_records') +
			pg_total_relation_size('session_events') + pg_total_relation_size('session_jobs')`).
		Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("session tables size: %w", err)
	}
	return n, nil
}
