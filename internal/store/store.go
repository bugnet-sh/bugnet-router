// Package store persists placements, jobs, capacity and the outbox in Postgres
package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultMaxConcurrent is the capacity given to a provider with no row yet
const DefaultMaxConcurrent = 50

// Placement states
const (
	PlacementPlacing     = "placing"
	PlacementReady       = "ready"
	PlacementTerminating = "terminating"
	PlacementTerminated  = "terminated"
	PlacementFailed      = "failed"
)

// Job states
const (
	JobRunning   = "running"
	JobExited    = "exited"
	JobCancelled = "cancelled"
	JobLost      = "lost"
)

// Order kinds
const (
	OrderStart  = "start"
	OrderExtend = "extend"
)

var (
	// ErrNotFound means the requested row does not exist
	ErrNotFound = errors.New("store: not found")
	// ErrUnknownHandle is ErrNotFound for a lookup by handle, with the same message
	ErrUnknownHandle = fmt.Errorf("%w", ErrNotFound)
	// ErrStateConflict means the row is not in a state that allows the change
	ErrStateConflict = errors.New("store: state conflict")
)

// Querier runs queries, a pool or one transaction
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Store runs queries on a pool or on a single transaction
type Store struct {
	q      Querier
	sealer RefSealer
	log    *slog.Logger
	// maxEvents caps one session record's events, DefaultSessionMaxEvents when 0
	maxEvents int
	// maxJobs caps one session record's jobs, DefaultSessionMaxJobs when 0
	maxJobs int
	// reserveSlots is how many slots a placement leaves free on its provider for probes, none unset
	reserveSlots int
}

// Option configures a Store
type Option func(*Store)

// WithRefSealer seals the credential fields of the provider refs the store writes and opens them
func WithRefSealer(r RefSealer) Option {
	return func(s *Store) { s.sealer = r }
}

// WithReserveSlots sets how many slots placements leave free on a provider for probes
func WithReserveSlots(n int) Option {
	return func(s *Store) { s.reserveSlots = max(n, 0) }
}

// WithLogger sets where the store reports a credential it cannot open, slog.Default when unset
func WithLogger(log *slog.Logger) Option {
	return func(s *Store) {
		if log != nil {
			s.log = log
		}
	}
}

// New returns a Store backed by pool
func New(pool *pgxpool.Pool, opts ...Option) *Store {
	s := &Store{q: pool, log: slog.Default()}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Tx returns a Store whose queries all run inside tx
func (s *Store) Tx(tx pgx.Tx) *Store {
	return &Store{
		q: tx, sealer: s.sealer, log: s.log, maxEvents: s.maxEvents,
		reserveSlots: s.reserveSlots,
	}
}

// WithTx runs fn in a transaction, committing when fn returns nil and rolling back otherwise
func (s *Store) WithTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.q.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// Ping checks that the database answers a query
func (s *Store) Ping(ctx context.Context) error {
	if _, err := s.q.Exec(ctx, "select 1"); err != nil {
		return fmt.Errorf("ping: %w", err)
	}
	return nil
}

func utc(t time.Time) time.Time { return t.UTC() }

func utcPtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
