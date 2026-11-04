// Package store persists placements, jobs, capacity and the outbox in Postgres
package store

import (
	"context"
	"errors"
	"fmt"
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
	orderStart  = "start"
	orderExtend = "extend"
)

var (
	// ErrNotFound means the requested row does not exist
	ErrNotFound = errors.New("store: not found")
	// ErrStateConflict means the row is not in a state that allows the change
	ErrStateConflict = errors.New("store: state conflict")
)

type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Store runs queries on a pool or on a single transaction
type Store struct {
	q querier
}

// New returns a Store backed by pool
func New(pool *pgxpool.Pool) *Store {
	return &Store{q: pool}
}

// Tx returns a Store whose queries all run inside tx
func (s *Store) Tx(tx pgx.Tx) *Store {
	return &Store{q: tx}
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

func utc(t time.Time) time.Time { return t.UTC() }

func utcPtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}
