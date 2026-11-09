// Package health shares what the replicas know about each provider's account through Postgres
package health

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

const (
	// DefaultEvery is how often Shared reads provider_health
	DefaultEvery = 10 * time.Second
	// staleAfter is how long the last reading stands when a read fails
	staleAfter  = time.Minute
	readTimeout = 2 * time.Second
)

// Shared implements placement.Health over the local tracker and the provider_health rows
type Shared struct {
	store *store.Store
	local *placement.Tracker
	every time.Duration
	now   func() time.Time
	log   *slog.Logger

	mu         sync.Mutex
	blocked    map[string]bool
	readAt     time.Time // when a read last worked
	next       time.Time // when the next read may start
	refreshing bool
}

// Option configures a Shared
type Option func(*Shared)

// WithEvery sets how often the rows are read, DefaultEvery when not positive
func WithEvery(d time.Duration) Option {
	return func(s *Shared) {
		if d > 0 {
			s.every = d
		}
	}
}

// WithClock replaces the time source
func WithClock(now func() time.Time) Option {
	return func(s *Shared) { s.now = now }
}

// NewShared builds the health source of a replica, local being its own failure tracker
func NewShared(
	st *store.Store, local *placement.Tracker, log *slog.Logger, opts ...Option,
) *Shared {
	s := &Shared{store: st, local: local, every: DefaultEvery, now: time.Now, log: log}
	if s.log == nil {
		s.log = slog.Default()
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Healthy is the local tracker's answer and a provider whose account is not blocked
func (s *Shared) Healthy(name string) bool {
	if !s.local.Healthy(name) {
		return false
	}
	return !s.view()[name]
}

// ReadyP95 is the local tracker's reading, until the shared one comes with the start samples
func (s *Shared) ReadyP95(name string) time.Duration { return s.local.ReadyP95(name) }

// view returns the blocked providers of the last reading, none once it is over a minute old
func (s *Shared) view() map[string]bool {
	now := s.now()
	s.mu.Lock()
	due := !s.refreshing && !now.Before(s.next)
	if due {
		s.refreshing = true
	}
	s.mu.Unlock()
	if due {
		s.read(now)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if now.Sub(s.readAt) > staleAfter {
		return nil
	}
	return s.blocked
}

// read replaces the reading from provider_health, a failed read keeping the last one
func (s *Shared) read(now time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
	defer cancel()
	rows, err := s.store.ProviderHealths(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refreshing = false
	s.next = now.Add(s.every)
	if err != nil {
		s.log.Warn("cannot read the shared provider health", "err", err)
		return
	}
	blocked := make(map[string]bool, len(rows))
	for _, r := range rows {
		blocked[r.Provider] = r.Blocked != ""
	}
	s.blocked, s.readAt = blocked, now
}
