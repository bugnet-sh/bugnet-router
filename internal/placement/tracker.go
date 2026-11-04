package placement

import (
	"math"
	"slices"
	"sync"
	"time"
)

const (
	failureLimit    = 3
	failureWindow   = 60 * time.Second
	unhealthyFor    = 120 * time.Second
	readySampleSize = 50
)

type providerState struct {
	failures       []time.Time
	unhealthyUntil time.Time
	ready          []time.Duration
}

// Tracker records provider outcomes and implements Health, safe for concurrent use
type Tracker struct {
	mu     sync.Mutex
	now    func() time.Time
	states map[string]*providerState
}

// NewTracker builds a tracker that reads time from now
func NewTracker(now func() time.Time) *Tracker {
	return &Tracker{now: now, states: map[string]*providerState{}}
}

func (t *Tracker) state(name string) *providerState {
	s, ok := t.states[name]
	if !ok {
		s = &providerState{}
		t.states[name] = s
	}
	return s
}

// RecordReady stores a ready duration and clears the consecutive failure count
func (t *Tracker) RecordReady(name string, d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.state(name)
	s.failures = nil
	s.ready = append(s.ready, d)
	if len(s.ready) > readySampleSize {
		s.ready = s.ready[len(s.ready)-readySampleSize:]
	}
}

// RecordFailure counts a failure and marks the provider unhealthy after a quick run of them
func (t *Tracker) RecordFailure(name string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.state(name)
	now := t.now()
	s.failures = append(s.failures, now)
	if len(s.failures) > failureLimit {
		s.failures = s.failures[len(s.failures)-failureLimit:]
	}
	if len(s.failures) == failureLimit && now.Sub(s.failures[0]) <= failureWindow {
		s.unhealthyUntil = now.Add(unhealthyFor)
	}
}

// Healthy reports whether the provider may be tried now
func (t *Tracker) Healthy(name string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	s, ok := t.states[name]
	return !ok || !t.now().Before(s.unhealthyUntil)
}

// ReadyP95 is the nearest-rank 95th percentile of recent ready durations, 0 without samples
func (t *Tracker) ReadyP95(name string) time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	s, ok := t.states[name]
	if !ok || len(s.ready) == 0 {
		return 0
	}
	sorted := slices.Clone(s.ready)
	slices.Sort(sorted)
	rank := int(math.Ceil(0.95 * float64(len(sorted))))
	return sorted[rank-1]
}
