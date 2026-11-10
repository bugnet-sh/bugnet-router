package pool

import (
	"context"
	"sync"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/store"
)

const (
	// warmthTTL is how old a replica's view of the pools may get before it reads again
	warmthTTL = 2 * time.Second
	// staleTarget is how old a stored target may get before it reads as the pool's min
	staleTarget = time.Minute
	// readTimeout bounds one refresh, so a slow Postgres never holds a start up
	readTimeout = time.Second
)

// Warmth is a replica's view of warm members and stored targets, read from Postgres every 2 s
type Warmth struct {
	st    *store.Store
	pools map[Key]Config
	now   func() time.Time

	mu      sync.Mutex
	read    time.Time
	warm    map[[2]string]int
	targets map[[2]string]store.PoolTarget
}

// NewWarmth builds a view over the configured pools, now being time.Now when nil
func NewWarmth(st *store.Store, pools []Config, now func() time.Time) *Warmth {
	if now == nil {
		now = time.Now
	}
	w := &Warmth{st: st, pools: map[Key]Config{}, now: now}
	for _, c := range pools {
		w.pools[c.Key] = c
	}
	return w
}

// refresh reads again when the view is older than warmthTTL, an error keeping the old view
func (w *Warmth) refresh() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if now := w.now(); !w.read.IsZero() && now.Sub(w.read) < warmthTTL {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
	defer cancel()
	warm, err := w.st.WarmCounts(ctx)
	if err != nil {
		return
	}
	targets, err := w.st.Targets(ctx)
	if err != nil {
		return
	}
	w.warm = warm
	w.targets = map[[2]string]store.PoolTarget{}
	for _, t := range targets {
		w.targets[[2]string{t.Provider, t.SizeName}] = t
	}
	w.read = w.now()
}

// HasPool reports whether a pool is configured for the provider and size
func (w *Warmth) HasPool(provider, size string) bool {
	if w == nil {
		return false
	}
	_, ok := w.pools[Key{provider, size}]
	return ok
}

// WarmAvailable reports whether the pool had a claimable member at the last read
func (w *Warmth) WarmAvailable(provider, size string) bool {
	if w == nil || !w.HasPool(provider, size) {
		return false
	}
	w.refresh()
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.warm[[2]string{provider, size}] > 0
}

// Target is the pool's stored target, its min when the row is over a minute old or missing
func (w *Warmth) Target(provider, size string) int {
	if w == nil {
		return 0
	}
	c, ok := w.pools[Key{provider, size}]
	if !ok {
		return 0
	}
	w.refresh()
	w.mu.Lock()
	defer w.mu.Unlock()
	t, ok := w.targets[[2]string{provider, size}]
	if !ok || w.now().Sub(t.ComputedAt) > staleTarget {
		return c.Min
	}
	return min(max(t.Target, c.Min), c.Max)
}
