package fetch

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/facts"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/rules"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// lockTTL and lockPoll are the cross-replica fetch lock and how often a waiter looks again
const (
	lockTTL  = 3 * time.Second
	lockPoll = 100 * time.Millisecond
	// tagRefTTL is how long a tag's commit is kept, since tags rarely move
	tagRefTTL = 10 * time.Minute
)

// Cache holds the estimator's three caches and joins fetches of one source
//
// Valkey answers first, then Postgres for facts, and each Valkey call that takes over 100 ms
// counts as a miss. Negative answers fall back to a small in-process map while Valkey is silent.
type Cache struct {
	vk      valkey.Client
	prefix  string
	st      *store.Store
	cfg     config.Estimator
	replica string
	log     *slog.Logger
	now     func() time.Time

	// Admit takes a token of the new-sources bucket before an upstream fetch, nil admits all
	Admit func(ctx context.Context) (time.Duration, bool)

	mu         sync.Mutex
	flights    map[string]*flight
	background int
	neg        map[string]negEntry
}

type negEntry struct {
	reason  rules.UnreadReason
	expires time.Time
}

type flight struct {
	done     chan struct{}
	cancel   context.CancelFunc
	detached bool
	finished bool
	facts    facts.Facts
	res      Resolved
	err      error
}

// NewCache returns a Cache over Valkey under prefix and the store
func NewCache(
	vk valkey.Client, prefix string, st *store.Store, cfg config.Estimator, replica string,
	log *slog.Logger,
) *Cache {
	if log == nil {
		log = slog.Default()
	}
	return &Cache{vk: vk, prefix: prefix, st: st, cfg: cfg, replica: replica, log: log,
		now: time.Now, flights: map[string]*flight{}, neg: map[string]negEntry{}}
}

func (c *Cache) key(parts ...string) string {
	k := c.prefix + "est"
	for _, p := range parts {
		k += ":" + p
	}
	return k
}

// get reads a Valkey string within vkTimeout, found false for a miss or an unanswered call
func (c *Cache) get(ctx context.Context, key string) (string, bool) {
	ctx, cancel := context.WithTimeout(ctx, vkTimeout)
	defer cancel()
	v, err := c.vk.Do(ctx, c.vk.B().Get().Key(key).Build()).ToString()
	if err != nil {
		if !valkey.IsValkeyNil(err) {
			c.log.Warn("estimator: valkey read", "err", err)
		}
		return "", false
	}
	return v, true
}

func (c *Cache) set(ctx context.Context, key, value string, ttl time.Duration) {
	ctx, cancel := context.WithTimeout(ctx, vkTimeout)
	defer cancel()
	cmd := c.vk.B().Set().Key(key).Value(value).Px(ttl).Build()
	if err := c.vk.Do(ctx, cmd).Error(); err != nil {
		c.log.Warn("estimator: valkey write", "err", err)
	}
}

/* Facts */

// factsKey names a source's facts at a revision under the extractor version
func (c *Cache) factsKey(kind, key, rev string) string {
	return c.key("facts", kind, key+"@"+rev+":"+facts.ExtractorVersion)
}

// Facts returns the cached facts of a source at a revision from Valkey, else from Postgres
func (c *Cache) Facts(ctx context.Context, kind, key, rev string) (facts.Facts, bool) {
	if v, ok := c.get(ctx, c.factsKey(kind, key, rev)); ok {
		var f facts.Facts
		if json.Unmarshal([]byte(v), &f) == nil {
			return f, true
		}
	}
	row, err := c.st.GetFacts(ctx, key, rev, facts.ExtractorVersion)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			c.log.Warn("estimator: read stored facts", "err", err)
		}
		return facts.Facts{}, false
	}
	var f facts.Facts
	if json.Unmarshal(row.Facts, &f) != nil {
		return facts.Facts{}, false
	}
	c.set(ctx, c.factsKey(kind, key, rev), string(row.Facts), c.cfg.CacheTTL)
	return f, true
}

// PutFacts stores canonical facts bytes in Valkey and Postgres
func (c *Cache) PutFacts(ctx context.Context, kind, key, rev string, b, sum []byte) {
	c.set(ctx, c.factsKey(kind, key, rev), string(b), c.cfg.CacheTTL)
	err := c.st.PutFacts(ctx, store.FactsRow{SourceKey: key, Revision: rev,
		ExtractorVersion: facts.ExtractorVersion, Facts: b, FactsSHA256: sum})
	if err != nil {
		c.log.Warn("estimator: store facts", "err", err)
	}
}

/* Refs */

// Ref returns the commit and the resolved ref cached for an unresolved ref path
func (c *Cache) Ref(ctx context.Context, kind, key, refHash string) (value string, ok bool) {
	return c.get(ctx, c.key(kind, "ref", key+":"+refHash))
}

// PutRef caches a commit for a ref path, for a tag's longer time when tag is set
func (c *Cache) PutRef(ctx context.Context, kind, key, refHash, value string, tag bool) {
	ttl := c.cfg.RefTTL
	if tag {
		ttl = tagRefTTL
	}
	c.set(ctx, c.key(kind, "ref", key+":"+refHash), value, ttl)
}

/* Negative answers */

// Neg returns the reason a source was found unreadable within negative_ttl
func (c *Cache) Neg(ctx context.Context, kind, key string) (rules.UnreadReason, bool) {
	if v, ok := c.get(ctx, c.key("neg", kind, key)); ok {
		return rules.UnreadReason(v), true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.neg[kind+key]
	if !ok || !e.expires.After(c.now()) {
		delete(c.neg, kind+key)
		return "", false
	}
	return e.reason, true
}

// PutNeg remembers an unreadable source for negative_ttl
func (c *Cache) PutNeg(ctx context.Context, kind, key string, reason rules.UnreadReason) {
	c.set(ctx, c.key("neg", kind, key), string(reason), c.cfg.NegativeTTL)
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.neg) > 1024 {
		for k, e := range c.neg {
			if !e.expires.After(c.now()) {
				delete(c.neg, k)
			}
		}
	}
	if len(c.neg) <= 1024 {
		c.neg[kind+key] = negEntry{reason, c.now().Add(c.cfg.NegativeTTL)}
	}
}

/* Single flight */

// lock takes the cross-replica fetch lock, true when Valkey is silent so one replica still goes on
func (c *Cache) lock(ctx context.Context, flightKey string) bool {
	ctx, cancel := context.WithTimeout(ctx, vkTimeout)
	defer cancel()
	cmd := c.vk.B().Set().Key(c.key("lock", flightKey)).Value(c.replica).Nx().Px(lockTTL).Build()
	err := c.vk.Do(ctx, cmd).Error()
	return err == nil || !valkey.IsValkeyNil(err)
}

// Do runs one fetch for a key in this replica and, through the lock, in the fleet
//
// lookup answers from the cache and run fetches. When the caller's context ends first, the fetch
// goes on for background_budget, in one of max_background slots, or is cancelled when none is free.
func (c *Cache) Do(
	ctx context.Context, flightKey string,
	lookup func(context.Context) (facts.Facts, Resolved, bool),
	run func(context.Context) (facts.Facts, Resolved, error),
) (facts.Facts, Resolved, error) {
	c.mu.Lock()
	fl := c.flights[flightKey]
	if fl == nil {
		fl = &flight{done: make(chan struct{})}
		bg, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.BackgroundBudget)
		fl.cancel = cancel
		c.flights[flightKey] = fl
		go func() {
			defer cancel()
			f, r, err := c.fetchOnce(bg, flightKey, lookup, run)
			c.mu.Lock()
			fl.facts, fl.res, fl.err, fl.finished = f, r, err, true
			delete(c.flights, flightKey)
			if fl.detached {
				c.background--
			}
			c.mu.Unlock()
			close(fl.done)
		}()
	}
	c.mu.Unlock()
	select {
	case <-fl.done:
		return fl.facts, fl.res, fl.err
	case <-ctx.Done():
		c.mu.Lock()
		if !fl.finished && !fl.detached {
			if c.background < c.cfg.MaxBackground {
				fl.detached = true
				c.background++
			} else {
				fl.cancel()
			}
		}
		c.mu.Unlock()
		return facts.Facts{}, Resolved{Unread: rules.UnreadTimeout}, nil
	}
}

func (c *Cache) fetchOnce(
	ctx context.Context, flightKey string,
	lookup func(context.Context) (facts.Facts, Resolved, bool),
	run func(context.Context) (facts.Facts, Resolved, error),
) (facts.Facts, Resolved, error) {
	if !c.lock(ctx, flightKey) {
		deadline := c.now().Add(lockTTL)
		for c.now().Before(deadline) {
			select {
			case <-ctx.Done():
				return facts.Facts{}, Resolved{}, ctx.Err()
			case <-time.After(lockPoll):
			}
			if f, r, ok := lookup(ctx); ok {
				return f, r, nil
			}
		}
	}
	if c.Admit != nil {
		if wait, ok := c.Admit(ctx); !ok {
			return facts.Facts{}, Resolved{}, &LimitedError{Wait: wait}
		}
	}
	return run(ctx)
}

// LimitedError means the new-sources bucket was empty, so nothing was fetched
type LimitedError struct{ Wait time.Duration }

func (e *LimitedError) Error() string { return "fetch: the new sources limit is spent" }

// Active returns how many fetches this replica runs past their caller's budget
func (c *Cache) Active() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.background
}
