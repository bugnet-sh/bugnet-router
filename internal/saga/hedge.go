package saga

import (
	"context"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/pricing"
	"github.com/bugnet-sh/bugnet-router/internal/ratelimit"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// Path is how a start's sandbox came to be
type Path string

// Start paths
const (
	PathCold     Path = store.PathCold
	PathPooled   Path = store.PathPooled
	PathReserved Path = store.PathReserved
)

// hedgeBucketKey is the router-wide bucket every hedge takes a token from
const hedgeBucketKey = "rl:hedge:all"

// Totals is where a hedge reads the whole-attempt p95 of a provider's paths
type Totals interface {
	// Totals are the cold and pooled whole-attempt p95 figures and the samples behind them, zero
	// for none
	Totals(provider string) (cold, pooled time.Duration, samples int)
}

// HedgerDeps are what a Hedger works with
type HedgerDeps struct {
	Settings config.Hedge
	Health   Totals
	Valkey   valkey.Client
	Prefix   string
	// CreatesPerSecond is each provider's create rate, which sizes its create bucket
	CreatesPerSecond map[string]int
}

// Hedger decides when a placing placement starts a second attempt beside the first
type Hedger struct {
	d      HedgerDeps
	limits *ratelimit.Limiter
}

// NewHedger builds a hedger over its settings, the shared health and the create buckets
func NewHedger(d HedgerDeps) *Hedger {
	return &Hedger{d: d, limits: ratelimit.New(d.Valkey, d.Prefix, 0, 0)}
}

// After is how long an attempt on provider by path runs before a hedge, false for never
//
// It is the p95 of whole attempts on that provider and path with at least min_samples behind it,
// else default_after, and never under min_after.
func (h *Hedger) After(provider string, path Path) (time.Duration, bool) {
	if h == nil || !h.d.Settings.Enabled {
		return 0, false
	}
	after := h.d.Settings.DefaultAfter
	if h.d.Health != nil {
		cold, pooled, n := h.d.Health.Totals(provider)
		p95 := cold
		if path == PathPooled {
			p95 = pooled
		}
		if p95 > 0 && n >= h.d.Settings.MinSamples {
			after = p95
		}
	}
	return max(after, h.d.Settings.MinAfter), true
}

// Allow reports whether next may run beside running, taking a hedge token when it may
//
// pre is the time running's sandbox already ran before the order and left is what remains of the
// placing budget. budget is what the earlier attempts left of the payment, which the worst case of
// both attempts must fit at r's markup. A refusal writes nothing, and a token is taken only once
// every other condition holds, so a refused hedge costs the bucket nothing.
func (h *Hedger) Allow(
	ctx context.Context, pkg catalog.Package, running, next placement.Candidate,
	pre, left time.Duration, budget int64, r pricing.Rule,
) bool {
	if h == nil || !h.d.Settings.Enabled {
		return false
	}
	name := next.Provider.Name()
	switch {
	case name == "boat" && !h.d.Settings.OntoBoat:
		return false
	case left < h.d.Settings.MinLeft:
		return false
	case h.paused(ctx, name):
		return false
	case !h.bucketFull(ctx, name):
		return false
	case !placement.HedgeFits(running, next, time.Duration(pkg.Minutes)*time.Minute, pre,
		h.d.Settings.LoserLife, budget, r):
		return false
	}
	ok, _, err := h.limits.TakeKey(ctx, ratelimit.Bucket{
		Key: hedgeBucketKey, PerSecond: ratelimit.PerMinute(h.d.Settings.PerMinute),
		Burst: h.d.Settings.Burst,
	})
	return err == nil && ok
}

// paused reports whether the provider asked for its creates to wait, a failed read meaning no
func (h *Hedger) paused(ctx context.Context, name string) bool {
	if h.d.Valkey == nil {
		return false
	}
	n, err := h.d.Valkey.Do(ctx, h.d.Valkey.B().Exists().Key(pauseKeyOf(h.d.Prefix, name)).
		Build()).AsInt64()
	return err == nil && n > 0
}

// bucketFull reports whether the provider's create bucket holds min_bucket_share of its burst, a
// provider without a bucket always passing and an unreadable one never, since a hedge is optional
func (h *Hedger) bucketFull(ctx context.Context, name string) bool {
	b, ok := createBucketOf(h.d.CreatesPerSecond, name)
	if !ok {
		return true
	}
	full, err := h.limits.PeekLevel(ctx, b, h.d.Settings.MinBucketShare)
	return err == nil && full
}
