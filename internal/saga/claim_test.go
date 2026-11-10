package saga_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/pool"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// claimer is a local provider that keeps a pool as Koyeb does, making a sandbox for each claim
type claimer struct {
	*local.Provider
	mu      *sync.Mutex
	asked   *[][2]string
	claims  *atomic.Int64
	refuses bool
}

func newClaimer(t *testing.T) claimer {
	t.Helper()
	return claimer{
		Provider: newLocal(t, local.Options{Name: "kept"}), mu: &sync.Mutex{},
		asked: &[][2]string{}, claims: &atomic.Int64{},
	}
}

func (c claimer) EnsurePool(context.Context, provider.PoolSpec) error { return nil }

func (c claimer) IdleHourlyCostMicros(provider.Size) int64 { return 0 }

func (c claimer) Claim(ctx context.Context, name, key string) (provider.Claim, error) {
	c.claims.Add(1)
	c.mu.Lock()
	*c.asked = append(*c.asked, [2]string{name, key})
	c.mu.Unlock()
	if c.refuses {
		return provider.Claim{}, &provider.Error{
			Code: provider.CodeCapacity, CanFailOver: true, Err: errClaimRefused,
		}
	}
	ref, err := c.Create(ctx, provider.CreateRequest{
		IdempotencyKey: key, Size: small, Lifetime: time.Hour,
		Labels: map[string]string{
			"bugnet": "1", "bugnet_deployment": "main", "bugnet_pool": "small",
		},
	})
	return provider.Claim{Ref: ref, Warm: true, CreatedAt: time.Now().Add(-time.Minute)}, err
}

var errClaimRefused = errors.New("the pool has no member")

// keptEnv is an env whose first provider keeps a pool of small members itself
func keptEnv(t *testing.T, c claimer) *env {
	t.Helper()
	settings := config.DefaultPools()
	settings.Pool = []config.Pool{{
		Provider: c.Name(), Size: "small", Min: 1, Max: 2, MaxAge: time.Hour,
	}}
	return newEnvWith(t, func(d *saga.Deps) {
		w := pool.NewWarmth(d.Store, pool.ConfigsFor(d.Registry, settings), nil)
		d.Pools = w
		d.Engine = placement.New(d.Registry, d.Tracker, w)
	}, c)
}

func TestPlacementClaimsFromAProviderKeptPool(t *testing.T) {
	c := newClaimer(t)
	e := keptEnv(t, c)
	p, _ := e.placeReady(t, "order-1")
	eq(t, p.StartPath, "pooled")
	eq(t, c.claims.Load(), int64(1))
	asked := *c.asked
	eq(t, asked[0][0], "bn-main-small")
	// The claim carries the attempt's key, which a retry of the order's attempt repeats
	atts, err := e.store.Attempts(t.Context(), p.ID)
	eq(t, err, nil)
	eq(t, len(atts), 1)
	eq(t, atts[0].Path, store.PathPooled)
	eq(t, atts[0].State, "ready")
	eq(t, atts[0].IdempotencyKey, asked[0][1])
	// The member row binds the claimed sandbox to the placement, which also owns its slot
	m, err := e.store.MemberByID(t.Context(), atts[0].MemberID)
	eq(t, err, nil)
	eq(t, m.Origin, store.OriginProvider)
	eq(t, m.State, store.MemberBound)
	eq(t, *m.PlacementID, p.ID)
	eq(t, m.HoldsSlot, false)
	eq(t, m.ProviderRef.ID, p.ProviderRef.ID)
	eq(t, p.SandboxCreatedAt != nil, true)
	eq(t, e.inUse(t, "kept"), 1)
	eq(t, e.recountOf(t, "kept"), 1)
	// The sample records the claim as the wake and the path as pooled
	var path string
	var wake int
	err = e.pool.QueryRow(t.Context(), `select start_path, coalesce(wake_ms, -1)
		from start_samples where placement_id = $1`, p.ID).Scan(&path, &wake)
	eq(t, err, nil)
	eq(t, path, "pooled")
	eq(t, wake >= 0, true)
}

func TestARefusedClaimFallsBackToACreate(t *testing.T) {
	c := newClaimer(t)
	c.refuses = true
	e := keptEnv(t, c)
	p, _ := e.placeReady(t, "order-1")
	eq(t, p.StartPath, "cold")
	eq(t, c.claims.Load(), int64(1))
	atts, err := e.store.Attempts(t.Context(), p.ID)
	eq(t, err, nil)
	eq(t, len(atts), 1)
	eq(t, atts[0].Path, store.PathCold)
	// The member the claim recorded ends retiring, and no slot leaks
	var retiring int
	err = e.pool.QueryRow(t.Context(), `select count(*) from pool_members
		where origin = 'provider' and state = 'retiring' and end_reason = 'failed'`).Scan(&retiring)
	eq(t, err, nil)
	eq(t, retiring, 1)
	eq(t, e.inUse(t, "kept"), 1)
	eq(t, e.recountOf(t, "kept"), 1)
}
