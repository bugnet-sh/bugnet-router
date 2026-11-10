package worker

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/pool"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

const (
	// fillingAfter is how old a filling member may get before the sweep treats its job as dead
	fillingAfter = 10 * time.Minute
	// warmGrace is how long past its pool's max age a warm member may live, as the provider keeps
	// it an hour longer than that, when the tick missed it
	warmGrace = time.Hour
	// defaultPoolAge is the max age assumed for a pool that is no longer configured
	defaultPoolAge = 24 * time.Hour
)

// poolLeftover is CP 18.6.9's rule for a sandbox with a bugnet_pool label, in its order
func (w *Worker) poolLeftover(
	ctx context.Context, prov provider.Provider, o provider.Summary,
) bool {
	// A placement that names the sandbox judges it, which the member row alone could not
	p, err := w.store.PlacementBySandbox(ctx, prov.Name(), o.Ref.ID)
	if err == nil {
		return w.judge(ctx, prov, o, p)
	}
	if !errors.Is(err, store.ErrNotFound) {
		w.log.Error("sweep find the placement of a pool sandbox", "provider", prov.Name(),
			"sandbox", o.Ref.ID, "err", err)
		return false
	}
	if id, perr := uuid.Parse(o.Labels["pool_member"]); perr == nil {
		mem, err := w.store.MemberByID(ctx, id)
		switch {
		case err == nil:
			return w.memberLeftover(ctx, prov, o, mem)
		case !errors.Is(err, store.ErrNotFound):
			w.log.Error("sweep load a pool member", "member", id, "err", err)
			return false
		}
	}
	// A member a provider-kept pool still holds is bounded by the pool
	if o.PoolHeld {
		return false
	}
	// Rows are inserted before every create, so this is a member whose row was lost
	if o.CreatedAt.IsZero() {
		return false
	}
	return w.now().Sub(o.CreatedAt) > unlabelledAfter
}

// memberLeftover is the sweep's table of member states
func (w *Worker) memberLeftover(
	ctx context.Context, prov provider.Provider, o provider.Summary, mem store.Member,
) bool {
	now := w.now()
	switch mem.State {
	case store.MemberFilling:
		return now.Sub(mem.CreatedAt) >= fillingAfter
	case store.MemberWarm:
		age := defaultPoolAge
		if d, ok := w.pools.MaxAge(pool.Key{Provider: mem.Provider, Size: mem.SizeName}); ok {
			age = d
		}
		born := mem.CreatedAt
		if mem.SandboxCreatedAt != nil {
			born = *mem.SandboxCreatedAt
		}
		return now.Sub(born) > age+warmGrace
	case store.MemberHeld:
		open, err := w.store.ReservationKeepsMember(ctx, mem.ID)
		if err != nil {
			w.log.Error("sweep load a reservation", "member", mem.ID, "err", err)
			return false
		}
		return !open
	case store.MemberBound:
		if mem.PlacementID == nil {
			return true
		}
		p, err := w.store.PlacementByID(ctx, *mem.PlacementID)
		if errors.Is(err, store.ErrNotFound) {
			return true
		}
		if err != nil {
			w.log.Error("sweep load the placement of a member", "member", mem.ID, "err", err)
			return false
		}
		return w.judge(ctx, prov, o, p)
	}
	// retiring and gone
	return true
}
