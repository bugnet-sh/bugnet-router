package saga

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/store"
)

// ErrGateUnavailable means a placement's gate could not be reached, prepared or repaired
var ErrGateUnavailable = errors.New("saga: the sandbox's gate is unavailable")

const (
	// openTimeout bounds start saga steps 2 and 3 run again for a ready placement
	openTimeout = 2 * time.Minute
	// repairTimeout bounds setup --repair and the PrepareGate after it
	repairTimeout = setupTimeout + gateTimeout
)

// PortRefused reports whether the placement's gate refuses port, or it is no TCP port
func (s *Saga) PortRefused(p store.Placement, port int) bool {
	if port < 1 || port > 65535 {
		return true
	}
	var ref provider.Ref
	if p.ProviderRef != nil {
		ref = *p.ProviderRef
	}
	return slices.Contains(s.d.Boot.denyPorts(p.Provider, ref), port)
}

// Gate returns the provider's route to a placement's gate and the key the gate admits
func (s *Saga) Gate(p store.Placement) (provider.GateRoute, string, error) {
	prov, ref, err := s.home(p)
	if err != nil {
		return provider.GateRoute{}, "", err
	}
	g, ok := prov.(provider.Gater)
	if !ok {
		return provider.GateRoute{}, "", fmt.Errorf("%w: %w", ErrGateUnavailable, errNoGate)
	}
	route, err := g.GateRoute(ref, s.d.Boot.gatePort())
	if err != nil {
		return provider.GateRoute{}, "", fmt.Errorf("%w: %w", ErrGateUnavailable, err)
	}
	key, err := s.d.Keys.GateKey(p.KeyID, p.ID)
	if err != nil {
		return provider.GateRoute{}, "", fmt.Errorf("gate key: %w", err)
	}
	return route, key, nil
}

// OpenGate runs start saga steps 2 and 3 once for a ready placement whose gate never answered
func (s *Saga) OpenGate(ctx context.Context, p store.Placement) (store.Placement, error) {
	if p.GateReadyAt != nil {
		return p, nil
	}
	// The first caller's steps serve every caller here, so its going away must not stop them
	v, err, _ := s.gates.Do(p.ID.String(), func() (any, error) {
		octx, cancel := context.WithTimeout(context.WithoutCancel(ctx), openTimeout)
		defer cancel()
		return s.openOnce(octx, p.ID)
	})
	if err != nil {
		return store.Placement{}, err
	}
	return v.(store.Placement), nil
}

// openOnce runs the gate steps under the placement's gate lock unless the gate is open already
func (s *Saga) openOnce(ctx context.Context, id uuid.UUID) (store.Placement, error) {
	var out store.Placement
	var failed error
	err := s.d.Store.WithLock(ctx, "gate:"+id.String(), func(ts *store.Store) error {
		cur, err := ts.PlacementByID(ctx, id)
		if err != nil {
			return err
		}
		out = cur
		if cur.GateReadyAt != nil {
			return nil
		}
		if cur.State != store.PlacementReady {
			return fmt.Errorf("%w: placement is %s", ErrNotReady, cur.State)
		}
		prov, ref, err := s.home(cur)
		if err != nil {
			return err
		}
		key, err := s.d.Keys.GateKey(cur.KeyID, cur.ID)
		if err != nil {
			return fmt.Errorf("gate key: %w", err)
		}
		ref, at, err := s.openGate(ctx, prov, ref, key)
		var gateAt *time.Time
		if err == nil {
			gateAt = &at
		} else {
			failed = fmt.Errorf("%w: %w", ErrGateUnavailable, err)
		}
		// The entries PrepareGate gave are kept even when the gate did not answer
		if err := ts.SetPlacementRoute(ctx, id, ref, gateAt); err != nil {
			return err
		}
		out.ProviderRef, out.GateReadyAt = &ref, gateAt
		if failed == nil {
			s.d.Log.Info("gate opened", "placement", id, "provider", prov.Name())
		}
		return nil
	})
	if err != nil {
		return store.Placement{}, err
	}
	return out, failed
}

// RepairGate runs setup --repair and PrepareGate again for a ready placement, keeping the meta
func (s *Saga) RepairGate(ctx context.Context, p store.Placement) (store.Placement, error) {
	if p.State != store.PlacementReady {
		return store.Placement{}, fmt.Errorf("%w: placement is %s", ErrNotReady, p.State)
	}
	prov, ref, err := s.home(p)
	if err != nil {
		return store.Placement{}, err
	}
	g, ok := prov.(provider.Gater)
	if !ok {
		return store.Placement{}, fmt.Errorf("%w: %w", ErrGateUnavailable, errNoGate)
	}
	ctx, cancel := context.WithTimeout(ctx, repairTimeout)
	defer cancel()
	sctx, scancel := context.WithTimeout(ctx, setupTimeout)
	_, err = runRoot(sctx, prov, ref, "setup", "--repair")
	scancel()
	if err != nil {
		return store.Placement{}, fmt.Errorf("%w: setup --repair: %w", ErrGateUnavailable, err)
	}
	pctx, pcancel := context.WithTimeout(ctx, gateTimeout)
	meta, err := g.PrepareGate(pctx, ref, s.d.Boot.gatePort())
	pcancel()
	if err != nil {
		return store.Placement{}, fmt.Errorf("%w: prepare the gate route: %w",
			ErrGateUnavailable, err)
	}
	ref = withMeta(ref, meta)
	if err := s.d.Store.SetPlacementRoute(ctx, p.ID, ref, nil); err != nil {
		return store.Placement{}, err
	}
	s.d.Log.Info("gate repaired", "placement", p.ID, "provider", prov.Name())
	p.ProviderRef = &ref
	return p, nil
}
