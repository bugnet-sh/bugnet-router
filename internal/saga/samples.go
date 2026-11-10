package saga

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/health"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// sampleOf starts the sample of an attempt that is about to call the provider
func (s *Saga) sampleOf(
	placement store.Placement, prov provider.Provider, size provider.Size, at time.Time,
) store.StartSample {
	return store.StartSample{
		Kind: store.SamplePlacement, PlacementID: placement.ID, Provider: prov.Name(),
		SizeName: size.Name, StartedAt: at, Replica: s.d.ReplicaID,
		CreateMS: store.NoStep, ReadyMS: store.NoStep, RunnerMS: store.NoStep,
		GateMS: store.NoStep, FirstCmdMS: store.NoStep, WarmCmdMS: store.NoStep,
		DestroyMS: store.NoStep,
	}
}

// recordSample stores m after the attempt it timed, its outcome taken from err and its cost
// estimated over the time since it began, a failed write only logging
func (s *Saga) recordSample(
	ctx context.Context, prov provider.Provider, size provider.Size, m store.StartSample, err error,
) {
	if r := runOf(ctx); r != nil {
		m.Hedge = r.race.hedged.Load()
	}
	switch {
	case err != nil && lostRun(ctx):
		// A loser's sample is kept, and the rollup leaves it out of health
		m.Outcome, m.ErrorCode = store.OutcomeHedgeLost, ErrorHedgeLost
	case health.Unrecorded(err):
		return
	default:
		m.Outcome, m.ErrorCode = health.Outcome(err)
	}
	if err != nil {
		m.Error = err.Error()
	}
	m.CostMicros = health.StartCost(prov, size, s.now().Sub(m.StartedAt))
	ctx = context.WithoutCancel(ctx)
	// The write stays off the attempt's path, and Wait still waits for it
	s.wg.Go(func() {
		ctx, cancel := step(ctx)
		defer cancel()
		if err := s.d.Store.InsertStartSample(ctx, m); err != nil {
			s.d.Log.Warn("cannot store a start sample", "provider", m.Provider, "err", err)
		}
	})
}

// TakeCreate spends a create token of name for a probe, false when none came in time
func (s *Saga) TakeCreate(ctx context.Context, name string) bool { return s.takeCreate(ctx, name) }

// CreatesPaused reports whether name asked for its creates to wait
func (s *Saga) CreatesPaused(ctx context.Context, name string) bool {
	return s.createsPaused(ctx, name)
}

// ProviderFailed acts on an error a probe met from name and reports whether it counts against
// the provider
func (s *Saga) ProviderFailed(ctx context.Context, name string, err error, creating bool) bool {
	return s.providerFailed(ctx, name, err, creating)
}

// KeyID is the newest data-plane root's id, which a probe's gate key derives from
func (s *Saga) KeyID() string { return s.keyID }

// ErrorHedgeLost is the error code of an attempt, and the outcome of its sample, that another
// attempt outran
const ErrorHedgeLost = "hedge_lost"

// hedgeRun reports whether the attempt ctx belongs to is a hedge
func hedgeRun(ctx context.Context) bool {
	r := runOf(ctx)
	return r != nil && r.hedge
}

// countsAgainst acts on an error of an attempt and reports whether it counts against the
// provider, which one cut short by a winner never does
func (s *Saga) countsAgainst(ctx context.Context, name string, err error, creating bool) bool {
	if lostRun(ctx) {
		return false
	}
	return s.providerFailed(ctx, name, err, creating)
}

// markHedged records on the placement that a second attempt ran, once a hedge holds its slot or
// its member, and says so on the samples of both attempts
func (s *Saga) markHedged(ctx context.Context, id uuid.UUID) {
	r := runOf(ctx)
	if r == nil || !r.hedge {
		return
	}
	r.race.hedged.Store(true)
	ctx, cancel := step(ctx)
	defer cancel()
	if err := s.d.Store.SetPlacementHedged(ctx, id); err != nil {
		s.d.Log.Warn("record a hedged placement", "placement", id, "err", err)
	}
}
