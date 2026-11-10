package saga

import (
	"context"

	"github.com/bugnet-sh/bugnet-router/internal/pool"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// CreateRetrying creates a sandbox the way a placement does, retrying a create whose outcome is
// unknown under the same key, for a pool fill
func (s *Saga) CreateRetrying(
	ctx context.Context, prov provider.Provider, req provider.CreateRequest,
) (provider.Ref, error) {
	return s.create(ctx, prov, req)
}

// BootPool boots a fresh member as far as setup's pool phase and harden
func (s *Saga) BootPool(
	ctx context.Context, prov provider.Provider, ref provider.Ref,
) (provider.Ref, pool.Booted, error) {
	ref, rep, err := s.Boot(ctx, prov, ref, BootPlan{Pool: true})
	return ref, pool.Booted{
		ReadyMS: rep.ReadyMS, RunnerMS: rep.RunnerMS, Image: recordOfManifest(rep.Image),
		Reseed: rep.Reseed,
	}, err
}

// DestroyRef destroys a sandbox, one already gone counting as destroyed
func (s *Saga) DestroyRef(ctx context.Context, prov provider.Provider, ref provider.Ref) error {
	return destroyRef(ctx, prov, ref)
}

// RecordSample stores a start sample, outcome and cost taken from err and the time since it began
func (s *Saga) RecordSample(
	ctx context.Context, prov provider.Provider, size provider.Size, m store.StartSample, err error,
) {
	s.recordSample(ctx, prov, size, m, err)
}

var _ pool.Saga = (*Saga)(nil)
