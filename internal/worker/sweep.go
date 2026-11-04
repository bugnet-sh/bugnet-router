package worker

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"

	"github.com/oynozan/bugnet-router/internal/jobargs"
	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/saga"
	"github.com/oynozan/bugnet-router/internal/store"
)

const (
	// expiredAfter is how long a sandbox may outlive its placement's deadline
	expiredAfter = 60 * time.Second
	// unlabelledAfter is how old a router sandbox without a placement label may get
	unlabelledAfter = 10 * time.Minute
)

type sweepWorker struct {
	river.WorkerDefaults[jobargs.Sweep]
	w *Worker
}

// Work logs every failure and goes on, the next sweep a minute later retries what was missed
func (sw *sweepWorker) Work(ctx context.Context, _ *river.Job[jobargs.Sweep]) error {
	sw.w.sweepSandboxes(ctx)
	sw.w.watchPlacements(ctx)
	return nil
}

// sweepSandboxes destroys every sandbox of this deployment on every provider no placement needs
func (w *Worker) sweepSandboxes(ctx context.Context) {
	for _, prov := range w.registry.Ordered() {
		lctx, cancel := context.WithTimeout(ctx, callTimeout)
		owned, err := prov.ListOwned(lctx, w.saga.OwnedLabels())
		cancel()
		if err != nil {
			w.log.Error("sweep list sandboxes", "provider", prov.Name(), "err", err)
			continue
		}
		for _, o := range owned {
			if w.leftover(ctx, prov, o) {
				w.destroy(ctx, prov, o.Ref)
			}
		}
	}
}

// leftover reports whether a sandbox still needs destroying, ending an expired placement first
func (w *Worker) leftover(ctx context.Context, prov provider.Provider, o provider.Summary) bool {
	id, err := uuid.Parse(o.Labels["placement"])
	if err != nil {
		if o.CreatedAt.IsZero() {
			w.log.Warn("sweep keeps a sandbox of unknown age without a placement label",
				"provider", prov.Name(), "sandbox", o.Ref.ID)
			return false
		}
		return w.now().Sub(o.CreatedAt) > unlabelledAfter
	}
	p, err := w.store.PlacementByID(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return true
	}
	if err != nil {
		w.log.Error("sweep load placement", "placement", id, "err", err)
		return false
	}
	switch {
	case p.State == store.PlacementTerminated || p.State == store.PlacementFailed:
		return true
	case p.ProviderRef != nil && (p.Provider != prov.Name() || p.ProviderRef.ID != o.Ref.ID):
		return true
	case p.Deadline != nil && w.now().Sub(*p.Deadline) > expiredAfter:
		// Terminate destroys the sandbox itself, a bare destroy only stops the bill when it fails
		err := w.saga.Terminate(ctx, p.ID, saga.ReasonDeadline)
		if err != nil {
			w.log.Error("sweep terminate expired placement", "placement", p.ID, "err", err)
		}
		return err != nil
	}
	return false
}

// destroy treats a sandbox that is already gone as destroyed
func (w *Worker) destroy(ctx context.Context, prov provider.Provider, ref provider.Ref) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	err := prov.Destroy(ctx, ref)
	if e, ok := provider.AsError(err); ok && e.Code == provider.CodeNotFound {
		err = nil
	}
	if err != nil {
		w.log.Error("sweep destroy sandbox", "provider", prov.Name(), "sandbox", ref.ID,
			"err", err)
		return
	}
	w.log.Info("sweep destroyed sandbox", "provider", prov.Name(), "sandbox", ref.ID)
}
