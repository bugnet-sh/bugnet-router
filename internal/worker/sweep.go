package worker

import (
	"context"
	"errors"
	"sync"
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

// Timeout bounds one sweep, whatever it skipped the next sweep a minute later picks up
func (*sweepWorker) Timeout(*river.Job[jobargs.Sweep]) time.Duration { return sweepTimeout }

// Work logs every failure and goes on, the next sweep a minute later retries what was missed
func (sw *sweepWorker) Work(ctx context.Context, _ *river.Job[jobargs.Sweep]) error {
	sw.w.sweepSandboxes(ctx)
	return nil
}

// sweepSandboxes sweeps every provider at once, so a slow one holds back only itself
func (w *Worker) sweepSandboxes(ctx context.Context) {
	var wg sync.WaitGroup
	for _, prov := range w.registry.Ordered() {
		wg.Go(func() { w.sweepProvider(ctx, prov) })
	}
	wg.Wait()
}

// sweepProvider destroys every sandbox of this deployment on prov that no placement needs
func (w *Worker) sweepProvider(ctx context.Context, prov provider.Provider) {
	lctx, cancel := context.WithTimeout(ctx, callTimeout)
	owned, err := prov.ListOwned(lctx, w.saga.OwnedLabels())
	cancel()
	if err != nil {
		// A listing cut short still names sandboxes that exist, so they are judged all the same
		w.log.Error("sweep list sandboxes", "provider", prov.Name(), "listed", len(owned),
			"err", err)
	}
	for i, o := range owned {
		if ctx.Err() != nil {
			w.log.Warn("sweep ran out of time, the next sweep goes on", "provider", prov.Name(),
				"left", len(owned)-i)
			return
		}
		if w.leftover(ctx, prov, o) {
			w.destroy(ctx, prov, o.Ref)
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
	now := w.now()
	switch {
	case p.State == store.PlacementTerminated || p.State == store.PlacementFailed:
		return true
	case p.ProviderRef != nil && (p.Provider != prov.Name() || p.ProviderRef.ID != o.Ref.ID):
		return true
	case p.State == store.PlacementTerminating && stuckTerminating(p, now),
		p.Deadline != nil && now.Sub(*p.Deadline) > expiredAfter:
		// Terminate destroys the sandbox itself, a bare destroy only stops the bill when it fails
		err := w.saga.Terminate(ctx, p.ID, now.Add(-expiredAfter))
		if errors.Is(err, saga.ErrNotDue) {
			return false
		}
		if err != nil {
			w.log.Error("sweep terminate expired placement", "placement", p.ID, "err", err)
		}
		return err != nil
	}
	return false
}

// stuckTerminating reports whether a stop started long enough ago to count as broken off
func stuckTerminating(p store.Placement, now time.Time) bool {
	return p.TerminatingAt == nil || now.Sub(*p.TerminatingAt) > terminatingAfter
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
