package worker

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"

	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/reconcile"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// reconcileTimeout is CP 13.4's, under River's 3-minute rescue
const reconcileTimeout = 90 * time.Second

type reconcileWorker struct {
	river.WorkerDefaults[jobargs.Reconcile]
	w *Worker
}

func (*reconcileWorker) Timeout(*river.Job[jobargs.Reconcile]) time.Duration {
	return reconcileTimeout
}

// Work reads the placement's sandbox and acts on what the provider says, once
func (rw *reconcileWorker) Work(ctx context.Context, job *river.Job[jobargs.Reconcile]) error {
	p, err := rw.w.store.PlacementByID(ctx, job.Args.PlacementID)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	ctx, span := rw.w.tel.InOrder(ctx, p, "reconcile")
	err = rw.w.reconciler.Reconcile(ctx, p, nil)
	span.End()
	return err
}

// queueLost inserts a reconcile job for each ready placement the listing lacks, shows unwell or
// shows with a lifetime off its deadline (CP 10.4), at most max_per_sweep new jobs, and only for
// a listing that finished (CP 10.3)
func (w *Worker) queueLost(
	ctx context.Context, prov provider.Provider, owned []provider.Summary, listStarted time.Time,
) {
	set := w.d.Reconcile
	ready, err := w.store.ReadySandboxes(ctx, prov.Name(), listStarted.Add(-set.ReadyGrace))
	if err != nil {
		w.log.Error("sweep list ready placements", "provider", prov.Name(), "err", err)
		return
	}
	listed := make(map[string]provider.Summary, len(owned))
	for _, o := range owned {
		listed[o.Ref.ID] = o
	}
	var ids []uuid.UUID
	for _, rs := range ready {
		o, ok := listed[rs.SandboxID]
		if !ok || unwell(o.State) ||
			reconcile.Drifted(prov.Name(), o.ExpiresAt, rs.Deadline, set.LifetimeDrift) {
			ids = append(ids, rs.PlacementID)
		}
	}
	queued := 0
	for _, id := range ids {
		if queued >= set.MaxPerSweep || ctx.Err() != nil {
			break
		}
		res, err := w.river.Insert(ctx, jobargs.Reconcile{PlacementID: id}, nil)
		if err != nil {
			w.log.Error("sweep insert reconcile job", "placement", id, "err", err)
			continue
		}
		if !res.UniqueSkippedAsDuplicate {
			queued++
		}
	}
	if len(ids) > queued {
		w.log.Warn("sweep left lost placements for the next sweep", "provider", prov.Name(),
			"found", len(ids), "queued", queued)
	}
}

// unwell reports a listed state the listing itself calls for a fresh reading on
func unwell(s provider.State) bool {
	switch s {
	case provider.StateDegraded, provider.StateStopped, provider.StateFailed, provider.StateGone:
		return true
	}
	return false
}
