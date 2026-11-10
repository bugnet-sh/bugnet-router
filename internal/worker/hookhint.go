package worker

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/riverqueue/river"

	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/reconcile"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

type hookHintWorker struct {
	river.WorkerDefaults[jobargs.HookHint]
	w *Worker
}

// Work re-reads the sandbox a verified webhook names (CP 5.5). River's 60 s bounds it.
func (hw *hookHintWorker) Work(ctx context.Context, job *river.Job[jobargs.HookHint]) error {
	w, a := hw.w, job.Args
	if dep := a.DeploymentLabel; dep != "" && dep != w.saga.OwnedLabels()["bugnet_deployment"] {
		return nil
	}
	p, err := w.hintPlacement(ctx, a)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	switch p.State {
	case store.PlacementReady, store.PlacementTerminating:
	case store.PlacementTerminated, store.PlacementFailed:
		// Only an abuse verdict acts on an ended placement, and only inside the late window
		if p.TerminatedAt == nil || w.now().Sub(*p.TerminatedAt) > w.d.Abuse.LateWindow {
			return nil
		}
	default:
		return nil
	}
	ctx, span := w.tel.InOrder(ctx, p, "hook.hint")
	err = w.reconciler.Reconcile(ctx, p, &reconcile.Observation{
		Source: "hook", EventID: a.EventID,
	})
	span.End()
	return err
}

// hintPlacement finds the placement by the event's label, else by the sandbox's id
func (w *Worker) hintPlacement(ctx context.Context, a jobargs.HookHint) (store.Placement, error) {
	if id, err := uuid.Parse(a.PlacementLabel); err == nil {
		p, err := w.store.PlacementByID(ctx, id)
		if err == nil && p.Provider != a.Provider {
			return store.Placement{}, store.ErrNotFound
		}
		return p, err
	}
	return w.store.PlacementBySandbox(ctx, a.Provider, a.SandboxID)
}
