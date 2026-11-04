package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/riverqueue/river"

	"github.com/oynozan/bugnet-router/internal/jobargs"
	"github.com/oynozan/bugnet-router/internal/saga"
	"github.com/oynozan/bugnet-router/internal/store"
)

// rescheduleAfter is how far ahead a deadline must be for its job to wait again instead of ending
const rescheduleAfter = time.Second

type deadlineWorker struct {
	river.WorkerDefaults[jobargs.Deadline]
	w *Worker
}

func (dw *deadlineWorker) Work(ctx context.Context, job *river.Job[jobargs.Deadline]) error {
	return dw.w.deadline(ctx, job.Args)
}

// deadline ends a placement whose paid time is up and waits again for one that was extended
func (w *Worker) deadline(ctx context.Context, args jobargs.Deadline) error {
	p, err := w.store.PlacementByID(ctx, args.PlacementID)
	if errors.Is(err, store.ErrNotFound) {
		w.log.Warn("deadline for an unknown placement", "placement", args.PlacementID)
		return nil
	}
	if err != nil {
		return err
	}
	// A terminating placement is ended at once, since an earlier try broke off before it finished
	switch p.State {
	case store.PlacementTerminating:
	case store.PlacementReady:
		if p.Deadline != nil && p.Deadline.Sub(w.now()) > rescheduleAfter {
			return w.waitAgain(ctx, args, *p.Deadline)
		}
	default:
		return nil
	}
	err = w.saga.Terminate(ctx, p.ID, w.now().Add(rescheduleAfter))
	switch {
	case errors.Is(err, saga.ErrNotDue):
		// An extension committed after the read above, so the job waits for the new deadline
		cur, err := w.store.PlacementByID(ctx, p.ID)
		if err != nil || cur.State != store.PlacementReady || cur.Deadline == nil {
			return err
		}
		return w.waitAgain(ctx, args, *cur.Deadline)
	case errors.Is(err, saga.ErrNotReady):
		return nil
	}
	return err
}

func (w *Worker) waitAgain(ctx context.Context, args jobargs.Deadline, at time.Time) error {
	if _, err := w.river.Insert(ctx, args, &river.InsertOpts{ScheduledAt: at}); err != nil {
		return fmt.Errorf("reschedule deadline: %w", err)
	}
	return nil
}
