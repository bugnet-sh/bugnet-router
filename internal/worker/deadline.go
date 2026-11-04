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
	// A terminating placement is ended again, since an earlier try broke off before it finished
	if p.State != store.PlacementReady && p.State != store.PlacementTerminating {
		return nil
	}
	if p.Deadline != nil && p.Deadline.Sub(w.now()) > rescheduleAfter {
		_, err := w.river.Insert(ctx, args, &river.InsertOpts{ScheduledAt: *p.Deadline})
		if err != nil {
			return fmt.Errorf("reschedule deadline: %w", err)
		}
		return nil
	}
	err = w.saga.Terminate(ctx, p.ID, saga.ReasonDeadline)
	if errors.Is(err, saga.ErrNotReady) {
		return nil
	}
	return err
}
