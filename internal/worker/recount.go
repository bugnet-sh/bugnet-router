package worker

import (
	"context"
	"errors"
	"time"

	"github.com/riverqueue/river"

	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
)

// recountTimeout is CP 13.4's, under River's 3-minute rescue
const recountTimeout = 30 * time.Second

type recountWorker struct {
	river.WorkerDefaults[jobargs.CapacityRecount]
	w *Worker
}

func (*recountWorker) Timeout(*river.Job[jobargs.CapacityRecount]) time.Duration {
	return recountTimeout
}

// Work never fails the job, since the next recount comes within minutes and a failed one only logs
func (rw *recountWorker) Work(ctx context.Context, _ *river.Job[jobargs.CapacityRecount]) error {
	rw.w.recount(ctx)
	return nil
}

// recount sets each provider's in_use to the slots its placements and probes hold (CP 10.5)
func (w *Worker) recount(ctx context.Context) {
	names, err := w.store.CapacityProviders(ctx)
	if err != nil {
		w.log.Warn("recount list providers", "err", err)
		return
	}
	for _, name := range names {
		if ctx.Err() != nil {
			return
		}
		before, after, err := w.store.RecountCapacity(ctx, name, probeSlotsKeep, w.now())
		switch {
		case errors.Is(err, context.Canceled):
			return
		case err != nil:
			w.log.Warn("recount capacity", "provider", name, "err", err)
		case before != after:
			w.log.Warn("capacity recount fixed drift", "provider", name, "was", before,
				"now", after)
		}
	}
}
