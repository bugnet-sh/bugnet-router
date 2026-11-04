package worker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"

	"github.com/oynozan/bugnet-router/internal/jobargs"
	"github.com/oynozan/bugnet-router/internal/saga"
)

const (
	// silentAfter is how long a placing saga may go without a heartbeat before it counts as gone
	silentAfter = 60 * time.Second
	// unbeatAfter is how long a placement may stay placing without ever heartbeating
	unbeatAfter = 2 * time.Minute
	// terminatingAfter is how long a stop may take before the watch finishes it
	terminatingAfter = 2 * time.Minute
	// endWorkers bounds how many placements one watch ends at a time
	endWorkers = 4
)

type watchWorker struct {
	river.WorkerDefaults[jobargs.Watch]
	w *Worker
}

// Timeout gives the watch a budget of its own, so the sweep's provider calls never use it up
func (*watchWorker) Timeout(*river.Job[jobargs.Watch]) time.Duration { return watchTimeout }

// Work logs every failure and goes on, the next watch retries what was missed
func (ww *watchWorker) Work(ctx context.Context, _ *river.Job[jobargs.Watch]) error {
	ww.w.watchPlacements(ctx)
	return nil
}

// watchPlacements fails placements left placing first, since that pass needs only the database
func (w *Worker) watchPlacements(ctx context.Context) {
	now := w.now()
	w.failStalled(ctx, now)
	if n, err := w.store.ReleaseEndedCapacity(ctx); err != nil {
		w.log.Error("watch release slots of ended placements", "err", err)
	} else if n > 0 {
		w.log.Warn("watch freed slots that ended placements still held", "slots", n)
	}
	w.endOverdue(ctx, now)
}

func (w *Worker) failStalled(ctx context.Context, now time.Time) {
	stalled, err := w.store.StalledPlacing(ctx, now.Add(-silentAfter), now.Add(-unbeatAfter))
	if err != nil {
		w.log.Error("watch list stalled placements", "err", err)
	}
	for _, id := range stalled {
		w.log.Warn("failing a placement whose saga went away", "placement", id)
		if err := w.saga.FailStuck(ctx, id, saga.ReasonPlacingStalled); err != nil {
			w.log.Error("watch fail stalled placement", "placement", id, "err", err)
		}
	}
}

// endOverdue ends placements past their deadline and finishes stops that broke off
func (w *Worker) endOverdue(ctx context.Context, now time.Time) {
	overdue, err := w.store.ReadyOrTerminatingPastDeadline(ctx, now)
	if err != nil {
		w.log.Error("watch list overdue placements", "err", err)
	}
	stuck, err := w.store.TerminatingSince(ctx, now.Add(-terminatingAfter))
	if err != nil {
		w.log.Error("watch list placements left terminating", "err", err)
	}
	seen := map[uuid.UUID]bool{}
	slots := make(chan struct{}, endWorkers)
	var wg sync.WaitGroup
	defer wg.Wait()
	for _, id := range append(overdue, stuck...) {
		if seen[id] {
			continue
		}
		seen[id] = true
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return
		}
		wg.Go(func() {
			defer func() { <-slots }()
			err := w.saga.Terminate(ctx, id, now)
			if err != nil && !errors.Is(err, saga.ErrNotReady) && !errors.Is(err, saga.ErrNotDue) {
				w.log.Error("watch end overdue placement", "placement", id, "err", err)
			}
		})
	}
}
