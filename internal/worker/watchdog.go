package worker

import (
	"context"
	"errors"
	"time"

	"github.com/oynozan/bugnet-router/internal/saga"
)

const (
	// silentAfter is how long a placing saga may go without a heartbeat before it counts as gone
	silentAfter = 60 * time.Second
	// unbeatAfter is how long a placement may stay placing without ever heartbeating
	unbeatAfter = 2 * time.Minute
)

// watchPlacements ends ready placements past their deadline and fails placements left placing
func (w *Worker) watchPlacements(ctx context.Context) {
	now := w.now()
	overdue, err := w.store.ReadyPastDeadline(ctx, now)
	if err != nil {
		w.log.Error("sweep list overdue placements", "err", err)
	}
	for _, id := range overdue {
		err := w.saga.Terminate(ctx, id, saga.ReasonDeadline)
		if err != nil && !errors.Is(err, saga.ErrNotReady) {
			w.log.Error("sweep terminate overdue placement", "placement", id, "err", err)
		}
	}
	stalled, err := w.store.StalledPlacing(ctx, now.Add(-silentAfter), now.Add(-unbeatAfter))
	if err != nil {
		w.log.Error("sweep list stalled placements", "err", err)
	}
	for _, id := range stalled {
		w.log.Warn("failing a placement whose saga went away", "placement", id)
		if err := w.saga.FailStuck(ctx, id, saga.ReasonPlacingStalled); err != nil {
			w.log.Error("sweep fail stalled placement", "placement", id, "err", err)
		}
	}
}
