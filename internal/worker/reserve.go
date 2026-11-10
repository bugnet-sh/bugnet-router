package worker

import (
	"context"
	"errors"
	"time"

	"github.com/riverqueue/river"

	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
)

// reserveWatchTimeout is CP 18.14's, under River's 3-minute rescue
const reserveWatchTimeout = 20 * time.Second

// reserveWatchEvery is the gap between reserve_watch runs
const reserveWatchEvery = 10 * time.Second

type reserveWatchWorker struct {
	river.WorkerDefaults[jobargs.ReserveWatch]
	w *Worker
}

func (*reserveWatchWorker) Timeout(*river.Job[jobargs.ReserveWatch]) time.Duration {
	return reserveWatchTimeout
}

// Work lapses and fails reservations, retires the members they leave held and sets or clears the
// brake, a failed run only logging since the next comes in seconds
func (rw *reserveWatchWorker) Work(ctx context.Context, _ *river.Job[jobargs.ReserveWatch]) error {
	rep, err := rw.w.saga.WatchReservations(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		rw.w.log.Warn("reserve watch", "err", err)
	}
	if rep.Lapsed+rep.Failed+rep.Retired > 0 {
		rw.w.log.Info("reserve watch ended reservations", "lapsed", rep.Lapsed,
			"failed", rep.Failed, "retired", rep.Retired)
	}
	return nil
}
