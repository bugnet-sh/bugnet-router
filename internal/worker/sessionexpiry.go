package worker

import (
	"context"
	"time"

	"github.com/riverqueue/river"

	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
)

// expiryBatch is the most records one session_expiry run deletes
const expiryBatch = 10_000

const expiryTimeout = 120 * time.Second

type sessionExpiryWorker struct {
	river.WorkerDefaults[jobargs.SessionExpiry]
	w *Worker
}

// Timeout keeps one run under the rescue
func (*sessionExpiryWorker) Timeout(*river.Job[jobargs.SessionExpiry]) time.Duration {
	return expiryTimeout
}

// Work deletes the records whose page ended and that no hold keeps
func (ew *sessionExpiryWorker) Work(
	ctx context.Context, _ *river.Job[jobargs.SessionExpiry],
) error {
	n, err := ew.w.store.ExpireSessionRecords(ctx, ew.w.now(), expiryBatch)
	if err != nil {
		return err
	}
	if n > 0 {
		ew.w.log.Info("session records deleted", "records", n)
	}
	return nil
}
