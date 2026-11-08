package worker

import (
	"context"
	"time"

	"github.com/riverqueue/river"

	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
)

// prepareTimeout covers ssh-up's 120 s install and its other steps, under the 3-minute rescue
const prepareTimeout = 150 * time.Second

type prepareSSHWorker struct {
	river.WorkerDefaults[jobargs.PrepareSSH]
	w *Worker
}

func (pw *prepareSSHWorker) Work(ctx context.Context, job *river.Job[jobargs.PrepareSSH]) error {
	return pw.w.saga.PrepareSSH(ctx, job.Args.PlacementID)
}

// Timeout bounds one prepare_ssh run
func (*prepareSSHWorker) Timeout(*river.Job[jobargs.PrepareSSH]) time.Duration {
	return prepareTimeout
}
