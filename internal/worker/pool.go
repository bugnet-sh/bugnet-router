package worker

import (
	"context"
	"errors"
	"time"

	"github.com/riverqueue/river"

	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/pool"
)

// The pool jobs' timeouts are CP 18.14's, each under River's 3-minute rescue
const (
	poolTickTimeout   = 15 * time.Second
	poolFillTimeout   = 170 * time.Second
	poolRetireTimeout = 60 * time.Second
	// poolSnooze is how long a fill waits when its provider has no create token
	poolSnooze = 15 * time.Second
)

type poolTickWorker struct {
	river.WorkerDefaults[jobargs.PoolTick]
	w *Worker
}

func (*poolTickWorker) Timeout(*river.Job[jobargs.PoolTick]) time.Duration {
	return poolTickTimeout
}

// Work sizes every pool and queues its fills, a failed tick only logging since the next comes soon
func (pw *poolTickWorker) Work(ctx context.Context, _ *river.Job[jobargs.PoolTick]) error {
	if err := pw.w.pools.Tick(ctx); err != nil && !errors.Is(err, context.Canceled) {
		pw.w.log.Warn("pool tick", "err", err)
	}
	return nil
}

type poolFillWorker struct {
	river.WorkerDefaults[jobargs.PoolFill]
	w *Worker
}

func (*poolFillWorker) Timeout(*river.Job[jobargs.PoolFill]) time.Duration {
	return poolFillTimeout
}

// Work fills one member, waiting for a create token by snoozing
func (pw *poolFillWorker) Work(ctx context.Context, j *river.Job[jobargs.PoolFill]) error {
	err := pw.w.pools.Fill(ctx, j.Args.Member)
	if errors.Is(err, pool.ErrSnooze) {
		return river.JobSnooze(poolSnooze)
	}
	return err
}

type poolRetireWorker struct {
	river.WorkerDefaults[jobargs.PoolRetire]
	w *Worker
}

func (*poolRetireWorker) Timeout(*river.Job[jobargs.PoolRetire]) time.Duration {
	return poolRetireTimeout
}

// Work destroys one member's sandbox and frees its slot, retried by River while it fails
func (pw *poolRetireWorker) Work(ctx context.Context, j *river.Job[jobargs.PoolRetire]) error {
	return pw.w.pools.Retire(ctx, j.Args.Member, j.Args.Reason)
}

// Pools returns the pool manager, which has no pool when the settings list none
func (w *Worker) Pools() *pool.Manager { return w.pools }
