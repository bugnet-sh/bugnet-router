package worker

import (
	"context"
	"time"

	"github.com/riverqueue/river"

	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
)

// SingleRunOpts are the insert options of the periodic sweep and outbox jobs
func SingleRunOpts() *river.InsertOpts { return singleRun() }

// Queues are the River queues and their workers that New configures for d
func Queues(d Deps) map[string]river.QueueConfig { return queues(d) }

// ApplyTimeout is how long one apply_secrets run may take
func ApplyTimeout() time.Duration { return (*applySecretsWorker).Timeout(nil, nil) }

// PrepareSSHTimeout is how long one prepare_ssh run may take
func PrepareSSHTimeout() time.Duration {
	return (&prepareSSHWorker{}).Timeout(&river.Job[jobargs.PrepareSSH]{})
}

// StatsEvery and StatsTimeout are the stats_snapshot job's period and run time
const (
	StatsEvery   = statsEvery
	StatsTimeout = statsTimeout
)

// Snapshot runs one stats_snapshot
func (w *Worker) Snapshot(ctx context.Context) error { return w.snapshot(ctx) }

// StatsJobTimeout is the timeout River gives one stats_snapshot run
func StatsJobTimeout() time.Duration { return (*statsWorker).Timeout(nil, nil) }
