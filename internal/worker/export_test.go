package worker

import "github.com/riverqueue/river"

// SingleRunOpts are the insert options of the periodic sweep and outbox jobs
func SingleRunOpts() *river.InsertOpts { return singleRun() }

// Queues are the River queues and their workers that New configures for d
func Queues(d Deps) map[string]river.QueueConfig { return queues(d) }
