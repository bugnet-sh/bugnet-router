package worker

import "github.com/riverqueue/river"

// SingleRunOpts are the insert options of the periodic sweep and outbox jobs
func SingleRunOpts() *river.InsertOpts { return singleRun() }
