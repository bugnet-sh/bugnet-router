// Package worker runs the router's River jobs for placement deadlines, the sweep and the outbox.
//
// New builds one River client that works all three kinds and a saga that inserts its deadline
// jobs through that client. A deadline job ends a placement once its paid time is up. The sweep
// runs every minute and destroys sandboxes no placement needs, terminates ready placements past
// their deadline and fails placements whose placing saga went away. The outbox job runs every
// second and posts each due event, signed, to the configured events URL.
package worker

import (
	"crypto/ed25519"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"

	"github.com/oynozan/bugnet-router/internal/jobargs"
	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/saga"
	"github.com/oynozan/bugnet-router/internal/store"
)

const (
	maxWorkers    = 10
	sweepEvery    = 60 * time.Second
	outboxEvery   = time.Second
	postTimeout   = 10 * time.Second
	callTimeout   = 30 * time.Second
	outboxRunTime = 30 * time.Second
)

// Deps are the worker's own settings next to the saga it builds
type Deps struct {
	EventsURL  string             // events stay pending when empty
	SigningKey ed25519.PrivateKey // signs every delivered event
	Now        func() time.Time   // time.Now when nil
}

// Worker works the deadline, sweep and outbox jobs
type Worker struct {
	d        Deps
	store    *store.Store
	registry *provider.Registry
	log      *slog.Logger
	river    *river.Client[pgx.Tx]
	saga     *saga.Saga
	http     *http.Client
	noURL    sync.Once
}

// New builds the River client that works the router's jobs and the saga inserting through it
func New(pool *pgxpool.Pool, sd saga.Deps, d Deps) (*Worker, error) {
	if len(d.SigningKey) != ed25519.PrivateKeySize {
		return nil, errors.New("worker: an Ed25519 event signing key is required")
	}
	if d.Now == nil {
		d.Now = time.Now
	}
	if sd.Log == nil {
		sd.Log = slog.Default()
	}
	w := &Worker{
		d:        d,
		store:    sd.Store,
		registry: sd.Registry,
		log:      sd.Log,
		http: &http.Client{
			Timeout: postTimeout,
			// A redirect would turn the POST into a GET, so any 3xx counts as a failed delivery
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
	workers := river.NewWorkers()
	river.AddWorker(workers, &deadlineWorker{w: w})
	river.AddWorker(workers, &sweepWorker{w: w})
	river.AddWorker(workers, &outboxWorker{w: w})
	rc, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		Queues:       map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: maxWorkers}},
		Workers:      workers,
		PeriodicJobs: periodicJobs(),
		Logger:       sd.Log,
	})
	if err != nil {
		return nil, err
	}
	sd.River = rc
	w.river = rc
	w.saga = saga.New(sd)
	return w, nil
}

// River returns the client to start and stop
func (w *Worker) River() *river.Client[pgx.Tx] { return w.river }

// Saga returns the saga the jobs terminate and fail placements through
func (w *Worker) Saga() *saga.Saga { return w.saga }

func (w *Worker) now() time.Time { return w.d.Now().UTC() }

// periodicJobs runs the sweep and the outbox at start and on their intervals, one of each at a time
func periodicJobs() []*river.PeriodicJob {
	single := &river.InsertOpts{UniqueOpts: river.UniqueOpts{ByState: []rivertype.JobState{
		rivertype.JobStateAvailable, rivertype.JobStatePending, rivertype.JobStateRunning,
		rivertype.JobStateScheduled,
	}}}
	return []*river.PeriodicJob{
		river.NewPeriodicJob(river.PeriodicInterval(sweepEvery),
			func() (river.JobArgs, *river.InsertOpts) { return jobargs.Sweep{}, single },
			&river.PeriodicJobOpts{RunOnStart: true}),
		river.NewPeriodicJob(river.PeriodicInterval(outboxEvery),
			func() (river.JobArgs, *river.InsertOpts) { return jobargs.Outbox{}, single },
			&river.PeriodicJobOpts{RunOnStart: true}),
	}
}
