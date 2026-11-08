// Package worker runs the router's River jobs for placement deadlines, the watch, the sweep, the
// outbox and apply_secrets.
//
// New builds one River client that works every kind and a saga that inserts its deadline jobs
// through that client. A deadline job ends a placement once its paid time is up. The watch runs
// every 30 s on a budget of its own, fails placements whose placing saga went away, frees slots
// ended placements still hold, ends placements past their deadline and finishes stops that broke
// off, and alerts while outbox events are stuck. The sweep runs every minute over this
// deployment's sandboxes on every provider at once and destroys the ones no placement needs. The
// outbox job runs every second and posts each due event, signed, to the configured events URL,
// retrying a failed one with backoff until it is delivered. A periodic job a crashed worker left
// running is rescued after a few minutes, so it holds back the next run of its kind only that
// long. Data-plane jobs run on the dataplane queue with workers of their own. apply_secrets
// writes a placement's whole secret set into its sandbox, one run at a time per placement under an
// advisory lock, so any number of queued jobs converge. A prepare_ssh job makes one ready
// placement's sshd ready through the saga, within 150 s.
package worker

import (
	"cmp"
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

	"github.com/bugnet-sh/bugnet-router/internal/dpkeys"
	"github.com/bugnet-sh/bugnet-router/internal/edgeevents"
	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

const (
	maxWorkers  = 10
	jobTimeout  = 60 * time.Second
	rescueAfter = 3 * time.Minute // a crashed worker's running job blocks its kind until rescued
	sweepEvery  = 60 * time.Second
	watchEvery  = 30 * time.Second
	// The sweep and the watch stop well before a rescue could start a second run beside them
	sweepTimeout  = 2 * time.Minute
	watchTimeout  = 2 * time.Minute
	outboxEvery   = time.Second
	postTimeout   = 10 * time.Second
	callTimeout   = 30 * time.Second
	outboxRunTime = 30 * time.Second
)

// dataplaneWorkers is the dataplane queue's workers when Deps leaves them unset
const dataplaneWorkers = 8

// Deps are the worker's own settings next to the saga it builds
type Deps struct {
	EventsURL  string             // events stay pending when empty
	SigningKey ed25519.PrivateKey // signs every delivered event
	Now        func() time.Time   // time.Now when nil
	// DataplaneWorkers works the dataplane queue, 8 when zero
	DataplaneWorkers int
	// Egress is where apply_secrets points every forwarder
	Egress Egress
}

// Worker works the deadline, sweep and outbox jobs
type Worker struct {
	d        Deps
	store    *store.Store
	registry *provider.Registry
	log      *slog.Logger
	river    *river.Client[pgx.Tx]
	saga     *saga.Saga
	keys     dpkeys.Keys
	events   *edgeevents.Bus
	http     *http.Client
	noURL    sync.Once
	stuckMu  sync.Mutex
	stuck    int // stuck outbox events at the last report
	// stuckReported is when the watch last reported stuck events
	stuckReported time.Time
}

// New builds the River client that works the router's jobs and the saga inserting through it
func New(pool *pgxpool.Pool, sd saga.Deps, d Deps) (*Worker, error) {
	if len(d.SigningKey) != ed25519.PrivateKeySize {
		return nil, errors.New("worker: an Ed25519 event signing key is required")
	}
	if d.Now == nil {
		d.Now = time.Now
	}
	d.DataplaneWorkers = cmp.Or(d.DataplaneWorkers, dataplaneWorkers)
	if sd.Log == nil {
		sd.Log = slog.Default()
	}
	w := &Worker{
		d:        d,
		store:    sd.Store,
		registry: sd.Registry,
		log:      sd.Log,
		keys:     sd.Keys,
		events:   sd.Events,
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
	river.AddWorker(workers, &watchWorker{w: w})
	river.AddWorker(workers, &outboxWorker{w: w})
	river.AddWorker(workers, &applySecretsWorker{w: w})
	river.AddWorker(workers, &prepareSSHWorker{w: w})
	rc, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		Queues:               queues(d),
		Workers:              workers,
		PeriodicJobs:         periodicJobs(),
		JobTimeout:           jobTimeout,
		RescueStuckJobsAfter: rescueAfter,
		Logger:               sd.Log,
	})
	if err != nil {
		return nil, err
	}
	sd.River = rc
	w.river = rc
	w.saga = saga.New(sd)
	return w, nil
}

// queues gives data-plane jobs their own queue, so slow Execs never delay deadlines or the outbox
func queues(d Deps) map[string]river.QueueConfig {
	return map[string]river.QueueConfig{
		river.QueueDefault:     {MaxWorkers: maxWorkers},
		jobargs.QueueDataplane: {MaxWorkers: cmp.Or(d.DataplaneWorkers, dataplaneWorkers)},
	}
}

// DataplaneWorkers is how many workers River runs on the dataplane queue
func (w *Worker) DataplaneWorkers() int { return w.d.DataplaneWorkers }

// River returns the client to start and stop
func (w *Worker) River() *river.Client[pgx.Tx] { return w.river }

// Saga returns the saga the jobs terminate and fail placements through
func (w *Worker) Saga() *saga.Saga { return w.saga }

func (w *Worker) now() time.Time { return w.d.Now().UTC() }

// periodicJobs runs the sweep, watch and outbox at start and on their intervals, one at a time
func periodicJobs() []*river.PeriodicJob {
	single := singleRun()
	return []*river.PeriodicJob{
		river.NewPeriodicJob(river.PeriodicInterval(sweepEvery),
			func() (river.JobArgs, *river.InsertOpts) { return jobargs.Sweep{}, single },
			&river.PeriodicJobOpts{RunOnStart: true}),
		river.NewPeriodicJob(river.PeriodicInterval(watchEvery),
			func() (river.JobArgs, *river.InsertOpts) { return jobargs.Watch{}, single },
			&river.PeriodicJobOpts{RunOnStart: true}),
		river.NewPeriodicJob(river.PeriodicInterval(outboxEvery),
			func() (river.JobArgs, *river.InsertOpts) { return jobargs.Outbox{}, single },
			&river.PeriodicJobOpts{RunOnStart: true}),
	}
}

// singleRun skips inserting a job while one of its kind waits or runs
func singleRun() *river.InsertOpts {
	return &river.InsertOpts{UniqueOpts: river.UniqueOpts{ByState: []rivertype.JobState{
		rivertype.JobStateAvailable, rivertype.JobStatePending, rivertype.JobStateRunning,
		rivertype.JobStateScheduled,
	}}}
}
