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
// placement's sshd ready through the saga, within 150 s. wallet_stop_live ends a blocked payer's
// live placements a few at a time, and wallet_expiry lifts timed wallet blocks every 10 minutes.
// With Deps.Stats set, stats_snapshot writes the public stats feed's row every 30 s. The session
// copy moves an ended placement's job output into its session record, and session expiry runs at
// start and hourly and deletes the records whose page ended and no hold keeps. For each provider
// that reports them, limits_poll writes the account's cap and use into the capacity row and
// account_check stores whether the account may start sandboxes. canary_tick runs every minute and
// queues a probe for each provider whose canary is due. A probe starts a tiny sandbox the way a
// placement does, times it, removes it and stores a start sample, on a queue of its own. The
// health_rollup job turns the last 15 minutes of samples into each provider's shared state every
// 30 s. A bench job runs one wave of a benchmark at a time, queueing its probes and snoozing until
// they have stored their samples. The retention job runs at start and daily at 04:00 UTC and
// deletes old rows in batches of 10,000.
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

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/dpkeys"
	"github.com/bugnet-sh/bugnet-router/internal/edgeevents"
	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/probe"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/secrets"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/streams"
	"github.com/bugnet-sh/bugnet-router/internal/wallets"
)

const (
	jobTimeout  = 60 * time.Second
	rescueAfter = 3 * time.Minute // a crashed worker's running job blocks its kind until rescued
	sweepEvery  = 60 * time.Second
	watchEvery  = 30 * time.Second
	// The sweep and the watch stop well before a rescue could start a second run beside them
	sweepTimeout       = 2 * time.Minute
	watchTimeout       = 2 * time.Minute
	outboxEvery        = time.Second
	postTimeout        = 10 * time.Second
	callTimeout        = 30 * time.Second
	outboxRunTime      = 30 * time.Second
	sessionExpiryEvery = time.Hour
)

// dataplaneWorkers and defaultWorkers are each queue's workers when Deps leaves them unset
const (
	dataplaneWorkers = 8
	defaultWorkers   = 20
	probeWorkers     = 4
)

// The session settings' defaults, for Deps that leave them unset
const (
	defaultJobOutput       = 64 << 10
	defaultPlacementOutput = 4 << 20
	defaultDiskBudget      = 8 << 30
)

// Deps are the worker's own settings next to the saga it builds
type Deps struct {
	EventsURL  string             // events stay pending when empty
	SigningKey ed25519.PrivateKey // signs every delivered event
	Now        func() time.Time   // time.Now when nil
	// DataplaneWorkers works the dataplane queue, 8 when zero
	DataplaneWorkers int
	// DefaultWorkers works the default queue, 20 when zero
	DefaultWorkers int
	// Egress is where apply_secrets points every forwarder
	Egress Egress
	// Sources reads each placement's egress source for apply_secrets, off when nil
	Sources *secrets.Sources
	// Stats turns on the stats_snapshot job, off when nil
	Stats *Stats
	// Sessions sizes the session copy and its disk budget
	Sessions config.Sessions
	// LimitsPoll sets the limit polls and account checks, each zero value at its default
	LimitsPoll config.LimitsPoll
	// MaxConcurrent is each provider's own cap setting, absent for the account's
	MaxConcurrent map[string]int
	// Health sets the canaries and the health rollup, each zero value at its default
	Health config.Health
	// EstimatesKeep, SourceFactsKeep and ObservationsKeep are how long retention keeps the
	// estimator's rows, each zero at its default
	EstimatesKeep, SourceFactsKeep, ObservationsKeep time.Duration
}

// Worker works the deadline, sweep and outbox jobs
type Worker struct {
	d        Deps
	store    *store.Store
	registry *provider.Registry
	log      *slog.Logger
	river    *river.Client[pgx.Tx]
	saga     *saga.Saga
	wallets  *wallets.Store
	keys     dpkeys.Keys
	events   *edgeevents.Bus
	http     *http.Client
	noURL    sync.Once
	stuckMu  sync.Mutex
	stuck    int // stuck outbox events at the last report
	// stuckReported is when the watch last reported stuck events
	stuckReported time.Time
	statsMu       sync.Mutex
	noTokenLogged time.Time
	// tokenRefused holds back the error log of a refused token until Payments answers again
	tokenRefused bool
	// bus reads job streams for the session copy
	bus  *streams.Bus
	disk diskGauge
	// checks counts each provider's failed account checks in a row
	checksMu sync.Mutex
	checks   map[string]failedChecks
	probe    *probe.Probe
	// probeAges is the age of each provider's oldest probe sandbox at the last sweep
	probesMu  sync.Mutex
	probeAges map[string]time.Duration
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
	d.DefaultWorkers = cmp.Or(d.DefaultWorkers, defaultWorkers)
	d.Sessions.JobOutput = cmp.Or(d.Sessions.JobOutput, defaultJobOutput)
	d.Sessions.PlacementOutput = cmp.Or(d.Sessions.PlacementOutput, defaultPlacementOutput)
	d.Sessions.DiskBudget = cmp.Or(d.Sessions.DiskBudget, defaultDiskBudget)
	if d.LimitsPoll == (config.LimitsPoll{}) {
		d.LimitsPoll = config.DefaultLimitsPoll()
	}
	if d.Health == (config.Health{}) {
		d.Health = config.DefaultHealth()
	}
	d.EstimatesKeep = cmp.Or(d.EstimatesKeep, defaultEstimatesKeep)
	d.SourceFactsKeep = cmp.Or(d.SourceFactsKeep, defaultSourceFactsKeep)
	d.ObservationsKeep = cmp.Or(d.ObservationsKeep, defaultObservationsKeep)
	if sd.Log == nil {
		sd.Log = slog.Default()
	}
	w := &Worker{
		checks:   map[string]failedChecks{},
		d:        d,
		store:    sd.Store,
		registry: sd.Registry,
		log:      sd.Log,
		bus:      streams.NewBus(sd.Valkey, sd.Prefix),
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
	river.AddWorker(workers, &walletStopLiveWorker{w: w})
	river.AddWorker(workers, &walletExpiryWorker{w: w})
	river.AddWorker(workers, &statsWorker{w: w})
	river.AddWorker(workers, &sessionCopyWorker{w: w})
	river.AddWorker(workers, &sessionExpiryWorker{w: w})
	river.AddWorker(workers, &limitsPollWorker{w: w})
	river.AddWorker(workers, &accountCheckWorker{w: w})
	river.AddWorker(workers, &canaryTickWorker{w: w})
	river.AddWorker(workers, &probeWorker{w: w})
	river.AddWorker(workers, &healthRollupWorker{w: w})
	river.AddWorker(workers, &benchWorker{w: w})
	river.AddWorker(workers, &retentionWorker{w: w})
	rc, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		Queues:               queues(d),
		Workers:              workers,
		PeriodicJobs:         periodicJobs(d, sd.Registry),
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
	w.probe = probe.New(probe.Deps{
		Registry: sd.Registry, Store: sd.Store, Saga: w.saga, Replica: sd.ReplicaID,
		Now: d.Now, Log: sd.Log,
	})
	ws := sd.Wallets.OrDefaults()
	w.wallets = wallets.New(sd.Store, wallets.Settings{
		Facilitators: ws.FacilitatorAddresses, StrikeThreshold: ws.StrikeThreshold,
		StrikeWindow: ws.StrikeWindow, StrikeBlockFor: ws.StrikeBlockFor,
		EmitEvents: ws.EmitEvents, Now: sd.Now,
	})
	return w, nil
}

// queues gives data-plane jobs their own queue, so slow Execs never delay deadlines or the outbox
func queues(d Deps) map[string]river.QueueConfig {
	return map[string]river.QueueConfig{
		river.QueueDefault:     {MaxWorkers: cmp.Or(d.DefaultWorkers, defaultWorkers)},
		jobargs.QueueDataplane: {MaxWorkers: cmp.Or(d.DataplaneWorkers, dataplaneWorkers)},
		jobargs.QueueProbes:    {MaxWorkers: cmp.Or(d.Health.ProbeWorkers, probeWorkers)},
	}
}

// DataplaneWorkers is how many workers River runs on the dataplane queue
func (w *Worker) DataplaneWorkers() int { return w.d.DataplaneWorkers }

// Workers is how many workers River runs on every queue together, each holding a connection
func (w *Worker) Workers() int {
	n := 0
	for _, q := range queues(w.d) {
		n += q.MaxWorkers
	}
	return n
}

// River returns the client to start and stop
func (w *Worker) River() *river.Client[pgx.Tx] { return w.river }

// Saga returns the saga the jobs terminate and fail placements through
func (w *Worker) Saga() *saga.Saga { return w.saga }

func (w *Worker) now() time.Time { return w.d.Now().UTC() }

// periodicJobs runs the sweep, watch, outbox and wallet expiry at start and on their intervals,
// and retention at start and daily, one at a time, and a limits poll and an account check for
// each provider that can give them
func periodicJobs(d Deps, reg *provider.Registry) []*river.PeriodicJob {
	single := singleRun()
	var out []*river.PeriodicJob
	for _, p := range periodic(d) {
		out = append(out, river.NewPeriodicJob(river.PeriodicInterval(p.every),
			func() (river.JobArgs, *river.InsertOpts) { return p.args, single },
			&river.PeriodicJobOpts{RunOnStart: true}))
	}
	out = append(out, river.NewPeriodicJob(dailyAt(retentionHour),
		func() (river.JobArgs, *river.InsertOpts) { return jobargs.Retention{}, single },
		&river.PeriodicJobOpts{RunOnStart: true}))
	for _, p := range polls(d, reg) {
		// The job's own unique options keep one run of each kind per provider
		out = append(out, river.NewPeriodicJob(river.PeriodicInterval(p.every),
			func() (river.JobArgs, *river.InsertOpts) { return p.args, nil },
			&river.PeriodicJobOpts{RunOnStart: true}))
	}
	return out
}

// polls lists the limits poll and account check of each provider that reads limits or accounts
func polls(d Deps, reg *provider.Registry) []periodicJob {
	var out []periodicJob
	if reg == nil {
		return nil
	}
	for _, p := range reg.Ordered() {
		name := p.Name()
		if _, ok := p.(provider.LimitsReader); ok {
			every := d.LimitsPoll.Every
			if name == "boat" {
				every = d.LimitsPoll.BoatEvery
			}
			out = append(out, periodicJob{every, jobargs.LimitsPoll{Provider: name}})
		}
		if _, ok := p.(provider.AccountChecker); ok {
			out = append(out, periodicJob{accountEvery, jobargs.AccountCheck{Provider: name}})
		}
	}
	return out
}

// periodicJob is one job kind the worker inserts on an interval
type periodicJob struct {
	every time.Duration
	args  river.JobArgs
}

func periodic(d Deps) []periodicJob {
	out := []periodicJob{
		{sweepEvery, jobargs.Sweep{}},
		{watchEvery, jobargs.Watch{}},
		{outboxEvery, jobargs.Outbox{}},
		{expiryEvery, jobargs.WalletExpiry{}},
		{sessionExpiryEvery, jobargs.SessionExpiry{}},
		{canaryEvery, jobargs.CanaryTick{}},
		{rollupEvery, jobargs.HealthRollup{}},
	}
	if d.Stats != nil {
		out = append(out, periodicJob{statsEvery, jobargs.StatsSnapshot{}})
	}
	return out
}

// singleRun skips inserting a job while one of its kind waits or runs
func singleRun() *river.InsertOpts {
	return &river.InsertOpts{UniqueOpts: river.UniqueOpts{ByState: []rivertype.JobState{
		rivertype.JobStateAvailable, rivertype.JobStatePending, rivertype.JobStateRunning,
		rivertype.JobStateScheduled,
	}}}
}
