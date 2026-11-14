package worker

import (
	"context"
	"time"

	"github.com/riverqueue/river"

	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

const (
	// retentionTimeout is CP 13.4's, under River's 3-minute rescue
	retentionTimeout = 120 * time.Second
	// retentionBudget is how long a run starts new batches, the rest being left to a run queued
	// behind it
	retentionBudget = 90 * time.Second
	retentionBatch  = 10000
	// retentionHour is the hour of day, UTC, retention runs at
	retentionHour = 4
)

// The ages of the rows with no setting of their own, from CP 13.4
const (
	benchesKeep       = 90 * 24 * time.Hour
	transferIndexKeep = 30 * 24 * time.Hour
	probeSlotsKeep    = 10 * time.Minute
	reservationsKeep  = 7 * 24 * time.Hour
	poolMembersKeep   = 30 * 24 * time.Hour
	poolMissesKeep    = 24 * time.Hour
	priceQuotesKeep   = 24 * time.Hour
)

// The ages of the estimator's rows when Deps leaves them unset
const (
	defaultEstimatesKeep    = 7 * 24 * time.Hour
	defaultSourceFactsKeep  = 7 * 24 * time.Hour
	defaultObservationsKeep = 90 * 24 * time.Hour
)

type retentionWorker struct {
	river.WorkerDefaults[jobargs.Retention]
	w *Worker
}

func (*retentionWorker) Timeout(*river.Job[jobargs.Retention]) time.Duration {
	return retentionTimeout
}

// Work deletes what it can within its budget and queues another run when rows are left
func (rw *retentionWorker) Work(ctx context.Context, _ *river.Job[jobargs.Retention]) error {
	if rw.w.retain(ctx) {
		_, err := rw.w.river.Insert(ctx, jobargs.Retention{}, nil)
		return err
	}
	return nil
}

// ages is how old a row of each table may get before retention deletes it
func (w *Worker) ages() map[store.RetentionTable]time.Duration {
	return map[store.RetentionTable]time.Duration{
		store.RetainStartSamples:     w.d.Health.SamplesKeep,
		store.RetainBenches:          benchesKeep,
		store.RetainTransferIndex:    transferIndexKeep,
		store.RetainProbeSlots:       probeSlotsKeep,
		store.RetainUsageObservation: w.d.ObservationsKeep,
		store.RetainEstimates:        w.d.EstimatesKeep,
		store.RetainPriceQuotes:      priceQuotesKeep,
		store.RetainSourceFacts:      w.d.SourceFactsKeep,
		store.RetainReservations:     reservationsKeep,
		store.RetainPoolMembers:      poolMembersKeep,
		store.RetainPoolMisses:       poolMissesKeep,
	}
}

// retain deletes old rows table by table in batches and reports whether any table still had a
// full batch when the budget ran out. A table that fails is logged and the others go on.
func (w *Worker) retain(ctx context.Context) (more bool) {
	now := w.now()
	stop := now.Add(retentionBudget)
	ages := w.ages()
	for _, t := range store.RetentionTables {
		for {
			n, err := w.store.Purge(ctx, t, now.Add(-ages[t]), retentionBatch)
			if err != nil {
				w.log.Error("retention failed", "table", t, "err", err)
				break
			}
			if n > 0 {
				w.log.Info("retention deleted rows", "table", t, "rows", n)
			}
			if n < retentionBatch {
				break
			}
			if !w.now().Before(stop) || ctx.Err() != nil {
				more = true
				break
			}
		}
	}
	return more
}

// dailyAt is a schedule that fires at one hour of the day, UTC
type dailyAt int

// Next returns the first time after t that is dailyAt o'clock UTC
func (h dailyAt) Next(t time.Time) time.Time {
	t = t.UTC()
	next := time.Date(t.Year(), t.Month(), t.Day(), int(h), 0, 0, 0, time.UTC)
	if !next.After(t) {
		next = next.AddDate(0, 0, 1)
	}
	return next
}

// dailyOffset is a schedule that fires at one offset from midnight, UTC
type dailyOffset time.Duration

// Next returns the first time after t that is the offset past a midnight UTC
func (o dailyOffset) Next(t time.Time) time.Time {
	t = t.UTC()
	next := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC).Add(time.Duration(o))
	if !next.After(t) {
		next = next.AddDate(0, 0, 1)
	}
	return next
}
