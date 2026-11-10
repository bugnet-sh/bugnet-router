package worker

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"

	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/pool"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/spend"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

const (
	// costTickTimeout and costDailyTimeout are CP 13.4's, under River's 3-minute rescue
	costTickTimeout  = 15 * time.Second
	costDailyTimeout = 120 * time.Second
	// costDayLayout is the form of a UTC day in a job's arguments
	costDayLayout = "2006-01-02"
	// costRecheckDays is how many days back an incomplete reading is read again
	costRecheckDays = 2
	// costGiveUp is how long after its first reading an incomplete day alerts anyway
	costGiveUp = 2 * 24 * time.Hour
	// costStale is how long a day's page chain may stand still before the tick starts it again
	costStale = time.Hour
	// maxSeenUnknown bounds the unknown ids one day keeps for the count
	maxSeenUnknown = 10000
)

type costTickWorker struct {
	river.WorkerDefaults[jobargs.CostDailyTick]
	w *Worker
}

func (*costTickWorker) Timeout(*river.Job[jobargs.CostDailyTick]) time.Duration {
	return costTickTimeout
}

// Work never fails the job, since the next tick comes the next day and a failed one only logs
func (cw *costTickWorker) Work(ctx context.Context, _ *river.Job[jobargs.CostDailyTick]) error {
	cw.w.costDailyTick(ctx)
	return nil
}

type costDailyWorker struct {
	river.WorkerDefaults[jobargs.CostDaily]
	w *Worker
}

func (*costDailyWorker) Timeout(*river.Job[jobargs.CostDaily]) time.Duration {
	return costDailyTimeout
}

// Work reads one page of the day, a failed read being tried again by River
func (cw *costDailyWorker) Work(ctx context.Context, j *river.Job[jobargs.CostDaily]) error {
	day, err := time.Parse(costDayLayout, j.Args.Day)
	if err != nil {
		return river.JobCancel(fmt.Errorf("cost_daily day %q: %w", j.Args.Day, err))
	}
	prov, ok := cw.w.registry.Get(j.Args.Provider)
	if !ok {
		return nil
	}
	reader, ok := prov.(provider.UsageReader)
	if !ok {
		return nil
	}
	return cw.w.costDailyPage(ctx, prov, reader, day)
}

// costDailyTick queues the check of the UTC day before for each provider that reports usage, and
// of the last days it had not finished (CP 10.7)
func (w *Worker) costDailyTick(ctx context.Context) {
	if w.registry == nil {
		return
	}
	now := w.now()
	yesterday := now.Truncate(24*time.Hour).AddDate(0, 0, -1)
	for _, prov := range w.registry.Ordered() {
		if _, ok := prov.(provider.UsageReader); !ok {
			continue
		}
		name := prov.Name()
		days, err := w.store.UnfinishedCostDays(ctx, name,
			yesterday.AddDate(0, 0, -costRecheckDays), now.Add(-costStale))
		if err != nil {
			w.log.Warn("cost tick list unfinished days", "provider", name, "err", err)
			continue
		}
		_, found, err := w.store.CostDayFor(ctx, name, yesterday, false)
		if err != nil {
			w.log.Warn("cost tick read yesterday", "provider", name, "err", err)
			continue
		}
		if !found && !slices.ContainsFunc(days, yesterday.Equal) {
			days = append(days, yesterday)
		}
		for _, day := range days {
			args := jobargs.CostDaily{Provider: name, Day: day.Format(costDayLayout)}
			if _, err := w.river.Insert(ctx, args, nil); err != nil {
				w.log.Warn("cost tick insert", "provider", name, "day", args.Day, "err", err)
			}
		}
	}
}

// ours is what our meter says the provider charged for the day, the attempts that made a sandbox
// in it and the probes that ran in it
func (w *Worker) ours(
	ctx context.Context, name string, from, to time.Time,
) (int64, error) {
	spans, err := w.store.SpansCreatedBetween(ctx, name, from, to)
	if err != nil {
		return 0, err
	}
	probes, err := w.store.ProbeCostBetween(ctx, name, from, to)
	if err != nil {
		return 0, err
	}
	cost := spend.SpansCost(spans, w.now(), func(n string) time.Duration {
		if p, ok := w.registry.Get(n); ok {
			return p.BillingIncrement()
		}
		return 0
	})
	members, err := w.store.MembersCreatedBetween(ctx, name, from, to)
	if err != nil {
		return 0, err
	}
	prices := pool.Prices(w.registry)
	for _, m := range members {
		hourly, parked := prices(m.Provider, m.SizeName)
		cost += spend.MemberCost(m, hourly, parked, w.now())
	}
	return cost + probes, nil
}

// costDailyPage reads one page of the provider's usage for the day into its row, and queues the
// next page in the same transaction that stores the cursor
func (w *Worker) costDailyPage(
	ctx context.Context, prov provider.Provider, reader provider.UsageReader, day time.Time,
) error {
	name := prov.Name()
	to := day.AddDate(0, 0, 1)
	row, found, err := w.store.CostDayFor(ctx, name, day, false)
	if err != nil {
		return err
	}
	now := w.now()
	switch {
	case found && row.Cursor == "" && row.TheirsComplete:
		return nil
	case !found || row.Cursor == "":
		// The first reading, or a day read to the end that the provider had not finished
		first := now
		if found {
			first = row.FirstCheckedAt
		}
		ours, err := w.ours(ctx, name, day, to)
		if err != nil {
			return err
		}
		row = store.CostDay{
			Provider: name, Day: day, OursMicros: ours, TheirsMicros: -1,
			TheirsComplete: true, FirstCheckedAt: first,
		}
	}
	ids, err := w.store.SandboxIDsBetween(ctx, name, day, to)
	if err != nil {
		return err
	}
	refs := make([]provider.Ref, len(ids))
	for i, id := range ids {
		refs[i] = provider.Ref{Provider: name, ID: id}
	}
	u, err := reader.ReadUsage(ctx, day, to, refs, row.Cursor)
	if err != nil {
		return fmt.Errorf("read %s usage of %s: %w", name, day.Format(costDayLayout), err)
	}
	if err := w.addUsage(ctx, name, &row, u); err != nil {
		return err
	}
	row.Cursor, row.CheckedAt = u.Next, now
	row.TheirsComplete = row.TheirsComplete && u.Complete
	err = w.store.WithTx(ctx, func(tx pgx.Tx) error {
		if err := w.store.Tx(tx).SaveCostDay(ctx, row); err != nil {
			return err
		}
		if u.Next == "" {
			return nil
		}
		_, err := w.river.InsertTx(ctx, tx, jobargs.CostDaily{
			Provider: name, Day: day.Format(costDayLayout),
		}, nil)
		return err
	})
	if err == nil && u.Next == "" {
		w.checkCost(row, now)
	}
	return err
}

// addUsage adds one page of usage to the day's row and counts the sandbox ids that are not ours
func (w *Worker) addUsage(
	ctx context.Context, name string, row *store.CostDay, u provider.Usage,
) error {
	figure, known := u.TotalMicros, u.TotalMicros >= 0
	if !known {
		figure = 0
		for _, s := range u.Sandboxes {
			if s.CostMicros >= 0 {
				figure, known = figure+s.CostMicros, true
			}
		}
	}
	if known {
		row.TheirsMicros = max(row.TheirsMicros, 0) + figure
	}
	ids := make([]string, len(u.Sandboxes))
	for i, s := range u.Sandboxes {
		ids[i] = s.ID
	}
	ours, err := w.store.OursAmong(ctx, name, ids)
	if err != nil {
		return err
	}
	for _, s := range u.Sandboxes {
		if s.ID == "" || ours[s.ID] || !w.unknownTo(u, s) ||
			slices.Contains(row.SeenUnknown, s.ID) || len(row.SeenUnknown) >= maxSeenUnknown {
			continue
		}
		row.SeenUnknown = append(row.SeenUnknown, s.ID)
	}
	row.UnknownIDs = len(row.SeenUnknown)
	return nil
}

// unknownTo reports whether a sandbox none of our attempts or probes made is unknown to every
// known deployment. An id the source ties to another known deployment is not, and a source that
// cannot name deployments gives unknown ids only while this deployment is the only known one.
func (w *Worker) unknownTo(u provider.Usage, s provider.SandboxUsage) bool {
	own := w.saga.OwnedLabels()["bugnet_deployment"]
	if !u.Attributed {
		return !slices.ContainsFunc(w.d.Reconcile.KnownDeployments,
			func(d string) bool { return d != own })
	}
	return s.Deployment == "" || s.Deployment == own ||
		!slices.Contains(w.d.Reconcile.KnownDeployments, s.Deployment)
}

// checkCost alerts on a day read to the end once the provider's reading is complete, or two days
// after the first, when the provider billed more than our meter by more than the allowed fraction
// or named sandboxes that are not ours
func (w *Worker) checkCost(row store.CostDay, now time.Time) {
	if !row.TheirsComplete && now.Sub(row.FirstCheckedAt) < costGiveUp {
		return
	}
	set := w.d.Reconcile
	if row.TheirsMicros >= 0 &&
		float64(row.TheirsMicros) > float64(row.OursMicros)*(1+set.CostAlertFraction) {
		w.log.Error("provider billed more than our meter", "provider", row.Provider,
			"day", row.Day.Format(costDayLayout), "theirs_micros", row.TheirsMicros,
			"ours_micros", row.OursMicros, "complete", row.TheirsComplete)
	}
	if row.UnknownIDs > 0 {
		w.log.Error("provider bills sandboxes that are not ours", "provider", row.Provider,
			"day", row.Day.Format(costDayLayout), "count", row.UnknownIDs)
	}
}
