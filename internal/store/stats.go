package store

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// StatsSnapshot is the one row of stats_snapshot
type StatsSnapshot struct {
	// Body is the served JSON, nil until a Payments part exists
	Body     []byte
	RouterAt time.Time
	// Payments is the last good Payments answer as checked, nil before the first
	Payments   []byte
	PaymentsAt time.Time
}

// ReadStatsSnapshot returns the snapshot row, ErrNotFound before the first run
func (s *Store) ReadStatsSnapshot(ctx context.Context) (StatsSnapshot, error) {
	var snap StatsSnapshot
	var paidAt *time.Time
	err := s.q.QueryRow(ctx, `
		select body, router_at, payments, payments_at from stats_snapshot where id = 1`).
		Scan(&snap.Body, &snap.RouterAt, &snap.Payments, &paidAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return StatsSnapshot{}, ErrNotFound
	}
	if err != nil {
		return StatsSnapshot{}, fmt.Errorf("select stats snapshot: %w", err)
	}
	snap.RouterAt = snap.RouterAt.UTC()
	if paidAt != nil {
		snap.PaymentsAt = paidAt.UTC()
	}
	return snap, nil
}

// WriteStatsSnapshot replaces the snapshot row in one upsert
func (s *Store) WriteStatsSnapshot(ctx context.Context, snap StatsSnapshot) error {
	var paidAt *time.Time
	if !snap.PaymentsAt.IsZero() {
		paidAt = utcPtr(&snap.PaymentsAt)
	}
	var payments any
	if snap.Payments != nil {
		payments = string(snap.Payments)
	}
	_, err := s.q.Exec(ctx, `
		insert into stats_snapshot (id, body, router_at, payments, payments_at, updated_at)
		values (1, $1, $2, $3::jsonb, $4, now())
		on conflict (id) do update set body = excluded.body, router_at = excluded.router_at,
			payments = excluded.payments, payments_at = excluded.payments_at,
			updated_at = excluded.updated_at`,
		snap.Body, utc(snap.RouterAt), payments, paidAt)
	if err != nil {
		return fmt.Errorf("write stats snapshot: %w", err)
	}
	return nil
}

// StatsSessionRow is one placement of the sessions route, before CP 20.6.3's checks
type StatsSessionRow struct {
	ID        uuid.UUID
	CreatedAt time.Time
	// ReadyAt is nil when the placement never started, EndedAt while it runs
	ReadyAt, EndedAt  *time.Time
	State, Provider   string
	PackageID         string
	TerminationReason string
	FailureReason     string
	Commands          int64
	// PriceMicros sums the package share of its start and extend orders
	PriceMicros int64
	// Payer, FeePayer and Tx are the start order's claims
	Payer, FeePayer, Tx string
	// PayerWasFeePayer says some order carried Payer as its fee_payer
	PayerWasFeePayer bool
}

// sessionsTimeout bounds one page query, so a slow page never holds a query slot for long
const sessionsTimeout = "2s"

// ListStatsSessions returns up to limit rows created after since, older than the cursor's row
func (s *Store) ListStatsSessions(
	ctx context.Context, since, beforeAt time.Time, beforeID uuid.UUID, limit int,
) ([]StatsSessionRow, error) {
	q := `
		select p.id, p.created_at, p.ready_at, coalesce(p.terminating_at, p.terminated_at),
			p.state, coalesce(p.provider, ''), p.package_id,
			coalesce(p.termination_reason, ''), coalesce(p.failure_reason, ''),
			(select count(*) from jobs j where j.placement_id = p.id),
			(select coalesce(sum(o.amount_micros), 0)::bigint from orders o
				where o.placement_id = p.id),
			coalesce(so.payer, ''), coalesce(so.fee_payer, ''), coalesce(so.tx, ''),
			so.payer is not null and exists (select 1 from orders f where f.fee_payer = so.payer)
		from placements p
		left join orders so on so.order_id = p.order_id
		where p.created_at > $1 and p.state <> $2`
	args := []any{utc(since), PlacementPlacing, limit}
	if !beforeAt.IsZero() {
		q += ` and (p.created_at, p.id) < ($4, $5)`
		args = append(args, utc(beforeAt), beforeID)
	}
	q += ` order by p.created_at desc, p.id desc limit $3`
	var out []StatsSessionRow
	err := s.WithTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `set local statement_timeout = '`+sessionsTimeout+`'`)
		if err != nil {
			return err
		}
		rows, err := tx.Query(ctx, q, args...)
		if err != nil {
			return err
		}
		out, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (StatsSessionRow, error) {
			var x StatsSessionRow
			err := r.Scan(&x.ID, &x.CreatedAt, &x.ReadyAt, &x.EndedAt, &x.State, &x.Provider,
				&x.PackageID, &x.TerminationReason, &x.FailureReason, &x.Commands,
				&x.PriceMicros, &x.Payer, &x.FeePayer, &x.Tx, &x.PayerWasFeePayer)
			x.CreatedAt = x.CreatedAt.UTC()
			x.ReadyAt, x.EndedAt = utcPtr(x.ReadyAt), utcPtr(x.EndedAt)
			return x, err
		})
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("list stats sessions: %w", err)
	}
	return out, nil
}

// StatsFigures holds the router's figures of CP 20.6.1, read at At
type StatsFigures struct {
	At time.Time
	// Active is the running placements per provider
	Active         map[string]int64
	Sandboxes      int64
	ComputeSeconds int64
	// SpentMicros is the provider cost meter's total, nil while the meter is unknown
	SpentMicros *int64
	// Days runs from the first started placement's day through At's day
	Days []StatsDay
	// FirstOrderAt is the first placement's created_at, which its start order shares, nil before
	// any
	FirstOrderAt *time.Time
	// Providers are the 7-day rows by name, each pointer nil while its source is unknown
	Providers []StatsProvider
}

// StatsDay is one UTC day of the router's figures
type StatsDay struct {
	Day time.Time
	// SpentMicros is nil while the provider cost meter is unknown
	SpentMicros    *int64
	Sandboxes      int64
	ComputeSeconds int64
	PeakActive     int64
	MedianSeconds  int64
}

// StatsProvider is one provider's last 7 days
type StatsProvider struct {
	Provider    string
	Sandboxes7d int64
	Active      int64
	// StartP50Ms and StartP95Ms are nil under 5 samples or while start samples are unknown
	StartP50Ms, StartP95Ms *int64
	// Failed7d is nil while start samples are unknown
	Failed7d *int64
}

// startStats is one provider's start samples of the last 7 days
type startStats struct {
	p50, p95 *int64
	failed   int64
}

// figuresTimeout bounds each query of one snapshot run, under the run's own timeout
const figuresTimeout = "10s"

// spans are the started placements as of $1, each start cut to its second so day shares add up
const spans = `
	with spans as (
		select provider, state, date_trunc('second', ready_at) as s,
			floor(greatest(extract(epoch from
				least($1::timestamptz, coalesce(terminating_at, terminated_at, $1::timestamptz))
				- ready_at), 0))::bigint as secs,
			case when state <> 'ready'
				then least($1::timestamptz, coalesce(terminating_at, terminated_at)) end as ended_at
		from placements where ready_at is not null and ready_at <= $1::timestamptz
	)`

// StatsFigures reads CP 20.6.1's figures in one read-only repeatable-read transaction
func (s *Store) StatsFigures(ctx context.Context, t time.Time) (StatsFigures, error) {
	t = t.UTC()
	f := StatsFigures{At: t, Active: map[string]int64{}, Days: []StatsDay{},
		Providers: []StatsProvider{}}
	b, ok := s.q.(interface {
		BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
	})
	if !ok {
		return f, errors.New("stats figures: the store must run on a pool")
	}
	tx, err := b.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly})
	if err != nil {
		return f, fmt.Errorf("stats figures: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	_, err = tx.Exec(ctx, `set local statement_timeout = '`+figuresTimeout+`';
		set local timezone = 'UTC'`)
	if err == nil {
		err = readFigures(ctx, tx, &f)
	}
	if err != nil {
		return StatsFigures{}, fmt.Errorf("stats figures: %w", err)
	}
	return f, nil
}

func readFigures(ctx context.Context, tx pgx.Tx, f *StatsFigures) error {
	t := f.At
	var first *time.Time
	err := tx.QueryRow(ctx, spans+`
		select count(*), coalesce(sum(secs), 0)::bigint, min(date_trunc('day', s)) from spans`,
		t).Scan(&f.Sandboxes, &f.ComputeSeconds, &first)
	if err != nil {
		return err
	}
	err = tx.QueryRow(ctx, `select min(created_at) from placements where created_at <= $1`, t).
		Scan(&f.FirstOrderAt)
	if err != nil {
		return err
	}
	f.FirstOrderAt = utcPtr(f.FirstOrderAt)
	rows, err := tx.Query(ctx, `
		select provider, count(*) from placements
		where state = 'ready' and provider is not null and ready_at <= $1
		group by provider`, t)
	if err != nil {
		return err
	}
	var name string
	var n int64
	_, err = pgx.ForEachRow(rows, []any{&name, &n}, func() error {
		f.Active[name] = n
		return nil
	})
	if err != nil {
		return err
	}
	spent, spentByDay, spentKnown, err := statsSpent(ctx, tx, t)
	if err != nil {
		return err
	}
	if spentKnown {
		f.SpentMicros = &spent
	}
	if first != nil {
		if f.Days, err = readDays(ctx, tx, t, first.UTC()); err != nil {
			return err
		}
		for i := range f.Days {
			if spentKnown {
				v := spentByDay[f.Days[i].Day]
				f.Days[i].SpentMicros = &v
			}
		}
	}
	f.Providers, err = readProviders(ctx, tx, t)
	return err
}

// readDays reads each day from first through t's day, a day with nothing at zero
func readDays(ctx context.Context, tx pgx.Tx, t, first time.Time) ([]StatsDay, error) {
	last := t.Truncate(24 * time.Hour)
	byDay := map[time.Time]*StatsDay{}
	var out []StatsDay
	for d := first; !d.After(last); d = d.AddDate(0, 0, 1) {
		out = append(out, StatsDay{Day: d})
	}
	for i := range out {
		byDay[out[i].Day] = &out[i]
	}
	type column struct {
		sql  string
		into func(*StatsDay) *int64
	}
	for _, c := range []column{
		{spans + `select date_trunc('day', s), count(*) from spans group by 1`,
			func(d *StatsDay) *int64 { return &d.Sandboxes }},
		// Each run's share of each day it touched, split at midnight
		{spans + `, cut as (select s, s + secs * interval '1 second' as e from spans)
			select d, sum(extract(epoch from
				least(e, d + interval '1 day') - greatest(s, d)))::bigint
			from cut, generate_series(date_trunc('day', s), date_trunc('day', e),
				interval '1 day') d
			group by d`,
			func(d *StatsDay) *int64 { return &d.ComputeSeconds }},
		{spans + `select date_trunc('day', ended_at),
				floor(percentile_cont(0.5) within group (order by secs))::bigint
			from spans where ended_at is not null group by 1`,
			func(d *StatsDay) *int64 { return &d.MedianSeconds }},
		// Ends sort before midnights and midnights before starts, so a day carries what ran into it
		{spans + `, events as (
				select s as at, 2 as rank, 1 as delta from spans
				union all
				select s + secs * interval '1 second', 0, -1 from spans
				where ended_at is not null
				union all
				select d, 1, 0 from generate_series($2::timestamptz, $1::timestamptz,
					interval '1 day') d
			), running as (
				select at, sum(delta) over (order by at, rank
					rows between unbounded preceding and current row) as n
				from events
			)
			select date_trunc('day', at), max(n) from running group by 1`,
			func(d *StatsDay) *int64 { return &d.PeakActive }},
	} {
		args := []any{t}
		if strings.Contains(c.sql, "$2") {
			args = append(args, first)
		}
		rows, err := tx.Query(ctx, c.sql, args...)
		if err != nil {
			return nil, err
		}
		var day time.Time
		var n int64
		_, err = pgx.ForEachRow(rows, []any{&day, &n}, func() error {
			if d := byDay[day.UTC()]; d != nil {
				*c.into(d) = n
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// readProviders reads each provider started on in the last 7 days, running, or sampled, by name
func readProviders(ctx context.Context, tx pgx.Tx, t time.Time) ([]StatsProvider, error) {
	rows, err := tx.Query(ctx, `
		select provider, count(*) filter (where ready_at > $1::timestamptz - interval '7 days'),
			count(*) filter (where state = 'ready')
		from placements
		where provider is not null and ready_at is not null and ready_at <= $1
			and (ready_at > $1::timestamptz - interval '7 days' or state = 'ready')
		group by provider`, t)
	if err != nil {
		return nil, err
	}
	byName := map[string]*StatsProvider{}
	var r StatsProvider
	_, err = pgx.ForEachRow(rows, []any{&r.Provider, &r.Sandboxes7d, &r.Active}, func() error {
		row := r
		byName[r.Provider] = &row
		return nil
	})
	if err != nil {
		return nil, err
	}
	samples, known, err := statsStartSamples(ctx, tx, t)
	if err != nil {
		return nil, err
	}
	for name := range samples {
		if byName[name] == nil {
			byName[name] = &StatsProvider{Provider: name}
		}
	}
	out := []StatsProvider{}
	for _, name := range slices.Sorted(maps.Keys(byName)) {
		p := *byName[name]
		if known {
			st := samples[name]
			p.StartP50Ms, p.StartP95Ms = st.p50, st.p95
			p.Failed7d = &st.failed
		}
		out = append(out, p)
	}
	return out, nil
}

// statsStartSamples is Task 33's switch point, knowing no start_samples until Task 33 merges
func statsStartSamples(
	_ context.Context, _ pgx.Tx, _ time.Time,
) (map[string]startStats, bool, error) {
	return nil, false, nil
}

// statsSpent is Task 35's switch point, knowing no placements.cost_micros until Task 35 merges
func statsSpent(
	_ context.Context, _ pgx.Tx, _ time.Time,
) (total int64, byDay map[time.Time]int64, known bool, err error) {
	return 0, nil, false, nil
}
