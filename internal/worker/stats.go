package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/riverqueue/river"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/stats"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

const (
	statsEvery = 30 * time.Second
	// statsTimeout stays under the period, so a slow run never overlaps the next one
	statsTimeout = 25 * time.Second
	// noTokenLogEvery repeats the empty token error while the token stays empty
	noTokenLogEvery = time.Minute
)

// Totals reads Payments' totals, a *stats.Payments in router-worker
type Totals interface {
	Totals(ctx context.Context) (stats.PaymentsPart, error)
}

// Stats is what the stats_snapshot job merges with the router's figures
type Stats struct {
	Payments Totals
	Catalog  *catalog.Catalog
	// Limits are the router's keys of the limits table
	Limits map[string]int64
}

type statsWorker struct {
	river.WorkerDefaults[jobargs.StatsSnapshot]
	w *Worker
}

// Timeout keeps one run inside its 30 s period
func (*statsWorker) Timeout(*river.Job[jobargs.StatsSnapshot]) time.Duration {
	return statsTimeout
}

// Work fails without a retry, since the next run starts 30 s later
func (sw *statsWorker) Work(ctx context.Context, _ *river.Job[jobargs.StatsSnapshot]) error {
	if sw.w.d.Stats == nil {
		return nil
	}
	return sw.w.snapshot(ctx)
}

// snapshot reads the router part at one time, keeps the last good Payments part, and writes both
func (w *Worker) snapshot(ctx context.Context) error {
	s := w.d.Stats
	t := w.now()
	f, err := w.store.StatsFigures(ctx, t)
	if err != nil {
		return err
	}
	part, err := s.Payments.Totals(ctx)
	paidAt := t
	var raw []byte
	if err == nil {
		w.paymentsAnswered()
		if raw, err = json.Marshal(part); err != nil {
			return err
		}
	} else {
		w.paymentsFailed(err, t)
		prev, err := w.store.ReadStatsSnapshot(ctx)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return err
		}
		if prev.Payments == nil {
			// The page stays at 503 until Payments first answers, which only a fresh database sees
			return w.store.WriteStatsSnapshot(ctx, store.StatsSnapshot{RouterAt: t})
		}
		if err := json.Unmarshal(prev.Payments, &part); err != nil {
			return fmt.Errorf("stored payments part: %w", err)
		}
		raw, paidAt = prev.Payments, prev.PaymentsAt
	}
	part, dropped := part.From(stats.PaymentsFloor(f))
	if dropped > 0 {
		w.log.Warn("stats: Payments days before the floor left out of daily", "count", dropped)
	}
	pkgs := s.Catalog.All()
	var servers []stats.Server
	var quote func(catalog.Package) int64
	if w.engine != nil {
		servers = stats.Servers(w.engine, w.registry, pkgs, w.placing)
		quote = stats.Quote(w.engine, w.placing)
	}
	body, err := stats.Encode(stats.Merge(f, part, paidAt, pkgs, quote, servers, s.Limits))
	if err != nil {
		return err
	}
	return w.store.WriteStatsSnapshot(ctx, store.StatsSnapshot{
		Body: body, RouterAt: t, Payments: raw, PaymentsAt: paidAt,
	})
}

// paymentsAnswered lets the next token refusal log at error level again
func (w *Worker) paymentsAnswered() {
	w.statsMu.Lock()
	defer w.statsMu.Unlock()
	w.tokenRefused = false
}

// paymentsFailed logs the failed check and never the body, a token problem at error level
func (w *Worker) paymentsFailed(err error, now time.Time) {
	w.statsMu.Lock()
	defer w.statsMu.Unlock()
	var ue *url.Error
	if errors.As(err, &ue) {
		err = ue.Err
	}
	switch {
	case errors.Is(err, stats.ErrNoToken):
		if now.Sub(w.noTokenLogged) >= noTokenLogEvery {
			w.noTokenLogged = now
			w.log.Error("stats: no Payments totals, set BUGNET_PAYMENTS_ROUTER_TOKEN in "+
				"router-worker", "err", err)
		}
	case errors.Is(err, stats.ErrToken):
		if !w.tokenRefused {
			w.tokenRefused = true
			w.log.Error("stats: Payments refused the router token, check "+
				"BUGNET_PAYMENTS_ROUTER_TOKEN in both services", "err", err)
		}
	default:
		w.log.Warn("stats: Payments totals unusable, keeping the last good part", "err", err)
	}
}
