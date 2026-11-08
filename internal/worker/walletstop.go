package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/riverqueue/river"

	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/wallets"
)

const (
	// stopLiveTimeout is CP 13.4's, under River's 3-minute rescue
	stopLiveTimeout = 150 * time.Second
	// stopLivePerRun bounds the ends of one run, so five slow destroys still fit its timeout
	stopLivePerRun = 5
	// stopLiveWait is how long a run that found a placement still placing waits to look again
	stopLiveWait = 15 * time.Second
	// stopLiveWaits bounds those waits, past which the placement is left to its owner
	stopLiveWaits = 20
)

type walletStopLiveWorker struct {
	river.WorkerDefaults[jobargs.WalletStopLive]
	w *Worker
}

func (*walletStopLiveWorker) Timeout(*river.Job[jobargs.WalletStopLive]) time.Duration {
	return stopLiveTimeout
}

func (sw *walletStopLiveWorker) Work(
	ctx context.Context, job *river.Job[jobargs.WalletStopLive],
) error {
	return sw.w.stopLive(ctx, job.Args)
}

// stopLive ends a blocked payer's ready placements with reason wallet_blocked, a few a run, and
// inserts itself again while more remain or one is still placing
func (w *Worker) stopLive(ctx context.Context, args jobargs.WalletStopLive) error {
	live, err := w.saga.LivePlacementsOf(ctx, args.Payer)
	if err != nil {
		return err
	}
	evidence, err := json.Marshal(map[string]string{"blocked_payer": args.Payer})
	if err != nil {
		return err
	}
	ended, placing, more := 0, false, false
	for _, p := range live {
		if p.State == store.PlacementPlacing {
			placing = true
			continue
		}
		if ended == stopLivePerRun {
			more = true
			break
		}
		_, err := w.saga.End(ctx, p.ID, saga.End{
			Reason: saga.ReasonWalletBlocked, Actor: wallets.ActorRouter, Evidence: evidence,
		})
		if err != nil && !errors.Is(err, saga.ErrNotReady) {
			return fmt.Errorf("end placement %s: %w", p.ID, err)
		}
		ended++
	}
	switch {
	case more:
		return w.insertStopLive(ctx, args, time.Time{})
	case !placing:
		return nil
	case args.Waits >= stopLiveWaits:
		w.log.Error("a blocked payer's placement is still placing, it is left running",
			"waits", args.Waits)
		return nil
	}
	args.Waits++
	return w.insertStopLive(ctx, args, w.now().Add(stopLiveWait))
}

func (w *Worker) insertStopLive(
	ctx context.Context, args jobargs.WalletStopLive, at time.Time,
) error {
	if _, err := w.river.Insert(ctx, args, &river.InsertOpts{ScheduledAt: at}); err != nil {
		return fmt.Errorf("insert wallet_stop_live again: %w", err)
	}
	return nil
}
