package worker

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"

	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
)

const (
	expiryEvery = 10 * time.Minute
	// expiryTimeout is CP 13.4's, under River's 3-minute rescue
	expiryTimeout = 30 * time.Second
	// expiryPerRun bounds one run's lifts, the next run takes the rest
	expiryPerRun = 500
)

type walletExpiryWorker struct {
	river.WorkerDefaults[jobargs.WalletExpiry]
	w *Worker
}

func (*walletExpiryWorker) Timeout(*river.Job[jobargs.WalletExpiry]) time.Duration {
	return expiryTimeout
}

func (ew *walletExpiryWorker) Work(ctx context.Context, _ *river.Job[jobargs.WalletExpiry]) error {
	return ew.w.expireBlocks(ctx)
}

// expireBlocks lifts the blocks whose blocked_until has passed, each under its wallet's lock
func (w *Worker) expireBlocks(ctx context.Context) error {
	payers, err := w.store.ExpiredBlocks(ctx, w.now(), expiryPerRun)
	if err != nil {
		return err
	}
	lifted := 0
	for _, payer := range payers {
		var ok bool
		err := w.store.WithTx(ctx, func(tx pgx.Tx) error {
			var err error
			ok, err = w.wallets.Expire(ctx, tx, payer)
			return err
		})
		if err != nil {
			return err
		}
		if ok {
			lifted++
		}
	}
	if lifted > 0 {
		w.log.Info("lifted wallet blocks whose time passed", "wallets", lifted)
	}
	return nil
}
