package worker

import (
	"context"
	"time"

	"github.com/riverqueue/river"

	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
)

// SingleRunOpts are the insert options of the periodic sweep and outbox jobs
func SingleRunOpts() *river.InsertOpts { return singleRun() }

// Queues are the River queues and their workers that New configures for d
func Queues(d Deps) map[string]river.QueueConfig { return queues(d) }

// ApplyTimeout is how long one apply_secrets run may take
func ApplyTimeout() time.Duration { return (*applySecretsWorker).Timeout(nil, nil) }

// PrepareSSHTimeout is how long one prepare_ssh run may take
func PrepareSSHTimeout() time.Duration {
	return (&prepareSSHWorker{}).Timeout(&river.Job[jobargs.PrepareSSH]{})
}

// StopLive runs one wallet_stop_live job's work
func (w *Worker) StopLive(ctx context.Context, args jobargs.WalletStopLive) error {
	return w.stopLive(ctx, args)
}

// StopLiveTimeout is how long one wallet_stop_live run may take
func StopLiveTimeout() time.Duration {
	return (&walletStopLiveWorker{}).Timeout(&river.Job[jobargs.WalletStopLive]{})
}

// ExpiryTimeout is how long one wallet_expiry run may take
func ExpiryTimeout() time.Duration {
	return (&walletExpiryWorker{}).Timeout(&river.Job[jobargs.WalletExpiry]{})
}

// Periodic names each periodic job kind with its interval
func Periodic() map[string]time.Duration {
	out := map[string]time.Duration{}
	for _, p := range periodic() {
		out[p.args.Kind()] = p.every
	}
	return out
}
