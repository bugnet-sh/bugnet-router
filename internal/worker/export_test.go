package worker

import (
	"context"
	"encoding/json"
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
	for _, p := range periodic(Deps{}) {
		out[p.args.Kind()] = p.every
	}
	return out
}

// StatsEvery and StatsTimeout are the stats_snapshot job's period and run time
const (
	StatsEvery   = statsEvery
	StatsTimeout = statsTimeout
)

// Snapshot runs one stats_snapshot
func (w *Worker) Snapshot(ctx context.Context) error { return w.snapshot(ctx) }

// StatsJobTimeout is the timeout River gives one stats_snapshot run
func StatsJobTimeout() time.Duration { return (*statsWorker).Timeout(nil, nil) }

// GoldenSpec marshals the spec bugnet-run's golden file holds, in the router's spec type
func GoldenSpec() []byte {
	b, err := json.Marshal(egressSpec{
		V: 1, Token: "bnt_AwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwM",
		Endpoint: "egress.bugnet.sh", IPs: []string{"203.0.113.7", "2001:db8::7"},
		Port: 443, Listen: "127.77.0.1:443",
		Hosts: []string{"agent402.tools", "api.example.com", "huggingface.co"},
		Env:   []egressEnv{{Name: "HF_TOKEN", Value: "bnsec_aaaaaaaaaaaaaaaaaaaaaaaaaa"}},
		Home:  "/home/boxd", X402Hosts: []string{"agent402.tools", "api.example.com"},
	})
	if err != nil {
		panic(err)
	}
	return b
}
