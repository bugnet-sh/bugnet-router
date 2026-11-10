package worker

import (
	"context"
	"errors"
	"time"

	"github.com/riverqueue/river"

	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// limitsTimeout is CP 8.6's, under River's 3-minute rescue
const limitsTimeout = 30 * time.Second

type limitsPollWorker struct {
	river.WorkerDefaults[jobargs.LimitsPoll]
	w *Worker
}

func (*limitsPollWorker) Timeout(*river.Job[jobargs.LimitsPoll]) time.Duration {
	return limitsTimeout
}

// Work never fails the job, since the next poll comes within a minute and a failed one only logs
func (lw *limitsPollWorker) Work(ctx context.Context, j *river.Job[jobargs.LimitsPoll]) error {
	lw.w.pollLimits(ctx, j.Args.Provider)
	return nil
}

// ResolveLimit picks the slots a provider offers, the smaller of its cap and the setting
func ResolveLimit(providerCap, setting int) (int, string) {
	switch {
	case providerCap > 0 && setting > 0 && setting < providerCap:
		return setting, store.LimitFromSettings
	case providerCap > 0:
		return providerCap, store.LimitFromProvider
	case setting > 0:
		return setting, store.LimitFromSettings
	}
	return store.DefaultMaxConcurrent, store.LimitFromDefault
}

// pollLimits reads one provider's cap and use into its capacity row, a failure keeping the row
func (w *Worker) pollLimits(ctx context.Context, name string) {
	p, ok := w.registry.Get(name)
	reader, can := provider.As[provider.LimitsReader](p)
	if !ok || !can {
		return
	}
	got, err := reader.ReadLimits(ctx)
	if err != nil {
		w.log.Warn("limits not read, the capacity row stays", "provider", name, "err", err)
		return
	}
	limit, source := ResolveLimit(got.MaxConcurrent, w.d.MaxConcurrent[name])
	err = w.store.WriteLimits(ctx, name, limit, source, got.Active, w.now())
	if err != nil && !errors.Is(err, context.Canceled) {
		w.log.Warn("limits not stored", "provider", name, "err", err)
	}
}
