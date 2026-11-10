package worker

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"time"

	"github.com/riverqueue/river"
	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const (
	// foreignTimeout is CP 13.4's, under River's 3-minute rescue
	foreignTimeout = 120 * time.Second
	// foreignBatch is how many services one run reads, since Koyeb needs a read for each
	foreignBatch = 200
	// foreignKeep is how long a half-finished pass keeps its place
	foreignKeep = 48 * time.Hour
)

type foreignWorker struct {
	river.WorkerDefaults[jobargs.ForeignCount]
	w *Worker
}

func (*foreignWorker) Timeout(*river.Job[jobargs.ForeignCount]) time.Duration {
	return foreignTimeout
}

// Work never fails the job, since the next count comes within the hour
func (fw *foreignWorker) Work(ctx context.Context, j *river.Job[jobargs.ForeignCount]) error {
	fw.w.foreignCount(ctx, j.Args.Provider)
	return nil
}

// listLabelled reads one page of the provider's sandboxes with the label bugnet=1, from its
// bounded listing where it has one and from the whole listing where it does not
func listLabelled(
	ctx context.Context, prov provider.Provider, cursor string,
) (provider.ForeignPage, error) {
	labels := map[string]string{"bugnet": "1"}
	if l, ok := provider.As[provider.ForeignLister](prov); ok {
		return l.ListLabelled(ctx, labels, cursor, foreignBatch)
	}
	owned, err := prov.ListOwned(ctx, labels)
	return provider.ForeignPage{Sandboxes: owned}, err
}

// foreignCount counts the provider's sandboxes no known deployment owns that are old enough, a
// pass over a provider with more than 200 services taking several runs (CP 10.6)
func (w *Worker) foreignCount(ctx context.Context, name string) {
	prov, ok := w.registry.Get(name)
	if !ok {
		return
	}
	curKey, sumKey := w.prefix+"foreign:cursor:"+name, w.prefix+"foreign:partial:"+name
	cursor, _ := w.vk.Do(ctx, w.vk.B().Get().Key(curKey).Build()).ToString()
	partial := 0
	if cursor != "" {
		n, err := w.vk.Do(ctx, w.vk.B().Get().Key(sumKey).Build()).AsInt64()
		if err != nil && !valkey.IsValkeyNil(err) {
			w.log.Warn("foreign count read partial", "provider", name, "err", err)
			return
		}
		partial = int(n)
	}
	page, err := listLabelled(ctx, prov, cursor)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			w.log.Warn("foreign count list", "provider", name, "err", err)
		}
		return
	}
	set := w.d.Reconcile
	known := append(slices.Clone(set.KnownDeployments), w.saga.OwnedLabels()["bugnet_deployment"])
	total := partial
	for _, s := range page.Sandboxes {
		// A sandbox of unknown age is not shown to be old
		if !s.CreatedAt.IsZero() && w.now().Sub(s.CreatedAt) > set.ForeignAlertAfter &&
			!slices.Contains(known, s.Labels["bugnet_deployment"]) {
			total++
		}
	}
	if page.Next != "" {
		w.vk.DoMulti(ctx,
			w.vk.B().Set().Key(curKey).Value(page.Next).Ex(foreignKeep).Build(),
			w.vk.B().Set().Key(sumKey).Value(strconv.Itoa(total)).Ex(foreignKeep).Build())
		return
	}
	w.vk.DoMulti(ctx, w.vk.B().Del().Key(curKey, sumKey).Build())
	if err := w.store.SetForeignSandboxes(ctx, name, total, w.now()); err != nil {
		w.log.Warn("foreign count store", "provider", name, "err", err)
		return
	}
	if total > 0 {
		w.log.Warn("sandboxes no known deployment owns", "provider", name, "count", total)
	}
}
