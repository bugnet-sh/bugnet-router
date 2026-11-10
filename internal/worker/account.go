package worker

import (
	"context"
	"time"

	"github.com/riverqueue/river"

	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// accountEvery is how often account_check runs, each run checking only the providers that are due
const accountEvery = time.Minute

// accountTimeout is CP 8.6's, under River's 3-minute rescue
const accountTimeout = time.Minute

type accountCheckWorker struct {
	river.WorkerDefaults[jobargs.AccountCheck]
	w *Worker
}

func (*accountCheckWorker) Timeout(*river.Job[jobargs.AccountCheck]) time.Duration {
	return accountTimeout
}

// Work never fails the job, since a check that cannot read the account proves nothing
func (aw *accountCheckWorker) Work(ctx context.Context, j *river.Job[jobargs.AccountCheck]) error {
	aw.w.checkAccount(ctx, j.Args.Provider, j.Args.Now)
	return nil
}

// failedChecks counts the checks in a row that could not read a provider's account
type failedChecks struct {
	n int
}

// accountFailures is the failed checks in a row after which the log says they keep failing
const accountFailures = 3

// checkAccount reads one provider's account when it is due or asked for, and stores the answer
func (w *Worker) checkAccount(ctx context.Context, name string, now bool) {
	p, ok := w.registry.Get(name)
	checker, can := provider.As[provider.AccountChecker](p)
	if !ok || !can {
		return
	}
	at := w.now()
	if !now && !w.accountDue(ctx, name, at) {
		return
	}
	acc, err := checker.CheckAccount(ctx)
	if err != nil {
		w.accountFailed(name, err)
		return
	}
	w.accountOK(name)
	change, err := w.store.RecordAccount(ctx, name, string(acc.Block), acc.Detail, at)
	if err != nil {
		w.log.Warn("account check not stored", "provider", name, "err", err)
		return
	}
	switch change {
	case store.AccountBlocked:
		w.log.Error("provider account blocked", "provider", name, "block", acc.Block,
			"detail", acc.Detail)
	case store.AccountCleared:
		w.log.Info("provider account cleared", "provider", name)
	}
}

// accountDue reports whether the last check is older than its interval, blocked ones being shorter
func (w *Worker) accountDue(ctx context.Context, name string, at time.Time) bool {
	rows, err := w.store.ProviderHealths(ctx)
	if err != nil {
		w.log.Warn("cannot read the last account check", "provider", name, "err", err)
		return true
	}
	for _, h := range rows {
		if h.Provider != name {
			continue
		}
		every := w.d.LimitsPoll.AccountEvery
		if h.Blocked != "" {
			every = w.d.LimitsPoll.AccountEveryBlocked
		}
		return h.AccountCheckedAt.IsZero() || at.Sub(h.AccountCheckedAt) >= every
	}
	return true
}

func (w *Worker) accountFailed(name string, err error) {
	w.checksMu.Lock()
	f := w.checks[name]
	f.n++
	w.checks[name] = f
	n := f.n
	w.checksMu.Unlock()
	if n >= accountFailures {
		w.log.Error("account checks keep failing, the account state stands", "provider", name,
			"failed", n, "err", err)
		return
	}
	w.log.Warn("account not read, the account state stands", "provider", name, "err", err)
}

func (w *Worker) accountOK(name string) {
	w.checksMu.Lock()
	delete(w.checks, name)
	w.checksMu.Unlock()
}
