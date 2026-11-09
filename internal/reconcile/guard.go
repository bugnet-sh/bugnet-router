package reconcile

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// guarded are the classes the correlation guard may hold back
var guarded = []string{
	string(ClassAbuse), string(ClassControlLost), string(ClassSandboxExited),
}

// Guard reports whether one provider's recent ends look like one incident (CP 6.4). It takes the
// provider's advisory lock until tx ends, so two placements cannot both pass as the last one.
func Guard(
	ctx context.Context, tx pgx.Tx, prov string, c Class, now time.Time, r Rules,
) (bool, error) {
	if !guards(c) {
		return false, nil
	}
	if err := store.LockProviderSignals(ctx, tx, prov); err != nil {
		return false, err
	}
	// This placement counts too, so the fourth one meets a maximum of three
	n, err := store.LostCount(ctx, tx, prov, guarded, now.Add(-r.CorrelatedWindow))
	if err != nil {
		return false, err
	}
	if n+1 > r.CorrelatedMax {
		return true, nil
	}
	blocked, err := store.ProviderBlock(ctx, tx, prov)
	if err != nil || blocked != "" {
		return blocked != "", err
	}
	return store.CanaryFailedSince(ctx, tx, prov, now.Add(-r.CorrelatedWindow))
}
