package saga

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// reasonFeatureUnavailable is the failure reason of x402 rows on a provider without secrets
const reasonFeatureUnavailable = "FEATURE_UNAVAILABLE"

// x402AtReady applies or fails the placement's x402 rows inside the ready transaction
func (s *Saga) x402AtReady(
	ctx context.Context, tx pgx.Tx, placement uuid.UUID, caps provider.Capabilities,
) error {
	ts := s.d.Store.Tx(tx)
	rows, err := ts.LoadX402ForApply(ctx, placement)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	if !caps.Secrets {
		return ts.MarkX402Applied(ctx, placement, rows, store.X402Failed, reasonFeatureUnavailable)
	}
	if _, err := s.d.River.InsertTx(ctx, tx, jobargs.ApplySecrets{PlacementID: placement},
		nil); err != nil {
		return fmt.Errorf("insert apply_secrets job: %w", err)
	}
	return nil
}
