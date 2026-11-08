package saga

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/ordertoken"
	"github.com/bugnet-sh/bugnet-router/internal/sessions"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// Session record defaults, the settings' own
const (
	defaultSessionKeep = 48 * time.Hour
	defaultAbuseHold   = 30 * 24 * time.Hour
)

// paymentClaims is the one place the payment event reads Task 28's Tx and AllowanceMicros claims
func paymentClaims(c ordertoken.Claims) (packageMicros int64, tx string) {
	return packageShare(c.AmountMicros, 0), ""
}

// packageShare is an order's amount less the x402 allowance it carries (XS 6.15)
func packageShare(amountMicros, allowanceMicros int64) int64 {
	return amountMicros - allowanceMicros
}

// endSession marks the ended placement's record and inserts its copy, both in tx
func (s *Saga) endSession(ctx context.Context, tx pgx.Tx, p store.Placement) error {
	end := store.SessionEnd{EndedAt: s.now(), Keep: s.d.SessionKeep}
	if p.TerminatedAt != nil {
		end.EndedAt = *p.TerminatedAt
	}
	if by, ok := sessions.ClosingEnd(p.TerminationReason); ok {
		end.ClosedBy, end.HoldUntil = by, s.now().Add(s.d.SessionAbuseHold)
	}
	if err := s.d.Store.Tx(tx).EndSessionRecord(ctx, p.ID, end); err != nil {
		return err
	}
	_, err := s.d.River.InsertTx(ctx, tx, jobargs.SessionCopy{PlacementID: p.ID},
		&river.InsertOpts{Queue: jobargs.QueueDataplane, UniqueOpts: river.UniqueOpts{
			ByArgs: true,
		}})
	return err
}

// sessionEnded writes the ended placement's last placement event
func (s *Saga) sessionEnded(ctx context.Context, id uuid.UUID) {
	if s.d.Sessions == nil {
		return
	}
	p, err := s.d.Store.PlacementByID(ctx, id)
	if err != nil {
		s.d.Log.Warn("read an ended placement for its session record", "placement", id,
			"err", err)
		return
	}
	pkg, _ := s.d.Catalog.Get(p.PackageID)
	s.d.Sessions.Placement(ctx, id, sessions.PhaseEnded, pkg, p.Deadline,
		sessions.EndedBecause(p.State, p.TerminationReason))
}

// sessionStarted writes a new placement's placed and start payment events
func (s *Saga) sessionStarted(
	ctx context.Context, p store.Placement, pkg catalog.Package, c ordertoken.Claims,
) {
	if s.d.Sessions == nil {
		return
	}
	s.d.Sessions.Placement(ctx, p.ID, sessions.PhasePlaced, pkg, nil, "")
	amount, tx := paymentClaims(c)
	s.d.Sessions.Payment(ctx, p.ID, string(ordertoken.KindStart), amount, tx)
}
