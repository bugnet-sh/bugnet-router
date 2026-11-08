package saga

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/bugnet-sh/bugnet-router/internal/ordertoken"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/wallet"
	"github.com/bugnet-sh/bugnet-router/internal/wallets"
)

// Wallet refusals, each answered as CP 7.9 says
var (
	// ErrPayerRequired refuses a start order without a payer while wallets.require_payer is set
	ErrPayerRequired = errors.New("saga: the order names no payer")
	// ErrWalletBlocked refuses an order of a blocked payer, recording nothing
	ErrWalletBlocked = errors.New("saga: the payer's wallet is blocked")
	// ErrWalletBusy refuses a start at the wallet's cap on live placements
	ErrWalletBusy = errors.New("saga: the wallet has as many sandboxes as it may run at once")
	// ErrWalletRateLimited refuses a start while one of the wallet's start buckets is empty
	ErrWalletRateLimited = errors.New("saga: the wallet started too many sandboxes, wait")
)

// Failure reasons of refused starts, recorded on a failed and refundable placement
const (
	ReasonWalletBusy        = "wallet_busy"
	ReasonWalletRateLimited = "wallet_rate_limited"
)

const (
	// walletTimeout bounds each Valkey call of the wallet limits, past it the start goes through
	walletTimeout = 500 * time.Millisecond
	// unguardedEvery spaces the step 4 log and the /monitor key
	unguardedEvery = time.Minute
	// unguardedFor is how long /monitor shows step 4 after it last happened
	unguardedFor = time.Hour
	// minRetry is the least wait a retried rate-limited order is told
	minRetry = time.Second
)

// WaitError is ErrWalletRateLimited with how long until the wallet's start buckets hold a token
type WaitError struct{ Wait time.Duration }

func (e *WaitError) Error() string { return ErrWalletRateLimited.Error() }

// Unwrap makes a WaitError match ErrWalletRateLimited
func (e *WaitError) Unwrap() error { return ErrWalletRateLimited }

// payer decides the payer an order counts under, logging every guard that took the claim away
func (s *Saga) payer(ctx context.Context, ts *store.Store, c ordertoken.Claims) (string, error) {
	payer, why, err := wallet.Decide(s.guard, wallet.Claim{Payer: c.Payer, FeePayer: c.FeePayer},
		func(addr string) (bool, error) { return ts.FeePayerSeen(ctx, addr) })
	if err != nil {
		return "", err
	}
	switch why {
	case wallet.AbsentIsFeePayer, wallet.AbsentFacilitator:
		s.d.Log.Error("the payer claim is a facilitator address, the order counts as unpaid by "+
			"a wallet", "order", c.OrderID, "reason", string(why))
	case wallet.AbsentUnguarded:
		s.unguarded(ctx, c.OrderID)
	}
	return payer, nil
}

// unguarded logs step 4 and marks it for /monitor, each at most once a minute
func (s *Saga) unguarded(ctx context.Context, order string) {
	now := s.now()
	last := s.unguardedAt.Load()
	if now.UnixNano()-last < int64(unguardedEvery) ||
		!s.unguardedAt.CompareAndSwap(last, now.UnixNano()) {
		return
	}
	s.d.Log.Error("no fee_payer came and wallets.facilitator_addresses is empty, so the payer "+
		"claim counts as absent", "order", order)
	vctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), walletTimeout)
	defer cancel()
	vk := s.d.Valkey
	cmd := vk.B().Set().Key(s.d.Prefix + wallet.UnguardedKey).Value(now.Format(time.RFC3339)).
		Ex(unguardedFor).Build()
	if err := vk.Do(vctx, cmd).Error(); err != nil {
		s.d.Log.Warn("mark the unguarded payer claim for /monitor", "err", err)
	}
}

// startLimit runs CP 7.7 steps 5 and 6, naming the failure reason of a refused start
func (s *Saga) startLimit(
	ctx context.Context, ts *store.Store, payer string,
) (string, time.Duration, error) {
	limit := s.w.AnonymousMaxConcurrent
	if payer != "" {
		limit = s.w.MaxConcurrent
	}
	n, err := ts.LiveCount(ctx, payer)
	if err != nil {
		return "", 0, err
	}
	if n >= limit {
		return ReasonWalletBusy, 0, nil
	}
	if payer == "" {
		return "", 0, nil
	}
	vctx, cancel := context.WithTimeout(ctx, walletTimeout)
	defer cancel()
	ok, wait, err := s.limits.TakeAll(vctx, wallets.StartBuckets(s.w, payer))
	switch {
	case err != nil && ctx.Err() != nil:
		return "", 0, ctx.Err()
	case err != nil:
		// The concurrency cap above still holds, so a start never waits on Valkey
		s.d.Log.Warn("wallet start buckets unread, letting the start through", "err", err)
	case !ok:
		return ReasonWalletRateLimited, wait, nil
	}
	return "", 0, nil
}

// refused runs CP 7.7 step 7, blocking the payer once its refusal bucket runs dry
func (s *Saga) refused(ctx context.Context, tx pgx.Tx, payer string, id uuid.UUID) error {
	vctx, cancel := context.WithTimeout(ctx, walletTimeout)
	ok, _, err := s.limits.TakeKey(vctx, wallets.RefusalBucket(s.w, payer))
	cancel()
	if err != nil {
		s.d.Log.Warn("wallet refusal bucket unread", "err", err)
		return nil
	}
	if ok {
		return nil
	}
	until := s.now().Add(s.w.RefusalBlockFor)
	err = s.wallets.Block(ctx, tx, wallets.Block{
		Payer: payer, Reason: wallets.ReasonRefusals, Until: &until, Actor: wallets.ActorRouter,
		PlacementID: id,
	})
	if errors.Is(err, wallets.ErrFacilitator) {
		s.d.Log.Error("a refused order's payer is a facilitator address, not blocked",
			"placement", id)
		return nil
	}
	return err
}

// startWait is how long a retried rate-limited order is told to wait
func (s *Saga) startWait(ctx context.Context, payer string) time.Duration {
	if payer == "" {
		return minRetry
	}
	vctx, cancel := context.WithTimeout(ctx, walletTimeout)
	defer cancel()
	_, wait, err := s.limits.PeekAll(vctx, wallets.StartBuckets(s.w, payer))
	if err != nil {
		s.d.Log.Warn("wallet start buckets unread for a retried order", "err", err)
	}
	return max(wait, minRetry)
}

// checkStart runs CP 7.7 steps 3 to 7 for a new start order, naming a refusal's failure reason
func (s *Saga) checkStart(
	ctx context.Context, tx pgx.Tx, np *store.NewPlacement,
) (time.Duration, error) {
	ts := s.d.Store.Tx(tx)
	if np.Payer == "" && s.w.RequirePayer {
		return 0, ErrPayerRequired
	}
	if np.Payer != "" {
		blocked, err := s.wallets.Blocked(ctx, tx, np.Payer, s.now())
		if err != nil {
			return 0, err
		}
		if blocked {
			return 0, ErrWalletBlocked
		}
	}
	if np.FailureReason != "" {
		return 0, nil
	}
	reason, wait, err := s.startLimit(ctx, ts, np.Payer)
	if err != nil || reason == "" {
		return 0, err
	}
	np.FailureReason = reason
	if np.Payer != "" {
		if err := s.refused(ctx, tx, np.Payer, np.ID); err != nil {
			return 0, err
		}
	}
	return wait, nil
}

// checkExtendPayer runs CP 7.7 step 4 for an extend order of a payer not yet recorded
func (s *Saga) checkExtendPayer(
	ctx context.Context, tx pgx.Tx, orderID, payer string,
) error {
	if _, err := s.d.Store.Tx(tx).OrderByID(ctx, orderID); err == nil {
		return nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return err
	}
	blocked, err := s.wallets.Blocked(ctx, tx, payer, s.now())
	if err != nil {
		return err
	}
	if blocked {
		return fmt.Errorf("%w: extend order %s", ErrWalletBlocked, orderID)
	}
	return nil
}
