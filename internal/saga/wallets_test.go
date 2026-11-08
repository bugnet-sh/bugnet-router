package saga_test

import (
	"context"
	"encoding/hex"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/handle"
	"github.com/bugnet-sh/bugnet-router/internal/ordertoken"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/wallet"
	"github.com/bugnet-sh/bugnet-router/internal/wallets"
)

const (
	payerA      = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
	payerB      = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
	payerC      = "DevFaci1itatorAddress1111111111111111111111"
	facilitator = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
)

// walletEnv is a saga with the payer claim on and facilitator listed, adjust changing the rest
func walletEnv(t *testing.T, adjust func(*config.Wallets)) *env {
	t.Helper()
	w := config.Wallets{UsePayer: true, FacilitatorAddresses: []string{facilitator}}
	if adjust != nil {
		adjust(&w)
	}
	return newEnvWith(t, func(d *saga.Deps) { d.Wallets = w },
		newLocal(t, local.Options{Name: "a"}))
}

func (e *env) paidToken(t *testing.T, order, payer, feePayer string) string {
	t.Helper()
	return e.token(t, ordertoken.Claims{
		OrderID: order, Kind: ordertoken.KindStart, PackageID: smallPkg, AmountMicros: smallPrice,
		Payer: payer, FeePayer: feePayer,
	})
}

func (e *env) createPaid(t *testing.T, order, payer string) (store.Placement, error) {
	t.Helper()
	p, _, err := e.saga.Create(t.Context(), e.paidToken(t, order, payer, facilitator),
		placement.Requirements{})
	return p, err
}

func (e *env) orderCount(t *testing.T) int {
	t.Helper()
	var n int
	if err := e.pool.QueryRow(t.Context(), `select count(*) from orders`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func (e *env) blockWallet(t *testing.T, payer string) {
	t.Helper()
	w := wallets.New(e.store, wallets.Settings{})
	err := e.store.WithTx(t.Context(), func(tx pgx.Tx) error {
		return w.Block(t.Context(), tx, wallets.Block{
			Payer: payer, Reason: wallets.ReasonOperator, Actor: "operator:test",
		})
	})
	if err != nil {
		t.Fatal(err)
	}
}

func count(errs []error, target error) int {
	n := 0
	for _, err := range errs {
		if err == nil && target == nil || target != nil && errors.Is(err, target) {
			n++
		}
	}
	return n
}

func TestSixOrdersOfOneWallet(t *testing.T) {
	e := walletEnv(t, nil)
	errs := make([]error, 6)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Go(func() {
			_, errs[i] = e.createPaid(t, "order-"+string(rune('a'+i)), payerA)
		})
	}
	wg.Wait()
	eq(t, count(errs, nil), 5)
	eq(t, count(errs, saga.ErrWalletBusy), 1)
	ev := e.oneEvent(t, saga.EventFailed)
	eq(t, ev.Reason, saga.ReasonWalletBusy)
	eq(t, ev.Refundable, true)
	n, err := e.store.LiveCount(t.Context(), payerA)
	eq(t, err, nil)
	eq(t, n, 5)
}

func TestStartBucketRefusesTheSixth(t *testing.T) {
	e := walletEnv(t, func(w *config.Wallets) { w.MaxConcurrent = 50 })
	for i := range 5 {
		if _, err := e.createPaid(t, "order-"+string(rune('a'+i)), payerA); err != nil {
			t.Fatal(err)
		}
	}
	p, err := e.createPaid(t, "order-f", payerA)
	var we *saga.WaitError
	eq(t, errors.As(err, &we), true)
	eq(t, errors.Is(err, saga.ErrWalletRateLimited), true)
	eq(t, we.Wait > 100*time.Second && we.Wait <= 120*time.Second, true)
	eq(t, p.State, store.PlacementFailed)
	eq(t, p.FailureReason, saga.ReasonWalletRateLimited)
	eq(t, p.Refundable, true)

	// Another wallet has its own buckets
	if _, err := e.createPaid(t, "order-g", payerB); err != nil {
		t.Fatal(err)
	}
	// A retry of the refused order answers the same code with a wait
	_, err = e.createPaid(t, "order-f", payerA)
	eq(t, errors.As(err, &we), true)
	eq(t, we.Wait >= time.Second, true)
}

func TestRetriedRefusalSameCode(t *testing.T) {
	e := walletEnv(t, func(w *config.Wallets) { w.MaxConcurrent = 1 })
	first, err := e.createPaid(t, "order-1", payerA)
	if err != nil {
		t.Fatal(err)
	}
	refused, err := e.createPaid(t, "order-2", payerA)
	eq(t, errors.Is(err, saga.ErrWalletBusy), true)
	e.saga.Wait()
	h, _ := handle.Derive(e.handleKey, "order-1")
	if _, err := e.saga.Stop(t.Context(), h); err != nil {
		t.Fatal(err)
	}
	eq(t, e.reload(t, first.ID).State, store.PlacementTerminated)
	again, err := e.createPaid(t, "order-2", payerA)
	eq(t, errors.Is(err, saga.ErrWalletBusy), true)
	eq(t, again.ID, refused.ID)
	eq(t, len(e.events(t, saga.EventFailed)), 1)
}

func TestBlockedWalletRecordsNothing(t *testing.T) {
	e := walletEnv(t, nil)
	p, h := e.placePaid(t, "order-1", payerA)
	e.blockWallet(t, payerA)
	orders, placements := e.orderCount(t), e.placementCount(t)
	_, err := e.createPaid(t, "order-2", payerA)
	eq(t, errors.Is(err, saga.ErrWalletBlocked), true)
	eq(t, e.orderCount(t), orders)
	eq(t, e.placementCount(t), placements)

	ext := e.paidExtend(t, "ext-1", payerA, h)
	_, err = e.saga.Extend(t.Context(), h, ext)
	eq(t, errors.Is(err, saga.ErrWalletBlocked), true)
	eq(t, e.orderCount(t), orders)
	eq(t, e.reload(t, p.ID).Deadline.Equal(*p.Deadline), true)

	// Another payer may still extend the blocked wallet's placement
	if _, err := e.saga.Extend(t.Context(), h, e.paidExtend(t, "ext-2", payerB, h)); err != nil {
		t.Fatal(err)
	}
	claims, err := e.store.ExtendClaims(t.Context(), p.ID)
	eq(t, err, nil)
	eq(t, len(claims), 1)
	eq(t, claims[0].Payer, payerB)
}

func TestReplayedExtendSkipsTheBlockCheck(t *testing.T) {
	e := walletEnv(t, nil)
	_, h := e.placePaid(t, "order-1", payerB)
	ext := e.paidExtend(t, "ext-1", payerA, h)
	first, err := e.saga.Extend(t.Context(), h, ext)
	if err != nil {
		t.Fatal(err)
	}
	e.blockWallet(t, payerA)
	again, err := e.saga.Extend(t.Context(), h, ext)
	eq(t, err, nil)
	eq(t, again.Deadline.Equal(*first.Deadline), true)
}

func TestEleventhRefusalBlocks(t *testing.T) {
	e := walletEnv(t, func(w *config.Wallets) { w.MaxConcurrent = 1; w.EmitEvents = true })
	if _, err := e.createPaid(t, "order-0", payerA); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	for i := 1; i <= 10; i++ {
		_, err := e.createPaid(t, "order-"+string(rune('a'+i)), payerA)
		eq(t, errors.Is(err, saga.ErrWalletBusy), true)
		w, err := e.store.Wallet(t.Context(), payerA, start)
		eq(t, err, nil)
		eq(t, w.Blocked(time.Now()), false)
	}
	_, err := e.createPaid(t, "order-z", payerA)
	eq(t, errors.Is(err, saga.ErrWalletBusy), true)
	w, err := e.store.Wallet(t.Context(), payerA, start)
	eq(t, err, nil)
	eq(t, w.Blocked(time.Now()), true)
	eq(t, w.BlockedReason, wallets.ReasonRefusals)
	eq(t, w.BlockedBy, wallets.ActorRouter)
	eq(t, w.BlockedUntil.Sub(start) > 59*time.Minute && w.BlockedUntil.Sub(start) < 61*time.Minute,
		true)
	pending, err := e.store.PendingOutbox(t.Context(), time.Now(), 100)
	eq(t, err, nil)
	blocks := 0
	for _, ev := range pending {
		if ev.EventType == wallets.EventBlocked {
			blocks++
		}
	}
	eq(t, blocks, 1)
	_, err = e.createPaid(t, "order-y", payerA)
	eq(t, errors.Is(err, saga.ErrWalletBlocked), true)
}

func TestPayerCountsAsAbsent(t *testing.T) {
	e := walletEnv(t, func(w *config.Wallets) { w.AnonymousMaxConcurrent = 100 })
	ctx := t.Context()
	cases := []struct{ order, payer, fee string }{
		{"is-fee-payer", payerC, payerC},
		{"listed", facilitator, payerB},
		{"no-claim", "", facilitator},
	}
	for _, c := range cases {
		p, _, err := e.saga.Create(ctx, e.paidToken(t, c.order, c.payer, c.fee),
			placement.Requirements{})
		if err != nil {
			t.Fatalf("%s: %v", c.order, err)
		}
		eq(t, p.Payer, "")
	}
	// A payer an earlier order carried as its fee payer counts as absent too
	p, _, err := e.saga.Create(ctx, e.paidToken(t, "seen", payerB, facilitator),
		placement.Requirements{})
	eq(t, err, nil)
	eq(t, p.Payer, "")
	o, err := e.store.OrderByID(ctx, "listed")
	eq(t, err, nil)
	eq(t, o.FeePayer, payerB)
	// The order keeps the claim the placement does not count under
	eq(t, o.Payer, facilitator)
	eq(t, o.AmountMicros, int64(smallPrice))
	paid, _, err := e.saga.Create(ctx, e.paidToken(t, "paid", payerA, facilitator),
		placement.Requirements{})
	eq(t, err, nil)
	eq(t, paid.Payer, payerA)
	o, err = e.store.OrderByID(ctx, "paid")
	eq(t, err, nil)
	eq(t, o.Payer, payerA)
}

func TestPayerWithoutAGuardCountsAsAbsent(t *testing.T) {
	e := newEnvWith(t, func(d *saga.Deps) { d.Wallets = config.Wallets{UsePayer: true} },
		newLocal(t, local.Options{Name: "a"}))
	p, _, err := e.saga.Create(t.Context(), e.paidToken(t, "o1", payerA, ""),
		placement.Requirements{})
	eq(t, err, nil)
	eq(t, p.Payer, "")
	n, err := e.vk.Do(t.Context(), e.vk.B().Exists().Key(e.prefix+wallet.UnguardedKey).Build()).
		AsInt64()
	eq(t, err, nil)
	eq(t, n, int64(1))
	// With a fee payer the same address counts
	p, _, err = e.saga.Create(t.Context(), e.paidToken(t, "o2", payerA, facilitator),
		placement.Requirements{})
	eq(t, err, nil)
	eq(t, p.Payer, payerA)
}

func TestPayerOffCountsEveryOrderAsAnonymous(t *testing.T) {
	e := newEnvWith(t, func(d *saga.Deps) {
		d.Wallets = config.Wallets{AnonymousMaxConcurrent: 1}
	}, newLocal(t, local.Options{Name: "a"}))
	p, err := e.createPaid(t, "o1", payerA)
	eq(t, err, nil)
	eq(t, p.Payer, "")
	_, err = e.createPaid(t, "o2", payerB)
	eq(t, errors.Is(err, saga.ErrWalletBusy), true)
}

func TestOrdersKeepThePayerClaim(t *testing.T) {
	e := newEnvWith(t, func(d *saga.Deps) { d.Wallets = config.Wallets{UsePayer: false} },
		newLocal(t, local.Options{Name: "a"}))
	p, h := e.placePaid(t, "o1", payerA)
	eq(t, p.Payer, "")
	if _, err := e.saga.Extend(t.Context(), h, e.paidExtend(t, "x1", payerB, h)); err != nil {
		t.Fatal(err)
	}
	for order, want := range map[string]string{"o1": payerA, "x1": payerB} {
		o, err := e.store.OrderByID(t.Context(), order)
		eq(t, err, nil)
		eq(t, o.Payer, want)
	}
	var placementPayer *string
	err := e.pool.QueryRow(t.Context(), `select payer from placements where id = $1`, p.ID).
		Scan(&placementPayer)
	eq(t, err, nil)
	eq(t, placementPayer == nil, true)
	// No wallet row, since no order counted under a payer
	_, err = e.store.Wallet(t.Context(), payerA, time.Now())
	eq(t, errors.Is(err, store.ErrNotFound), true)
}

func TestPayerRequired(t *testing.T) {
	e := walletEnv(t, func(w *config.Wallets) { w.RequirePayer = true })
	_, _, err := e.saga.Create(t.Context(), e.paidToken(t, "o1", "", facilitator),
		placement.Requirements{})
	eq(t, errors.Is(err, saga.ErrPayerRequired), true)
	eq(t, e.orderCount(t), 0)
	if _, err := e.createPaid(t, "o2", payerA); err != nil {
		t.Fatal(err)
	}
}

func TestUnderpaidUsesPackageShare(t *testing.T) {
	e := newEnv(t, newLocal(t, local.Options{Name: "a"}))
	tok := func(order string, amount int64) string {
		return e.token(t, ordertoken.Claims{
			OrderID: order, Kind: ordertoken.KindStart, PackageID: smallPkg,
			AmountMicros: amount, AllowanceMicros: 1000000,
		})
	}
	p, _, err := e.saga.Create(t.Context(), tok("paid", 1053000), placement.Requirements{})
	if err != nil {
		t.Fatal(err)
	}
	o, err := e.store.OrderByID(t.Context(), "paid")
	eq(t, err, nil)
	eq(t, o.AmountMicros, int64(53000))
	eq(t, p.PaidMicros, int64(53000))
	_, _, err = e.saga.Create(t.Context(), tok("short", 1049000), placement.Requirements{})
	eq(t, errors.Is(err, saga.ErrUnderpaid), true)
}

func TestCreateWaitsForABlockInFlight(t *testing.T) {
	e := walletEnv(t, nil)
	ctx := t.Context()
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	w := wallets.New(e.store, wallets.Settings{})
	err = w.Block(ctx, tx, wallets.Block{Payer: payerA, Reason: "abuse", Actor: "router"})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := e.createPaid(t, "o1", payerA)
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("the create ran while the block held the wallet's lock: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		eq(t, errors.Is(err, saga.ErrWalletBlocked), true)
	case <-time.After(5 * time.Second):
		t.Fatal("the create never ran")
	}
}

func TestAnExtendWithoutAPayerHoldsNoLock(t *testing.T) {
	a := newCapped(t, t.TempDir(), 0)
	e := newEnv(t, a)
	_, h := e.placeReady(t, "order-1")
	a.hold.Store(true)
	done := make(chan error, 1)
	go func() {
		_, err := e.saga.Extend(context.WithoutCancel(t.Context()), h,
			e.extendToken(t, "ext-1", morePkg, morePrice, h))
		done <- err
	}()
	<-a.held
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, _, err := e.saga.Create(ctx, e.startToken(t, "order-2"), placement.Requirements{})
	close(a.release)
	eq(t, err, nil)
	eq(t, <-done, nil)
}

func TestAnExtensionPayerIsSeen(t *testing.T) {
	e := walletEnv(t, nil)
	_, h := e.placePaid(t, "order-1", payerA)
	if _, err := e.store.Wallet(t.Context(), payerB, time.Now()); !errors.Is(err,
		store.ErrNotFound) {
		t.Fatalf("payer B before any order: %v", err)
	}
	if _, err := e.saga.Extend(t.Context(), h, e.paidExtend(t, "ext-1", payerB, h)); err != nil {
		t.Fatal(err)
	}
	w, err := e.store.Wallet(t.Context(), payerB, time.Now())
	eq(t, err, nil)
	eq(t, w.Payer, payerB)
}

// placePaid places a ready placement whose start order payer paid
func (e *env) placePaid(t *testing.T, order, payer string) (store.Placement, string) {
	t.Helper()
	p, h, err := e.saga.Create(t.Context(), e.paidToken(t, order, payer, facilitator),
		placement.Requirements{})
	if err != nil {
		t.Fatal(err)
	}
	e.saga.Wait()
	p = e.reload(t, p.ID)
	eq(t, p.State, store.PlacementReady)
	return p, h
}

func (e *env) paidExtend(t *testing.T, order, payer, h string) string {
	t.Helper()
	sha, err := handle.Hash(h)
	if err != nil {
		t.Fatal(err)
	}
	return e.token(t, ordertoken.Claims{
		OrderID: order, Kind: ordertoken.KindExtend, PackageID: morePkg, AmountMicros: morePrice,
		HandleSHA256: hex.EncodeToString(sha), Payer: payer, FeePayer: facilitator,
	})
}

// stalled is a Valkey that takes every command and never answers until released
type stalled struct {
	valkey.Client
	released chan struct{}
}

func (s stalled) Do(ctx context.Context, cmd valkey.Completed) valkey.ValkeyResult {
	select {
	case <-ctx.Done():
	case <-s.released:
	}
	return s.Client.Do(ctx, cmd)
}

func (s stalled) DoMulti(ctx context.Context, cmds ...valkey.Completed) []valkey.ValkeyResult {
	select {
	case <-ctx.Done():
	case <-s.released:
	}
	return s.Client.DoMulti(ctx, cmds...)
}

func TestStartsGoThroughWhenValkeyStalls(t *testing.T) {
	var logs syncBuffer
	vk := stalled{released: make(chan struct{})}
	e := newEnvWith(t, func(d *saga.Deps) {
		d.Wallets = config.Wallets{UsePayer: true, FacilitatorAddresses: []string{facilitator}}
		d.Log = slog.New(slog.NewTextHandler(&logs, nil))
		vk.Client = d.Valkey
		d.Valkey = vk
	}, newLocal(t, local.Options{Name: "a"}))
	t.Cleanup(func() { close(vk.released) })
	for i := range 5 {
		start := time.Now()
		if _, err := e.createPaid(t, "order-"+string(rune('a'+i)), payerA); err != nil {
			t.Fatalf("start %d with Valkey stalled: %v", i+1, err)
		}
		if took := time.Since(start); took > 3*time.Second {
			t.Fatalf("start %d took %v with Valkey stalled", i+1, took)
		}
	}
	if n := strings.Count(logs.String(),
		`level=WARN msg="wallet start buckets unread, letting the start through"`); n != 5 {
		t.Fatalf("%d warnings for 5 starts:\n%s", n, logs.String())
	}
	// The Postgres cap still holds
	_, err := e.createPaid(t, "order-f", payerA)
	eq(t, errors.Is(err, saga.ErrWalletBusy), true)
}

func TestOrdersKeepTheTxClaim(t *testing.T) {
	const (
		startTx = "36CpwVLKDRH7UU96nxCVFzCR8DNoDFCxNu19SKnFiTWW" +
			"TgJUs3HQbSHTb2zHgh2uo8yqUVZz2MjWanciehRuemgr"
		extendTx = "2GxEQDvUNB1J644jDee6TixtWGkyKRydgKnTXmVKohxq" +
			"vUqm7uMfhRnnCWgiWyiqDTX1DAJ2fB2BU8dWS4E7moZ5"
	)
	e := newEnv(t, newLocal(t, local.Options{Name: "a"}))
	p, h, err := e.saga.Create(t.Context(), e.token(t, ordertoken.Claims{
		OrderID: "o1", Kind: ordertoken.KindStart, PackageID: smallPkg, AmountMicros: smallPrice,
		Tx: startTx,
	}), placement.Requirements{})
	if err != nil {
		t.Fatal(err)
	}
	e.saga.Wait()
	eq(t, e.reload(t, p.ID).State, store.PlacementReady)
	sha, err := handle.Hash(h)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.saga.Extend(t.Context(), h, e.token(t, ordertoken.Claims{
		OrderID: "x1", Kind: ordertoken.KindExtend, PackageID: morePkg, AmountMicros: morePrice,
		HandleSHA256: hex.EncodeToString(sha), Tx: extendTx,
	})); err != nil {
		t.Fatal(err)
	}
	for order, want := range map[string]string{"o1": startTx, "x1": extendTx} {
		o, err := e.store.OrderByID(t.Context(), order)
		eq(t, err, nil)
		eq(t, o.Tx, want)
	}
}

// costly bills 40000 micros for the small package's 10 minutes
type costly struct{ *local.Provider }

func (costly) HourlyCostMicros(provider.Size) int64 { return 240000 }

func TestMarginUsesPackageShare(t *testing.T) {
	e := newEnv(t, costly{newLocal(t, local.Options{Name: "a"})})
	tok := e.token(t, ordertoken.Claims{
		OrderID: "paid", Kind: ordertoken.KindStart, PackageID: smallPkg,
		AmountMicros: 1053000, AllowanceMicros: 1000000,
	})
	// 40000 against the 1053000 paid would pass the 0.5 floor, against the package it fails
	_, _, err := e.saga.Create(t.Context(), tok, placement.Requirements{})
	eq(t, errors.Is(err, placement.ErrNoCandidate), true)
}
