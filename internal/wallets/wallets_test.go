package wallets_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
	"github.com/bugnet-sh/bugnet-router/internal/wallets"
)

const (
	payer       = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
	facilitator = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
	feePayer    = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
)

// eq fails the test when got and want differ
func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

type env struct {
	pool *pgxpool.Pool
	st   *store.Store
	w    *wallets.Store
	now  time.Time
}

func newEnv(t *testing.T, emit bool) *env {
	t.Helper()
	ctx := t.Context()
	url := testenv.DatabaseURL(t)
	if err := store.Migrate(ctx, url); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	e := &env{pool: pool, st: store.New(pool), now: time.Now().UTC().Truncate(time.Second)}
	e.w = wallets.New(e.st, wallets.Settings{
		Facilitators: []string{facilitator}, StrikeThreshold: 2, StrikeWindow: 24 * time.Hour,
		StrikeBlockFor: 168 * time.Hour, EmitEvents: emit, Now: func() time.Time { return e.now },
	})
	return e
}

func (e *env) tx(t *testing.T, fn func(pgx.Tx) error) error {
	t.Helper()
	return e.st.WithTx(t.Context(), fn)
}

func (e *env) block(t *testing.T, b wallets.Block) error {
	t.Helper()
	return e.tx(t, func(tx pgx.Tx) error { return e.w.Block(t.Context(), tx, b) })
}

func (e *env) wallet(t *testing.T) store.Wallet {
	t.Helper()
	w, err := e.st.Wallet(t.Context(), payer, e.now.Add(-24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func (e *env) outbox(t *testing.T) []string {
	t.Helper()
	rows, err := e.pool.Query(t.Context(),
		`select event_type || ' ' || payload::text from outbox order by created_at, id`)
	if err != nil {
		t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestBlockAndUnblock(t *testing.T) {
	e := newEnv(t, true)
	ctx := t.Context()
	until := e.now.Add(time.Hour)
	err := e.block(t, wallets.Block{
		Payer: payer, Reason: wallets.ReasonOperator, Until: &until, Actor: "operator:ann",
		Evidence: json.RawMessage(`{"ticket":7}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	w := e.wallet(t)
	eq(t, w.Blocked(e.now), true)
	eq(t, w.Blocked(until), false)
	eq(t, *w.BlockedUntil, until)
	eq(t, w.BlockedReason, wallets.ReasonOperator)
	eq(t, w.BlockedBy, "operator:ann")
	blocked, err := e.w.Blocked(ctx, e.pool, payer, e.now)
	eq(t, err, nil)
	eq(t, blocked, true)

	e.now = e.now.Add(time.Minute)
	err = e.tx(t, func(tx pgx.Tx) error {
		return e.w.Unblock(ctx, tx, payer, "operator:bob", "paid back")
	})
	if err != nil {
		t.Fatal(err)
	}
	blocked, _ = e.w.Blocked(ctx, e.pool, payer, e.now)
	eq(t, blocked, false)
	evs, err := e.st.WalletEvents(ctx, payer, 50)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(evs), 2)
	eq(t, []string{evs[0].Kind, evs[0].Reason, evs[0].Actor},
		[]string{"unblock", "paid back", "operator:bob"})
	eq(t, []string{evs[1].Kind, evs[1].Reason, evs[1].Actor},
		[]string{"block", "operator", "operator:ann"})
	eq(t, string(evs[1].Evidence), `{"ticket": 7}`)
	out := e.outbox(t)
	eq(t, len(out), 2)
	eq(t, out[1], `wallet.unblocked {"payer": "`+payer+`"}`)

	err = e.tx(t, func(tx pgx.Tx) error {
		return e.w.Unblock(ctx, tx, feePayer, "operator:bob", "")
	})
	eq(t, errors.Is(err, store.ErrNotFound), true)
}

func TestBlockNeverShortens(t *testing.T) {
	e := newEnv(t, true)
	err := e.block(t, wallets.Block{Payer: payer, Reason: "abuse", Actor: "router"})
	if err != nil {
		t.Fatal(err)
	}
	hour := e.now.Add(time.Hour)
	e.now = e.now.Add(time.Minute)
	if err := e.block(t, wallets.Block{
		Payer: payer, Reason: "operator", Until: &hour, Actor: "operator:ann",
	}); err != nil {
		t.Fatal(err)
	}
	w := e.wallet(t)
	eq(t, w.BlockedUntil == nil, true)
	eq(t, []string{w.BlockedReason, w.BlockedBy}, []string{"abuse", "router"})
	evs, err := e.st.WalletEvents(t.Context(), payer, 50)
	eq(t, err, nil)
	eq(t, []string{evs[0].Kind, evs[0].Reason, evs[0].Actor},
		[]string{"block", "operator", "operator:ann"})
	out := e.outbox(t)
	eq(t, len(out), 2)
	eq(t, strings.Contains(out[1], `"reason": "abuse"`), true)
	eq(t, strings.Contains(out[1], `"until": null`), true)

	// A block over a shorter one replaces it whole
	e2 := newEnv(t, false)
	if err := e2.block(t, wallets.Block{
		Payer: payer, Reason: "refusals", Until: &hour, Actor: "router",
	}); err != nil {
		t.Fatal(err)
	}
	if err := e2.block(t, wallets.Block{
		Payer: payer, Reason: "operator", Actor: "operator:ann",
	}); err != nil {
		t.Fatal(err)
	}
	w = e2.wallet(t)
	eq(t, w.BlockedUntil == nil, true)
	eq(t, []string{w.BlockedReason, w.BlockedBy}, []string{"operator", "operator:ann"})
}

func TestFacilitatorBlockRefused(t *testing.T) {
	e := newEnv(t, true)
	err := e.block(t, wallets.Block{Payer: facilitator, Reason: "abuse", Actor: "router"})
	eq(t, errors.Is(err, wallets.ErrFacilitator), true)
	_, _, err = e.st.CreatePlacement(t.Context(), store.NewPlacement{
		HandleSHA256: []byte("h1"), OrderID: "o1", PackageID: "p", FeePayer: feePayer,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = e.block(t, wallets.Block{Payer: feePayer, Reason: "abuse", Actor: "router"})
	eq(t, errors.Is(err, wallets.ErrFacilitator), true)
	err = e.tx(t, func(tx pgx.Tx) error {
		_, err := e.w.Strike(t.Context(), tx, wallets.Strike{Payer: feePayer, Actor: "router"})
		return err
	})
	eq(t, errors.Is(err, wallets.ErrFacilitator), true)
	eq(t, len(e.outbox(t)), 0)
}

func TestStrikeBlocksAtTheThreshold(t *testing.T) {
	e := newEnv(t, false)
	strike := func() bool {
		var hit bool
		err := e.tx(t, func(tx pgx.Tx) error {
			var err error
			hit, err = e.w.Strike(t.Context(), tx, wallets.Strike{
				Payer: payer, Reason: "control_lost", Actor: "router",
			})
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		return hit
	}
	eq(t, strike(), false)
	e.now = e.now.Add(25 * time.Hour)
	eq(t, strike(), false)
	e.now = e.now.Add(time.Hour)
	eq(t, strike(), true)
	w := e.wallet(t)
	eq(t, w.BlockedReason, wallets.ReasonStrikes)
	eq(t, *w.BlockedUntil, e.now.Add(168*time.Hour))
}

func TestBlockWaitsForTheWalletLock(t *testing.T) {
	e := newEnv(t, false)
	ctx := t.Context()
	held, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Rollback(context.Background()) }()
	if err := e.st.Tx(held).LockWallets(ctx, payer); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- e.block(t, wallets.Block{Payer: payer, Reason: "abuse", Actor: "router"})
	}()
	select {
	case err := <-done:
		t.Fatalf("the block ran while the lock was held: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	if err := held.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		eq(t, err, nil)
	case <-time.After(5 * time.Second):
		t.Fatal("the block never ran")
	}
}
