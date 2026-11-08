package api_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/jackc/pgx/v5"

	"github.com/bugnet-sh/bugnet-router/internal/handle"
	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/ordertoken"
)

// extendPaid extends a placement by the extension package, paid by payer
func (e *env) extendPaid(t *testing.T, h, payer string) {
	t.Helper()
	sha, err := handle.Hash(h)
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.client.ExtendPlacement(t.Context(), connect.NewRequest(
		&routerv1.ExtendPlacementRequest{Handle: h, OrderToken: e.token(t, ordertoken.Claims{
			OrderID: fmt.Sprintf("extend-%d", e.orders.Add(1)), Kind: ordertoken.KindExtend,
			PackageID: morePkg, AmountMicros: morePrice, HandleSHA256: hex.EncodeToString(sha),
			Payer: payer, FeePayer: facilitator,
		})}))
	if err != nil {
		t.Fatal(err)
	}
}

func TestBlockWalletStopsLivePlacements(t *testing.T) {
	e := newEnv(t, options{wallets: payerWallets(nil)})
	e.readyPaid(t, payerA)
	resp, err := e.blockWallet(t, &routerv1.BlockWalletRequest{
		Payer: payerA, Actor: "ann", StopLive: true,
	})
	eq(t, err, nil)
	eq(t, resp.GetWallet().GetBlocked(), true)
	var payers []string
	rows, err := e.pool.Query(t.Context(),
		`select args from river_job where kind = 'wallet_stop_live'`)
	eq(t, err, nil)
	for rows.Next() {
		var raw []byte
		eq(t, rows.Scan(&raw), nil)
		var args jobargs.WalletStopLive
		eq(t, json.Unmarshal(raw, &args), nil)
		payers = append(payers, args.Payer)
	}
	eq(t, rows.Err(), nil)
	eq(t, payers, []string{payerA})

	// Without stop_live nothing is queued
	if _, err := e.blockWallet(t, &routerv1.BlockWalletRequest{
		Payer: payerB, Actor: "ann",
	}); err != nil {
		t.Fatal(err)
	}
	var n int
	eq(t, e.pool.QueryRow(t.Context(),
		`select count(*) from river_job where kind = 'wallet_stop_live'`).Scan(&n), nil)
	eq(t, n, 1)
}

func TestUnblockLogsOnlyALiftedBlock(t *testing.T) {
	var logs syncBuffer
	e := newEnv(t, options{
		wallets: payerWallets(nil), log: slog.New(slog.NewTextHandler(&logs, nil)),
	})
	op := e.operator(t, operatorToken)
	if _, err := e.blockWallet(t, &routerv1.BlockWalletRequest{
		Payer: payerA, Actor: "ann",
	}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		_, err := op.UnblockWallet(t.Context(), connect.NewRequest(&routerv1.UnblockWalletRequest{
			Payer: payerA, Actor: "ann",
		}))
		eq(t, err, nil)
	}
	eq(t, strings.Count(logs.String(), `msg="operator unblocked a wallet"`), 1)
}

// claimReads counts the queries that read a placement's extension claims
type claimReads struct{ n atomic.Int64 }

func (c *claimReads) TraceQueryStart(
	ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData,
) context.Context {
	if strings.Contains(data.SQL, "select distinct payer, coalesce(fee_payer") {
		c.n.Add(1)
	}
	return ctx
}

func (*claimReads) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func TestGetWalletDecidesEachPlacementsPayersOnce(t *testing.T) {
	reads := &claimReads{}
	e := newEnv(t, options{wallets: payerWallets(nil), tracer: reads})
	h := e.readyPaid(t, payerA)
	e.extendPaid(t, h, payerB)
	op := e.operator(t, operatorToken)
	for _, payer := range []string{payerA, payerB} {
		reads.n.Store(0)
		got, err := op.GetWallet(t.Context(), connect.NewRequest(&routerv1.GetWalletRequest{
			Payer: payer,
		}))
		eq(t, err, nil)
		eq(t, len(got.Msg.GetLive()), 1)
		eq(t, got.Msg.GetLive()[0].GetExtendPayers(), []string{payerB})
		eq(t, reads.n.Load(), int64(1))
	}
}
