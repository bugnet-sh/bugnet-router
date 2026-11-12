package api_test

import (
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"github.com/valkey-io/valkey-go"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/handle"
	"github.com/bugnet-sh/bugnet-router/internal/ordertoken"
)

const (
	payerA      = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
	payerB      = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
	payerC      = "DevFaci1itatorAddress1111111111111111111111"
	facilitator = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
)

// paidCatalog has a start of 10000 and an extension of 50000
const paidCatalog = `{"packages": [
	{"id": "small-10m", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10, "price_micros": 10000},
	{"id": "small-10m-more", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10, "price_micros": 50000, "extension_of": "small-10m"}
]}`

// payerWallets turns the payer claim on with the facilitator listed
func payerWallets(adjust func(*config.Wallets)) config.Wallets {
	w := config.Wallets{UsePayer: true, FacilitatorAddresses: []string{facilitator}}
	if adjust != nil {
		adjust(&w)
	}
	return w
}

// createPaid starts a placement whose start order payer paid
func (e *env) createPaid(
	t *testing.T, payer string,
) (*connect.Response[routerv1.CreatePlacementResponse], error) {
	t.Helper()
	order := fmt.Sprintf("order-%d", e.orders.Add(1))
	return e.createWith(t, e.token(t, ordertoken.Claims{
		OrderID: order, Kind: ordertoken.KindStart, PackageID: smallPkg, AmountMicros: smallPrice,
		Payer: payer, FeePayer: facilitator,
	}))
}

func (e *env) readyPaid(t *testing.T, payer string) string {
	t.Helper()
	resp, err := e.createPaid(t, payer)
	if err != nil {
		t.Fatal(err)
	}
	h := resp.Msg.GetHandle()
	e.waitReady(t, h)
	return h
}

func (e *env) checkPayer(
	t *testing.T, payer, kind string,
) *routerv1.CheckPayerResponse {
	t.Helper()
	resp, err := e.payments(t).CheckPayer(t.Context(), connect.NewRequest(
		&routerv1.CheckPayerRequest{
			Payer: payer, FeePayer: facilitator, PackageId: smallPkg, Kind: kind,
		}))
	if err != nil {
		t.Fatalf("check payer: %v", err)
	}
	return resp.Msg
}

func (e *env) blockWallet(
	t *testing.T, req *routerv1.BlockWalletRequest,
) (*routerv1.BlockWalletResponse, error) {
	t.Helper()
	resp, err := e.operator(t, operatorToken).BlockWallet(t.Context(), connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// bucketTokens reads a bucket's stored micro-tokens, empty when it has none stored
func (e *env) bucketTokens(t *testing.T, key string) string {
	t.Helper()
	v, err := e.vk.Do(t.Context(), e.vk.B().Hget().Key(e.prefix+key).Field("tokens").Build()).
		ToString()
	if err != nil {
		return ""
	}
	return v
}

// valkeyState reads every hash and string under the test's prefix
func (e *env) valkeyState(t *testing.T) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, k := range e.keys(t) {
		typ, err := e.vk.Do(t.Context(), e.vk.B().Type().Key(k).Build()).ToString()
		if err != nil {
			t.Fatal(err)
		}
		switch typ {
		case "hash":
			m, err := e.vk.Do(t.Context(), e.vk.B().Hgetall().Key(k).Build()).AsStrMap()
			if err != nil {
				t.Fatal(err)
			}
			keys := slices.Sorted(maps.Keys(m))
			for _, f := range keys {
				out[k] += f + "=" + m[f] + ";"
			}
		case "string":
			out[k], _ = e.vk.Do(t.Context(), e.vk.B().Get().Key(k).Build()).ToString()
		default:
			out[k] = typ
		}
	}
	return out
}

func TestPlacementPaidMicros(t *testing.T) {
	e := newEnv(t, options{catalog: paidCatalog})
	resp, err := e.createWith(t, e.startToken(t, smallPkg, 10000))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, resp.Msg.GetPlacement().GetPaidMicros(), int64(10000))
	h := resp.Msg.GetHandle()
	eq(t, e.waitReady(t, h).GetPaidMicros(), int64(10000))
	ext, err := e.client.ExtendPlacement(t.Context(), connect.NewRequest(
		&routerv1.ExtendPlacementRequest{Handle: h, OrderToken: e.extendToken(t, h, morePkg,
			50000)}))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, ext.Msg.GetPlacement().GetPaidMicros(), int64(60000))
	eq(t, e.get(t, h, 0, 0).GetPaidMicros(), int64(60000))
	eq(t, e.stop(t, h).GetPaidMicros(), int64(60000))
	ended := e.get(t, h, 0, 0)
	eq(t, ended.GetState(), routerv1.PlacementState_PLACEMENT_STATE_TERMINATED)
	eq(t, ended.GetPaidMicros(), int64(60000))
}

func TestPlacementPaidMicrosLeavesTheAllowanceOut(t *testing.T) {
	e := newEnv(t, options{catalog: paidCatalog})
	resp, err := e.createWith(t, e.token(t, ordertoken.Claims{
		OrderID: "with-allowance", Kind: ordertoken.KindStart, PackageID: smallPkg,
		AmountMicros: 1010000, AllowanceMicros: 1000000,
	}))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, resp.Msg.GetPlacement().GetPaidMicros(), int64(10000))
}

func TestWalletRefusalsThroughTheAPI(t *testing.T) {
	e := newEnv(t, options{wallets: payerWallets(func(w *config.Wallets) {
		w.MaxConcurrent, w.StartsBurst = 1, 1
	})})
	e.readyPaid(t, payerA)
	_, err := e.createPaid(t, payerA)
	mustCode(t, err, connect.CodeResourceExhausted, "WALLET_BUSY")

	h := e.readyPaid(t, payerB)
	e.stop(t, h)
	_, err = e.createPaid(t, payerB)
	mustCode(t, err, connect.CodeResourceExhausted, "WALLET_RATE_LIMITED")
	var ce *connect.Error
	if !errors.As(err, &ce) {
		t.Fatal(err)
	}
	var wait time.Duration
	for _, d := range ce.Details() {
		if v, err := d.Value(); err == nil {
			if ri, ok := v.(*errdetails.RetryInfo); ok {
				wait = ri.GetRetryDelay().AsDuration()
			}
		}
	}
	eq(t, wait > 100*time.Second && wait <= 120*time.Second, true)

	if _, err := e.blockWallet(t, &routerv1.BlockWalletRequest{
		Payer: payerC, Reason: "test", Actor: "ann",
	}); err != nil {
		t.Fatal(err)
	}
	_, err = e.createPaid(t, payerC)
	mustCode(t, err, connect.CodePermissionDenied, "WALLET_BLOCKED")

	r := newEnv(t, options{wallets: payerWallets(func(w *config.Wallets) {
		w.RequirePayer = true
	})})
	_, err = r.createWith(t, r.startToken(t, smallPkg, smallPrice))
	mustCode(t, err, connect.CodePermissionDenied, "PAYER_REQUIRED")
}

func TestCheckPayerVerdicts(t *testing.T) {
	e := newEnv(t, options{wallets: payerWallets(func(w *config.Wallets) {
		w.MaxConcurrent, w.StartsBurst = 1, 1
	})})
	eq(t, e.checkPayer(t, payerA, "start").GetVerdict(),
		routerv1.PayerVerdict_PAYER_VERDICT_OK)
	e.readyPaid(t, payerA)
	eq(t, e.checkPayer(t, payerA, "start").GetVerdict(),
		routerv1.PayerVerdict_PAYER_VERDICT_BUSY)
	eq(t, e.checkPayer(t, payerA, "extend").GetVerdict(),
		routerv1.PayerVerdict_PAYER_VERDICT_OK)

	e.stop(t, e.readyPaid(t, payerB))
	got := e.checkPayer(t, payerB, "start")
	eq(t, got.GetVerdict(), routerv1.PayerVerdict_PAYER_VERDICT_RATE_LIMITED)
	wait := got.GetRetryAfter().AsDuration()
	eq(t, wait > 100*time.Second && wait <= 120*time.Second, true)

	until := time.Now().Add(time.Hour).Truncate(time.Second)
	if _, err := e.blockWallet(t, &routerv1.BlockWalletRequest{
		Payer: payerC, Reason: "test", Actor: "ann", Until: timestamppb.New(until),
	}); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"start", "extend"} {
		got := e.checkPayer(t, payerC, kind)
		eq(t, got.GetVerdict(), routerv1.PayerVerdict_PAYER_VERDICT_BLOCKED)
		eq(t, got.GetBlockedUntil().AsTime().Equal(until), true)
	}
	// The facilitator counts as no payer, checked against the anonymous cap
	eq(t, e.checkPayer(t, facilitator, "start").GetVerdict(),
		routerv1.PayerVerdict_PAYER_VERDICT_OK)

	for _, req := range []*routerv1.CheckPayerRequest{
		{Payer: "0x" + payerA[2:], Kind: "start"},
		{Payer: payerA, FeePayer: "l" + facilitator[1:], Kind: "start"},
	} {
		_, err := e.payments(t).CheckPayer(t.Context(), connect.NewRequest(req))
		mustCode(t, err, connect.CodeInvalidArgument, "PAYER_INVALID")
	}
	_, err := e.payments(t).CheckPayer(t.Context(), connect.NewRequest(
		&routerv1.CheckPayerRequest{Payer: payerA, Kind: "refund"}))
	mustCode(t, err, connect.CodeInvalidArgument, "")
}

func TestCheckPayerChangesNothing(t *testing.T) {
	e := newEnv(t, options{wallets: payerWallets(func(w *config.Wallets) {
		w.MaxConcurrent, w.StartsBurst = 1, 1
	})})
	e.readyPaid(t, payerA)
	e.stop(t, e.readyPaid(t, payerB))
	if _, err := e.blockWallet(t, &routerv1.BlockWalletRequest{
		Payer: payerC, Reason: "test", Actor: "ann",
	}); err != nil {
		t.Fatal(err)
	}
	e.saga.Wait()
	rows, contents, state := e.rowCounts(t), e.walletRows(t), e.valkeyState(t)
	verdicts := map[routerv1.PayerVerdict]bool{}
	for range 3 {
		for _, payer := range []string{payerA, payerB, payerC, facilitator, ""} {
			for _, kind := range []string{"start", "extend"} {
				verdicts[e.checkPayer(t, payer, kind).GetVerdict()] = true
			}
		}
	}
	eq(t, len(verdicts), 4)
	eq(t, e.rowCounts(t), rows)
	eq(t, e.walletRows(t), contents)
	eq(t, e.valkeyState(t), state)
}

// walletRows hashes the contents of the tables CheckPayer reads, so an update shows too
func (e *env) walletRows(t *testing.T) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, table := range []string{"wallets", "wallet_events", "orders", "placements"} {
		var sum string
		err := e.pool.QueryRow(t.Context(), `select coalesce(md5(string_agg(r::text, ','
			order by r::text)), '') from `+table+` r`).Scan(&sum)
		if err != nil {
			t.Fatal(err)
		}
		out[table] = sum
	}
	return out
}

func TestPaymentsServiceRefusesOtherTokens(t *testing.T) {
	e := newEnv(t, options{})
	for _, tok := range []string{internalToken, edgeToken, operatorToken, ""} {
		_, err := routerv1connect.NewPaymentsServiceClient(e.httpClient(t, tok), e.url).
			CheckPayer(t.Context(), connect.NewRequest(&routerv1.CheckPayerRequest{
				Payer: payerA, Kind: "start",
			}))
		wantCode(t, err, connect.CodeUnauthenticated)
	}
	for _, tok := range []string{internalToken, paymentsToken, ""} {
		_, err := e.operator(t, tok).BlockWallet(t.Context(), connect.NewRequest(
			&routerv1.BlockWalletRequest{Payer: payerA, Actor: "ann"}))
		wantCode(t, err, connect.CodeUnauthenticated)
	}
}

func TestCheckPayerSaysOKWhenValkeyHangs(t *testing.T) {
	var logs syncBuffer
	addr, replies := valve(t)
	e := newEnv(t, options{
		wallets:   payerWallets(func(w *config.Wallets) { w.MaxConcurrent, w.StartsBurst = 1, 1 }),
		apiValkey: addr, log: slog.New(slog.NewTextHandler(&logs, nil)),
	})
	e.readyPaid(t, payerA)
	e.stop(t, e.readyPaid(t, payerB))
	eq(t, e.checkPayer(t, payerB, "start").GetVerdict(),
		routerv1.PayerVerdict_PAYER_VERDICT_RATE_LIMITED)

	replies.Lock()
	t.Cleanup(replies.Unlock)
	start := time.Now()
	eq(t, e.checkPayer(t, payerB, "start").GetVerdict(), routerv1.PayerVerdict_PAYER_VERDICT_OK)
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("CheckPayer took %v with Valkey not answering", took)
	}
	if !strings.Contains(logs.String(),
		`level=WARN msg="api: wallet start buckets unread for CheckPayer"`) {
		t.Fatalf("no warning that the start buckets were not read:\n%s", logs.String())
	}
	// The Postgres cap still holds
	eq(t, e.checkPayer(t, payerA, "start").GetVerdict(),
		routerv1.PayerVerdict_PAYER_VERDICT_BUSY)
}

func TestOperatorCallsWriteAuditRows(t *testing.T) {
	e := newEnv(t, options{wallets: payerWallets(nil)})
	op := e.operator(t, operatorToken)
	ctx := t.Context()
	blocked, err := e.blockWallet(t, &routerv1.BlockWalletRequest{
		Payer: payerA, Reason: "phishing", Actor: "ann",
	})
	if err != nil {
		t.Fatal(err)
	}
	w := blocked.GetWallet()
	eq(t, w.GetBlocked(), true)
	eq(t, w.GetBlockedReason(), "operator")
	eq(t, w.GetBlockedBy(), "operator:ann")
	eq(t, w.GetBlockedUntil() == nil, true)

	list, err := op.ListWallets(ctx, connect.NewRequest(&routerv1.ListWalletsRequest{
		BlockedOnly: true,
	}))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(list.Msg.GetWallets()), 1)
	eq(t, list.Msg.GetWallets()[0].GetPayer(), payerA)

	time.Sleep(10 * time.Millisecond)
	un, err := op.UnblockWallet(ctx, connect.NewRequest(&routerv1.UnblockWalletRequest{
		Payer: payerA, Note: "appeal granted", Actor: "bob",
	}))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, un.Msg.GetWallet().GetBlocked(), false)

	got, err := op.GetWallet(ctx, connect.NewRequest(&routerv1.GetWalletRequest{Payer: payerA}))
	if err != nil {
		t.Fatal(err)
	}
	var rows []string
	for _, ev := range got.Msg.GetEvents() {
		rows = append(rows, ev.GetKind()+" "+ev.GetReason()+" "+ev.GetActor())
	}
	eq(t, rows, []string{
		"unblock appeal granted operator:bob", "block operator operator:ann",
	})
	eq(t, got.Msg.GetEvents()[1].GetEvidenceJson(), `{"note": "phishing"}`)

	h := e.readyPaid(t, payerA)
	got, err = op.GetWallet(ctx, connect.NewRequest(&routerv1.GetWalletRequest{Payer: payerA}))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, got.Msg.GetWallet().GetLivePlacements(), uint32(1))
	eq(t, len(got.Msg.GetLive()), 1)
	eq(t, got.Msg.GetLive()[0].GetPayer(), payerA)
	eq(t, got.Msg.GetLive()[0].GetProvider(), "local")
	e.stop(t, h)

	_, err = op.GetWallet(ctx, connect.NewRequest(&routerv1.GetWalletRequest{Payer: payerB}))
	wantCode(t, err, connect.CodeNotFound)
	_, err = op.UnblockWallet(ctx, connect.NewRequest(&routerv1.UnblockWalletRequest{
		Payer: payerB, Actor: "bob",
	}))
	wantCode(t, err, connect.CodeNotFound)
}

func TestOperatorDecidesExtensionPayersAgain(t *testing.T) {
	e := newEnv(t, options{wallets: payerWallets(nil)})
	op := e.operator(t, operatorToken)
	if _, err := e.blockWallet(t, &routerv1.BlockWalletRequest{
		Payer: payerC, Actor: "ann",
	}); err != nil {
		t.Fatal(err)
	}
	h := e.readyPaid(t, payerA)
	sha, err := handle.Hash(h)
	if err != nil {
		t.Fatal(err)
	}
	extend := func(payer, feePayer string) {
		t.Helper()
		_, err := e.client.ExtendPlacement(t.Context(), connect.NewRequest(
			&routerv1.ExtendPlacementRequest{Handle: h, OrderToken: e.token(t, ordertoken.Claims{
				OrderID: fmt.Sprintf("extend-%d", e.orders.Add(1)), Kind: ordertoken.KindExtend,
				PackageID: morePkg, AmountMicros: morePrice,
				HandleSHA256: hex.EncodeToString(sha), Payer: payer, FeePayer: feePayer,
			})}))
		if err != nil {
			t.Fatal(err)
		}
	}
	extend(payerB, facilitator)
	// A payer equal to its fee payer counts as none, so the blocked wallet may extend
	extend(payerC, payerC)
	extend(payerA, facilitator)

	got, err := op.GetWallet(t.Context(), connect.NewRequest(&routerv1.GetWalletRequest{
		Payer: payerA,
	}))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(got.Msg.GetLive()), 1)
	eq(t, got.Msg.GetLive()[0].GetExtendPayers(), []string{payerB})
	got, err = op.GetWallet(t.Context(), connect.NewRequest(&routerv1.GetWalletRequest{
		Payer: payerB,
	}))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(got.Msg.GetLive()), 1)
	// The order keeps payerC's claim, which counts for no wallet
	got, err = op.GetWallet(t.Context(), connect.NewRequest(&routerv1.GetWalletRequest{
		Payer: payerC,
	}))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(got.Msg.GetLive()), 0)
	var claims int
	if err := e.pool.QueryRow(t.Context(), `select count(*) from orders where payer = $1`,
		payerC).Scan(&claims); err != nil {
		t.Fatal(err)
	}
	eq(t, claims, 1)
	e.stop(t, h)
}

func TestOperatorReadsAreLogged(t *testing.T) {
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
	_, err := op.GetWallet(t.Context(), connect.NewRequest(&routerv1.GetWalletRequest{
		Payer: payerA,
	}))
	eq(t, err, nil)
	_, err = op.ListWallets(t.Context(), connect.NewRequest(&routerv1.ListWalletsRequest{
		BlockedOnly: true,
	}))
	eq(t, err, nil)
	for _, want := range []string{
		`level=INFO msg="operator read a wallet" actor=operator payer=` + payerA,
		`level=INFO msg="operator listed wallets" actor=operator blocked_only=true`,
	} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("no %q in the log:\n%s", want, logs.String())
		}
	}
}

func TestOperatorWalletRefusals(t *testing.T) {
	e := newEnv(t, options{wallets: payerWallets(nil)})
	cases := []struct {
		req    *routerv1.BlockWalletRequest
		code   connect.Code
		reason string
	}{
		{&routerv1.BlockWalletRequest{Payer: facilitator, Actor: "ann"},
			connect.CodeFailedPrecondition, "FACILITATOR_ADDRESS"},
		{&routerv1.BlockWalletRequest{Payer: payerA}, connect.CodeInvalidArgument, ""},
		{&routerv1.BlockWalletRequest{Payer: "0x12", Actor: "ann"},
			connect.CodeInvalidArgument, "PAYER_INVALID"},
		{&routerv1.BlockWalletRequest{
			Payer: payerA, Actor: "ann", Until: timestamppb.New(time.Now().Add(-time.Minute)),
		}, connect.CodeInvalidArgument, ""},
	}
	for i, c := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			_, err := e.blockWallet(t, c.req)
			mustCode(t, err, c.code, c.reason)
		})
	}
	var events int64
	if err := e.pool.QueryRow(t.Context(), `select count(*) from wallet_events`).
		Scan(&events); err != nil {
		t.Fatal(err)
	}
	eq(t, events, int64(0))
}

func TestListWalletsPages(t *testing.T) {
	e := newEnv(t, options{wallets: payerWallets(nil)})
	op := e.operator(t, operatorToken)
	for _, p := range []string{payerA, payerB, payerC} {
		if _, err := e.blockWallet(t, &routerv1.BlockWalletRequest{
			Payer: p, Actor: "ann",
		}); err != nil {
			t.Fatal(err)
		}
	}
	var seen []string
	token := ""
	for {
		resp, err := op.ListWallets(t.Context(), connect.NewRequest(&routerv1.ListWalletsRequest{
			Limit: 2, PageToken: token,
		}))
		if err != nil {
			t.Fatal(err)
		}
		for _, w := range resp.Msg.GetWallets() {
			seen = append(seen, w.GetPayer())
		}
		if token = resp.Msg.GetNextPageToken(); token == "" {
			break
		}
	}
	eq(t, seen, slices.Sorted(slices.Values([]string{payerA, payerB, payerC})))
	_, err := op.ListWallets(t.Context(), connect.NewRequest(&routerv1.ListWalletsRequest{
		PageToken: "not-a-token",
	}))
	wantCode(t, err, connect.CodeInvalidArgument)
}

func TestThreeOperationBucketsTogether(t *testing.T) {
	// Slow buckets keep each key a second after a take, where a full one expires
	e := newEnv(t, options{
		wallets:     payerWallets(func(w *config.Wallets) { w.OpsPerSecond, w.OpsBurst = 1, 2 }),
		providerOps: map[string]int{"local": 1}, opsPerSecond: 1, opsBurst: 2,
	})
	h := e.readyPaid(t, payerA)
	id := e.get(t, h, 0, 0).GetPlacementId()
	e.exec(t, sh(h, "true"))
	opsKey, walletKey := "ops:"+id, "rl:wallet:ops:"+payerA
	ops, wallet := e.bucketTokens(t, opsKey), e.bucketTokens(t, walletKey)
	if ops == "" || wallet == "" || e.bucketTokens(t, "rl:acct:local") == "" {
		t.Fatalf("the first operation took from %q %q", ops, wallet)
	}
	_, err := e.client.Exec(t.Context(), connect.NewRequest(sh(h, "true")))
	mustCode(t, err, connect.CodeResourceExhausted, "RATE_LIMITED")
	eq(t, e.bucketTokens(t, opsKey), ops)
	eq(t, e.bucketTokens(t, walletKey), wallet)
	eq(t, e.stop(t, h).GetState(), routerv1.PlacementState_PLACEMENT_STATE_TERMINATED)
}

func TestGPUOperationsSpendNoAccountBucket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "settings.toml")
	if err := os.WriteFile(path, []byte("[boat]\nops_per_second = 1\n[lium]\nops_per_second = 1\n"+
		"[hf]\nops_per_second = 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Lium's and HF's rate paces their adapters' API calls, which a user operation never makes
	eq(t, []int{cfg.Lium.OpsPerSecond, cfg.HF.OpsPerSecond}, []int{1, 1})
	for _, name := range []string{"boat", "lium", "hf"} {
		t.Run(name, func(t *testing.T) {
			// A slow bucket keeps its key a second after a take
			e := newEnv(t, options{
				providerName: name, providerOps: cfg.ProviderOps, opsPerSecond: 1, opsBurst: 2,
			})
			h, p := e.readyPlacement(t)
			e.exec(t, sh(h, "true"))
			if e.bucketTokens(t, "ops:"+p.GetPlacementId()) == "" {
				t.Fatal("the exec took nothing from the placement's bucket")
			}
			eq(t, e.bucketTokens(t, "rl:acct:"+name) != "", name == "boat")
		})
	}
}

func TestWatchJobTakesFromThreeBuckets(t *testing.T) {
	e := newEnv(t, options{
		wallets:     payerWallets(func(w *config.Wallets) { w.OpsPerSecond, w.OpsBurst = 1, 2 }),
		providerOps: map[string]int{"local": 1}, opsPerSecond: 1, opsBurst: 2,
	})
	h := e.readyPaid(t, payerA)
	id := e.get(t, h, 0, 0).GetPlacementId()
	ran := sh(h, "echo ran")
	ran.Until = &routerv1.Until{Exit: true}
	job := e.exec(t, ran).GetJobId()
	keys := []string{"ops:" + id, "rl:wallet:ops:" + payerA, "rl:acct:local"}
	// A missing key is a full bucket, and a take leaves the key for a second
	reset := func() {
		t.Helper()
		for _, k := range keys {
			if err := e.vk.Do(t.Context(), e.vk.B().Del().Key(e.prefix+k).Build()).
				Error(); err != nil {
				t.Fatal(err)
			}
		}
	}
	watch := func() error {
		_, err := e.watchJob(t, &routerv1.WatchJobRequest{Handle: h, JobId: job})
		return err
	}
	reset()
	if err := watch(); err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if e.bucketTokens(t, k) == "" {
			t.Fatalf("the attach took nothing from %s", k)
		}
	}
	for _, empty := range keys {
		reset()
		err := e.vk.Do(t.Context(), e.vk.B().Eval().Script(`
			local n = redis.call('TIME')
			redis.call('HSET', KEYS[1], 'tokens', 0, 'at', n[1] .. string.format('%06d', n[2]))
			redis.call('PEXPIRE', KEYS[1], 60000)`).Numkeys(1).Key(e.prefix+empty).Build()).
			Error()
		if err != nil && !valkey.IsValkeyNil(err) {
			t.Fatal(err)
		}
		err = watch()
		mustCode(t, err, connect.CodeResourceExhausted, "RATE_LIMITED")
		for _, k := range keys {
			if k != empty && e.bucketTokens(t, k) != "" {
				t.Fatalf("an attach refused on %s took from %s", empty, k)
			}
		}
	}
	eq(t, e.stop(t, h).GetState(), routerv1.PlacementState_PLACEMENT_STATE_TERMINATED)
}

func TestWalletOperationBucketIsShared(t *testing.T) {
	e := newEnv(t, options{wallets: payerWallets(func(w *config.Wallets) {
		w.OpsPerSecond, w.OpsBurst = 1, 1
	})})
	first, second := e.readyPaid(t, payerA), e.readyPaid(t, payerA)
	e.exec(t, sh(first, "true"))
	_, err := e.client.Exec(t.Context(), connect.NewRequest(sh(second, "true")))
	mustCode(t, err, connect.CodeResourceExhausted, "RATE_LIMITED")
	// A placement without a payer has no wallet bucket
	other, _ := e.readyPlacement(t)
	e.exec(t, sh(other, "true"))
}
