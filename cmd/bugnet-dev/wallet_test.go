package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bugnet-sh/bugnet-router/internal/ordertoken"
)

const (
	payerAddr = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
	feeAddr   = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
	paidTx    = "36CpwVLKDRH7UU96nxCVFzCR8DNoDFCxNu19SKnFiTWW" +
		"TgJUs3HQbSHTb2zHgh2uo8yqUVZz2MjWanciehRuemgr"
)

func TestTokenCarriesThePaymentClaims(t *testing.T) {
	pub, priv := newKey(t)
	out, stderr, code := runCLI(t, env(nil), "token", "--priv", priv, "--package", "small-10m",
		"--amount", "1053000", "--payer", payerAddr, "--fee-payer", feeAddr, "--tx", paidTx,
		"--allowance-micros", "1000000")
	if code != 0 {
		t.Fatalf("token exited %d: %s", code, stderr)
	}
	c, err := ordertoken.Verify(pub, strings.TrimSpace(out), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.Payer, payerAddr)
	eq(t, c.FeePayer, feeAddr)
	eq(t, c.Tx, paidTx)
	eq(t, c.AllowanceMicros, int64(1000000))
	eq(t, c.AmountMicros, int64(1053000))
}

func TestBugnetDevAllowanceFlag(t *testing.T) {
	pub, priv := newKey(t)
	packages := filepath.Join(t.TempDir(), "packages.json")
	if err := os.WriteFile(packages, []byte(catalogJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	settings := writeSettings(t, `packages_file = "`+packages+`"`)
	out, stderr, code := runCLI(t, env(map[string]string{"BUGNET_ORDER_PRIVATE_KEY": priv}),
		"--config", settings, "token", "--package", "small-10m", "--allowance-micros", "1000000")
	if code != 0 {
		t.Fatalf("token exited %d: %s", code, stderr)
	}
	c, err := ordertoken.Verify(pub, strings.TrimSpace(out), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	// Priced from the catalog, the allowance rides on top of the package's price
	eq(t, c.AllowanceMicros, int64(1000000))
	eq(t, c.AmountMicros, int64(1050000))
	eq(t, c.PackageMicros(), int64(50000))
}

func TestTokenRefusesBadPaymentClaims(t *testing.T) {
	_, priv := newKey(t)
	h := "sbx_aaaaaaaaaaaaaaaaaaaaaaaaaa"
	for name, args := range map[string][]string{
		"bad payer":           {"--payer", "0x12"},
		"bad fee payer":       {"--fee-payer", "l" + feeAddr[1:]},
		"short tx":            {"--tx", paidTx[:80]},
		"negative allowance":  {"--allowance-micros", "-1"},
		"allowance over paid": {"--allowance-micros", "2"},
		"allowance on extend": {"--kind", "extend", "--handle", h, "--allowance-micros", "1"},
	} {
		args := append([]string{"token", "--priv", priv, "--package", "p", "--amount", "1"},
			args...)
		if _, _, code := runCLI(t, env(nil), args...); code != 2 {
			t.Errorf("%s: exited %d, want 2", name, code)
		}
	}
}

// walletOperator answers the wallet RPCs and records each request
type walletOperator struct {
	routerv1connect.UnimplementedOperatorServiceHandler
	got chan proto.Message
}

var shownWallet = &routerv1.Wallet{
	Payer: payerAddr, Blocked: true, BlockedReason: "operator", BlockedBy: "operator:founder",
	BlockedAt:    timestamppb.New(time.Date(2026, 11, 8, 9, 0, 0, 0, time.UTC)),
	FirstSeen:    timestamppb.New(time.Date(2026, 11, 7, 9, 0, 0, 0, time.UTC)),
	LastSeen:     timestamppb.New(time.Date(2026, 11, 8, 8, 0, 0, 0, time.UTC)),
	BlockedUntil: nil, Strikes: 1, LivePlacements: 2,
}

func (o *walletOperator) BlockWallet(
	_ context.Context, req *connect.Request[routerv1.BlockWalletRequest],
) (*connect.Response[routerv1.BlockWalletResponse], error) {
	o.got <- req.Msg
	return connect.NewResponse(&routerv1.BlockWalletResponse{Wallet: shownWallet}), nil
}

func (o *walletOperator) UnblockWallet(
	_ context.Context, req *connect.Request[routerv1.UnblockWalletRequest],
) (*connect.Response[routerv1.UnblockWalletResponse], error) {
	o.got <- req.Msg
	return connect.NewResponse(&routerv1.UnblockWalletResponse{
		Wallet: &routerv1.Wallet{Payer: payerAddr},
	}), nil
}

func (o *walletOperator) GetWallet(
	_ context.Context, req *connect.Request[routerv1.GetWalletRequest],
) (*connect.Response[routerv1.GetWalletResponse], error) {
	o.got <- req.Msg
	return connect.NewResponse(&routerv1.GetWalletResponse{
		Wallet: shownWallet,
		Live: []*routerv1.OperatorPlacement{{
			PlacementId: "p-1", State: routerv1.PlacementState_PLACEMENT_STATE_READY,
			Provider: "local", PackageId: "small-10m",
		}},
		Events: []*routerv1.WalletEvent{{
			Kind: "block", Actor: "operator:founder", Reason: "spam",
			CreatedAt: timestamppb.New(time.Date(2026, 11, 8, 9, 0, 0, 0, time.UTC)),
		}},
	}), nil
}

func (o *walletOperator) ListWallets(
	_ context.Context, req *connect.Request[routerv1.ListWalletsRequest],
) (*connect.Response[routerv1.ListWalletsResponse], error) {
	o.got <- req.Msg
	return connect.NewResponse(&routerv1.ListWalletsResponse{
		Wallets: []*routerv1.Wallet{shownWallet}, NextPageToken: payerAddr,
	}), nil
}

func serveOperator(t *testing.T, o *walletOperator) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(routerv1connect.NewOperatorServiceHandler(o))
	srv := httptest.NewUnstartedServer(mux)
	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	srv.Config.Protocols = &protocols
	srv.Start()
	t.Cleanup(srv.Close)
	return writeSettings(t, "[api]\nlisten_addr = \""+srv.Listener.Addr().String()+"\"\n")
}

func TestWalletCommands(t *testing.T) {
	o := &walletOperator{got: make(chan proto.Message, 1)}
	settings := serveOperator(t, o)
	vars := env(map[string]string{"BUGNET_OPERATOR_TOKEN": strings.Repeat("o", 32),
		"USER": "founder"})
	run := func(args ...string) string {
		t.Helper()
		out, stderr, code := runCLI(t, vars, append([]string{"--config", settings}, args...)...)
		if code != 0 {
			t.Fatalf("%v exited %d: %s", args, code, stderr)
		}
		return out
	}
	out := run("wallet", "block", payerAddr, "--reason", "spam", "--until", "24h")
	got := (<-o.got).(*routerv1.BlockWalletRequest)
	eq(t, got.GetPayer(), payerAddr)
	eq(t, got.GetReason(), "spam")
	eq(t, got.GetActor(), "founder")
	until := time.Until(got.GetUntil().AsTime())
	eq(t, until > 23*time.Hour && until <= 24*time.Hour, true)
	eq(t, strings.Contains(out, "blocked_until=lifted by an operator\n"), true)
	eq(t, strings.HasPrefix(out, "payer="+payerAddr+"\nblocked=true\n"), true)

	run("wallet", "block", "--reason", "spam", "--stop-live", "--actor", "oncall",
		"--until", "2026-12-01T00:00:00Z", payerAddr)
	got = (<-o.got).(*routerv1.BlockWalletRequest)
	eq(t, got.GetStopLive(), true)
	eq(t, got.GetActor(), "oncall")
	eq(t, got.GetUntil().AsTime(), time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC))

	run("wallet", "unblock", payerAddr, "--note", "appeal")
	un := (<-o.got).(*routerv1.UnblockWalletRequest)
	eq(t, un.GetNote(), "appeal")
	eq(t, un.GetActor(), "founder")

	out = run("wallet", "show", payerAddr)
	<-o.got
	eq(t, strings.Contains(out, "live p-1 state=ready provider=local package_id=small-10m\n"),
		true)
	eq(t, strings.Contains(out,
		"event 2026-11-08T09:00:00Z kind=block actor=operator:founder reason=\"spam\"\n"), true)

	out = run("wallet", "list", "--blocked", "--limit", "5")
	list := (<-o.got).(*routerv1.ListWalletsRequest)
	eq(t, list.GetBlockedOnly(), true)
	eq(t, list.GetLimit(), uint32(5))
	eq(t, out, payerAddr+" blocked=true reason=operator live=2 strikes=1\n"+
		"next_page_token="+payerAddr+"\n")

	for name, args := range map[string][]string{
		"no subcommand":      {"wallet"},
		"unknown subcommand": {"wallet", "burn", payerAddr},
		"no payer":           {"wallet", "show"},
		"two payers":         {"wallet", "show", payerAddr, payerAddr},
		"no reason":          {"wallet", "block", payerAddr},
		"bad until":          {"wallet", "block", payerAddr, "--reason", "r", "--until", "soon"},
	} {
		args := append([]string{"--config", settings}, args...)
		if _, _, code := runCLI(t, vars, args...); code != 2 {
			t.Errorf("%s: exited %d, want 2", name, code)
		}
	}
	noUser := env(map[string]string{"BUGNET_OPERATOR_TOKEN": strings.Repeat("o", 32)})
	if _, _, code := runCLI(t, noUser, "--config", settings, "wallet", "unblock",
		payerAddr); code != 2 {
		t.Errorf("no --actor and no $USER exited %d, want 2", code)
	}
}
