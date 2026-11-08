package api

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/wallet"
	"github.com/bugnet-sh/bugnet-router/internal/wallets"
)

// OperatorServer implements routerv1connect.OperatorServiceHandler for bugnet-dev's operators
type OperatorServer struct {
	// Answers Unimplemented for every RPC of the contract without a method here
	routerv1connect.UnimplementedOperatorServiceHandler
	s *Server
}

var _ routerv1connect.OperatorServiceHandler = (*OperatorServer)(nil)

const (
	// walletEventsShown is how many of a wallet's newest events GetWallet answers
	walletEventsShown = 50
	walletPageDefault = 100
	walletPageMax     = 1000
)

var errStopLive = errors.New(
	"api: stop_live ends live placements through wallet_stop_live, which is not built yet")

// BlockWallet blocks a payer's new orders until the time given, or until an operator lifts it
func (o *OperatorServer) BlockWallet(
	ctx context.Context, req *connect.Request[routerv1.BlockWalletRequest],
) (*connect.Response[routerv1.BlockWalletResponse], error) {
	m, s := req.Msg, o.s
	payer, actor, err := operatorTarget(m.GetPayer(), m.GetActor())
	if err != nil {
		return nil, err
	}
	if m.GetStopLive() {
		return nil, withInfo(connect.NewError(connect.CodeFailedPrecondition, errStopLive),
			reasonFeatureUnavailable)
	}
	var until *time.Time
	if m.GetUntil() != nil {
		t := m.GetUntil().AsTime()
		if !t.After(time.Now()) {
			return nil, invalid("until must be in the future")
		}
		until = &t
	}
	var evidence json.RawMessage
	if m.GetReason() != "" {
		if evidence, err = json.Marshal(map[string]string{"note": m.GetReason()}); err != nil {
			return nil, s.status(err)
		}
	}
	err = s.d.Store.WithTx(ctx, func(tx pgx.Tx) error {
		return s.wl.Block(ctx, tx, wallets.Block{
			Payer: payer, Reason: wallets.ReasonOperator, Until: until, Actor: actor,
			Evidence: evidence,
		})
	})
	if err != nil {
		return nil, s.status(err)
	}
	s.d.Log.Info("operator blocked a wallet", "actor", actor, "payer", payer)
	w, err := s.walletMsg(ctx, payer)
	if err != nil {
		return nil, s.status(err)
	}
	return connect.NewResponse(&routerv1.BlockWalletResponse{Wallet: w}), nil
}

// UnblockWallet lifts a payer's block
func (o *OperatorServer) UnblockWallet(
	ctx context.Context, req *connect.Request[routerv1.UnblockWalletRequest],
) (*connect.Response[routerv1.UnblockWalletResponse], error) {
	m, s := req.Msg, o.s
	payer, actor, err := operatorTarget(m.GetPayer(), m.GetActor())
	if err != nil {
		return nil, err
	}
	err = s.d.Store.WithTx(ctx, func(tx pgx.Tx) error {
		return s.wl.Unblock(ctx, tx, payer, actor, m.GetNote())
	})
	if err != nil {
		return nil, s.status(err)
	}
	s.d.Log.Info("operator unblocked a wallet", "actor", actor, "payer", payer)
	w, err := s.walletMsg(ctx, payer)
	if err != nil {
		return nil, s.status(err)
	}
	return connect.NewResponse(&routerv1.UnblockWalletResponse{Wallet: w}), nil
}

// GetWallet answers a wallet with its live placements and its newest events
func (o *OperatorServer) GetWallet(
	ctx context.Context, req *connect.Request[routerv1.GetWalletRequest],
) (*connect.Response[routerv1.GetWalletResponse], error) {
	s := o.s
	payer, err := parsePayer(req.Msg.GetPayer())
	if err != nil {
		return nil, err
	}
	w, err := s.walletMsg(ctx, payer)
	if err != nil {
		return nil, s.status(err)
	}
	live, err := s.d.Store.LivePlacementsOf(ctx, payer)
	if err != nil {
		return nil, s.status(err)
	}
	out := &routerv1.GetWalletResponse{Wallet: w}
	for _, p := range live {
		op, err := s.operatorPlacement(ctx, p)
		if err != nil {
			return nil, s.status(err)
		}
		out.Live = append(out.Live, op)
	}
	evs, err := s.d.Store.WalletEvents(ctx, payer, walletEventsShown)
	if err != nil {
		return nil, s.status(err)
	}
	for _, e := range evs {
		out.Events = append(out.Events, walletEventMsg(e))
	}
	return connect.NewResponse(out), nil
}

// ListWallets lists wallets in payer order, a page at a time
func (o *OperatorServer) ListWallets(
	ctx context.Context, req *connect.Request[routerv1.ListWalletsRequest],
) (*connect.Response[routerv1.ListWalletsResponse], error) {
	m, s := req.Msg, o.s
	limit := int(m.GetLimit())
	switch {
	case limit == 0:
		limit = walletPageDefault
	case limit > walletPageMax:
		return nil, invalid("limit must be at most %d", walletPageMax)
	}
	after := m.GetPageToken()
	if after != "" {
		if _, err := wallet.ParseSolana(after); err != nil {
			return nil, invalid("page_token is not one this router gave")
		}
	}
	now := time.Now()
	ws, err := s.d.Store.ListWallets(ctx, store.WalletPage{
		BlockedOnly: m.GetBlockedOnly(), After: after, Limit: limit + 1, Now: now,
		StrikesSince: now.Add(-s.w.StrikeWindow),
	})
	if err != nil {
		return nil, s.status(err)
	}
	out := &routerv1.ListWalletsResponse{}
	if len(ws) > limit {
		ws = ws[:limit]
		out.NextPageToken = ws[limit-1].Payer
	}
	for _, w := range ws {
		out.Wallets = append(out.Wallets, walletMsg(w, now))
	}
	return connect.NewResponse(out), nil
}

// operatorTarget checks a state-changing call's payer and names its actor operator:<name>
func operatorTarget(payer, actor string) (string, string, error) {
	if actor == "" {
		return "", "", invalid("actor is required")
	}
	p, err := parsePayer(payer)
	return p, "operator:" + actor, err
}

func parsePayer(payer string) (string, error) {
	p, err := wallet.ParseSolana(payer)
	if err != nil {
		return "", withInfo(invalid("payer must be a canonical base58 Solana address"),
			reasonPayerInvalid)
	}
	return p, nil
}

// walletMsg reads a payer's wallet, store.ErrNotFound when the router never saw it
func (s *Server) walletMsg(ctx context.Context, payer string) (*routerv1.Wallet, error) {
	now := time.Now()
	w, err := s.d.Store.Wallet(ctx, payer, now.Add(-s.w.StrikeWindow))
	if err != nil {
		return nil, err
	}
	return walletMsg(w, now), nil
}

func walletMsg(w store.Wallet, now time.Time) *routerv1.Wallet {
	out := &routerv1.Wallet{
		Payer: w.Payer, FirstSeen: timestamppb.New(w.FirstSeen),
		LastSeen: timestamppb.New(w.LastSeen), Blocked: w.Blocked(now),
		Strikes: uint32(w.Strikes), LivePlacements: uint32(w.Live),
	}
	if out.Blocked {
		out.BlockedAt = timestamppb.New(*w.BlockedAt)
		out.BlockedReason, out.BlockedBy = w.BlockedReason, w.BlockedBy
		if w.BlockedUntil != nil {
			out.BlockedUntil = timestamppb.New(*w.BlockedUntil)
		}
	}
	return out
}

func walletEventMsg(e store.WalletEvent) *routerv1.WalletEvent {
	out := &routerv1.WalletEvent{
		Id: e.ID.String(), Kind: e.Kind, Reason: e.Reason, Provider: e.Provider,
		EvidenceJson: string(e.Evidence), Actor: e.Actor, CreatedAt: timestamppb.New(e.CreatedAt),
	}
	if e.PlacementID != uuid.Nil {
		out.PlacementId = e.PlacementID.String()
	}
	return out
}

// operatorPlacement describes a placement for an operator, with every payer that differs
func (s *Server) operatorPlacement(
	ctx context.Context, p store.Placement,
) (*routerv1.OperatorPlacement, error) {
	extend, err := s.d.Store.ExtendPayers(ctx, p.ID)
	if err != nil {
		return nil, err
	}
	out := &routerv1.OperatorPlacement{
		PlacementId: p.ID.String(), State: placementState(p.State), Provider: p.Provider,
		PackageId: p.PackageID, CreatedAt: timestamppb.New(p.CreatedAt), Payer: p.Payer,
		ExtendPayers: extend, SourceLabel: p.SourceLabel, TerminationReason: p.TerminationReason,
	}
	if p.ProviderRef != nil {
		out.ProviderSandboxId = p.ProviderRef.ID
	}
	if p.Deadline != nil {
		out.Deadline = timestamppb.New(*p.Deadline)
	}
	return out, nil
}
