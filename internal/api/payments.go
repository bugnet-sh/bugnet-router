package api

import (
	"cmp"
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/wallet"
	"github.com/bugnet-sh/bugnet-router/internal/wallets"
)

// PaymentsServer implements routerv1connect.PaymentsServiceHandler for Payments
type PaymentsServer struct {
	// Answers Unimplemented for every RPC of the contract without a method here
	routerv1connect.UnimplementedPaymentsServiceHandler
	s *Server
}

var _ routerv1connect.PaymentsServiceHandler = (*PaymentsServer)(nil)

// GetOrder says what became of an order, redeemed false when the router never took it
func (ps *PaymentsServer) GetOrder(
	ctx context.Context, req *connect.Request[routerv1.GetOrderRequest],
) (*connect.Response[routerv1.GetOrderResponse], error) {
	id := req.Msg.GetOrderId()
	if id == "" {
		return nil, invalid("order_id is required")
	}
	o, err := ps.s.d.Store.OrderByID(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return connect.NewResponse(&routerv1.GetOrderResponse{}), nil
	}
	if err != nil {
		return nil, ps.s.status(err)
	}
	p, err := ps.s.d.Store.PlacementByID(ctx, o.PlacementID)
	if err != nil {
		return nil, ps.s.status(err)
	}
	msg := placementMsg(p)
	return connect.NewResponse(&routerv1.GetOrderResponse{
		Redeemed:       true,
		Kind:           o.Kind,
		PlacementState: p.State,
		// An extension is recorded only once applied, so a refund is the start order's
		Refundable:        o.Kind == store.OrderStart && p.Refundable,
		FailureReason:     msg.GetFailureReason(),
		TerminationReason: msg.GetTerminationReason(),
		EndedAt:           msg.GetEndedAt(),
	}), nil
}

// CheckPayer says whether an order of the payer would be refused now, writing and spending nothing
func (ps *PaymentsServer) CheckPayer(
	ctx context.Context, req *connect.Request[routerv1.CheckPayerRequest],
) (*connect.Response[routerv1.CheckPayerResponse], error) {
	m, s := req.Msg, ps.s
	kind := m.GetKind()
	if kind != store.OrderStart && kind != store.OrderExtend {
		return nil, invalid("kind must be start or extend")
	}
	for _, addr := range []string{m.GetPayer(), m.GetFeePayer()} {
		if _, err := wallet.ParseSolana(addr); addr != "" && err != nil {
			ce := invalid("payer and fee_payer must be canonical base58 Solana addresses")
			return nil, withInfo(ce, reasonPayerInvalid)
		}
	}
	payer, _, err := wallet.Decide(s.guard,
		wallet.Claim{Payer: m.GetPayer(), FeePayer: m.GetFeePayer()},
		func(addr string) (bool, error) { return s.d.Store.FeePayerSeen(ctx, addr) })
	if err != nil {
		return nil, s.status(err)
	}
	answer := func(r *routerv1.CheckPayerResponse) (
		*connect.Response[routerv1.CheckPayerResponse], error,
	) {
		return connect.NewResponse(r), nil
	}
	now := time.Now()
	if payer != "" {
		w, err := s.d.Store.Wallet(ctx, payer, now)
		switch {
		case errors.Is(err, store.ErrNotFound):
		case err != nil:
			return nil, s.status(err)
		case w.Blocked(now):
			r := &routerv1.CheckPayerResponse{Verdict: routerv1.PayerVerdict_PAYER_VERDICT_BLOCKED}
			if w.BlockedUntil != nil {
				r.BlockedUntil = timestamppb.New(*w.BlockedUntil)
			}
			return answer(r)
		}
	}
	ok := &routerv1.CheckPayerResponse{Verdict: routerv1.PayerVerdict_PAYER_VERDICT_OK}
	if kind == store.OrderExtend {
		return answer(ok)
	}
	limit := s.w.AnonymousMaxConcurrent
	if payer != "" {
		limit = s.w.MaxConcurrent
	}
	n, err := s.d.Store.LiveCount(ctx, payer)
	if err != nil {
		return nil, s.status(err)
	}
	if n >= limit {
		return answer(&routerv1.CheckPayerResponse{
			Verdict: routerv1.PayerVerdict_PAYER_VERDICT_BUSY,
		})
	}
	if payer == "" {
		return answer(ok)
	}
	pctx, cancel := context.WithTimeout(ctx, opsTimeout)
	defer cancel()
	has, wait, err := s.ops.PeekAll(pctx, wallets.StartBuckets(s.w, payer))
	switch {
	case err != nil && gaveUp(ctx):
		return nil, s.status(cmp.Or(ctx.Err(), context.DeadlineExceeded))
	case err != nil:
		// CreatePlacement lets a start through when Valkey does not answer, so this does too
		s.d.Log.Warn("api: wallet start buckets unread for CheckPayer", "err", err)
	case !has:
		return answer(&routerv1.CheckPayerResponse{
			Verdict:    routerv1.PayerVerdict_PAYER_VERDICT_RATE_LIMITED,
			RetryAfter: durationpb.New(wait),
		})
	}
	return answer(ok)
}

// LookupPlacement names the placement a handle hash or a start order points to, for Payments
func (ps *PaymentsServer) LookupPlacement(ctx context.Context,
	req *connect.Request[routerv1.LookupPlacementRequest],
) (*connect.Response[routerv1.LookupPlacementResponse], error) {
	var p store.Placement
	var err error
	switch key := req.Msg.GetKey().(type) {
	case *routerv1.LookupPlacementRequest_HandleSha256:
		sha, ok := lowerHex32(key.HandleSha256)
		if !ok {
			return nil, invalid("handle_sha256 must be 64 lower-case hex characters")
		}
		p, err = ps.s.d.Store.PlacementByHandle(ctx, sha)
	case *routerv1.LookupPlacementRequest_StartOrderId:
		if key.StartOrderId == "" {
			return nil, invalid("start_order_id is required")
		}
		p, err = ps.s.d.Store.PlacementByStartOrder(ctx, key.StartOrderId)
		if errors.Is(err, store.ErrNotFound) {
			err = errOrderUnknown
		}
	default:
		return nil, invalid("handle_sha256 or start_order_id is required")
	}
	if err != nil {
		return nil, ps.s.status(err)
	}
	out := &routerv1.LookupPlacementResponse{
		PlacementId:  p.ID.String(),
		StartOrderId: p.OrderID,
		State:        p.State,
		X402Capable:  ps.s.lookupCapable(p),
		PackageId:    p.PackageID,
	}
	if p.ReadyAt != nil && p.Deadline != nil {
		out.Deadline = timestamppb.New(*p.Deadline)
	}
	if p.TerminatedAt != nil {
		out.EndedAt = timestamppb.New(*p.TerminatedAt)
	}
	return connect.NewResponse(out), nil
}

// lookupCapable is needs_secrets until the placement was ready, then its provider's Secrets
func (s *Server) lookupCapable(p store.Placement) bool {
	if p.ReadyAt == nil {
		return p.NeedsSecrets
	}
	prov, ok := s.d.Registry.Get(p.Provider)
	return ok && s.d.Registry.Capabilities(prov).Secrets
}

// lowerHex32 decodes s when it is exactly 64 lower-case hex characters
func lowerHex32(s string) ([]byte, bool) {
	if len(s) != 64 || strings.ToLower(s) != s {
		return nil, false
	}
	b, err := hex.DecodeString(s)
	return b, err == nil
}
