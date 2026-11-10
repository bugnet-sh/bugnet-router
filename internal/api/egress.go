package api

import (
	"cmp"
	"context"
	"errors"
	"math"
	"net/netip"
	"slices"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bugnet-sh/bugnet-router/internal/budget"
	"github.com/bugnet-sh/bugnet-router/internal/secrets"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/x402"
)

const (
	// defaultMaxTunnels is DP 10.5's tunnels a placement may hold open
	defaultMaxTunnels = 64
	// defaultRequestsInFlight is edge_budget.egress_requests_in_flight's default
	defaultRequestsInFlight = 32
)

var errTunnelUnknown = errors.New("api: no placement holds that tunnel caller")

// EgressServer implements routerv1connect.EgressServiceHandler for bugnet-egress
type EgressServer struct {
	// Answers Unimplemented for every RPC of the contract without a method here
	routerv1connect.UnimplementedEgressServiceHandler
	s *Server
}

var _ routerv1connect.EgressServiceHandler = (*EgressServer)(nil)

// ReportEgressUsage adds tunnel bytes and unpaid x402 use and answers the egress bytes left
func (g *EgressServer) ReportEgressUsage(
	ctx context.Context, req *connect.Request[routerv1.ReportEgressUsageRequest],
) (*connect.Response[routerv1.ReportEgressUsageResponse], error) {
	left, _, err := g.s.report(ctx, budget.Egress, budget.ByEdge, req.Msg.GetUsage())
	if err != nil {
		return nil, err
	}
	if err := g.s.reportX402(ctx, req.Msg.GetX402Unpaid()); err != nil {
		return nil, err
	}
	return connect.NewResponse(&routerv1.ReportEgressUsageResponse{BytesRemaining: left}), nil
}

// CheckEgressLeases answers each ready placement's deadline, egress bytes left and secrets version
func (g *EgressServer) CheckEgressLeases(
	ctx context.Context, req *connect.Request[routerv1.CheckEgressLeasesRequest],
) (*connect.Response[routerv1.CheckEgressLeasesResponse], error) {
	ps, left, err := g.s.leases(ctx, budget.Egress, req.Msg.GetPlacementIds())
	if err != nil {
		return nil, err
	}
	leases := make(map[string]*routerv1.EgressLease, len(ps))
	for _, p := range ps {
		leases[p.ID.String()] = &routerv1.EgressLease{
			Deadline: timestamppb.New(*p.Deadline), BytesRemaining: left[p.ID],
			SecretsVersion: uint64(max(p.SecretsVersion, 0)),
		}
	}
	return connect.NewResponse(&routerv1.CheckEgressLeasesResponse{Leases: leases}), nil
}

// WatchEgressEvents streams the edge events after the cursor, from now when it is empty
func (g *EgressServer) WatchEgressEvents(
	ctx context.Context, req *connect.Request[routerv1.WatchEgressEventsRequest],
	stream *connect.ServerStream[routerv1.WatchEgressEventsResponse],
) error {
	send := func(c string, ev *routerv1.EdgeEvent) error {
		return stream.Send(&routerv1.WatchEgressEventsResponse{Cursor: c, Event: ev})
	}
	return g.s.watch(ctx, req.Peer(), req.Msg.GetCursor(), send)
}

// ResolveTunnel answers a placement's tunnel terms to its sandbox's token or to the fetcher
func (g *EgressServer) ResolveTunnel(
	ctx context.Context, req *connect.Request[routerv1.ResolveTunnelRequest],
) (*connect.Response[routerv1.ResolveTunnelResponse], error) {
	s := g.s
	var p store.Placement
	var err error
	fetch := false
	switch c := req.Msg.GetCaller().(type) {
	case *routerv1.ResolveTunnelRequest_TokenSha256:
		p, err = s.tunnelByToken(ctx, c.TokenSha256)
	case *routerv1.ResolveTunnelRequest_FetchPlacementId:
		fetch = true
		p, err = s.tunnelByID(ctx, c.FetchPlacementId)
	default:
		return nil, invalid("caller is required")
	}
	if err == nil {
		err = s.tunnelServable(ctx, p)
	}
	if err != nil {
		return nil, s.status(err)
	}
	ctx = s.d.Telemetry.JoinOrder(ctx, p)
	lim := s.limit(p, budget.Egress)
	left, err := s.meter.RemainingAll(ctx, budget.Egress, []budget.Limit{lim})
	if err != nil {
		return nil, s.status(err)
	}
	held, err := s.d.Store.Secrets(ctx, p.ID)
	if err != nil {
		return nil, s.status(err)
	}
	var block *routerv1.X402Config
	var check *routerv1.TunnelSourceCheck
	if !fetch {
		if block, err = s.x402Block(ctx, p); err != nil {
			return nil, s.status(err)
		}
		if s.d.Sources != nil {
			observed, err := netip.ParseAddr(req.Msg.GetSourceIp())
			if err != nil || observed.Zone() != "" {
				observed = netip.Addr{}
			}
			check = s.d.Sources.Answer(ctx, p, observed)
		}
	}
	return connect.NewResponse(&routerv1.ResolveTunnelResponse{
		X402: block, SourceCheck: check, PlacementId: p.ID.String(),
		Deadline:  timestamppb.New(*p.Deadline),
		CaVersion: uint32(p.EgressCAVersion), Secrets: s.egressSecrets(held, fetch),
		BytesRemaining: left[p.ID], MaxTunnels: defaultMaxTunnels,
		SecretsVersion: uint64(max(p.SecretsVersion, 0)),
		BytesPerSecond: uint64(max(lim.BytesPerSecond, 0)),
		RequestsInFlight: uint32(max(
			cmp.Or(s.d.Budget.EgressRequestsInFlight, defaultRequestsInFlight), 0)),
		OrderTraceparent: orderTraceparent(ctx),
	}), nil
}

// x402Block is the placement's x402 services, caps and unpaid use, nil without a live row
func (s *Server) x402Block(ctx context.Context, p store.Placement) (*routerv1.X402Config, error) {
	rows, err := s.d.Store.X402Services(ctx, p.ID)
	if err != nil {
		return nil, err
	}
	rows = slices.DeleteFunc(rows, func(r store.X402Service) bool {
		return r.State == store.X402Disabled
	})
	if len(rows) == 0 {
		return nil, nil
	}
	unpaid, err := s.meter.X402Unpaid(ctx, p.ID)
	if err != nil {
		return nil, err
	}
	c := &routerv1.X402Config{
		Enabled: s.d.X402.Enabled, AccountId: p.OrderID,
		UnpaidRequestsPerHost: s.d.X402.UnpaidRequestsPerHost,
		UnpaidBytesPerHost:    s.d.X402.UnpaidBytesPerHost,
		UnpaidUsed:            map[string]*routerv1.X402Unpaid{},
	}
	for _, r := range rows {
		if x402.Blocked(r.Spec.Host, s.d.X402) {
			continue
		}
		c.Services = append(c.Services, &routerv1.X402Service{
			ServiceId: r.ServiceID, Host: r.Spec.Host, Prefix: r.Spec.Prefix,
			Method: r.Spec.Method, MaxPriceMicros: r.Spec.MaxPriceMicros, PayTo: r.Spec.PayTo,
			PaidPath: r.Spec.PaidPath,
		})
		if u, ok := unpaid[r.Spec.Host]; ok {
			c.UnpaidUsed[r.Spec.Host] = &routerv1.X402Unpaid{
				Requests: uint32(min(u.Requests, math.MaxUint32)), Bytes: u.Bytes,
			}
		}
	}
	return c, nil
}

// tunnelByToken finds the placement holding a tunnel token's digest
func (s *Server) tunnelByToken(ctx context.Context, sum []byte) (store.Placement, error) {
	if len(sum) != 32 {
		return store.Placement{}, errTunnelUnknown
	}
	p, err := s.d.Store.PlacementByEgressToken(ctx, sum)
	if errors.Is(err, store.ErrNotFound) {
		err = errTunnelUnknown
	}
	return p, err
}

// tunnelByID finds the placement the fetcher acts for
func (s *Server) tunnelByID(ctx context.Context, raw string) (store.Placement, error) {
	id, err := uuid.Parse(raw)
	if err != nil {
		return store.Placement{}, errTunnelUnknown
	}
	p, err := s.d.Store.PlacementByID(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		err = errTunnelUnknown
	}
	return p, err
}

// tunnelServable refuses a placement that is not ready, has no CA yet or spent its egress budget
func (s *Server) tunnelServable(ctx context.Context, p store.Placement) error {
	if p.State != store.PlacementReady || p.Deadline == nil || p.EgressCAVersion <= 0 {
		return errNotReady
	}
	spent, err := s.meter.SpentFor(ctx, budget.Egress, s.limit(p, budget.Egress))
	if err != nil {
		return err
	}
	if spent {
		return errBudgetSpent
	}
	return nil
}

// egressSecrets are the placement's secrets a caller may swap, the fetcher's including fetch_only
func (s *Server) egressSecrets(held []store.Secret, fetch bool) []*routerv1.EgressSecret {
	out := make([]*routerv1.EgressSecret, 0, len(held))
	for _, x := range held {
		if x.State == store.SecretFailed || (x.FetchOnly && !fetch) {
			continue
		}
		hosts := slices.Clone(x.Hosts)
		if !s.d.Secrets.CustomHosts {
			hosts = slices.DeleteFunc(hosts, secrets.Custom)
		}
		out = append(out, &routerv1.EgressSecret{
			PlaceholderSha256: x.PlaceholderSHA256,
			Spec: &routerv1.SecretSpec{
				Name: x.Name, Hosts: hosts, HeaderNames: x.HeaderNames, FetchOnly: x.FetchOnly,
			},
			Value:       &routerv1.SealedValue{KeyId: x.KeyID, Box: x.Sealed},
			CustomHosts: customOf(hosts),
		})
	}
	return out
}

// customOf lists the hosts outside every preset
func customOf(hosts []string) []string {
	return slices.DeleteFunc(slices.Clone(hosts), func(h string) bool { return !secrets.Custom(h) })
}
