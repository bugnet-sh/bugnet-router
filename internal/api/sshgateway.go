package api

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"github.com/google/uuid"
	"github.com/valkey-io/valkey-go"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bugnet-sh/bugnet-router/internal/budget"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/sshkeys"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

const (
	// defaultMaxConns is ssh.max_connections_per_placement's default
	defaultMaxConns = 8
	// defaultLease is ssh.connection_lease's default
	defaultLease = 60 * time.Second
	// defaultPrepare is ssh.prepare_timeout's default
	defaultPrepare = 40 * time.Second
	// prepareGap spaces the reads of a placement's sshd record while prepare_ssh runs
	prepareGap = 250 * time.Millisecond
	// maxSessions and maxForwards are DP 9.5's caps on one connection's channels
	maxSessions = 10
	maxForwards = 32
	// sshPath is the gate's SSH path
	sshPath = "/__bugnet/ssh"
	// connsLife is how long a placement's lease set outlives its deadline
	connsLife = time.Hour
)

var (
	errGrantUnknown   = errors.New("api: no SSH grant stands for that alias and key")
	errGrantInvalid   = errors.New("api: the SSH grant is not one the router made")
	errTooManyConns   = errors.New("api: the placement has its most SSH connections")
	errLeaseMalformed = errors.New("api: the lease id is malformed")
)

// leaseShape is a lease id as Acquire makes it
var leaseShape = regexp.MustCompile(`^[0-9a-f]{32}$`)

// leaseScript takes or renews a lease on Valkey's own clock, answering its Unix ms expiry or 0
var leaseScript = valkey.NewLuaScript(`
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local expiry = now + tonumber(ARGV[1])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
if not redis.call('ZSCORE', KEYS[1], ARGV[3]) and
	redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[2]) then
	return 0
end
redis.call('ZADD', KEYS[1], expiry, ARGV[3])
if redis.call('PEXPIRETIME', KEYS[1]) < tonumber(ARGV[4]) then
	redis.call('PEXPIREAT', KEYS[1], ARGV[4])
end
return expiry
`)

// SSHGatewayServer implements routerv1connect.SshGatewayServiceHandler for bugnet-ssh
type SSHGatewayServer struct {
	// Answers Unimplemented for every RPC of the contract without a method here
	routerv1connect.UnimplementedSshGatewayServiceHandler
	s *Server
}

var _ routerv1connect.SshGatewayServiceHandler = (*SSHGatewayServer)(nil)

// AuthorizeSshKey answers the grant of an alias and key, changing and counting nothing
func (g *SSHGatewayServer) AuthorizeSshKey(
	ctx context.Context, req *connect.Request[routerv1.AuthorizeSshKeyRequest],
) (*connect.Response[routerv1.AuthorizeSshKeyResponse], error) {
	s, m := g.s, req.Msg
	if len(s.d.Keys.Roots()) == 0 {
		return nil, s.status(errNoSSHGateway)
	}
	// Every denial is the same error, so an alias, a deadline and a key cannot be told apart
	if !sshkeys.ValidAlias(m.GetAlias()) {
		return nil, s.status(errGrantUnknown)
	}
	key, err := sshkeys.ParseWire(m.GetPublicKey())
	if err != nil {
		return nil, s.status(errGrantUnknown)
	}
	p, err := s.d.Store.PlacementBySSHAlias(ctx, m.GetAlias())
	if errors.Is(err, store.ErrNotFound) {
		err = errGrantUnknown
	}
	if err == nil {
		err = s.keyStands(ctx, p, key.Fingerprint)
	}
	if err != nil {
		return nil, s.status(err)
	}
	return connect.NewResponse(&routerv1.AuthorizeSshKeyResponse{
		GrantId: s.d.Keys.GrantID(p.ID, key.Fingerprint), PlacementId: p.ID.String(),
		Deadline: timestamppb.New(*p.Deadline), MaxSessions: maxSessions,
		MaxForwards: maxForwards,
	}), nil
}

// AcquireSshConnection takes one of the placement's connection leases for a proven key
func (g *SSHGatewayServer) AcquireSshConnection(
	ctx context.Context, req *connect.Request[routerv1.AcquireSshConnectionRequest],
) (*connect.Response[routerv1.AcquireSshConnectionResponse], error) {
	s := g.s
	p, _, err := s.grant(ctx, req.Msg.GetGrantId())
	if err != nil {
		return nil, s.status(err)
	}
	left, err := s.edgeLeft(ctx, p)
	if err != nil {
		return nil, s.status(err)
	}
	id := newLeaseID()
	until, err := s.lease(ctx, p, id)
	if err != nil {
		return nil, s.status(err)
	}
	s.sshEvent(ctx, p.ID, "start")
	return connect.NewResponse(&routerv1.AcquireSshConnectionResponse{
		LeaseId: id, LeaseExpiresAt: timestamppb.New(until), Budget: s.edgeBudget(p, left),
	}), nil
}

// RenewSshConnection extends a lease, taking it again after a lapse while there is room
func (g *SSHGatewayServer) RenewSshConnection(
	ctx context.Context, req *connect.Request[routerv1.RenewSshConnectionRequest],
) (*connect.Response[routerv1.RenewSshConnectionResponse], error) {
	s, m := g.s, req.Msg
	p, _, err := s.grant(ctx, m.GetGrantId())
	if err != nil {
		return nil, s.status(err)
	}
	if !leaseShape.MatchString(m.GetLeaseId()) {
		return nil, s.status(errLeaseMalformed)
	}
	until, err := s.lease(ctx, p, m.GetLeaseId())
	if err != nil {
		return nil, s.status(err)
	}
	return connect.NewResponse(&routerv1.RenewSshConnectionResponse{
		LeaseExpiresAt: timestamppb.New(until),
	}), nil
}

// ReleaseSshConnection gives a lease back, also once the placement has ended
func (g *SSHGatewayServer) ReleaseSshConnection(
	ctx context.Context, req *connect.Request[routerv1.ReleaseSshConnectionRequest],
) (*connect.Response[routerv1.ReleaseSshConnectionResponse], error) {
	s, m := g.s, req.Msg
	placement, _, err := s.d.Keys.ParseGrantID(m.GetGrantId())
	if err != nil {
		return nil, s.status(errGrantInvalid)
	}
	if !leaseShape.MatchString(m.GetLeaseId()) {
		return nil, s.status(errLeaseMalformed)
	}
	removed, err := s.d.Valkey.Do(ctx, s.d.Valkey.B().Zrem().Key(s.connsKey(placement)).
		Member(m.GetLeaseId()).Build()).AsInt64()
	if err != nil {
		return nil, s.status(err)
	}
	// A lease released twice, or expired already, writes one end at most
	if removed > 0 {
		s.sshEvent(ctx, placement, "end")
	}
	return connect.NewResponse(&routerv1.ReleaseSshConnectionResponse{}), nil
}

// OpenSshUpstream authorises the gateway's key in the sandbox once and answers how to reach it
func (g *SSHGatewayServer) OpenSshUpstream(
	ctx context.Context, req *connect.Request[routerv1.OpenSshUpstreamRequest],
) (*connect.Response[routerv1.OpenSshUpstreamResponse], error) {
	s := g.s
	p, _, err := s.grant(ctx, req.Msg.GetGrantId())
	if err != nil {
		return nil, s.status(err)
	}
	line, fp, err := sshkeys.GatewayLine(req.Msg.GetGatewayPublicKey())
	if err != nil {
		return nil, s.status(err)
	}
	prov, err := s.home(p)
	if err != nil {
		return nil, s.status(err)
	}
	native := s.d.Registry.NativeSSH(prov)
	if !native {
		if err := s.awaitPrepared(ctx, p); err != nil {
			return nil, s.sshStatus(p, err)
		}
		opened, err := s.d.Saga.OpenGate(ctx, p)
		if err != nil {
			return nil, s.gateStatus(p, err)
		}
		p = opened
	}
	up, err := s.d.Saga.AuthorizeGateway(ctx, p.ID, line, fp)
	if err != nil {
		return nil, s.sshStatus(p, err)
	}
	out := &routerv1.OpenSshUpstreamResponse{
		User: up.Host.SSHUser, HostKeys: []string{up.Host.HostKey},
		Deadline: timestamppb.New(*p.Deadline),
	}
	if native {
		out.Transport = &routerv1.OpenSshUpstreamResponse_Tcp{
			Tcp: &routerv1.TcpUpstream{Address: up.Address},
		}
		return connect.NewResponse(out), nil
	}
	// The user is the provider's exec user, whatever the sandbox's own ssh-up printed
	out.User = cmp.Or(prov.Capabilities().ExecUser, "root")
	route, key, err := s.d.Saga.Gate(p)
	if err != nil {
		return nil, s.gateStatus(p, err)
	}
	gu := gateUpstream(route, key)
	out.Transport = &routerv1.OpenSshUpstreamResponse_Websocket{
		Websocket: &routerv1.WebSocketUpstream{
			Url: wsURL(route.BaseURL) + sshPath, Headers: gu.GetHeaders(), Cookies: gu.GetCookies(),
		},
	}
	return connect.NewResponse(out), nil
}

// CheckSshGrants answers the deadline of each grant that still stands
func (g *SSHGatewayServer) CheckSshGrants(
	ctx context.Context, req *connect.Request[routerv1.CheckSshGrantsRequest],
) (*connect.Response[routerv1.CheckSshGrantsResponse], error) {
	s := g.s
	byGrant := map[string]store.SSHKeyRef{}
	var ids []string
	var refs []store.SSHKeyRef
	for _, id := range req.Msg.GetGrantIds() {
		placement, fp, err := s.d.Keys.ParseGrantID(id)
		if err != nil {
			continue
		}
		ref := store.SSHKeyRef{Placement: placement, Fingerprint: fp}
		byGrant[id] = ref
		refs = append(refs, ref)
		ids = append(ids, placement.String())
	}
	ps, err := s.live(ctx, ids)
	if err != nil {
		return nil, s.status(err)
	}
	keys, err := s.d.Store.LiveSSHKeys(ctx, refs)
	if err != nil {
		return nil, s.status(err)
	}
	deadlines := make(map[uuid.UUID]time.Time, len(ps))
	for _, p := range ps {
		if standing(p) {
			deadlines[p.ID] = *p.Deadline
		}
	}
	out := make(map[string]*timestamppb.Timestamp, len(byGrant))
	for id, ref := range byGrant {
		if dl, ok := deadlines[ref.Placement]; ok && keys[ref] {
			out[id] = timestamppb.New(dl)
		}
	}
	return connect.NewResponse(&routerv1.CheckSshGrantsResponse{Deadlines: out}), nil
}

// ReportSshUsage adds SSH bytes to the edge budget and answers the bytes left and the pace
func (g *SSHGatewayServer) ReportSshUsage(
	ctx context.Context, req *connect.Request[routerv1.ReportSshUsageRequest],
) (*connect.Response[routerv1.ReportSshUsageResponse], error) {
	left, pace, err := g.s.report(ctx, budget.Edge, budget.BySSH, req.Msg.GetUsage())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&routerv1.ReportSshUsageResponse{
		BytesRemaining: left, BytesPerSecond: pace,
	}), nil
}

// WatchSshEvents streams the edge events after the cursor, from now when it is empty
func (g *SSHGatewayServer) WatchSshEvents(
	ctx context.Context, req *connect.Request[routerv1.WatchSshEventsRequest],
	stream *connect.ServerStream[routerv1.WatchSshEventsResponse],
) error {
	send := func(c string, ev *routerv1.EdgeEvent) error {
		return stream.Send(&routerv1.WatchSshEventsResponse{Cursor: c, Event: ev})
	}
	return g.s.watch(ctx, req.Peer(), req.Msg.GetCursor(), send)
}

/* Helpers */

// grant returns the placement and fingerprint of a grant that still stands
func (s *Server) grant(ctx context.Context, id string) (store.Placement, string, error) {
	placement, fp, err := s.d.Keys.ParseGrantID(id)
	if err != nil {
		return store.Placement{}, "", errGrantInvalid
	}
	p, err := s.d.Store.PlacementByID(ctx, placement)
	if errors.Is(err, store.ErrNotFound) {
		return store.Placement{}, "", errGrantUnknown
	}
	if err == nil {
		err = s.keyStands(ctx, p, fp)
	}
	if err != nil {
		return store.Placement{}, "", err
	}
	return p, fp, nil
}

// keyStands checks that a placement is in its paid time with SSH and holds the key unremoved
func (s *Server) keyStands(ctx context.Context, p store.Placement, fingerprint string) error {
	if !standing(p) {
		return errGrantUnknown
	}
	prov, err := s.home(p)
	if err != nil || !s.d.Registry.Capabilities(prov).SSH {
		return errGrantUnknown
	}
	_, err = s.d.Store.LiveSSHKey(ctx, p.ID, fingerprint)
	if errors.Is(err, store.ErrNotFound) {
		return errGrantUnknown
	}
	return err
}

// connsKey is the sorted set of a placement's connection leases by expiry
func (s *Server) connsKey(placement uuid.UUID) string {
	return s.d.Prefix + "ssh:conns:" + placement.String()
}

// lease takes or renews lease id on the placement, answering when it lapses
func (s *Server) lease(ctx context.Context, p store.Placement, id string) (time.Time, error) {
	life := cmp.Or(s.d.SSH.ConnectionLease, defaultLease)
	most := cmp.Or(s.d.SSH.MaxConnectionsPerPlacement, defaultMaxConns)
	args := []string{
		strconv.FormatInt(life.Milliseconds(), 10), strconv.Itoa(most), id,
		ms(p.Deadline.Add(connsLife)),
	}
	n, err := leaseScript.Exec(ctx, s.d.Valkey, []string{s.connsKey(p.ID)}, args).AsInt64()
	switch {
	case err != nil:
		return time.Time{}, err
	case n == 0:
		return time.Time{}, errTooManyConns
	}
	return time.UnixMilli(n), nil
}

// sshEvent writes the start or end of an SSH connection to the placement's session record, the
// gateway reporting no byte counts to the router
func (s *Server) sshEvent(ctx context.Context, id uuid.UUID, phase string) {
	if s.d.SessionWriter != nil {
		s.d.SessionWriter.SSH(ctx, id, phase, 0, 0)
	}
}

// awaitPrepared waits up to ssh.prepare_timeout for the placement's sshd, asking for it again
func (s *Server) awaitPrepared(ctx context.Context, p store.Placement) error {
	until := time.Now().Add(cmp.Or(s.d.SSH.PrepareTimeout, defaultPrepare))
	asked := false
	for {
		_, err := s.d.Store.SSHHost(ctx, p.ID)
		switch {
		case err == nil:
			return nil
		case !errors.Is(err, store.ErrNotFound):
			return err
		case !asked:
			asked = true
			if err := s.d.Saga.EnqueuePrepareSSH(ctx, p.ID); err != nil {
				s.d.Log.Warn("api: enqueue prepare_ssh", "placement", p.ID, "err", err)
			}
		}
		if time.Until(until) <= 0 {
			return saga.ErrSSHPreparing
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(min(prepareGap, time.Until(until))):
		}
	}
}

// sshStatus logs why a sandbox's SSH is not ready and answers without quoting it
func (s *Server) sshStatus(p store.Placement, err error) error {
	if errors.Is(err, saga.ErrSSHPreparing) {
		s.d.Log.Warn("api: ssh preparing", "placement", p.ID, "err", err)
	}
	return s.gateStatus(p, err)
}

// wsURL turns the gate's origin into its WebSocket origin
func wsURL(base string) string {
	if rest, ok := strings.CutPrefix(base, "https://"); ok {
		return "wss://" + rest
	}
	if rest, ok := strings.CutPrefix(base, "http://"); ok {
		return "ws://" + rest
	}
	return base
}

// newLeaseID returns 16 random bytes in hex
func newLeaseID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// ms is t in Unix milliseconds
func ms(t time.Time) string {
	return strconv.FormatInt(t.UnixMilli(), 10)
}
