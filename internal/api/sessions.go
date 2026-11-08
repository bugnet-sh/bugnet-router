package api

import (
	"cmp"
	"context"
	"crypto/hmac"
	"errors"
	"regexp"
	"slices"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bugnet-sh/bugnet-router/internal/sessions"
	"github.com/bugnet-sh/bugnet-router/internal/solana"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

const (
	// maxMCPEvents is the most mcp events one AppendSessionEvents call carries
	maxMCPEvents = 50
	// appendWindow is how long after the end mcp events still land, so a stop's own call lands
	appendWindow = 10 * time.Minute
	// eventSkew is how far an event's own time may sit outside its record's life
	eventSkew = time.Minute
)

// Session record states the page shows
const (
	sessionRunning = "running"
	sessionEnded   = "ended"
	sessionExpired = "expired"
)

var (
	errSessionUnknown = errors.New("api: no session record has that id and key")
	errOrderUnknown   = errors.New("api: no placement has that start order")
	errNoLive         = errors.New("api: this router serves no live session output")

	sessionIDShape  = regexp.MustCompile(`^[a-z2-7]{16}$`)
	sessionKeyShape = regexp.MustCompile(`^bnk_[A-Za-z0-9_-]{43}$`)
	outcomeShape    = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)
	networkShape    = regexp.MustCompile(`^[-a-z0-9]{3,8}:[-_a-zA-Z0-9]{1,32}$`)
)

// SessionPageServer implements routerv1connect.SessionPageServiceHandler for the sessions role
type SessionPageServer struct {
	// Answers Unimplemented for every RPC of the contract without a method here
	routerv1connect.UnimplementedSessionPageServiceHandler
	s    *Server
	live *liveView
}

var _ routerv1connect.SessionPageServiceHandler = (*SessionPageServer)(nil)

/* Links */

// sessionKeyID is the root a placement's session key derives from
func (s *Server) sessionKeyID(p store.Placement) string {
	if p.KeyID != "" || len(s.d.Keys.Roots()) == 0 {
		return p.KeyID
	}
	return s.d.Keys.Current().ID
}

// sessionLink is the record's page link, empty once the page ended or with no page origin
func (s *Server) sessionLink(p store.Placement, rec store.SessionRecord) string {
	if s.d.Sessions.BaseURL == "" || !rec.Open(time.Now()) || len(s.d.Keys.Roots()) == 0 {
		return ""
	}
	key, err := s.d.Keys.SessionKey(s.sessionKeyID(p), rec.ID, rec.Generation)
	if err != nil {
		s.d.Log.Warn("api: derive a session key", "placement", p.ID, "err", err)
		return ""
	}
	return s.d.Sessions.BaseURL + "/s/" + rec.ID + "#k=" + key
}

// answer is placementMsg with the session link, which a failed read leaves empty
func (s *Server) answer(ctx context.Context, p store.Placement) *routerv1.Placement {
	msg := placementMsg(p)
	if s.d.Sessions.BaseURL == "" {
		return msg
	}
	rec, err := s.d.Store.SessionRecordByPlacement(ctx, p.ID)
	switch {
	case errors.Is(err, store.ErrNotFound):
	case err != nil:
		s.d.Log.Warn("api: read a session record for its link", "placement", p.ID, "err", err)
	default:
		msg.SessionUrl = s.sessionLink(p, rec)
	}
	return msg
}

/* The gateway */

// AppendSessionEvents stores the gateway's tool calls, leaving out each one of a bad shape
func (s *Server) AppendSessionEvents(
	ctx context.Context, req *connect.Request[routerv1.AppendSessionEventsRequest],
) (*connect.Response[routerv1.AppendSessionEventsResponse], error) {
	m := req.Msg
	if len(m.GetEvents()) > maxMCPEvents {
		return nil, invalid("at most %d events a call", maxMCPEvents)
	}
	p, err := s.placement(ctx, m.GetHandle())
	if err != nil {
		return nil, s.status(err)
	}
	out := &routerv1.AppendSessionEventsResponse{}
	rec, err := s.d.Store.SessionRecordByPlacement(ctx, p.ID)
	if errors.Is(err, store.ErrNotFound) {
		return connect.NewResponse(out), nil
	}
	if err != nil {
		return nil, s.status(err)
	}
	now := time.Now()
	if end := cmp.Or(rec.EndedAt, p.TerminatedAt); end != nil && now.After(end.Add(appendWindow)) {
		return connect.NewResponse(out), nil
	}
	var events []sessions.Fields
	for i, ev := range m.GetEvents() {
		if !slices.Contains(s.d.Sessions.MCPTools, ev.GetTool()) ||
			!outcomeShape.MatchString(ev.GetOutcome()) {
			out.Rejected = append(out.Rejected, uint32(i))
			continue
		}
		at := now
		if t := ev.GetAt(); t.IsValid() && t.AsTime().After(rec.CreatedAt.Add(-eventSkew)) &&
			t.AsTime().Before(now.Add(eventSkew)) {
			at = t.AsTime()
		}
		events = append(events, sessions.Fields{
			"at": at.UTC().Format(time.RFC3339Nano), "tool": ev.GetTool(),
			"outcome": ev.GetOutcome(), "duration_ms": ev.GetDurationMs(),
		})
	}
	// One transaction, so the gateway's retry of a failed call stores no event twice
	if err := s.d.SessionWriter.StoreAll(ctx, p.ID, sessions.KindMCP, events); err != nil {
		return nil, s.status(err)
	}
	return connect.NewResponse(out), nil
}

// RotateSessionLink ends the link named when it is the current one, and answers the current link
func (s *Server) RotateSessionLink(
	ctx context.Context, req *connect.Request[routerv1.RotateSessionLinkRequest],
) (*connect.Response[routerv1.RotateSessionLinkResponse], error) {
	p, err := s.placement(ctx, req.Msg.GetHandle())
	if err != nil {
		return nil, s.status(err)
	}
	rec, err := s.d.Store.SessionRecordByPlacement(ctx, p.ID)
	if errors.Is(err, store.ErrNotFound) {
		return connect.NewResponse(&routerv1.RotateSessionLinkResponse{}), nil
	}
	if err != nil {
		return nil, s.status(err)
	}
	link := s.sessionLink(p, rec)
	if link != "" && hmac.Equal([]byte(link), []byte(req.Msg.GetSessionUrl())) {
		if rec.Generation, err = s.d.Store.RotateSessionGeneration(ctx, rec.ID,
			rec.Generation); err != nil {
			return nil, s.status(err)
		}
		link = s.sessionLink(p, rec)
	}
	return connect.NewResponse(&routerv1.RotateSessionLinkResponse{SessionUrl: link}), nil
}

/* The page */

// sessionFor finds the record an id and key name, any miss answering errSessionUnknown alike
func (s *Server) sessionFor(
	ctx context.Context, id, key string,
) (store.SessionRecord, store.Placement, error) {
	if !sessionIDShape.MatchString(id) || !sessionKeyShape.MatchString(key) {
		return store.SessionRecord{}, store.Placement{}, errSessionUnknown
	}
	rec, err := s.d.Store.SessionRecordByID(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		// The same read and HMAC as a wrong key, so the time taken never tells a live id
		_, _ = s.d.Store.PlacementByID(ctx, uuid.Nil)
		s.d.Keys.CheckSessionKey(s.sessionKeyID(store.Placement{}), id, 0, key)
		return store.SessionRecord{}, store.Placement{}, errSessionUnknown
	}
	if err != nil {
		return store.SessionRecord{}, store.Placement{}, err
	}
	p, err := s.d.Store.PlacementByID(ctx, rec.PlacementID)
	if err != nil {
		return store.SessionRecord{}, store.Placement{}, err
	}
	if !s.d.Keys.CheckSessionKey(s.sessionKeyID(p), rec.ID, rec.Generation, key) {
		return store.SessionRecord{}, store.Placement{}, errSessionUnknown
	}
	return rec, p, nil
}

// GetSessionRecord answers the record's state, with the record once ended or the live snapshot
func (ss *SessionPageServer) GetSessionRecord(
	ctx context.Context, req *connect.Request[routerv1.GetSessionRecordRequest],
) (*connect.Response[routerv1.GetSessionRecordResponse], error) {
	s := ss.s
	rec, p, err := s.sessionFor(ctx, req.Msg.GetId(), req.Msg.GetKey())
	if err != nil {
		return nil, s.status(err)
	}
	out := &routerv1.GetSessionRecordResponse{State: sessionExpired}
	switch {
	case !rec.Open(time.Now()):
	case !ended(p.State):
		out.State = sessionRunning
		now := time.Now()
		if out.LiveJson, err = sessions.Snapshot(ctx, s.d.Store, ss.live.bus, p,
			s.snapshotDeadline(p, now), now); err != nil {
			return nil, s.status(err)
		}
	default:
		out.State = sessionEnded
		if out.RecordJson, err = sessions.BuildRecord(ctx, s.d.Store, p.ID); err != nil {
			return nil, s.status(err)
		}
	}
	return connect.NewResponse(out), nil
}

// snapshotDeadline is the deadline, or while placing the earliest one the paid minutes allow
func (s *Server) snapshotDeadline(p store.Placement, now time.Time) time.Time {
	if p.Deadline != nil {
		return *p.Deadline
	}
	pkg, _ := s.d.Catalog.Get(p.PackageID)
	return now.Add(time.Duration(pkg.Minutes) * time.Minute)
}

// DeleteSessionRecord closes the page now and deletes the record's rows unless a hold keeps them
func (ss *SessionPageServer) DeleteSessionRecord(
	ctx context.Context, req *connect.Request[routerv1.DeleteSessionRecordRequest],
) (*connect.Response[routerv1.DeleteSessionRecordResponse], error) {
	s := ss.s
	rec, _, err := s.sessionFor(ctx, req.Msg.GetId(), req.Msg.GetKey())
	if err == nil && rec.ClosedAt == nil {
		err = s.d.Store.DeleteSessionRecordNow(ctx, rec.ID, time.Now())
	}
	if err != nil {
		return nil, s.status(err)
	}
	return connect.NewResponse(&routerv1.DeleteSessionRecordResponse{}), nil
}

/* Payments */

// AppendAllowanceEvent stores Payments' allowance event on the start order's record, once a phase
func (ps *PaymentsServer) AppendAllowanceEvent(
	ctx context.Context, req *connect.Request[routerv1.AppendAllowanceEventRequest],
) (*connect.Response[routerv1.AppendAllowanceEventResponse], error) {
	s, m := ps.s, req.Msg
	ev := m.GetEvent()
	kept := ev.GetPhase() == "kept"
	switch {
	case m.GetOrderId() == "":
		return nil, invalid("order_id is required")
	case ev.GetPhase() != "paid" && ev.GetPhase() != "refunded" && !kept:
		return nil, invalid("phase must be paid, refunded or kept")
	case ev.GetAmountMicros() < 0:
		return nil, invalid("amount_micros must be 0 or more")
	case kept && (ev.GetAmountMicros() != 0 || ev.GetKeptMicros() <= 0 || ev.GetTx() != ""):
		return nil, invalid("kept needs amount_micros 0, kept_micros above 0 and no tx")
	case !kept && !validSignature(ev.GetTx()):
		return nil, invalid("tx must be the base58 of a 64-byte signature")
	case !validMint(ev.GetAsset()):
		return nil, invalid("asset must be a base58 mint")
	case !networkShape.MatchString(ev.GetNetwork()):
		return nil, invalid("network must be CAIP-2")
	}
	p, err := s.startPlacement(ctx, m.GetOrderId())
	if err != nil {
		return nil, s.status(err)
	}
	rec, err := s.d.Store.SessionRecordByPlacement(ctx, p.ID)
	switch {
	case errors.Is(err, store.ErrNotFound):
		return connect.NewResponse(&routerv1.AppendAllowanceEventResponse{}), nil
	case err != nil:
		return nil, s.status(err)
	case rec.ExpiresAt != nil && !time.Now().Before(*rec.ExpiresAt):
		return connect.NewResponse(&routerv1.AppendAllowanceEventResponse{}), nil
	}
	f := sessions.Fields{
		"phase": ev.GetPhase(), "amount_micros": ev.GetAmountMicros(), "asset": ev.GetAsset(),
		"network": ev.GetNetwork(), "tx": ev.GetTx(),
	}
	if kept {
		delete(f, "tx")
		f["kept_micros"] = ev.GetKeptMicros()
	}
	if err := s.d.SessionWriter.Allowance(ctx, p.ID, f); err != nil {
		return nil, s.status(err)
	}
	return connect.NewResponse(&routerv1.AppendAllowanceEventResponse{}), nil
}

// startPlacement is the placement a start order made, errOrderUnknown for any other id
func (s *Server) startPlacement(ctx context.Context, orderID string) (store.Placement, error) {
	o, err := s.d.Store.OrderByID(ctx, orderID)
	if errors.Is(err, store.ErrNotFound) || (err == nil && o.Kind != store.OrderStart) {
		return store.Placement{}, errOrderUnknown
	}
	if err != nil {
		return store.Placement{}, err
	}
	return s.d.Store.PlacementByID(ctx, o.PlacementID)
}

func validSignature(tx string) bool {
	_, ok := solana.Signature(tx)
	return ok
}

func validMint(asset string) bool {
	_, ok := solana.PublicKey(asset)
	return ok
}

/* Operators */

// GetSessionHold answers a held record's page JSON, the record no public link serves
func (o *OperatorServer) GetSessionHold(
	ctx context.Context, req *connect.Request[routerv1.GetSessionHoldRequest],
) (*connect.Response[routerv1.GetSessionHoldResponse], error) {
	s := o.s
	id, err := uuid.Parse(req.Msg.GetPlacementId())
	if err != nil {
		return nil, invalid("placement_id must be a placement's id")
	}
	rec, err := s.d.Store.SessionRecordByPlacement(ctx, id)
	if errors.Is(err, store.ErrNotFound) || (err == nil && !rec.Held(time.Now())) {
		err = errSessionUnknown
	}
	if err != nil {
		return nil, s.status(err)
	}
	raw, err := sessions.BuildRecord(ctx, s.d.Store, id)
	if err != nil {
		return nil, s.status(err)
	}
	return connect.NewResponse(&routerv1.GetSessionHoldResponse{
		RecordJson: raw, ClosedBy: rec.ClosedBy, HoldUntil: timestamppb.New(*rec.HoldUntil),
	}), nil
}
