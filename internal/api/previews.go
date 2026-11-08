package api

import (
	"cmp"
	"context"
	"errors"
	"maps"
	"slices"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/valkey-io/valkey-go"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/oynozan/bugnet-router/internal/gateclient"
	"github.com/oynozan/bugnet-router/internal/preview"
	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/saga"
	"github.com/oynozan/bugnet-router/internal/store"
)

const (
	// healthWait bounds the first gate health check of CreatePreview
	healthWait = 2 * time.Second
	// recheckWait is how long CreatePreview checks the gate again after a repair
	recheckWait = 5 * time.Second
	// recheckGap spaces those checks
	recheckGap = 250 * time.Millisecond
	// portWait bounds the listening check of CreatePreview
	portWait = 2 * time.Second
	// repairEvery is how often one placement's gate may be repaired
	repairEvery = time.Minute
	// defaultMaxPreviews is previews.max_per_placement's default
	defaultMaxPreviews = 8
	// maxActor and maxNote bound what an operator call names, as the router's log keeps it
	maxActor = 100
	maxNote  = 1000
)

var (
	errPortRefused    = errors.New("api: previews refuse that port")
	errPreviewUnknown = errors.New("api: no such preview")
	errTokenInvalid   = errors.New("api: the preview token is not valid")
	errNoPreviews     = errors.New("api: this router serves no previews")
	errNoFeature      = errors.New("api: the placement's provider serves no previews")
)

/* Gateway */

// CreatePreview answers the link to one port of a ready placement, the same link every time
func (s *Server) CreatePreview(
	ctx context.Context, req *connect.Request[routerv1.CreatePreviewRequest],
) (*connect.Response[routerv1.CreatePreviewResponse], error) {
	port := req.Msg.GetPort()
	if port < 1 || port > 65535 {
		return nil, s.status(errPortRefused)
	}
	p, err := s.placement(ctx, req.Msg.GetHandle())
	if err != nil {
		return nil, s.status(err)
	}
	prov, err := s.home(p)
	if err != nil {
		return nil, s.status(err)
	}
	switch {
	case !s.d.Registry.Capabilities(prov).Preview:
		return nil, s.status(errNoFeature)
	case s.d.PreviewDomain == "" || len(s.d.Keys.Roots()) == 0:
		return nil, s.status(errNoPreviews)
	}
	if err := s.spend(ctx, p); err != nil {
		return nil, s.status(err)
	}
	if s.d.Saga.PortRefused(p, int(port)) {
		return nil, s.status(errPortRefused)
	}
	if p, err = s.gateUp(ctx, p); err != nil {
		return nil, s.gateStatus(p, err)
	}
	listening := s.listening(ctx, p, int(port))
	most := cmp.Or(s.d.Previews.MaxPerPlacement, defaultMaxPreviews)
	pv, err := s.d.Store.UpsertPreview(ctx, p.ID, int(port), most, preview.NewID())
	if err != nil {
		return nil, s.status(err)
	}
	tok, err := s.d.Keys.PreviewToken(cmp.Or(p.KeyID, s.d.Keys.Current().ID), pv.ID,
		pv.Generation)
	if err != nil {
		return nil, s.status(err)
	}
	return connect.NewResponse(&routerv1.CreatePreviewResponse{
		Url: preview.URL(pv.ID, s.d.PreviewDomain, tok), Token: tok,
		ExpiresAt: timestamppb.New(*p.Deadline), Listening: listening, PreviewId: pv.ID,
	}), nil
}

// gateUp opens a gate that never answered, checks it, and repairs it once when it fails
func (s *Server) gateUp(ctx context.Context, p store.Placement) (store.Placement, error) {
	p, err := s.openGate(ctx, p)
	if err != nil {
		return p, err
	}
	if err := s.health(ctx, p, healthWait); err == nil {
		return p, nil
	}
	if _, fixed, err := s.repairGate(ctx, p); err != nil {
		s.d.Log.Warn("api: gate repair", "placement", p.ID, "err", err)
	} else {
		p = fixed
	}
	until := time.Now().Add(recheckWait)
	for {
		err := s.health(ctx, p, min(healthWait, time.Until(until)))
		if err == nil {
			return p, nil
		}
		if time.Until(until) < recheckGap {
			return p, err
		}
		select {
		case <-ctx.Done():
			return p, ctx.Err()
		case <-time.After(recheckGap):
		}
	}
}

// openGate runs the gate steps when the gate never answered, keeping p when they fail
func (s *Server) openGate(ctx context.Context, p store.Placement) (store.Placement, error) {
	opened, err := s.d.Saga.OpenGate(ctx, p)
	if err != nil {
		return p, err
	}
	return opened, nil
}

// health asks the placement's gate for its health, waiting at most wait
func (s *Server) health(ctx context.Context, p store.Placement, wait time.Duration) error {
	route, key, err := s.d.Saga.Gate(p)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	if err := gateclient.Health(ctx, route, key); err != nil {
		return errors.Join(saga.ErrGateUnavailable, err)
	}
	return nil
}

// listening asks the gate whether something accepts on port, false when it cannot tell
func (s *Server) listening(ctx context.Context, p store.Placement, port int) bool {
	route, key, err := s.d.Saga.Gate(p)
	if err != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, portWait)
	defer cancel()
	ok, err := gateclient.PortListening(ctx, route, key, port)
	if err != nil {
		s.d.Log.Warn("api: port check", "placement", p.ID, "err", err)
	}
	return ok
}

// gateStatus logs why a gate is unavailable and answers without quoting it
func (s *Server) gateStatus(p store.Placement, err error) error {
	if errors.Is(err, saga.ErrGateUnavailable) {
		s.d.Log.Warn("api: gate unavailable", "placement", p.ID, "err", err)
	}
	return s.status(err)
}

// repairGate runs one repair of the placement's gate unless one ran in the last minute
func (s *Server) repairGate(
	ctx context.Context, p store.Placement,
) (bool, store.Placement, error) {
	key := s.d.Prefix + "gate:repair:" + p.ID.String()
	err := s.d.Valkey.Do(ctx, s.d.Valkey.B().Set().Key(key).Value("1").Nx().
		Ex(repairEvery).Build()).Error()
	switch {
	case valkey.IsValkeyNil(err):
		return false, p, nil
	case err != nil:
		return false, p, err
	}
	// A caller going away mid-repair must not leave the gate half set up for the minute
	rctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	defer context.AfterFunc(s.life, cancel)()
	fixed, err := s.d.Saga.RepairGate(rctx, p)
	if err != nil {
		return true, p, err
	}
	return true, fixed, nil
}

/* Edge */

// ResolvePreview answers a preview's route to its gate, budget and interstitial fields
func (e *EdgeServer) ResolvePreview(
	ctx context.Context, req *connect.Request[routerv1.ResolvePreviewRequest],
) (*connect.Response[routerv1.ResolvePreviewResponse], error) {
	s := e.s
	pv, p, err := s.previewFor(ctx, req.Msg.GetPreviewId(), req.Msg.GetToken())
	if err != nil {
		return nil, s.status(err)
	}
	if _, err := s.home(p); err != nil {
		return nil, s.status(err)
	}
	if !time.Now().Before(*p.Deadline) {
		return nil, s.status(errNotReady)
	}
	left, err := s.edgeLeft(ctx, p)
	if err != nil {
		return nil, s.status(err)
	}
	route, key, err := s.d.Saga.Gate(p)
	if err != nil {
		return nil, s.gateStatus(p, err)
	}
	out := &routerv1.ResolvePreviewResponse{
		PlacementId: p.ID.String(), Port: uint32(pv.Port), Upstream: gateUpstream(route, key),
		ExpiresAt: timestamppb.New(*p.Deadline), Generation: pv.Generation,
		Budget: s.edgeBudget(p, left), Interstitial: s.d.Previews.Interstitial,
		SourceLabel: p.SourceLabel,
	}
	if p.ReadyAt != nil {
		out.StartedAt = timestamppb.New(*p.ReadyAt)
	}
	return connect.NewResponse(out), nil
}

// RepairPreview repairs the gate behind a preview, at most once a minute per placement
func (e *EdgeServer) RepairPreview(
	ctx context.Context, req *connect.Request[routerv1.RepairPreviewRequest],
) (*connect.Response[routerv1.RepairPreviewResponse], error) {
	s := e.s
	_, p, err := s.previewFor(ctx, req.Msg.GetPreviewId(), req.Msg.GetToken())
	if err != nil {
		return nil, s.status(err)
	}
	if _, err := s.home(p); err != nil {
		return nil, s.status(err)
	}
	ran, _, err := s.repairGate(ctx, p)
	if err != nil {
		return nil, s.gateStatus(p, errors.Join(saga.ErrGateUnavailable, err))
	}
	return connect.NewResponse(&routerv1.RepairPreviewResponse{Repaired: ran}), nil
}

// previewFor finds a preview and its placement once the token is the preview's
func (s *Server) previewFor(
	ctx context.Context, id, token string,
) (store.Preview, store.Placement, error) {
	if !preview.ValidID(id) {
		return store.Preview{}, store.Placement{}, errPreviewUnknown
	}
	pv, err := s.d.Store.PreviewByID(ctx, id)
	switch {
	case errors.Is(err, store.ErrNotFound):
		return store.Preview{}, store.Placement{}, errPreviewUnknown
	case err != nil:
		return store.Preview{}, store.Placement{}, err
	}
	switch preview.Check(s.d.Keys, pv.ID, pv.Generation, token) {
	case preview.Invalid:
		return store.Preview{}, store.Placement{}, errTokenInvalid
	case preview.Earlier:
		if pv.RevokedAt == nil {
			return store.Preview{}, store.Placement{}, errTokenInvalid
		}
	}
	if pv.RevokedAt != nil {
		return store.Preview{}, store.Placement{}, store.ErrPreviewRevoked
	}
	p, err := s.d.Store.PlacementByID(ctx, pv.PlacementID)
	if err != nil {
		return store.Preview{}, store.Placement{}, err
	}
	return pv, p, nil
}

// gateUpstream is the route the edge proxies through, with the gate key added
func gateUpstream(r provider.GateRoute, gateKey string) *routerv1.GateUpstream {
	out := &routerv1.GateUpstream{
		BaseUrl: r.BaseURL, BlockedPathPrefixes: slices.Clone(r.BlockedPrefixes),
	}
	for _, name := range slices.Sorted(maps.Keys(r.Header)) {
		if name == gateclient.HeaderKey {
			continue
		}
		out.Headers = append(out.Headers,
			&routerv1.HeaderValue{Name: name, Value: r.Header[name]})
	}
	out.Headers = append(out.Headers,
		&routerv1.HeaderValue{Name: gateclient.HeaderKey, Value: gateKey})
	for _, name := range slices.Sorted(maps.Keys(r.Cookies)) {
		out.Cookies = append(out.Cookies,
			&routerv1.HeaderValue{Name: name, Value: r.Cookies[name]})
	}
	return out
}

// revokedPreviews lists the held previews whose route no longer stands
func (s *Server) revokedPreviews(
	ctx context.Context, held []*routerv1.HeldPreview,
) ([]string, error) {
	ids := make([]string, 0, len(held))
	for _, h := range held {
		if preview.ValidID(h.GetPreviewId()) {
			ids = append(ids, h.GetPreviewId())
		}
	}
	rows, err := s.d.Store.PreviewsByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	byID := make(map[string]store.Preview, len(rows))
	for _, r := range rows {
		byID[r.ID] = r
	}
	var out []string
	for _, h := range held {
		r, ok := byID[h.GetPreviewId()]
		stale := !ok || r.RevokedAt != nil || r.Generation != h.GetGeneration()
		if stale && !slices.Contains(out, h.GetPreviewId()) {
			out = append(out, h.GetPreviewId())
		}
	}
	return out, nil
}

/* Operators */

// RevokePreview kills every link of one preview and keeps its port from getting another
func (o *OperatorServer) RevokePreview(
	ctx context.Context, req *connect.Request[routerv1.RevokePreviewRequest],
) (*connect.Response[routerv1.RevokePreviewResponse], error) {
	s, m := o.s, req.Msg
	switch {
	case m.GetActor() == "" || len(m.GetActor()) > maxActor:
		return nil, invalid("actor must be 1 to %d bytes", maxActor)
	case len(m.GetReason()) > maxNote:
		return nil, invalid("reason must be at most %d bytes", maxNote)
	case !preview.ValidID(m.GetPreviewId()):
		return nil, s.status(errPreviewUnknown)
	}
	pv, err := s.d.Store.RevokePreview(ctx, m.GetPreviewId())
	if errors.Is(err, store.ErrNotFound) {
		err = errPreviewUnknown
	}
	if err != nil {
		return nil, s.status(err)
	}
	s.events.Publish(ctx, &routerv1.EdgeEvent{Event: &routerv1.EdgeEvent_PreviewRevoked{
		PreviewRevoked: &routerv1.PreviewRevoked{
			PlacementId: pv.PlacementID.String(), PreviewId: pv.ID,
		},
	}})
	s.d.Log.Info("api: preview revoked", "actor", m.GetActor(), "preview", pv.ID,
		"placement", pv.PlacementID, "reason", m.GetReason())
	return connect.NewResponse(&routerv1.RevokePreviewResponse{}), nil
}
