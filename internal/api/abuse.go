package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/url"
	"path"
	"slices"
	"strings"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bugnet-sh/bugnet-router/internal/preview"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/transfer"
)

const (
	reasonTargetUnknown = "TARGET_UNKNOWN"
	// operatorSource is the signal source and the actor prefix of an operator call
	operatorSource = "operator"
	// transferTokenPrefix starts every transfer token
	transferTokenPrefix = "bnx_"
	signalsDefault      = 100
	signalsMax          = 1000
)

var errTargetUnknown = errors.New("api: no placement matches that target")

// signalClasses names the class column of each SignalClass
var signalClasses = map[routerv1.SignalClass]string{
	routerv1.SignalClass_SIGNAL_CLASS_ABUSE:           saga.ReasonAbuse,
	routerv1.SignalClass_SIGNAL_CLASS_CONTROL_LOST:    saga.ReasonControlLost,
	routerv1.SignalClass_SIGNAL_CLASS_PROVIDER_FAILED: saga.ReasonProviderFailed,
	routerv1.SignalClass_SIGNAL_CLASS_ACCOUNT:         store.SignalClassAccount,
	routerv1.SignalClass_SIGNAL_CLASS_SANDBOX_EXITED:  saga.ReasonSandboxExited,
}

// FindPlacement finds the placement an abuse report names
func (o *OperatorServer) FindPlacement(
	ctx context.Context, req *connect.Request[routerv1.FindPlacementRequest],
) (*connect.Response[routerv1.FindPlacementResponse], error) {
	s := o.s
	p, err := s.findTarget(ctx, req.Msg.GetTarget())
	if err != nil {
		return nil, s.status(err)
	}
	extend, err := s.extendPayers(ctx, p)
	if err != nil {
		return nil, s.status(err)
	}
	s.d.Log.Info("operator found a placement", "actor", readActor, "placement", p.ID)
	return connect.NewResponse(&routerv1.FindPlacementResponse{
		Placement: operatorPlacementOf(p, extend),
	}), nil
}

// EndPlacement ends a placement for abuse or by operator choice
func (o *OperatorServer) EndPlacement(
	ctx context.Context, req *connect.Request[routerv1.EndPlacementRequest],
) (*connect.Response[routerv1.EndPlacementResponse], error) {
	m, s := req.Msg, o.s
	switch {
	case !validActor(m.GetActor()):
		return nil, invalid("actor must be 1 to %d bytes", maxActor)
	case len(m.GetNote()) > maxNote:
		return nil, invalid("note must be at most %d bytes", maxNote)
	}
	reason := saga.ReasonAbuse
	switch m.GetReason() {
	case routerv1.OperatorEndReason_OPERATOR_END_REASON_ABUSE:
	case routerv1.OperatorEndReason_OPERATOR_END_REASON_OPERATOR:
		reason = saga.ReasonOperator
	default:
		return nil, invalid("reason must be ABUSE or OPERATOR")
	}
	p, err := s.findTarget(ctx, m.GetTarget())
	if err != nil {
		return nil, s.status(err)
	}
	evidence, err := json.Marshal(map[string]string{
		"source": operatorSource, "note": m.GetNote(),
	})
	if err != nil {
		return nil, s.status(err)
	}
	e := saga.End{
		Reason: reason, Refundable: m.GetRefundable() && reason == saga.ReasonOperator,
		Evidence: evidence, Actor: operatorSource + ":" + m.GetActor(),
	}
	var blocked []string
	if reason == saga.ReasonAbuse && (p.State == store.PlacementTerminated ||
		p.State == store.PlacementFailed) {
		// An ended placement keeps its reason and its money, and only its payers are acted on
		_, blocked, err = s.d.Saga.FlagEnded(ctx, p.ID, e)
	} else {
		_, blocked, err = s.d.Saga.EndBlocked(ctx, p.ID, e)
	}
	if err != nil {
		return nil, s.status(err)
	}
	s.d.Log.Info("operator ended a placement", "actor", m.GetActor(), "placement", p.ID,
		"reason", reason, "blocked", len(blocked))
	cur, err := s.d.Store.PlacementByID(ctx, p.ID)
	if err != nil {
		return nil, s.status(err)
	}
	extend, err := s.extendPayers(ctx, cur)
	if err != nil {
		return nil, s.status(err)
	}
	return connect.NewResponse(&routerv1.EndPlacementResponse{
		Placement: operatorPlacementOf(cur, extend), BlockedPayers: blocked,
	}), nil
}

// findTarget resolves an abuse report's target to its placement, errTargetUnknown for none
func (s *Server) findTarget(
	ctx context.Context, t *routerv1.PlacementTarget,
) (store.Placement, error) {
	var p store.Placement
	var err error
	switch {
	case t.GetPlacementId() != "":
		id, perr := uuid.Parse(t.GetPlacementId())
		if perr != nil {
			return store.Placement{}, invalid("placement_id must be a UUID")
		}
		p, err = s.d.Store.PlacementByID(ctx, id)
	case t.GetPreviewId() != "":
		if !preview.ValidID(t.GetPreviewId()) {
			return store.Placement{}, errTargetUnknown
		}
		p, err = s.d.Store.PlacementByPreview(ctx, t.GetPreviewId())
	case t.GetTransfer() != "":
		p, err = s.findTransfer(ctx, t.GetTransfer())
	case t.GetProviderSandbox() != nil:
		ps := t.GetProviderSandbox()
		if ps.GetProvider() == "" || ps.GetSandboxId() == "" {
			return store.Placement{}, invalid("provider_sandbox needs a provider and a sandbox_id")
		}
		p, err = s.d.Store.PlacementBySandbox(ctx, ps.GetProvider(), ps.GetSandboxId())
	default:
		return store.Placement{}, invalid("target is required")
	}
	if errors.Is(err, store.ErrNotFound) {
		return store.Placement{}, errTargetUnknown
	}
	return p, err
}

// findTransfer finds the placement of a transfer URL or token, through transfer_index and then
// through the Valkey record while it lives
func (s *Server) findTransfer(ctx context.Context, raw string) (store.Placement, error) {
	tok := strings.TrimSpace(raw)
	if strings.Contains(tok, "://") {
		u, err := url.Parse(tok)
		if err != nil {
			return store.Placement{}, errTargetUnknown
		}
		tok = path.Base(strings.TrimRight(u.Path, "/"))
	}
	if !strings.HasPrefix(tok, transferTokenPrefix) {
		return store.Placement{}, errTargetUnknown
	}
	sum := sha256.Sum256([]byte(tok))
	p, err := s.d.Store.PlacementByTransfer(ctx, sum[:])
	if !errors.Is(err, store.ErrNotFound) {
		return p, err
	}
	rec, err := s.xfers.Get(ctx, transfer.ID(sum[:]))
	if errors.Is(err, transfer.ErrUnknown) {
		return store.Placement{}, errTargetUnknown
	}
	if err != nil {
		return store.Placement{}, err
	}
	return s.d.Store.PlacementByID(ctx, rec.Placement)
}

// ListSignals lists provider signals, newest first
func (o *OperatorServer) ListSignals(
	ctx context.Context, req *connect.Request[routerv1.ListSignalsRequest],
) (*connect.Response[routerv1.ListSignalsResponse], error) {
	m, s := req.Msg, o.s
	f := store.SignalFilter{Provider: m.GetProvider(), HeldOnly: m.GetHeldOnly()}
	if m.GetSince() != nil {
		f.Since = m.GetSince().AsTime()
	}
	if c := m.GetSignalClass(); c != routerv1.SignalClass_SIGNAL_CLASS_UNSPECIFIED {
		name, ok := signalClasses[c]
		if !ok {
			return nil, invalid("signal_class is not one the router knows")
		}
		f.Class = name
	}
	f.Limit = int(m.GetLimit())
	if f.Limit == 0 {
		f.Limit = signalsDefault
	}
	f.Limit = min(f.Limit, signalsMax)
	got, err := s.d.Store.ListSignals(ctx, f)
	if err != nil {
		return nil, s.status(err)
	}
	out := &routerv1.ListSignalsResponse{}
	for _, sg := range got {
		out.Signals = append(out.Signals, signalMsg(sg))
	}
	s.d.Log.Info("operator listed signals", "actor", readActor, "count", len(out.Signals))
	return connect.NewResponse(out), nil
}

func signalMsg(sg store.Signal) *routerv1.Signal {
	out := &routerv1.Signal{
		Id: sg.ID.String(), SignalClass: signalClassOf(sg.Class), Provider: sg.Provider,
		Source: sg.Source, ObservedJson: string(sg.Observed), Action: sg.Action,
		CreatedAt: timestamppb.New(sg.CreatedAt),
	}
	if sg.PlacementID != uuid.Nil {
		out.PlacementId = sg.PlacementID.String()
	}
	return out
}

func signalClassOf(class string) routerv1.SignalClass {
	for c, name := range signalClasses {
		if name == class {
			return c
		}
	}
	return routerv1.SignalClass_SIGNAL_CLASS_UNSPECIFIED
}

// ListProviders lists each provider's health, account state and capacity
func (o *OperatorServer) ListProviders(
	ctx context.Context, _ *connect.Request[routerv1.ListProvidersRequest],
) (*connect.Response[routerv1.ListProvidersResponse], error) {
	s := o.s
	healths, err := s.d.Store.ProviderHealths(ctx)
	if err != nil {
		return nil, s.status(err)
	}
	caps, err := s.d.Store.Capacities(ctx)
	if err != nil {
		return nil, s.status(err)
	}
	var names []string
	for _, h := range healths {
		names = append(names, h.Provider)
	}
	for _, c := range caps {
		names = append(names, c.Provider)
	}
	if s.d.Registry != nil {
		for _, p := range s.d.Registry.Ordered() {
			names = append(names, p.Name())
		}
	}
	slices.Sort(names)
	out := &routerv1.ListProvidersResponse{}
	for _, name := range slices.Compact(names) {
		st := &routerv1.ProviderStatus{Provider: name, State: store.HealthOK, SuccessLowerBound: 1}
		for _, h := range healths {
			if h.Provider != name {
				continue
			}
			st.State, st.Blocked = h.State, h.Blocked
			st.StateSince = timestamppb.New(h.StateSince)
			st.Samples, st.SuccessLowerBound = uint32(h.Samples), h.SuccessLB
			st.ReadyP95Ms = uint32(h.ReadyP95 / time.Millisecond)
			st.ForeignSandboxes = uint32(h.ForeignSandboxes)
		}
		for _, c := range caps {
			if c.Provider != name {
				continue
			}
			st.MaxConcurrent, st.InUse = uint32(c.MaxConcurrent), uint32(c.InUse)
			st.ExternalInUse, st.LimitSource = uint32(c.ExternalInUse), c.LimitSource
			if !c.PolledAt.IsZero() {
				st.PolledAt = timestamppb.New(c.PolledAt)
			}
		}
		st.ImageAccepted = true
		if s.d.Registry != nil {
			if p, ok := s.d.Registry.Get(name); ok {
				st.ImageVersion = s.d.Registry.ImageVersion(p)
				st.ImageAccepted = s.d.Registry.ImageAccepted(p, s.d.ImageAccept)
			}
		}
		out.Providers = append(out.Providers, st)
	}
	s.d.Log.Info("operator listed providers", "actor", readActor)
	return connect.NewResponse(out), nil
}

// ListImageUse counts live placements by provider and base image version, so an old artifact is
// retired only once nothing runs on it
func (o *OperatorServer) ListImageUse(
	ctx context.Context, req *connect.Request[routerv1.ListImageUseRequest],
) (*connect.Response[routerv1.ListImageUseResponse], error) {
	s := o.s
	rows, err := s.d.Store.ImageUse(ctx, req.Msg.GetProvider())
	if err != nil {
		return nil, s.status(err)
	}
	out := &routerv1.ListImageUseResponse{}
	for _, r := range rows {
		use := &routerv1.ImageUse{Provider: r.Provider, ImageVersion: r.Version,
			LivePlacements: uint32(r.Live)}
		if !r.LastReadyAt.IsZero() && r.LastReadyAt.Unix() > 0 {
			use.LastReadyAt = timestamppb.New(r.LastReadyAt)
		}
		out.Uses = append(out.Uses, use)
	}
	s.d.Log.Info("operator listed image use", "actor", readActor)
	return connect.NewResponse(out), nil
}
