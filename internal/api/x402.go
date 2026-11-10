package api

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bugnet-sh/bugnet-router/internal/secrets"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/x402"
)

const (
	// defaultMaxX402Services and defaultMaxX402Price are [x402] defaults
	defaultMaxX402Services = 5
	defaultMaxX402Price    = 500_000
)

var (
	errX402Disabled    = errors.New("api: x402 is switched off on this router")
	errNoX402Feature   = errors.New("api: the placement cannot reach x402 hosts")
	errTooManyServices = errors.New("api: the change passes the x402 services a sandbox may hold")
	errTooManyHosts    = errors.New("api: the change passes the 32 hosts a sandbox may reach")
)

// entryText is each per-service refusal's message, which never quotes the entry
var entryText = map[string]string{
	reasonHostRefused:      "an x402 service's host is refused",
	reasonPrefixRefused:    "an x402 service's prefix or paid path is refused",
	reasonServiceDuplicate: "the change names an x402 service twice",
	reasonMethodRefused:    "an x402 service's method must be GET or POST",
	reasonPriceRefused:     "an x402 service's price cap is not above 0 or is over the most",
	reasonPayToRefused:     "an x402 service's pay_to is not canonical base58 of 32 bytes",
	reasonHostInUse:        "an x402 service's host is a secret's host",
	reasonHostNotAllowed:   "an x402 service's host is outside the hosts this package may reach",
}

// serviceStates maps a stored state to the one the answers show
var serviceStates = map[string]routerv1.X402ServiceState{
	store.X402Applying: routerv1.X402ServiceState_X402_SERVICE_STATE_APPLYING,
	store.X402Active:   routerv1.X402ServiceState_X402_SERVICE_STATE_ACTIVE,
	store.X402Failed:   routerv1.X402ServiceState_X402_SERVICE_STATE_FAILED,
	store.X402Disabled: routerv1.X402ServiceState_X402_SERVICE_STATE_DISABLED,
}

// x402Plan is a change that passed every check, as EnableX402 stores it
type x402Plan struct {
	// put holds the add entries as rows, in request order
	put []store.X402Service
	// disable holds the ids to disable, in request order
	disable []string
}

// CheckX402 runs the checks of a change without making it, spending no operation token
func (s *Server) CheckX402(
	ctx context.Context, req *connect.Request[routerv1.CheckX402Request],
) (*connect.Response[routerv1.CheckX402Response], error) {
	m := req.Msg
	var p store.Placement
	var rows []store.X402Service
	if m.GetHandle() != "" {
		var err error
		if ctx, p, err = s.placement(ctx, m.GetHandle()); err != nil {
			return nil, s.status(err)
		}
		if rows, err = s.d.Store.X402Services(ctx, p.ID); err != nil {
			return nil, s.status(err)
		}
	}
	if _, err := s.checkX402Change(ctx, p, rows, m.GetAdd(), m.GetDisable()); err != nil {
		return nil, s.status(err)
	}
	return connect.NewResponse(&routerv1.CheckX402Response{}), nil
}

// EnableX402 makes a change idempotent by content, then waits for the apply when asked
func (s *Server) EnableX402(
	ctx context.Context, req *connect.Request[routerv1.EnableX402Request],
) (*connect.Response[routerv1.EnableX402Response], error) {
	m := req.Msg
	if !secrets.ValidKey(m.GetRequestId()) {
		return nil, invalid("request_id must be 1 to 128 printable ASCII characters")
	}
	ctx, p, err := s.placement(ctx, m.GetHandle())
	if err != nil {
		return nil, s.status(err)
	}
	var res x402Result
	err = s.d.Store.WithTx(ctx, func(tx pgx.Tx) error {
		var err error
		res, err = s.enableInTx(ctx, tx, p.ID, m)
		return err
	})
	if err != nil {
		return nil, s.status(err)
	}
	if res.bumped {
		s.secretsChanged(ctx, p.ID)
	}
	s.d.Log.Debug("api: x402 change", "request_id", m.GetRequestId(), "stored", res.stored)
	if m.GetWaitActive() && res.ready {
		s.awaitX402(ctx, p.ID, res.added)
	}
	rows, err := s.d.Store.X402Services(ctx, p.ID)
	if err != nil {
		return nil, s.status(err)
	}
	out := &routerv1.EnableX402Response{}
	for _, r := range rows {
		out.Services = append(out.Services, s.x402Info(r))
	}
	return connect.NewResponse(out), nil
}

// x402Result is what a stored change leaves for the code after its commit
type x402Result struct {
	// stored is true when the change differed from the stored rows
	stored bool
	// ready is true when the placement was ready, so an apply runs or ran
	ready bool
	// bumped is true when secrets_version moved, so egress must drop its cached set
	bumped bool
	// added holds the ids of every add entry, written now or before
	added []string
}

// enableInTx checks the change under the placement's row lock and stores what differs
func (s *Server) enableInTx(
	ctx context.Context, tx pgx.Tx, id uuid.UUID, m *routerv1.EnableX402Request,
) (x402Result, error) {
	ts := s.d.Store.Tx(tx)
	cur, err := ts.PlacementForUpdate(ctx, id)
	if err != nil {
		return x402Result{}, err
	}
	rows, err := ts.X402Services(ctx, id)
	if err != nil {
		return x402Result{}, err
	}
	plan, err := s.checkX402Change(ctx, cur, rows, m.GetAdd(), m.GetDisable())
	if err != nil {
		return x402Result{}, err
	}
	ready := cur.State == store.PlacementReady
	res := x402Result{ready: ready}
	for _, r := range plan.put {
		res.added = append(res.added, r.ServiceID)
	}
	if ready && s.d.River == nil {
		return x402Result{}, errNoX402Feature
	}
	puts, disables, pinned := changes(rows, plan)
	if len(puts) == 0 && len(disables) == 0 {
		return res, nil
	}
	if err := s.spend(ctx, cur); err != nil {
		return x402Result{}, err
	}
	res.stored = true
	for _, r := range puts {
		if err := ts.PutX402Service(ctx, r); err != nil {
			return x402Result{}, err
		}
	}
	for _, sid := range disables {
		if err := ts.DisableX402Service(ctx, id, sid); err != nil {
			return x402Result{}, err
		}
	}
	if res.bumped = len(disables) > 0 || pinned; res.bumped {
		if err := ts.BumpSecretsVersion(ctx, id, ready); err != nil {
			return x402Result{}, err
		}
	}
	if ready {
		if err := s.insertApply(ctx, tx, id); err != nil {
			return x402Result{}, err
		}
	}
	return res, nil
}

// changes picks the puts and disables that differ from the stored rows, and a put's pin move
func changes(
	rows []store.X402Service, plan x402Plan,
) (puts []store.X402Service, disables []string, pinned bool) {
	byID := map[string]store.X402Service{}
	for _, r := range rows {
		byID[r.ServiceID] = r
	}
	for _, r := range plan.put {
		old, had := byID[r.ServiceID]
		if had && old.State != store.X402Failed && old.State != store.X402Disabled &&
			old.Spec == r.Spec {
			continue
		}
		puts = append(puts, r)
		live := old.State == store.X402Applying || old.State == store.X402Active
		pinned = pinned || had && live && (old.Spec.MaxPriceMicros != r.Spec.MaxPriceMicros ||
			old.Spec.PayTo != r.Spec.PayTo || old.Spec.PaidPath != r.Spec.PaidPath)
	}
	for _, id := range plan.disable {
		if byID[id].State != store.X402Disabled {
			disables = append(disables, id)
		}
	}
	return puts, disables, pinned
}

// awaitX402 rereads the added rows until none is applying or apply_wait passes
func (s *Server) awaitX402(ctx context.Context, id uuid.UUID, added []string) {
	if len(added) == 0 {
		return
	}
	timer := time.NewTimer(cmp.Or(s.d.Secrets.ApplyWait, defaultApplyWait))
	defer timer.Stop()
	tick := time.NewTicker(applyPoll)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
		case <-timer.C:
			return
		case <-ctx.Done():
			return
		case <-s.ctx.Done():
			return
		}
		rows, err := s.d.Store.X402Services(ctx, id)
		if err != nil || !slices.ContainsFunc(rows, func(r store.X402Service) bool {
			return slices.Contains(added, r.ServiceID) && r.State == store.X402Applying
		}) {
			return
		}
	}
}

// ListX402 answers the placement's services and refusals in any state, spending no token
func (s *Server) ListX402(
	ctx context.Context, req *connect.Request[routerv1.ListX402Request],
) (*connect.Response[routerv1.ListX402Response], error) {
	ctx, p, err := s.placement(ctx, req.Msg.GetHandle())
	if err != nil {
		return nil, s.status(err)
	}
	rows, err := s.d.Store.X402Services(ctx, p.ID)
	if err != nil {
		return nil, s.status(err)
	}
	refusals, err := s.d.Store.X402Refusals(ctx, p.ID)
	if err != nil {
		return nil, s.status(err)
	}
	out := &routerv1.ListX402Response{Enabled: s.d.X402.Enabled}
	for _, r := range rows {
		out.Services = append(out.Services, s.x402Info(r))
	}
	for _, r := range refusals {
		out.Refusals = append(out.Refusals, &routerv1.X402Refusal{
			Reason: r.Reason, Count: count32(r.Count), LastAt: timestamppb.New(r.LastAt),
		})
	}
	return connect.NewResponse(out), nil
}

// checkX402Change runs every check of XS 6.3 for a placement and a change, EnableX402 too
func (s *Server) checkX402Change(ctx context.Context, p store.Placement,
	rows []store.X402Service, add []*routerv1.X402ServiceSpec, disable []string) (x402Plan, error) {
	if !s.d.X402.Enabled {
		return x402Plan{}, errX402Disabled
	}
	// A zero placement is CheckX402 without a handle, which runs only the placement-free checks
	placed := p.ID != uuid.Nil
	if placed {
		if err := s.x402Capable(p); err != nil {
			return x402Plan{}, err
		}
	}
	if len(add) == 0 && len(disable) == 0 {
		return x402Plan{}, nil
	}
	var secretHosts, allow []string
	if placed {
		var err error
		if secretHosts, err = s.d.Store.SecretHosts(ctx, p.ID); err != nil {
			return x402Plan{}, err
		}
		if pkg, ok := s.d.Catalog.Get(p.PackageID); ok {
			allow = pkg.EgressAllow
		}
	}
	plan := x402Plan{disable: disable}
	maxPrice := cmp.Or(s.d.X402.MaxPriceMicros, defaultMaxX402Price)
	for _, e := range add {
		spec := specOf(e)
		id := x402.ServiceID(spec.Method, spec.Host, spec.Prefix)
		var r x402.Refusal
		switch err := x402.CheckSpec(spec, maxPrice, s.d.HostRules); {
		case errors.As(err, &r):
			return x402Plan{}, refuseEntry(connect.CodeInvalidArgument, r.Reason, e)
		case err != nil:
			return x402Plan{}, err
		case slices.Contains(disable, id) || slices.ContainsFunc(plan.put,
			func(row store.X402Service) bool { return row.ServiceID == id }):
			return x402Plan{}, refuseEntry(connect.CodeInvalidArgument, reasonServiceDuplicate, e)
		case slices.Contains(secretHosts, spec.Host):
			return x402Plan{}, refuseEntry(connect.CodeFailedPrecondition, reasonHostInUse, e)
		case len(allow) > 0 && !slices.Contains(allow, spec.Host):
			return x402Plan{}, refuseEntry(connect.CodeFailedPrecondition, reasonHostNotAllowed, e)
		}
		plan.put = append(plan.put, store.X402Service{PlacementID: p.ID, ServiceID: id, Spec: spec})
	}
	for _, id := range disable {
		if !placed || !slices.ContainsFunc(rows,
			func(r store.X402Service) bool { return r.ServiceID == id }) {
			return x402Plan{}, unknownService(id)
		}
	}
	most := cmp.Or(s.d.X402.MaxServices, defaultMaxX402Services)
	if !placed {
		if len(add) > most {
			return x402Plan{}, errTooManyServices
		}
		return plan, nil
	}
	if err := fits(rows, plan, secretHosts, most); err != nil {
		return x402Plan{}, err
	}
	return plan, nil
}

// fits refuses a change that brings in a service or host and ends over a cap
func fits(rows []store.X402Service, plan x402Plan, secretHosts []string, most int) error {
	live := map[string]string{}
	for _, r := range rows {
		if r.State != store.X402Disabled {
			live[r.ServiceID] = r.Spec.Host
		}
	}
	before := hostsOf(live, secretHosts)
	newService := false
	for _, r := range plan.put {
		_, had := live[r.ServiceID]
		newService = newService || !had
		live[r.ServiceID] = r.Spec.Host
	}
	for _, id := range plan.disable {
		delete(live, id)
	}
	after := hostsOf(live, secretHosts)
	newHost := slices.ContainsFunc(after, func(h string) bool {
		return !slices.Contains(before, h)
	})
	// Only what a change brings in can fail a cap, so a placement over a lowered cap may shrink
	switch {
	case newService && len(live) > most:
		return errTooManyServices
	case newHost && len(after) > secrets.MaxHostsPerSandbox:
		return errTooManyHosts
	}
	return nil
}

// hostsOf is the distinct hosts of the services and the secrets
func hostsOf(services map[string]string, secretHosts []string) []string {
	out := slices.Clone(secretHosts)
	for _, h := range services {
		out = append(out, h)
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// x402Capable refuses a placement that is not placing or ready, or cannot reach x402 hosts
func (s *Server) x402Capable(p store.Placement) error {
	switch p.State {
	case store.PlacementPlacing:
		if !p.NeedsSecrets {
			return errNoX402Feature
		}
		return nil
	case store.PlacementReady:
		prov, err := s.home(p)
		if err != nil {
			return err
		}
		if !s.d.Registry.Capabilities(prov).Secrets {
			return errNoX402Feature
		}
		return nil
	}
	return fmt.Errorf("%w: placement is %s", errNotReady, p.State)
}

// x402Info renders one row, FAILED with HOST_BLOCKED when x402.Blocked holds
func (s *Server) x402Info(row store.X402Service) *routerv1.X402ServiceInfo {
	info := &routerv1.X402ServiceInfo{
		ServiceId: row.ServiceID,
		Spec: &routerv1.X402ServiceSpec{
			Host: row.Spec.Host, Prefix: row.Spec.Prefix, Method: row.Spec.Method,
			MaxPriceMicros: row.Spec.MaxPriceMicros, PayTo: row.Spec.PayTo,
			Indexed: row.Spec.Indexed, PaidPath: row.Spec.PaidPath,
		},
		State:     serviceStates[row.State],
		Requests:  count32(row.Requests),
		Paid:      count32(row.Paid),
		Uncertain: count32(row.Uncertain),
	}
	if row.State == store.X402Failed {
		info.FailureReason = row.FailureReason
	}
	if row.State != store.X402Disabled && x402.Blocked(row.Spec.Host, s.d.X402) {
		info.State = routerv1.X402ServiceState_X402_SERVICE_STATE_FAILED
		info.FailureReason = reasonHostBlocked
	}
	return info
}

// refuseEntry answers reason with ErrorInfo metadata service_id and host of one add entry
func refuseEntry(code connect.Code, reason string, e *routerv1.X402ServiceSpec) error {
	text := cmp.Or(entryText[reason], "an x402 service is refused")
	return withMeta(connect.NewError(code, errors.New(text)), reason, map[string]string{
		"service_id": x402.ServiceID(e.GetMethod(), e.GetHost(), e.GetPrefix()),
		"host":       e.GetHost(),
	})
}

// unknownService answers SERVICE_UNKNOWN naming the id, which has no host
func unknownService(id string) error {
	return withMeta(connect.NewError(connect.CodeNotFound,
		errors.New("the change disables an x402 service the placement lacks")),
		reasonServiceUnknown, map[string]string{"service_id": id})
}

func specOf(e *routerv1.X402ServiceSpec) x402.Spec {
	return x402.Spec{
		Host: e.GetHost(), Prefix: e.GetPrefix(), Method: e.GetMethod(),
		MaxPriceMicros: e.GetMaxPriceMicros(), PayTo: e.GetPayTo(), Indexed: e.GetIndexed(),
		PaidPath: e.GetPaidPath(),
	}
}

// count32 is a stored count as the answers carry it, 0 for a negative one
func count32(n int) uint32 {
	return uint32(max(0, min(n, 1<<32-1)))
}
