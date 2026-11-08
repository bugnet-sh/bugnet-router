// Package saga places paid sandboxes on providers, extends their deadlines and tears them down.
//
// Create records the order and a placing placement, then places it in a goroutine that
// heartbeats the placement while it runs. A package no provider can host is recorded as failed
// and refundable at once. Each candidate gets a capacity slot, a create under an idempotency key
// derived from the order, attempt and provider, a wait for ready and, where needed, the runner
// upload. A placement holds at most one slot, recorded on its row, so no path frees it twice. A
// create whose outcome is unknown is retried with the same key on the same provider, a known
// failure that allows it fails over to the next candidate, anything else fails the placement as
// refundable. Once placing ends, every sandbox labelled for the placement other than the ready
// one is destroyed. FailStuck fails a placement whose placing saga went away in the same way.
// Every sandbox is labelled with its placement and the saga's deployment. Every state change
// commits together with its outbox event and, for a deadline, its River job, and is then
// published on Valkey for live readers. A placement records the data-plane root it was created
// under, gets its edge and egress budgets in the transaction that makes it ready, and grows them
// in each extension's. The edges hear of a placement leaving ready through PlacementEnded, and
// of an extension through DeadlineMoved once both spent guards are cleared.
//
// Boot runs after every create, whether or not the runner was uploaded. It runs bugnet-run setup
// with the gate key, the CA and the env lines, then harden where the provider has a hardening rule,
// then PrepareGate and three health checks through the route, then ssh-up when the order needs
// SSH, then setup again to start the sampler, so the installs of harden and ssh-up never count as
// the user's disk. A failed setup, or a failed
// harden where it is required, fails the attempt over as boot_failed. A gate that never answers
// fails it over only when the order needs previews or SSH, and is logged otherwise. The ready
// transaction records when the gate answered and the CA version the sandbox trusts. A restricted
// package's create carries its egress allowlist, and only a provider with egress control takes it.
// OpenGate runs the gate steps again for a ready placement whose gate never answered, and
// RepairGate runs setup --repair and PrepareGate, both one replica at a time under a Postgres
// advisory lock per placement. Both store the route's meta. OpenGate records when the gate
// answered, and RepairGate leaves the gate time to its caller.
//
// The ready transaction records the sshd ssh-up made ready and mints the placement's SSH alias
// when the registry gives its provider SSH. PrepareSSH, run by the prepare_ssh job, makes the sshd
// of a placement whose order did not ask for SSH ready later. AuthorizeGateway puts the gateway's
// key for a placement in its sshd once per key, through ssh-up or, for Boat's own sshd, through
// the provider, both one replica at a time under an advisory lock per placement. The end of a
// placement forgets its gateway keys.
//
// Create and Extend first decide the payer an order counts under, then check it under that
// payer's transaction lock in the transaction that records the order. A blocked payer is refused
// with nothing recorded, and a start over the wallet's cap or start buckets is recorded failed and
// refundable, as CP 7.7 and 7.9 say.
package saga

import (
	"bytes"
	"cmp"
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/valkey-io/valkey-go"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bugnet-sh/bugnet-router/internal/budget"
	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/dpkeys"
	"github.com/bugnet-sh/bugnet-router/internal/edgeevents"
	"github.com/bugnet-sh/bugnet-router/internal/handle"
	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/ordertoken"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/ratelimit"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/wallet"
	"github.com/bugnet-sh/bugnet-router/internal/wallets"
)

// Errors the API maps to status codes
var (
	ErrBadToken        = errors.New("saga: bad order token")
	ErrUnderpaid       = errors.New("saga: amount below the package price")
	ErrUnknownPackage  = errors.New("saga: unknown package")
	ErrNotReady        = errors.New("saga: placement is not ready")
	ErrHandleMismatch  = errors.New("saga: order token is for another handle")
	ErrNotExtension    = errors.New("saga: package does not extend this placement")
	ErrExceedsLifetime = errors.New("saga: extension outlasts the provider's maximum lifetime")
	// ErrNotDue means a deadline termination found the deadline moved past the time it was due
	ErrNotDue = errors.New("saga: placement deadline is not due")
)

// ReasonStop is the termination reason when the placement's owner stops it
const ReasonStop = "stop"

// ReasonNoCandidate is the failure reason when no provider can host the package
const ReasonNoCandidate = "no_candidate"

// ReasonDeadline is the termination reason when the placement's paid time runs out
const ReasonDeadline = "deadline"

// ReasonPlacingStalled is the failure reason when the saga placing a placement stopped heartbeating
const ReasonPlacingStalled = "placing_stalled"

// Failure reasons are stable codes the gateway may show, the attempts and logs keep the details
const (
	reasonFull       = "capacity_full"        // every candidate provider had no free slot
	reasonExhausted  = "candidates_exhausted" // placing ran out of candidates after the pre-check
	reasonBootFailed = "boot_failed"
	reasonNoRunner   = "runner_unavailable"
	reasonInternal   = "internal_error"
	reasonCreate     = "create_failed" // a create failed without a provider error code
)

// defaultDeployment labels the sandboxes of a saga whose Deps name no deployment
const defaultDeployment = "main"

const (
	maxAttempts      = 3
	placingBudget    = 30 * time.Second
	lifetimeSlack    = 5 * time.Minute
	terminateTimeout = 60 * time.Second
	heartbeatEvery   = 10 * time.Second
)

// cleanupTimeout bounds each store or provider call of placing so Wait cannot hang on one
var cleanupTimeout = 30 * time.Second

// Delays before each retry of a create whose outcome is unknown
var unknownBackoff = []time.Duration{
	200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond,
}

// Attempt states
const (
	attemptCreating = "creating"
	attemptReady    = "ready"
	attemptFailed   = "failed"
)

// Deps are the services and settings a Saga works with
type Deps struct {
	Store             *store.Store
	Engine            *placement.Engine
	Registry          *provider.Registry
	Catalog           *catalog.Catalog
	River             *river.Client[pgx.Tx] // insert only
	Valkey            valkey.Client
	Prefix            string
	OrderKey          ed25519.PublicKey
	HandleKey         []byte
	MarginFloor       float64
	Tracker           *placement.Tracker
	ReplicaID         string            // recorded as the saga owner of the placements it places
	DeploymentID      string            // labels every sandbox, "main" when empty
	HeartbeatInterval time.Duration     // 10 s when zero
	Runners           map[string][]byte // architecture to its bugnet-run build
	Now               func() time.Time  // time.Now when nil
	Log               *slog.Logger      // slog.Default when nil
	// Events carries the edge events, none are sent when nil
	Events *edgeevents.Bus
	// Budget sizes each placement's edge and egress budgets
	Budget config.EdgeBudget
	// Keys are the data-plane roots, a new placement records the newest one's id
	Keys dpkeys.Keys
	// Boot holds what each sandbox's setup and harden specs carry
	Boot BootSettings
	// SSH holds what each sandbox's ssh-up spec carries
	SSH SSHSettings
	// Wallets are the payer and wallet settings, a zero limit taking its default
	Wallets config.Wallets
}

// Saga runs placements from order to teardown
type Saga struct {
	d        Deps
	wg       sync.WaitGroup
	destroys singleflight.Group // keyed by placement, so racing terminations share one destroy
	gates    singleflight.Group // keyed by placement, so racing gate openings share one run
	meter    *budget.Meter
	keyID    string // the newest data-plane root's id, empty without keys
	w        config.Wallets
	guard    wallet.Guard
	limits   *ratelimit.Limiter
	wallets  *wallets.Store
	// unguardedAt is when step 4 of the payer decision was last logged, in Unix nanoseconds
	unguardedAt atomic.Int64
}

var errNoKeys = errors.New("saga: no data-plane keys to create a placement under")

// New builds a saga over deps
func New(deps Deps) *Saga {
	if deps.Now == nil {
		deps.Now = time.Now
	}
	if deps.Log == nil {
		deps.Log = slog.Default()
	}
	if deps.HeartbeatInterval <= 0 {
		deps.HeartbeatInterval = heartbeatEvery
	}
	if deps.DeploymentID == "" {
		deps.DeploymentID = defaultDeployment
	}
	w := deps.Wallets.OrDefaults()
	s := &Saga{
		d: deps, meter: budget.New(deps.Valkey, deps.Prefix, deps.Budget), w: w,
		guard:  wallet.Guard{UsePayer: w.UsePayer, Facilitators: w.FacilitatorAddresses},
		limits: ratelimit.New(deps.Valkey, deps.Prefix, 0, 0),
		wallets: wallets.New(deps.Store, wallets.Settings{
			Facilitators: w.FacilitatorAddresses, StrikeThreshold: w.StrikeThreshold,
			StrikeWindow: w.StrikeWindow, StrikeBlockFor: w.StrikeBlockFor,
			EmitEvents: w.EmitEvents, Now: deps.Now,
		}),
	}
	if roots := deps.Keys.Roots(); len(roots) > 0 {
		s.keyID = roots[0].ID
	}
	return s
}

// Wait blocks until every placing goroutine has finished
func (s *Saga) Wait() { s.wg.Wait() }

func (s *Saga) now() time.Time { return s.d.Now().UTC() }

/* Create */

// Create records a verified start order and places it, failing at once with ErrNoCandidate
func (s *Saga) Create(
	ctx context.Context, token string, req placement.Requirements,
) (store.Placement, string, error) {
	c, err := s.verify(token, ordertoken.KindStart)
	if err != nil {
		return store.Placement{}, "", err
	}
	pkg, err := s.paidPackage(c)
	if err != nil {
		return store.Placement{}, "", err
	}
	if s.keyID == "" {
		return store.Placement{}, "", errNoKeys
	}
	h, sha := handle.Derive(s.d.HandleKey, c.OrderID)
	np := store.NewPlacement{
		ID: uuid.New(), HandleSHA256: sha, OrderID: c.OrderID, PackageID: pkg.ID, KeyID: s.keyID,
		NeedsSecrets: req.Secrets, FeePayer: c.FeePayer, AmountMicros: c.PackageMicros(),
	}
	_, err = s.d.Engine.Candidates(s.inputs(pkg, req, nil))
	switch {
	case errors.Is(err, placement.ErrNoCandidate):
		np.FailureReason = ReasonNoCandidate
	case err != nil:
		return store.Placement{}, "", fmt.Errorf("find candidates: %w", err)
	}
	p, created, wait, err := s.record(ctx, c, np)
	if err != nil {
		return store.Placement{}, "", fmt.Errorf("record placement: %w", err)
	}
	if !bytes.Equal(p.HandleSHA256, sha) {
		return store.Placement{}, "", fmt.Errorf(
			"%w: order %s belongs to another placement", ErrBadToken, c.OrderID)
	}
	if p.State == store.PlacementFailed {
		var refusal error
		switch p.FailureReason {
		case ReasonNoCandidate:
			refusal = fmt.Errorf("%w for package %s", placement.ErrNoCandidate, pkg.ID)
		case ReasonWalletBusy:
			refusal = ErrWalletBusy
		case ReasonWalletRateLimited:
			if !created {
				wait = s.startWait(ctx, p.Payer)
			}
			refusal = &WaitError{Wait: wait}
		}
		if refusal != nil {
			if created {
				s.publish(ctx, newEvent(p, p.FailureReason))
			}
			return p, h, refusal
		}
	}
	if created {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.place(context.WithoutCancel(ctx), p, pkg, req)
		}()
	}
	return p, h, nil
}

// record stores the order and its placement under the payer's lock, as CP 7.7 says
func (s *Saga) record(
	ctx context.Context, c ordertoken.Claims, np store.NewPlacement,
) (store.Placement, bool, time.Duration, error) {
	var p store.Placement
	var created bool
	var wait time.Duration
	err := s.d.Store.WithTx(ctx, func(tx pgx.Tx) error {
		ts := s.d.Store.Tx(tx)
		var err error
		if np.Payer, err = s.payer(ctx, ts, c); err != nil {
			return err
		}
		if err := ts.LockWallets(ctx, np.Payer); err != nil {
			return err
		}
		// The order keeps the claim and the placement the decision (CP 7.1, 7.5)
		id, inserted, err := ts.InsertOrder(ctx, store.Order{
			ID: np.OrderID, Kind: store.OrderStart, PlacementID: np.ID, Payer: c.Payer,
			FeePayer: np.FeePayer, AmountMicros: np.AmountMicros,
		})
		if err != nil {
			return err
		}
		if !inserted {
			// A retried order answers as it did, with no check run again
			p, err = ts.PlacementByID(ctx, id)
			return err
		}
		if wait, err = s.checkStart(ctx, tx, &np); err != nil {
			return err
		}
		if np.Payer != "" {
			if err := ts.SeeWallet(ctx, np.Payer, s.now()); err != nil {
				return err
			}
		}
		if p, err = ts.InsertPlacement(ctx, np); err != nil {
			return err
		}
		created = true
		if np.FailureReason == "" {
			return nil
		}
		return s.d.Store.InsertOutbox(ctx, tx, EventFailed, newEvent(p, np.FailureReason))
	})
	return p, created, wait, err
}

func (s *Saga) inputs(
	pkg catalog.Package, req placement.Requirements, tried map[string]bool,
) placement.Inputs {
	return placement.Inputs{
		Package: pkg, Requirements: req, MarginFloor: s.d.MarginFloor, Exclude: tried,
	}
}

func (s *Saga) verify(token string, kind ordertoken.Kind) (ordertoken.Claims, error) {
	c, err := ordertoken.Verify(s.d.OrderKey, token, s.now())
	if err != nil {
		return c, fmt.Errorf("%w: %w", ErrBadToken, err)
	}
	if c.Kind != kind {
		return c, fmt.Errorf("%w: %s token, want %s: %w", ErrBadToken, c.Kind, kind,
			ordertoken.ErrBadKind)
	}
	return c, nil
}

// paidPackage returns the token's package once the amount paid covers its price
func (s *Saga) paidPackage(c ordertoken.Claims) (catalog.Package, error) {
	pkg, err := s.lookupPackage(c.PackageID)
	if err != nil {
		return pkg, err
	}
	// The x402 allowance rides along in a start's amount and never pays for the package
	if paid := c.PackageMicros(); paid < pkg.PriceMicros {
		return pkg, fmt.Errorf("%w: paid %d, %s costs %d", ErrUnderpaid, paid, pkg.ID,
			pkg.PriceMicros)
	}
	return pkg, nil
}

// lookupPackage returns the catalog's package with this ID or ErrUnknownPackage
func (s *Saga) lookupPackage(id string) (catalog.Package, error) {
	pkg, ok := s.d.Catalog.Get(id)
	if !ok {
		return pkg, fmt.Errorf("%w %q", ErrUnknownPackage, id)
	}
	return pkg, nil
}

/* Placing */

// outcome is how one attempt ended
type outcome struct {
	done     bool          // the placement left placing, so placing stops
	kept     *provider.Ref // the sandbox the placement became ready on
	failOver bool
	reason   string
	detail   string
}

// place tries candidates until one is ready or the attempt or time budget runs out
func (s *Saga) place(
	ctx context.Context, p store.Placement, pkg catalog.Package, req placement.Requirements,
) {
	stopHeartbeat := s.heartbeat(ctx, p.ID)
	defer stopHeartbeat()
	budget, cancel := context.WithTimeout(ctx, placingBudget)
	defer cancel()
	tried := map[string]bool{}
	var attempted []provider.Provider
	reason, detail := reasonExhausted, ""
	full := false
	for n := 1; n <= maxAttempts && budget.Err() == nil; {
		cands, err := s.d.Engine.Candidates(s.inputs(pkg, req, tried))
		if err != nil {
			if !errors.Is(err, placement.ErrNoCandidate) {
				reason, detail = reasonInternal, fmt.Sprintf("find candidates: %v", err)
			} else if n == 1 && full {
				reason = reasonFull
			}
			break
		}
		c := cands[0]
		name := c.Provider.Name()
		tried[name] = true
		// Without a build the sandbox could never run a job, so the provider is passed over
		if caps := c.Provider.Capabilities(); caps.NeedsRunnerUpload &&
			len(s.d.Runners[caps.RunnerArch()]) == 0 {
			s.d.Log.Error("no bugnet-run build for a provider that needs one", "provider", name,
				"arch", caps.RunnerArch())
			reason, detail = reasonNoRunner, "no "+caps.RunnerArch()+" build for "+name
			continue
		}
		reserved, err := s.reserve(ctx, p.ID, name)
		if err != nil {
			reason, detail = reasonInternal, fmt.Sprintf("reserve on %s: %v", name, err)
			break
		}
		if !reserved {
			full = true
			continue
		}
		attempted = append(attempted, c.Provider)
		out := s.attempt(ctx, budget, p, pkg, req, c, n)
		if out.done {
			// One attempt on a provider that keys its creates left no sandbox beside the kept one
			if out.kept == nil || len(attempted) > 1 ||
				!c.Provider.Capabilities().OneSandboxPerKey {
				s.sweep(ctx, p.ID, attempted, out.kept)
			}
			return
		}
		reason, detail = out.reason, out.detail
		if !out.failOver {
			break
		}
		n++
	}
	s.fail(ctx, p, reason, detail, attempted)
}

// heartbeat shows the placement is still being placed by this replica until stop is called
func (s *Saga) heartbeat(ctx context.Context, id uuid.UUID) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	beat := func() {
		bctx, bcancel := step(ctx)
		defer bcancel()
		err := s.d.Store.HeartbeatPlacement(bctx, id, s.d.ReplicaID, s.now())
		if err != nil && !errors.Is(err, store.ErrStateConflict) && ctx.Err() == nil {
			s.d.Log.Warn("placing heartbeat", "placement", id, "err", err)
		}
	}
	beat()
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(s.d.HeartbeatInterval)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				beat()
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func (s *Saga) reserve(ctx context.Context, id uuid.UUID, providerName string) (bool, error) {
	ctx, cancel := step(ctx)
	defer cancel()
	return s.d.Store.ReservePlacementCapacity(ctx, id, providerName)
}

// attempt holds a capacity slot on c's provider and gives it back unless the sandbox is ready
func (s *Saga) attempt(
	ctx, budget context.Context, p store.Placement, pkg catalog.Package,
	req placement.Requirements, c placement.Candidate, n int,
) outcome {
	prov := c.Provider
	name := prov.Name()
	rec := store.Attempt{
		PlacementID:    p.ID,
		Attempt:        n,
		Provider:       name,
		IdempotencyKey: idempotencyKey(p.OrderID, n, name),
		State:          attemptCreating,
	}
	if err := s.recordAttempt(ctx, rec); err != nil {
		s.release(ctx, p.ID)
		return outcome{reason: reasonInternal, detail: err.Error()}
	}
	started := s.now()
	ref, err := s.create(budget, prov, provider.CreateRequest{
		IdempotencyKey: rec.IdempotencyKey,
		Size:           c.Size,
		Labels:         s.labels(p.ID),
		Lifetime:       time.Duration(pkg.Minutes)*time.Minute + lifetimeSlack,
		Egress:         s.d.Boot.egressAllow(pkg.EgressAllow),
	})
	if err != nil {
		s.d.Tracker.RecordFailure(name)
		s.abandon(ctx, prov, rec, nil, err)
		return outcome{
			failOver: canFailOver(err), reason: createReason(err),
			detail: fmt.Sprintf("create on %s: %v", name, err),
		}
	}
	ref, boot, err := s.Boot(ctx, prov, ref, BootPlan{
		Placement: p.ID, KeyID: p.KeyID, NeedPreview: req.Preview, NeedSSH: req.SSH,
		Restricted: pkg.EgressAllow,
	})
	if err != nil {
		noBuild := errors.Is(err, errNoRunnerBuild)
		reason := reasonNoRunner
		if !noBuild {
			reason = reasonBootFailed
			s.d.Tracker.RecordFailure(name)
		}
		s.abandon(ctx, prov, rec, &ref, err)
		return outcome{
			failOver: !noBuild, reason: reason, detail: fmt.Sprintf("boot on %s: %v", name, err),
		}
	}
	return s.commitReady(ctx, p, pkg, c, rec, ref, boot, started)
}

// create retries a create whose outcome is unknown under the same key on the same provider
func (s *Saga) create(
	ctx context.Context, prov provider.Provider, req provider.CreateRequest,
) (provider.Ref, error) {
	for i := 0; ; i++ {
		ref, err := prov.Create(ctx, req)
		e, ok := provider.AsError(err)
		if err == nil || !ok || e.Code != provider.CodeOutcomeUnknown || i == len(unknownBackoff) {
			return ref, err
		}
		s.d.Log.Warn("create outcome unknown, retrying", "provider", prov.Name(),
			"key", req.IdempotencyKey, "err", err)
		timer := time.NewTimer(unknownBackoff[i])
		select {
		case <-ctx.Done():
			timer.Stop()
			return provider.Ref{}, err
		case <-timer.C:
		}
	}
}

// createReason names a failed create by its provider code, which carries no provider name
func createReason(err error) string {
	if e, ok := provider.AsError(err); ok {
		return "provider_" + string(e.Code)
	}
	return reasonCreate
}

// canFailOver never allows an unknown outcome, since the first sandbox may still boot and bill
func canFailOver(err error) bool {
	e, ok := provider.AsError(err)
	return ok && e.CanFailOver && e.Code != provider.CodeOutcomeUnknown
}

// commitReady starts the paid clock, or gives the sandbox back when the placement moved on
func (s *Saga) commitReady(
	ctx context.Context, p store.Placement, pkg catalog.Package, c placement.Candidate,
	rec store.Attempt, ref provider.Ref, boot BootReport, started time.Time,
) outcome {
	prov := c.Provider
	readyAt := s.now()
	s.d.Tracker.RecordReady(prov.Name(), readyAt.Sub(started))
	deadline := readyAt.Add(time.Duration(pkg.Minutes) * time.Minute)
	lctx, cancel := step(ctx)
	err := prov.SetLifetime(lctx, ref, deadline.Add(lifetimeSlack))
	cancel()
	if err != nil {
		// The lifetime set at create still outlasts the deadline unless ready took the full slack
		s.d.Log.Warn("set lifetime at ready", "placement", p.ID, "provider", prov.Name(),
			"err", err)
	}
	var ev Event
	tctx, cancel := step(ctx)
	defer cancel()
	err = s.d.Store.WithTx(tctx, func(tx pgx.Tx) error {
		ts := s.d.Store.Tx(tx)
		err := ts.MarkPlacementReady(tctx, p.ID, ref, c.Size.Name, readyAt, deadline)
		if err != nil {
			return err
		}
		var gateAt *time.Time
		if boot.GateReady {
			gateAt = &boot.GateReadyAt
		}
		if err := ts.SetPlacementGate(tctx, p.ID, gateAt, boot.CAVersion); err != nil {
			return err
		}
		if boot.SSH != nil {
			if err := ts.PutSSHHost(tctx, *boot.SSH); err != nil {
				return err
			}
		}
		if s.d.Registry.Capabilities(prov).SSH {
			s.mintAlias(tctx, ts, p.ID)
		}
		if _, _, err := s.addBudgets(tctx, ts, p.ID, pkg); err != nil {
			return err
		}
		ready := rec
		ready.State = attemptReady
		if err := ts.RecordAttempt(tctx, ready); err != nil {
			return err
		}
		if err := s.insertDeadline(tctx, tx, p.ID, deadline); err != nil {
			return err
		}
		cur, err := ts.PlacementByID(tctx, p.ID)
		if err != nil {
			return err
		}
		ev = newEvent(cur, "")
		return s.d.Store.InsertOutbox(tctx, tx, EventReady, ev)
	})
	if err != nil {
		s.abandon(ctx, prov, rec, &ref, err)
		if errors.Is(err, store.ErrStateConflict) {
			s.d.Log.Info("placement left placing before it was ready", "placement", p.ID)
			return outcome{done: true}
		}
		return outcome{reason: reasonInternal, detail: fmt.Sprintf("commit ready: %v", err)}
	}
	s.publish(ctx, ev)
	return outcome{done: true, kept: &ref}
}

// abandon destroys an attempt's sandbox if it has one, records the failure and frees the slot
func (s *Saga) abandon(
	ctx context.Context, prov provider.Provider, rec store.Attempt, ref *provider.Ref, err error,
) {
	if ref != nil {
		s.destroy(ctx, prov, *ref)
	}
	rec.State = attemptFailed
	rec.Error = err.Error()
	if e, ok := provider.AsError(err); ok {
		rec.ErrorCode = string(e.Code)
	}
	if err := s.recordAttempt(ctx, rec); err != nil {
		s.d.Log.Error("record failed attempt", "placement", rec.PlacementID, "err", err)
	}
	s.release(ctx, rec.PlacementID)
}

func (s *Saga) recordAttempt(ctx context.Context, rec store.Attempt) error {
	ctx, cancel := step(ctx)
	defer cancel()
	return s.d.Store.RecordAttempt(ctx, rec)
}

// fail records a refundable failure and destroys whatever this placement left on any provider
func (s *Saga) fail(
	ctx context.Context, p store.Placement, reason, detail string, attempted []provider.Provider,
) {
	s.d.Log.Warn("placement failed", "placement", p.ID, "reason", reason, "detail", detail)
	var ev Event
	tctx, cancel := step(ctx)
	err := s.d.Store.WithTx(tctx, func(tx pgx.Tx) error {
		var err error
		if ev, err = s.recordFailed(tctx, tx, p.ID, reason); err != nil {
			return err
		}
		// The slot goes with the failure, so a failed release leaves the placement to FailStuck
		_, err = s.d.Store.Tx(tx).ReleasePlacementCapacity(tctx, p.ID)
		return err
	})
	cancel()
	if err != nil {
		s.d.Log.Error("mark placement failed", "placement", p.ID, "reason", reason, "err", err)
	} else {
		s.publish(ctx, ev)
	}
	s.sweep(ctx, p.ID, attempted, nil)
}

// FailStuck fails a placing placement whose saga went away as placing would, else does nothing
func (s *Saga) FailStuck(ctx context.Context, id uuid.UUID, reason string) error {
	var ev Event
	failed := false
	err := s.d.Store.WithTx(ctx, func(tx pgx.Tx) error {
		ts := s.d.Store.Tx(tx)
		cur, err := ts.PlacementForUpdate(ctx, id)
		if err != nil || cur.State != store.PlacementPlacing {
			return err
		}
		if ev, err = s.recordFailed(ctx, tx, id, reason); err != nil {
			return err
		}
		attempts, err := ts.Attempts(ctx, id)
		if err != nil {
			return err
		}
		for _, a := range attempts {
			if a.State != attemptCreating {
				continue
			}
			a.State, a.Error = attemptFailed, reason
			if err := ts.RecordAttempt(ctx, a); err != nil {
				return err
			}
		}
		// The slot goes with the failure, so it cannot leak when the cleanup below never runs
		if _, err := ts.ReleasePlacementCapacity(ctx, id); err != nil {
			return err
		}
		failed = true
		return nil
	})
	if err != nil {
		return fmt.Errorf("fail stuck placement: %w", err)
	}
	if !failed {
		return nil
	}
	// The failure is committed, so cleanup runs to the end without holding up the caller
	ctx = context.WithoutCancel(ctx)
	s.publish(ctx, ev)
	s.wg.Go(func() { s.sweep(ctx, id, s.d.Registry.Ordered(), nil) })
	return nil
}

// recordFailed marks a placing placement failed and refundable with its placement.failed event
func (s *Saga) recordFailed(
	ctx context.Context, tx pgx.Tx, id uuid.UUID, reason string,
) (Event, error) {
	ts := s.d.Store.Tx(tx)
	if err := ts.MarkPlacementFailed(ctx, id, reason, true); err != nil {
		return Event{}, err
	}
	if err := ts.DeleteSecretsOf(ctx, id); err != nil {
		return Event{}, err
	}
	cur, err := ts.PlacementByID(ctx, id)
	if err != nil {
		return Event{}, err
	}
	ev := newEvent(cur, reason)
	return ev, s.d.Store.InsertOutbox(ctx, tx, EventFailed, ev)
}

// sweep destroys every sandbox labelled for the placement on provs except keep
func (s *Saga) sweep(
	ctx context.Context, id uuid.UUID, provs []provider.Provider, keep *provider.Ref,
) {
	for _, prov := range provs {
		lctx, cancel := step(ctx)
		owned, err := prov.ListOwned(lctx, s.labels(id))
		cancel()
		if err != nil {
			// A listing cut short still names sandboxes to destroy
			s.d.Log.Error("list sandboxes to destroy", "placement", id,
				"provider", prov.Name(), "listed", len(owned), "err", err)
		}
		for _, o := range owned {
			if keep != nil && prov.Name() == keep.Provider && o.Ref.ID == keep.ID {
				continue
			}
			s.destroy(ctx, prov, o.Ref)
		}
	}
}

/* Extend */

// Extend moves a ready placement's deadline by an extension order's minutes, once per order
func (s *Saga) Extend(ctx context.Context, h string, token string) (store.Placement, error) {
	c, err := s.verify(token, ordertoken.KindExtend)
	if err != nil {
		return store.Placement{}, err
	}
	sha, err := hashHandle(h)
	if err != nil {
		return store.Placement{}, err
	}
	if want, err := hex.DecodeString(c.HandleSHA256); err != nil || !hmac.Equal(want, sha) {
		return store.Placement{}, ErrHandleMismatch
	}
	pkg, err := s.paidPackage(c)
	if err != nil {
		return store.Placement{}, err
	}
	p, err := s.d.Store.PlacementByHandle(ctx, sha)
	if err != nil {
		return store.Placement{}, err
	}
	x, err := s.checkExtend(p, pkg, func(p store.Placement) (bool, error) {
		return s.d.Store.HasExtendOrder(ctx, c.OrderID, p.ID)
	})
	if err != nil {
		return store.Placement{}, err
	}
	if x.replay {
		return s.replayExtend(ctx, x.p), nil
	}

	prov, ref, by := x.prov, x.ref, x.by
	var applied bool
	var ev Event
	id := p.ID
	// The lifetime is set inside the transaction so a refusal also takes back the extension
	err = s.d.Store.WithTx(ctx, func(tx pgx.Tx) error {
		ts := s.d.Store.Tx(tx)
		payer, err := s.payer(ctx, ts, c)
		if err != nil {
			return err
		}
		// Without a payer nothing below is keyed on the wallet, so no lock is held across
		// SetLifetime that every create without a payer would wait on
		if payer != "" {
			if err := ts.LockWallets(ctx, payer); err != nil {
				return err
			}
			if err := s.checkExtendPayer(ctx, tx, c.OrderID, payer); err != nil {
				return err
			}
		}
		p, applied, err = ts.ExtendWithOrder(ctx, store.Order{
			ID: c.OrderID, PlacementID: id, Payer: c.Payer, FeePayer: c.FeePayer,
			AmountMicros: c.PackageMicros(),
		}, by)
		if err != nil || !applied {
			return err
		}
		if payer != "" {
			if err := ts.SeeWallet(ctx, payer, s.now()); err != nil {
				return err
			}
		}
		edge, egress, err := s.addBudgets(ctx, ts, id, pkg)
		if err != nil {
			return err
		}
		p.EdgeBudgetBytes += edge
		p.EgressBudgetBytes += egress
		// A concurrent extension may have moved the deadline since the check above
		if err := withinLifetime(prov, p, *p.Deadline); err != nil {
			return err
		}
		lctx, cancel := step(ctx)
		defer cancel()
		if err := prov.SetLifetime(lctx, ref, p.Deadline.Add(lifetimeSlack)); err != nil {
			return fmt.Errorf("set lifetime: %w", err)
		}
		if err := s.insertDeadline(ctx, tx, id, *p.Deadline); err != nil {
			return err
		}
		ev = newEvent(p, "")
		ev.ExtendOrderID = c.OrderID
		return s.d.Store.InsertOutbox(ctx, tx, EventExtended, ev)
	})
	if errors.Is(err, store.ErrStateConflict) {
		if cur, rerr := s.d.Store.PlacementByID(ctx, id); rerr == nil &&
			cur.State != store.PlacementReady {
			return store.Placement{}, fmt.Errorf("%w: %w", ErrNotReady, err)
		}
		return store.Placement{}, fmt.Errorf("%w: %w", ErrBadToken, err)
	}
	if err != nil {
		return store.Placement{}, fmt.Errorf("extend deadline: %w", err)
	}
	if !applied {
		return s.replayExtend(ctx, p), nil
	}
	s.publish(ctx, ev)
	s.deadlineMoved(ctx, p)
	return p, nil
}

// addBudgets adds pkg's edge and egress bytes to the placement's budgets in ts and returns them
func (s *Saga) addBudgets(
	ctx context.Context, ts *store.Store, id uuid.UUID, pkg catalog.Package,
) (int64, int64, error) {
	edge := budget.BytesFor(pkg, s.d.Budget, budget.Edge)
	egress := budget.BytesFor(pkg, s.d.Budget, budget.Egress)
	return edge, egress, ts.AddPlacementBudgets(ctx, id, edge, egress)
}

// deadlineMoved records the grown budgets, moves the counters' expiry and tells the edges
func (s *Saga) deadlineMoved(ctx context.Context, p store.Placement) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), publishTimeout)
	defer cancel()
	if p.Deadline == nil {
		return
	}
	err := s.meter.Grow(ctx, p.ID, p.EdgeBudgetBytes, p.EgressBudgetBytes, *p.Deadline)
	if err != nil {
		// Reports then compare against the budgets they read, and the spent guards stand
		s.d.Log.Warn("record the grown budgets after an extension", "placement", p.ID, "err", err)
	}
	if err := s.meter.MoveDeadline(ctx, p.ID, *p.Deadline); err != nil {
		// The placement's next report with bytes moves them too
		s.d.Log.Warn("keep the used counters after an extension", "placement", p.ID, "err", err)
	}
	if s.d.Events == nil {
		return
	}
	limit := budget.Limit{Placement: p.ID, Deadline: *p.Deadline}
	edge, egress := limit, limit
	edge.Budget, egress.Budget = p.EdgeBudgetBytes, p.EgressBudgetBytes
	edgeLeft, err := s.meter.Remaining(ctx, budget.Edge, edge)
	var egressLeft uint64
	if err == nil {
		egressLeft, err = s.meter.Remaining(ctx, budget.Egress, egress)
	}
	if err != nil {
		// The edges ask CheckEdgeLeases before cutting at the old deadline, which repairs this
		s.d.Log.Warn("read bytes left after an extension, no DeadlineMoved sent",
			"placement", p.ID, "err", err)
		return
	}
	s.d.Events.Publish(ctx, &routerv1.EdgeEvent{Event: &routerv1.EdgeEvent_DeadlineMoved{
		DeadlineMoved: &routerv1.DeadlineMoved{
			PlacementId: p.ID.String(), Deadline: timestamppb.New(*p.Deadline),
			EdgeBytesRemaining: edgeLeft, EgressBytesRemaining: egressLeft,
		},
	}})
}

// CheckExtend answers as Extend would for a fresh order of the package now, changing nothing
func (s *Saga) CheckExtend(
	ctx context.Context, h, packageID string,
) (store.Placement, time.Time, error) {
	sha, err := hashHandle(h)
	if err != nil {
		return store.Placement{}, time.Time{}, err
	}
	pkg, err := s.lookupPackage(packageID)
	if err != nil {
		return store.Placement{}, time.Time{}, err
	}
	p, err := s.d.Store.PlacementByHandle(ctx, sha)
	if err != nil {
		return store.Placement{}, time.Time{}, err
	}
	x, err := s.checkExtend(p, pkg, nil)
	if err != nil {
		return store.Placement{}, time.Time{}, err
	}
	return x.p, x.p.Deadline.Add(x.by), nil
}

// Extension is a package that can extend a placement, with the refusal extending by it gets now
type Extension struct {
	Package catalog.Package
	Refusal error // nil, or one wrapping ErrNotReady or ErrExceedsLifetime
}

// ListExtensions checks each package that can extend the placement as CheckExtend would
func (s *Saga) ListExtensions(ctx context.Context, h string) ([]Extension, error) {
	sha, err := hashHandle(h)
	if err != nil {
		return nil, err
	}
	// One read serves every package, so the listing never mixes two states of the placement
	p, err := s.d.Store.PlacementByHandle(ctx, sha)
	if err != nil {
		return nil, err
	}
	var out []Extension
	for _, pkg := range s.d.Catalog.All() {
		_, err := s.checkExtend(p, pkg, nil)
		switch {
		case errors.Is(err, ErrNotExtension):
			continue
		case err != nil && !errors.Is(err, ErrNotReady) && !errors.Is(err, ErrExceedsLifetime):
			return nil, err
		}
		out = append(out, Extension{Package: pkg, Refusal: err})
	}
	slices.SortFunc(out, byListing)
	return out, nil
}

// byListing orders extensions by minutes, then price, then id
func byListing(a, b Extension) int {
	return cmp.Or(cmp.Compare(a.Package.Minutes, b.Package.Minutes),
		cmp.Compare(a.Package.PriceMicros, b.Package.PriceMicros),
		cmp.Compare(a.Package.ID, b.Package.ID))
}

// extension is a placement that passed the extend checks, with what extending it needs
type extension struct {
	p      store.Placement
	prov   provider.Provider
	ref    provider.Ref
	by     time.Duration
	replay bool // the order already extended the placement, so the later checks were skipped
}

// checkExtend makes the checks every extend caller shares, asking replayed before the ready one
func (s *Saga) checkExtend(
	p store.Placement, pkg catalog.Package, replayed func(store.Placement) (bool, error),
) (extension, error) {
	var err error
	if pkg.ID != p.PackageID && pkg.ExtensionOf != p.PackageID {
		return extension{}, fmt.Errorf("%w: %s on %s", ErrNotExtension, pkg.ID, p.PackageID)
	}
	x := extension{p: p, by: time.Duration(pkg.Minutes) * time.Minute}
	if replayed != nil {
		if x.replay, err = replayed(p); err != nil {
			return extension{}, err
		}
		if x.replay {
			return x, nil
		}
	}
	if p.State != store.PlacementReady {
		return extension{}, fmt.Errorf("%w: placement is %s", ErrNotReady, p.State)
	}
	if x.prov, x.ref, err = s.home(p); err != nil {
		return extension{}, err
	}
	if err := withinLifetime(x.prov, p, p.Deadline.Add(x.by)); err != nil {
		return extension{}, err
	}
	return x, nil
}

// withinLifetime refuses a deadline whose sandbox would outlast the provider's maximum lifetime
func withinLifetime(prov provider.Provider, p store.Placement, deadline time.Time) error {
	limit := prov.Capabilities().MaxLifetime
	if life := deadline.Add(lifetimeSlack).Sub(p.CreatedAt); limit > 0 && life > limit {
		// The limit can come from the settings file, so the refusal the caller sees leaves it out
		return fmt.Errorf("%w: %v from creation", ErrExceedsLifetime, life.Round(time.Second))
	}
	return nil
}

// replayExtend answers a repeated extend order with the placement as it is now
func (s *Saga) replayExtend(ctx context.Context, p store.Placement) store.Placement {
	if p.State != store.PlacementReady {
		return p
	}
	prov, ref, err := s.home(p)
	if err == nil {
		err = prov.SetLifetime(ctx, ref, p.Deadline.Add(lifetimeSlack))
	}
	if err != nil {
		s.d.Log.Warn("set lifetime on a repeated extend", "placement", p.ID, "err", err)
	}
	return p
}

/* Stop and terminate */

// Stop ends a ready placement at its owner's request
func (s *Saga) Stop(ctx context.Context, h string) (store.Placement, error) {
	sha, err := hashHandle(h)
	if err != nil {
		return store.Placement{}, err
	}
	p, err := s.d.Store.PlacementByHandle(ctx, sha)
	if err != nil {
		return store.Placement{}, err
	}
	return s.terminate(ctx, p, ReasonStop, func(ctx context.Context) (bool, error) {
		return s.d.Store.MarkPlacementTerminating(ctx, p.ID, ReasonStop, s.now())
	})
}

// Terminate ends a ready placement whose deadline is not after due and finishes a terminating one
func (s *Saga) Terminate(ctx context.Context, placementID uuid.UUID, due time.Time) error {
	p, err := s.d.Store.PlacementByID(ctx, placementID)
	if err != nil {
		return err
	}
	_, err = s.terminate(ctx, p, ReasonDeadline, func(ctx context.Context) (bool, error) {
		return s.d.Store.MarkPlacementDue(ctx, p.ID, ReasonDeadline, due, s.now())
	})
	return err
}

// endReasons maps a termination reason to the EndReason the edges get
var endReasons = map[string]routerv1.EndReason{
	ReasonStop:     routerv1.EndReason_END_REASON_STOPPED,
	ReasonDeadline: routerv1.EndReason_END_REASON_EXPIRED,
}

// placementEnded tells the edges to close all of the placement's traffic
func (s *Saga) placementEnded(ctx context.Context, id uuid.UUID, reason string) {
	if s.d.Events == nil {
		return
	}
	s.d.Events.Publish(ctx, &routerv1.EdgeEvent{Event: &routerv1.EdgeEvent_PlacementEnded{
		PlacementEnded: &routerv1.PlacementEnded{
			PlacementId: id.String(), Reason: endReasons[reason],
		},
	}})
}

// terminate marks the placement, destroys its sandbox, then ends it and frees its slot once
func (s *Saga) terminate(
	ctx context.Context, p store.Placement, reason string,
	mark func(context.Context) (bool, error),
) (store.Placement, error) {
	switch p.State {
	case store.PlacementTerminated:
		return p, nil
	case store.PlacementPlacing, store.PlacementFailed:
		return store.Placement{}, fmt.Errorf("%w: placement is %s", ErrNotReady, p.State)
	}
	// A caller hanging up halfway must not leave the sandbox running
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), terminateTimeout)
	defer cancel()
	moved, err := mark(ctx)
	if err != nil {
		return store.Placement{}, err
	}
	if moved {
		// The edges cut the placement's traffic now, not once the destroy is done
		s.placementEnded(ctx, p.ID, reason)
	} else {
		if p, err = s.d.Store.PlacementByID(ctx, p.ID); err != nil {
			return store.Placement{}, err
		}
		switch p.State {
		case store.PlacementTerminated:
			return p, nil
		case store.PlacementReady:
			return store.Placement{}, fmt.Errorf("%w: deadline %v", ErrNotDue, p.Deadline)
		case store.PlacementPlacing, store.PlacementFailed:
			return store.Placement{}, fmt.Errorf("%w: placement is %s", ErrNotReady, p.State)
		}
	}
	prov, ref, err := s.home(p)
	if err != nil {
		return store.Placement{}, err
	}
	// A stop retried while the destroy is in flight waits for it instead of calling the provider
	_, err, _ = s.destroys.Do(p.ID.String(), func() (any, error) {
		return nil, destroyRef(ctx, prov, ref)
	})
	if err != nil {
		return store.Placement{}, fmt.Errorf("destroy sandbox: %w", err)
	}

	var ended store.Placement
	var ev Event
	already := false
	err = s.d.Store.WithTx(ctx, func(tx pgx.Tx) error {
		ts := s.d.Store.Tx(tx)
		cur, err := ts.PlacementForUpdate(ctx, p.ID)
		if err != nil {
			return err
		}
		if cur.State == store.PlacementTerminated {
			ended, already = cur, true
			return nil
		}
		if err := ts.MarkPlacementTerminated(ctx, p.ID, s.now()); err != nil {
			return err
		}
		if _, err := ts.ReleasePlacementCapacity(ctx, p.ID); err != nil {
			return err
		}
		if err := ts.DeleteGatewayKeys(ctx, p.ID); err != nil {
			return err
		}
		if err := ts.DeleteSecretsOf(ctx, p.ID); err != nil {
			return err
		}
		if ended, err = ts.PlacementByID(ctx, p.ID); err != nil {
			return err
		}
		// The transition to terminating recorded why, whoever finishes the placement
		ev = newEvent(ended, cmp.Or(ended.TerminationReason, ReasonDeadline))
		return s.d.Store.InsertOutbox(ctx, tx, EventTerminated, ev)
	})
	if err != nil {
		return store.Placement{}, fmt.Errorf("mark terminated: %w", err)
	}
	if !already {
		s.publish(ctx, ev)
	}
	return ended, nil
}

/* Helpers */

// idempotencyKey names one attempt on one provider so a retried create finds the same sandbox
func idempotencyKey(orderID string, attempt int, providerName string) string {
	sum := sha256.Sum256([]byte(orderID + "|" + strconv.Itoa(attempt) + "|" + providerName))
	return hex.EncodeToString(sum[:])[:32]
}

// OwnedLabels selects every sandbox this saga's deployment created
func (s *Saga) OwnedLabels() map[string]string {
	return map[string]string{"bugnet": "1", "bugnet_deployment": s.d.DeploymentID}
}

func (s *Saga) labels(id uuid.UUID) map[string]string {
	l := s.OwnedLabels()
	l["placement"] = id.String()
	return l
}

// hashHandle reports a malformed handle as unknown, like a well formed one never issued
func hashHandle(h string) ([]byte, error) {
	sha, err := handle.Hash(h)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", store.ErrUnknownHandle, err)
	}
	return sha, nil
}

// home returns the provider and sandbox of a placement that became ready
func (s *Saga) home(p store.Placement) (provider.Provider, provider.Ref, error) {
	if p.ProviderRef == nil {
		return nil, provider.Ref{}, fmt.Errorf("placement %s has no sandbox", p.ID)
	}
	prov, ok := s.d.Registry.Get(p.Provider)
	if !ok {
		return nil, provider.Ref{}, fmt.Errorf("provider %q is not configured", p.Provider)
	}
	return prov, *p.ProviderRef, nil
}

func (s *Saga) insertDeadline(ctx context.Context, tx pgx.Tx, id uuid.UUID, at time.Time) error {
	_, err := s.d.River.InsertTx(ctx, tx, jobargs.Deadline{PlacementID: id},
		&river.InsertOpts{ScheduledAt: at})
	if err != nil {
		return fmt.Errorf("insert deadline job: %w", err)
	}
	return nil
}

// destroyRef treats a sandbox that is already gone as destroyed
func destroyRef(ctx context.Context, prov provider.Provider, ref provider.Ref) error {
	err := prov.Destroy(ctx, ref)
	if e, ok := provider.AsError(err); ok && e.Code == provider.CodeNotFound {
		return nil
	}
	return err
}

func (s *Saga) destroy(ctx context.Context, prov provider.Provider, ref provider.Ref) {
	ctx, cancel := step(ctx)
	defer cancel()
	if err := destroyRef(ctx, prov, ref); err != nil {
		s.d.Log.Error("destroy sandbox", "provider", prov.Name(), "sandbox", ref.ID, "err", err)
	}
}

// release frees the slot the placement holds, if any, so a repeat or a racing caller frees nothing
func (s *Saga) release(ctx context.Context, id uuid.UUID) {
	ctx, cancel := step(ctx)
	defer cancel()
	if _, err := s.d.Store.ReleasePlacementCapacity(ctx, id); err != nil {
		s.d.Log.Error("release capacity", "placement", id, "err", err)
	}
}

// step bounds one store or provider call so a stuck dependency cannot hold placing forever
func step(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, cleanupTimeout)
}
