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
//
// End ends a placement for a reason of CP 3.3 with the wallet actions of CP 6.4, under every
// payer's wallet lock and then the placement row, the order Extend takes them in. FlagEnded runs
// the abuse actions on a placement that already ended. placement.terminated prices a refundable
// end's unused seconds per order, newest first.
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
	"go.opentelemetry.io/otel/trace"
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
	"github.com/bugnet-sh/bugnet-router/internal/pool"
	"github.com/bugnet-sh/bugnet-router/internal/pricing"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/ratelimit"
	"github.com/bugnet-sh/bugnet-router/internal/sessions"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/telemetry"
	"github.com/bugnet-sh/bugnet-router/internal/telemetry/attr"
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

// ReasonPriceChanged is the failure reason when a start paid less than its package's price now
const ReasonPriceChanged = "price_changed"

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
	// issuedEvery spaces the log of an issued_at claim the router cannot use
	issuedEvery = time.Minute
)

// LifetimeSlack is how long a sandbox outlives its placement's deadline at the provider
const LifetimeSlack = lifetimeSlack

// EndTimeout bounds one End's destroy, which runs on past its caller's context
const EndTimeout = terminateTimeout

// cleanupTimeout bounds each store or provider call of placing so Wait cannot hang on one
var cleanupTimeout = 30 * time.Second

// destroyTimeout bounds each sandbox destroy of placing separately from its store calls
var destroyTimeout = cleanupTimeout

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
	Rule              pricing.Rule // prices every candidate
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
	// StatsMaxBytes is the largest stats file a stop reads, 4 KiB when zero
	StatsMaxBytes int64
	// Boot holds what each sandbox's setup and harden specs carry
	Boot BootSettings
	// SSH holds what each sandbox's ssh-up spec carries
	SSH SSHSettings
	// Wallets are the payer and wallet settings, a zero limit taking its default
	Wallets config.Wallets
	// Abuse holds the correlated window and maximum a provider outage is told by, each zero value
	// at its default
	Abuse config.Abuse
	// Sessions writes the session page's events, none when nil
	Sessions *sessions.Writer
	// SessionKeep is how long a record lives after its placement ends, 48 h when zero
	SessionKeep time.Duration
	// SessionAbuseHold keeps an abuse or operator end's record, 30 days when zero
	SessionAbuseHold time.Duration
	// CreatesPerSecond is each provider's create rate, absent for no bucket
	CreatesPerSecond map[string]int
	// Pools is the view of warm pool members that marks candidates warm, none when nil
	Pools *pool.Warmth
	// WarmFirst puts a candidate with a warm member ahead of every cold one, pools.order
	WarmFirst bool
	// PoolRoom is the lifetime a claimed member must still have past its package, one hour when
	// zero
	PoolRoom time.Duration
	// Reserve are the Reserve settings, the defaults when its hold is zero
	Reserve config.Reserve
	// ImageAccept is images.accept, the base image versions creates may start from
	ImageAccept []string
	// ImageChannel is images.channel, the channel every accepted build must name
	ImageChannel string
	// ReserveShare is the share of a pool's target open reservations may hold, pools.reserve_share,
	// one half when zero
	ReserveShare float64
	// QuoteHold is how long an estimate's quote holds for an order, none when zero
	QuoteHold time.Duration
	// Hedger starts a second attempt beside one that runs long, none for no hedges
	Hedger *Hedger
	// Telemetry makes the saga's spans, telemetry.Off when nil
	Telemetry *telemetry.Telemetry
}

// Saga runs placements from order to teardown
type Saga struct {
	d        Deps
	tr       trace.Tracer
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
	// issuedAt is when an unusable issued_at claim was last logged, in Unix nanoseconds
	issuedAt atomic.Int64
	// beforeMark runs between End's first read and its row lock, set only by tests
	beforeMark func()
	// bg runs the goroutines that keep reservations alive, which Close ends
	bg       sync.WaitGroup
	bgCtx    context.Context
	bgCancel context.CancelFunc
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
	if deps.Telemetry == nil {
		deps.Telemetry = telemetry.Off()
	}
	deps.PoolRoom = cmp.Or(deps.PoolRoom, time.Hour)
	if deps.Reserve.Hold == 0 {
		deps.Reserve = config.DefaultReserve()
	}
	deps.ReserveShare = cmp.Or(deps.ReserveShare, 0.5)
	deps.SessionKeep = cmp.Or(deps.SessionKeep, defaultSessionKeep)
	deps.SessionAbuseHold = cmp.Or(deps.SessionAbuseHold, defaultAbuseHold)
	w := deps.Wallets.OrDefaults()
	deps.Abuse = deps.Abuse.OrDefaults()
	s := &Saga{
		d: deps, w: w, tr: deps.Telemetry.Tracer("bugnet-router/saga"),
		meter: budget.New(deps.Valkey, deps.Prefix, deps.Budget).
			WithWalletCap(w.BytesPerDay),
		guard:  wallet.Guard{UsePayer: w.UsePayer, Facilitators: w.FacilitatorAddresses},
		limits: ratelimit.New(deps.Valkey, deps.Prefix, 0, 0),
		wallets: wallets.New(deps.Store, wallets.Settings{
			Facilitators: w.FacilitatorAddresses, StrikeThreshold: w.StrikeThreshold,
			StrikeWindow: w.StrikeWindow, StrikeBlockFor: w.StrikeBlockFor,
			EmitEvents: w.EmitEvents, Now: deps.Now,
		}),
	}
	s.bgCtx, s.bgCancel = context.WithCancel(context.Background())
	if roots := deps.Keys.Roots(); len(roots) > 0 {
		s.keyID = roots[0].ID
	}
	return s
}

// Wait blocks until every placing and preparing goroutine has finished
func (s *Saga) Wait() { s.wg.Wait() }

// Close stops the goroutines that keep reservations alive, which a reservation ready for its order
// leaves running until it is bound, released or lapsed
func (s *Saga) Close() {
	s.bgCancel()
	s.bg.Wait()
}

func (s *Saga) now() time.Time { return s.d.Now().UTC() }

/* Create */

// CreateInput is one CreatePlacement call
type CreateInput struct {
	Token        string
	Requirements placement.Requirements
	// Source is the estimate's resolved source, which the placement copies, nil for none
	Source *store.PlacementSource
	// QuoteID binds the quote's reservation when the order matches it, empty for none
	QuoteID string
	// EstimateID is the estimate whose quote the order may hold, uuid.Nil for none
	EstimateID uuid.UUID
}

// Create records a verified start order and places it, binding a matching reservation
//
// A package no provider can host fails at once with ErrNoCandidate, unless the quote's ready
// reservation matches, since a prepared sandbox then exists. An order whose payment no server's
// price fits fails at once with an UnderpaidError, unless its estimate's quote still holds a
// server that takes it.
func (s *Saga) Create(
	ctx context.Context, in CreateInput,
) (store.Placement, string, error) {
	arrived := s.now()
	req := in.Requirements
	c, err := s.verify(in.Token, ordertoken.KindStart)
	if err != nil {
		return store.Placement{}, "", err
	}
	pkg, err := s.lookupPackage(c.PackageID)
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
		SecondsBought: int64(pkg.Minutes) * 60,
		Tx:            c.Tx,
		Source:        in.Source,
		PaidAt:        s.paidAt(c, arrived),
	}
	bind := s.bindTo(in, pkg)
	t := terms{paid: c.PackageMicros()}
	t.prefer = s.heldServer(ctx, in.EstimateID, pkg, t.paid)
	if np.FailureReason, err = s.startRefusal(ctx, c, pkg, req, t, bind); err != nil {
		return store.Placement{}, "", fmt.Errorf("find candidates: %w", err)
	}
	rec, err := s.record(ctx, c, np, bind)
	if err != nil {
		return store.Placement{}, "", fmt.Errorf("record placement: %w", err)
	}
	p, wait := rec.p, rec.wait
	if !bytes.Equal(p.HandleSHA256, sha) {
		return store.Placement{}, "", fmt.Errorf(
			"%w: order %s belongs to another placement", ErrBadToken, c.OrderID)
	}
	var root trace.Span
	if rec.created {
		ctx, root = s.d.Telemetry.StartOrderRoot(ctx, p)
		s.sessionStarted(ctx, p, pkg, c)
	} else {
		ctx = s.d.Telemetry.JoinOrder(ctx, p)
	}
	if p.State == store.PlacementFailed {
		var refusal error
		switch p.FailureReason {
		case ReasonNoCandidate:
			refusal = fmt.Errorf("%w for package %s", placement.ErrNoCandidate, pkg.ID)
		case ReasonPriceChanged:
			refusal = underpaid(c, pkg, s.startPrice(pkg, req))
		case ReasonWalletBusy:
			refusal = ErrWalletBusy
		case ReasonWalletRateLimited:
			if !rec.created {
				wait = s.startWait(ctx, p.Payer)
			}
			refusal = &WaitError{Wait: wait}
		}
		if refusal != nil {
			if rec.created {
				s.publish(ctx, newEvent(p, p.FailureReason))
				s.sessionEnded(ctx, p.ID)
			}
			endRoot(root, p)
			return p, h, refusal
		}
	}
	if rec.bound != nil {
		s.afterBind(ctx, rec.bound, p, pkg, arrived)
		endRoot(root, p)
		return p, h, nil
	}
	if rec.created {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			pctx := context.WithoutCancel(ctx)
			s.place(pctx, p, pkg, req, t, rec.awaiting, arrived)
			s.endOrderRoot(pctx, root, p.ID)
		}()
	}
	return p, h, nil
}

// terms are what a start order paid for its package and the server its quote holds, nil for none
type terms struct {
	paid   int64
	prefer *placement.Server
}

// heldServer is the server an estimate's quote holds pkg on when paid covers it, nil for none
func (s *Saga) heldServer(
	ctx context.Context, estimate uuid.UUID, pkg catalog.Package, paid int64,
) *placement.Server {
	if estimate == uuid.Nil || s.d.QuoteHold <= 0 {
		return nil
	}
	q, ok, err := s.d.Store.QuoteFor(ctx, estimate, pkg.ID, s.d.QuoteHold)
	if err != nil {
		s.d.Log.Warn("read an order's quote, it starts at the price now", "estimate", estimate,
			"package", pkg.ID, "err", err)
		return nil
	}
	if !ok || paid < q.PriceMicros {
		return nil
	}
	return &placement.Server{Provider: q.Provider, Size: q.SizeName}
}

// startRefusal is why a start cannot begin, empty when a candidate fits what it paid
func (s *Saga) startRefusal(
	ctx context.Context, c ordertoken.Claims, pkg catalog.Package, req placement.Requirements,
	t terms, bind *bindRequest,
) (string, error) {
	if !covers(c, pkg.PriceMicros) {
		return ReasonPriceChanged, nil
	}
	// The quoted server is exempt from the budget, so the price the order was quoted starts there
	_, err := s.within(pkg, req, nil, t.paid, t.paid, t.prefer)
	switch {
	case err == nil:
		return "", nil
	case !errors.Is(err, placement.ErrNoCandidate):
		return "", err
	}
	if !pkg.FixedPrice {
		// A server takes the start at its price now, so the payment is what fell short
		if _, ok := s.d.Engine.Quote(s.quoteInputs(pkg, req)); ok {
			return ReasonPriceChanged, nil
		}
	}
	if s.readyFits(ctx, bind) {
		return "", nil
	}
	return ReasonNoCandidate, nil
}

// paidAt is when the order was paid as the 5 s target measures it, logging a claim it cannot use
// once a minute
func (s *Saga) paidAt(c ordertoken.Claims, arrived time.Time) time.Time {
	at, ok := c.PaidAt(arrived)
	if !ok {
		now := s.now()
		last := s.issuedAt.Load()
		if now.UnixNano()-last >= int64(issuedEvery) &&
			s.issuedAt.CompareAndSwap(last, now.UnixNano()) {
			s.d.Log.Warn("an order's issued_at is not in the 60 s before it arrived, so the "+
				"time it arrived counts as its payment time", "order", c.OrderID)
		}
	}
	return at
}

// recorded is what the order's transaction did
type recorded struct {
	p       store.Placement
	created bool
	wait    time.Duration
	// bound is the reservation's sandbox that became the placement's, nil for none
	bound *boundStart
	// awaiting is the reservation still preparing that the placement waits for, nil for none
	awaiting *store.Reservation
}

// record stores the order and its placement under the payer's lock, as CP 7.7 says, and binds the
// quote's reservation after the wallet checks pass and before the placement is inserted (CP 18.5.5)
func (s *Saga) record(
	ctx context.Context, c ordertoken.Claims, np store.NewPlacement, bind *bindRequest,
) (recorded, error) {
	var out recorded
	err := s.d.Store.WithTx(ctx, func(tx pgx.Tx) error {
		out = recorded{}
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
			FeePayer: np.FeePayer, AmountMicros: np.AmountMicros, SecondsBought: np.SecondsBought,
			Tx: np.Tx,
		})
		if err != nil {
			return err
		}
		if !inserted {
			// A retried order answers as it did, with no check run again
			out.p, err = ts.PlacementByID(ctx, id)
			return err
		}
		if out.wait, err = s.checkStart(ctx, tx, &np); err != nil {
			return err
		}
		if np.Payer != "" {
			if err := ts.SeeWallet(ctx, np.Payer, s.now()); err != nil {
				return err
			}
		}
		// A refused or unmatched order keeps its fresh id, so the reservation's stays free
		var res *store.Reservation
		if bind != nil && np.FailureReason == "" {
			if res, err = s.reservationFor(ctx, tx, bind); err != nil {
				return err
			}
			if res != nil {
				np.ID, np.KeyID = res.PlacementID, res.KeyID
				if err := ts.RepointOrder(ctx, np.OrderID, np.ID); err != nil {
					return err
				}
			}
		}
		if out.p, err = ts.InsertPlacement(ctx, np); err != nil {
			return err
		}
		out.created = true
		// The record is made with the placement, so the first answer carries its link
		if _, err := ts.CreateSessionRecord(ctx, out.p.ID); err != nil {
			return err
		}
		if np.FailureReason != "" {
			if err := s.endSession(ctx, tx, out.p); err != nil {
				return err
			}
			return s.d.Store.InsertOutbox(ctx, tx, EventFailed, newEvent(out.p, np.FailureReason))
		}
		if res == nil {
			return nil
		}
		if res.State == store.ReservationReady && res.Prepared != nil {
			if out.bound, err = s.bindInTx(ctx, tx, out.p, bind.pkg, *res); err != nil {
				return err
			}
			out.p, err = ts.PlacementByID(ctx, out.p.ID)
			return err
		}
		if err := ts.AwaitReservation(ctx, res.ID); err != nil {
			return err
		}
		out.awaiting = res
		return nil
	})
	return out, err
}

func (s *Saga) inputs(
	pkg catalog.Package, req placement.Requirements, tried map[string]bool,
) placement.Inputs {
	return placement.Inputs{
		Package: pkg, Requirements: req, Rule: s.d.Rule, Exclude: tried,
		WarmFirst: s.d.WarmFirst, ImageAccept: s.d.ImageAccept,
	}
}

// within are the candidates of an attempt that fit left of paid, ErrNoCandidate when none does
func (s *Saga) within(
	pkg catalog.Package, req placement.Requirements, tried map[string]bool, paid, left int64,
	prefer *placement.Server,
) ([]placement.Candidate, error) {
	in := s.inputs(pkg, req, tried)
	in.Prefer = prefer
	switch {
	case pkg.FixedPrice:
		// A fixed price is the same on every server and its fit holds the server's cost under it
	case paid == 0:
		// A budget of 0 reads as none, so an order that paid nothing keeps the servers priced 0
	case left <= 0:
		return nil, placement.ErrNoCandidate
	default:
		in.BudgetMicros = left
	}
	cands, err := s.d.Engine.Candidates(in)
	if err != nil || pkg.FixedPrice || paid != 0 {
		return cands, err
	}
	cands = slices.DeleteFunc(cands, func(c placement.Candidate) bool { return c.PriceMicros > 0 })
	if len(cands) == 0 {
		return nil, placement.ErrNoCandidate
	}
	return cands, nil
}

// quoteInputs ask for pkg as Estimate and PricePackage quote it
func (s *Saga) quoteInputs(pkg catalog.Package, req placement.Requirements) placement.Inputs {
	return placement.Inputs{
		Package: pkg, Requirements: req, Rule: s.d.Rule, ImageAccept: s.d.ImageAccept,
	}
}

// startPrice is what a start of pkg costs now, its fixed price or the live quote's
func (s *Saga) startPrice(pkg catalog.Package, req placement.Requirements) int64 {
	if pkg.FixedPrice {
		return pkg.PriceMicros
	}
	q, _ := s.d.Engine.Quote(s.quoteInputs(pkg, req))
	return q.PriceMicros
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

// UnderpaidError is ErrUnderpaid with what the order paid and the price the router asks now
type UnderpaidError struct {
	PackageID   string
	PaidMicros  int64
	PriceMicros int64
}

func (e *UnderpaidError) Error() string {
	return fmt.Sprintf("%v: paid %d, %s costs %d", ErrUnderpaid, e.PaidMicros, e.PackageID,
		e.PriceMicros)
}

// Unwrap makes an UnderpaidError match ErrUnderpaid
func (e *UnderpaidError) Unwrap() error { return ErrUnderpaid }

// covers reports whether the order paid price, the x402 allowance and fee left out
func covers(c ordertoken.Claims, price int64) bool {
	return c.PackageMicros() >= price
}

// underpaid is the refusal of an order that paid less than price, what pkg costs now
func underpaid(c ordertoken.Claims, pkg catalog.Package, price int64) *UnderpaidError {
	return &UnderpaidError{PackageID: pkg.ID, PaidMicros: c.PackageMicros(), PriceMicros: price}
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

func (s *Saga) reserve(
	ctx context.Context, id uuid.UUID, providerName string, attempt int,
) (bool, error) {
	ctx, cancel := step(ctx)
	defer cancel()
	// A hedge holds a slot of its own, so the first attempt's stays where it is
	if r := runOf(ctx); r != nil && r.hedge {
		return s.d.Store.ReserveHedgeCapacity(ctx, id, providerName, attempt)
	}
	return s.d.Store.ReservePlacementCapacity(ctx, id, providerName, attempt)
}

// wantsPool reports whether an order for pkg on c would have taken a pool member if one waited
func (s *Saga) wantsPool(pkg catalog.Package, c placement.Candidate) bool {
	return len(pkg.EgressAllow) == 0 && s.d.Pools.HasPool(c.Provider.Name(), c.Size.Name)
}

// countMiss records one miss for the order the first time it wants a pool and finds it empty
func (s *Saga) countMiss(ctx context.Context, c placement.Candidate, missed *atomic.Bool) {
	if !missed.CompareAndSwap(false, true) {
		return
	}
	ctx, cancel := step(ctx)
	defer cancel()
	err := s.d.Store.RecordMiss(ctx, c.Provider.Name(), c.Size.Name, s.now())
	if err != nil {
		s.d.Log.Warn("record pool miss", "provider", c.Provider.Name(), "err", err)
	}
}

// attemptCold holds a capacity slot on c's provider and gives it back unless the sandbox is ready
func (s *Saga) attemptCold(
	ctx, budget context.Context, p store.Placement, pkg catalog.Package,
	req placement.Requirements, c placement.Candidate, n int, missed *atomic.Bool,
) outcome {
	prov := c.Provider
	name := prov.Name()
	if !s.takeCreate(ctx, name) {
		s.release(ctx, p.ID, n)
		return outcome{
			failOver: true, reason: reasonRateLimited, detail: "no create token for " + name,
		}
	}
	if s.wantsPool(pkg, c) && !c.Warm {
		s.countMiss(ctx, c, missed)
	}
	hourly, fee := quote(prov, c.Size)
	rec := store.Attempt{
		PlacementID:    p.ID,
		Attempt:        n,
		Provider:       name,
		IdempotencyKey: idempotencyKey(p.OrderID, n, name),
		State:          attemptCreating,
		SizeName:       c.Size.Name,
		HourlyMicros:   hourly,
		StartFeeMicros: fee,
		Path:           store.PathCold,
		Hedge:          hedgeRun(ctx),
	}
	if err := s.recordAttempt(ctx, rec); err != nil {
		s.release(ctx, p.ID, n)
		return outcome{reason: reasonInternal, detail: err.Error()}
	}
	s.markHedged(ctx, p.ID)
	started := s.now()
	smp := s.sampleOf(p, prov, c.Size, started)
	smp.StartPath = store.PathCold
	wctx, wcancel := work(budget)
	ref, err := s.create(wctx, prov, provider.CreateRequest{
		IdempotencyKey: rec.IdempotencyKey,
		Size:           c.Size,
		Labels:         s.labels(p.ID),
		Lifetime:       time.Duration(pkg.Minutes)*time.Minute + lifetimeSlack,
		Egress:         s.d.Boot.egressAllow(pkg.EgressAllow),
	})
	wcancel()
	smp.CreateMS = int(s.now().Sub(started).Milliseconds())
	smp.SandboxID = ref.ID
	if err == nil {
		rec.SandboxID = ref.ID
		if err := s.recordAttempt(ctx, rec); err != nil {
			s.d.Log.Error("record attempt sandbox", "placement", p.ID, "err", err)
		}
	}
	if err != nil {
		if s.countsAgainst(ctx, name, err, true) {
			s.d.Tracker.RecordFailure(name)
		}
		s.recordSample(ctx, prov, c.Size, smp, err)
		s.abandon(ctx, prov, rec, nil, err)
		return outcome{
			failOver: canFailOver(err), reason: createReason(err),
			detail: fmt.Sprintf("create on %s: %v", name, err),
		}
	}
	bctx, bcancel := work(ctx)
	ref, boot, err := s.Boot(bctx, prov, ref, BootPlan{
		Placement: p.ID, KeyID: p.KeyID, NeedPreview: req.Preview, NeedSSH: req.SSH,
		Restricted: pkg.EgressAllow,
	})
	bcancel()
	if boot.ReadyMS >= 0 {
		smp.ReadyMS = smp.CreateMS + boot.ReadyMS
	}
	smp.RunnerMS, smp.GateMS = boot.RunnerMS, boot.GateMS
	if boot.Image != nil {
		smp.ImageVersion = boot.Image.Version
	}
	smp.Reseed = boot.Reseed
	if err != nil {
		noBuild := errors.Is(err, errNoRunnerBuild)
		reason := reasonNoRunner
		if !noBuild {
			reason = reasonBootFailed
			if errors.Is(err, errImageMismatch) {
				reason = reasonImageMismatch
			}
			if s.countsAgainst(ctx, name, err, false) {
				s.d.Tracker.RecordFailure(name)
			}
			s.recordSample(ctx, prov, c.Size, smp, err)
		}
		s.abandon(ctx, prov, rec, &ref, err)
		return outcome{
			failOver: !noBuild, reason: reason, detail: fmt.Sprintf("boot on %s: %v", name, err),
		}
	}
	s.stampReady(&smp, p)
	s.recordSample(ctx, prov, c.Size, smp, nil)
	prep := Prepared{
		Ref: ref, KeyID: p.KeyID, GateReadyAt: boot.GateReadyAt, SSHHost: boot.SSH,
		CAVersion: boot.CAVersion, GateMS: boot.GateMS, Image: boot.Image,
	}
	return s.commitReady(ctx, p, pkg, c, rec, prep, nil, started)
}

// attemptWarm claims a warm member for c when the engine marked it warm and runs the pooled
// attempt on it, reporting false when no attempt ran and the candidate goes on as a cold create
//
// The claim, the attempt row and the slot move are one transaction. A claim that finds no member
// records a miss in that transaction and a member that cannot be woken is retired, both falling
// to a cold create on the same candidate.
func (s *Saga) attemptWarm(
	ctx, budget context.Context, p store.Placement, pkg catalog.Package,
	req placement.Requirements, c placement.Candidate, n int, missed *atomic.Bool,
) (outcome, bool) {
	if !c.Warm {
		return outcome{}, false
	}
	// A provider that keeps its own pool hands members out by claim, never by a row of ours
	if cl, ok := provider.As[provider.PoolClaimer](c.Provider); ok {
		return s.attemptClaim(ctx, budget, cl, p, pkg, req, c, n)
	}
	prov := c.Provider
	name := prov.Name()
	hourly, fee := quote(prov, c.Size)
	rec := store.Attempt{
		PlacementID: p.ID, Attempt: n, Provider: name, State: attemptCreating,
		SizeName: c.Size.Name, HourlyMicros: hourly, StartFeeMicros: fee, Path: store.PathPooled,
		IdempotencyKey: idempotencyKey(p.OrderID, n, name), Hedge: hedgeRun(ctx),
	}
	started := s.now()
	var m store.Member
	claimed := false
	cctx, cancel := step(ctx)
	err := s.d.Store.WithTx(cctx, func(tx pgx.Tx) error {
		ts := s.d.Store.Tx(tx)
		if err := ts.RecordAttempt(cctx, rec); err != nil {
			return err
		}
		// The member must be young enough for the package, its extension room and the slack
		bornAfter := time.Time{}
		if life := prov.Capabilities().MaxLifetime; life > 0 {
			need := time.Duration(pkg.Minutes)*time.Minute + s.d.PoolRoom + lifetimeSlack
			bornAfter = s.now().Add(need - life)
		}
		var ok bool
		var err error
		m, ok, err = ts.ClaimWarm(cctx, name, c.Size.Name, bornAfter,
			store.Holder{Placement: p.ID, Attempt: n})
		if err != nil {
			return err
		}
		if !ok {
			if s.wantsPool(pkg, c) && !missed.Load() {
				if err := ts.RecordMiss(cctx, name, c.Size.Name, s.now()); err != nil {
					return err
				}
				missed.Store(true)
			}
			return nil
		}
		claimed = true
		rec.IdempotencyKey, rec.SandboxID, rec.MemberID = m.IdempotencyKey, "", m.ID
		if m.ProviderRef != nil {
			rec.SandboxID = m.ProviderRef.ID
		}
		return ts.RecordAttempt(cctx, rec)
	})
	cancel()
	switch {
	case err == nil && !claimed:
		return outcome{}, false
	case err != nil:
		return outcome{reason: reasonInternal, detail: fmt.Sprintf("claim on %s: %v", name, err),
			failOver: true}, true
	case m.ProviderRef == nil:
		s.retireMember(ctx, m.ID, store.EndFailed, "claimed a member without a sandbox")
		s.release(ctx, p.ID, n)
		return outcome{}, false
	}
	s.markHedged(ctx, p.ID)
	smp := s.sampleOf(p, prov, c.Size, started)
	smp.StartPath, smp.MemberID, smp.SandboxID = store.PathPooled, m.ID, m.ProviderRef.ID
	ref := *m.ProviderRef
	if pk, ok := provider.As[provider.Parker](prov); ok {
		wctx, wcancel := work(budget)
		err := pk.Unpark(wctx, ref)
		wcancel()
		smp.WakeMS = int(s.now().Sub(started).Milliseconds())
		if err != nil {
			s.d.Log.Warn("pooled sandbox did not wake, creating cold", "provider", name,
				"member", m.ID, "err", err)
			s.recordSample(ctx, prov, c.Size, smp, err)
			s.retireMember(ctx, m.ID, store.EndFailed, "unpark failed")
			s.release(ctx, p.ID, n)
			return outcome{}, false
		}
	}
	pctx, pcancel := work(ctx)
	prep, err := s.Prepare(pctx, prov, ref, p.ID, req, true)
	pcancel()
	smp.GateMS = prep.GateMS
	// The fill's Boot read the manifest and the member row kept it
	prep.Image = manifestOfRecord(m.Image)
	if prep.Image != nil {
		smp.ImageVersion = prep.Image.Version
	}
	smp.Reseed = prep.Reseed
	if err != nil {
		if s.countsAgainst(ctx, name, err, false) {
			s.d.Tracker.RecordFailure(name)
		}
		s.recordSample(ctx, prov, c.Size, smp, err)
		s.abandon(ctx, prov, rec, nil, err)
		return outcome{
			failOver: true, reason: reasonBootFailed,
			detail: fmt.Sprintf("prepare on %s: %v", name, err),
		}, true
	}
	s.stampReady(&smp, p)
	s.recordSample(ctx, prov, c.Size, smp, nil)
	return s.commitReady(ctx, p, pkg, c, rec, prep, &m, started), true
}

// stampReady records on an attempt's sample how long the order took from its arrival and from its
// payment to the sandbox being ready
func (s *Saga) stampReady(smp *store.StartSample, p store.Placement) {
	now := s.now()
	smp.BindMS = max(1, int(now.Sub(p.CreatedAt).Milliseconds()))
	if p.PaidAt != nil {
		smp.PaidToReadyMS = max(1, int(now.Sub(*p.PaidAt).Milliseconds()))
	}
}

// create retries a create whose outcome is unknown under the same key on the same provider
func (s *Saga) create(
	ctx context.Context, prov provider.Provider, req provider.CreateRequest,
) (provider.Ref, error) {
	for i := 0; ; i++ {
		ref, err := prov.Create(ctx, req)
		e, ok := provider.AsError(err)
		if ok && e.Code == provider.CodeOutcomeUnknown {
			// A create that may still boot and bill bars a hedge beside it
			if r := runOf(ctx); r != nil {
				r.unknown.Store(true)
			}
		}
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
//
// A pooled attempt passes the member it claimed, whose row stays bound to the placement.
func (s *Saga) commitReady(
	ctx context.Context, p store.Placement, pkg catalog.Package, c placement.Candidate,
	rec store.Attempt, prep Prepared, mem *store.Member, started time.Time,
) outcome {
	ctx, span := s.span(ctx, "saga.commit_ready")
	defer span.End()
	prov := c.Provider
	ref := prep.Ref
	readyAt := s.now()
	if mem == nil {
		s.d.Tracker.RecordReady(prov.Name(), readyAt.Sub(started))
	}
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
		var err error
		path := store.PathCold
		if mem != nil {
			path = store.PathPooled
		}
		ev, err = s.readyWrites(tctx, tx, p, pkg, prov, c.Size.Name, rec, prep, mem, path, readyAt,
			deadline)
		return err
	})
	if err != nil {
		s.abandon(ctx, prov, rec, &ref, err)
		if errors.Is(err, store.ErrStateConflict) {
			s.d.Log.Info("placement left placing before it was ready", "placement", p.ID)
			return outcome{done: true}
		}
		return outcome{reason: reasonInternal, detail: fmt.Sprintf("commit ready: %v", err)}
	}
	// The winner's commit cuts every other attempt short at once
	if r := runOf(ctx); r != nil {
		r.race.win(r)
	}
	s.publish(ctx, ev)
	if s.d.Sessions != nil {
		s.d.Sessions.Placement(ctx, p.ID, sessions.PhaseReady, pkg, &deadline, "")
	}
	return outcome{done: true, kept: &ref}
}

// readyWrites makes a placing placement ready inside tx with everything that goes with it, the
// gate and CA records, the SSH host and alias, the budgets, the attempt, the deadline job and the
// ready event
func (s *Saga) readyWrites(
	ctx context.Context, tx pgx.Tx, p store.Placement, pkg catalog.Package,
	prov provider.Provider, size string, rec store.Attempt, prep Prepared, mem *store.Member,
	path string, readyAt, deadline time.Time,
) (Event, error) {
	ts := s.d.Store.Tx(tx)
	err := ts.MarkPlacementReady(ctx, p.ID, prep.Ref, size, readyAt, deadline,
		recordOfManifest(prep.Image))
	if err != nil {
		return Event{}, err
	}
	// A ready that is not the bind frees the reservation the placement awaited, if any, and moves
	// the winner's slot to the placement row, freeing the one the row held for a hedge's loser
	if path != store.PathReserved {
		if err := s.releaseAwaitedTx(ctx, tx, p.ID); err != nil {
			return Event{}, err
		}
		if err := ts.WinAttemptSlot(ctx, p.ID, rec.Attempt); err != nil {
			return Event{}, err
		}
	}
	if err := s.x402AtReady(ctx, tx, p.ID, s.d.Registry.Capabilities(prov)); err != nil {
		return Event{}, err
	}
	var gateAt *time.Time
	if !prep.GateReadyAt.IsZero() {
		gateAt = &prep.GateReadyAt
	}
	if err := ts.SetPlacementGate(ctx, p.ID, gateAt, prep.CAVersion); err != nil {
		return Event{}, err
	}
	var bornAt *time.Time
	if mem != nil {
		bornAt = mem.SandboxCreatedAt
	}
	if err := ts.SetPlacementStart(ctx, p.ID, path, bornAt); err != nil {
		return Event{}, err
	}
	if prep.SSHHost != nil {
		if err := ts.PutSSHHost(ctx, *prep.SSHHost); err != nil {
			return Event{}, err
		}
	}
	if s.d.Registry.Capabilities(prov).SSH {
		s.mintAlias(ctx, ts, p.ID)
	}
	if _, _, err := s.addBudgets(ctx, ts, p.ID, pkg); err != nil {
		return Event{}, err
	}
	ready := rec
	ready.State = attemptReady
	if err := ts.RecordAttempt(ctx, ready); err != nil {
		return Event{}, err
	}
	if err := s.insertDeadline(ctx, tx, p.ID, deadline); err != nil {
		return Event{}, err
	}
	cur, err := ts.PlacementByID(ctx, p.ID)
	if err != nil {
		return Event{}, err
	}
	ev := newEvent(cur, "")
	return ev, s.d.Store.InsertOutbox(ctx, tx, EventReady, ev)
}

// abandon destroys an attempt's sandbox if it has one, records the failure and frees the slot
//
// A pooled attempt retires its member instead, which destroys the sandbox through pool_retire and
// leaves the slot to the placement row that took it over.
func (s *Saga) abandon(
	ctx context.Context, prov provider.Provider, rec store.Attempt, ref *provider.Ref, err error,
) {
	ctx, span := s.span(ctx, "saga.abandon", attr.Reason(errorReason(err)))
	defer span.End()
	if rec.MemberID != uuid.Nil {
		s.retireMember(ctx, rec.MemberID, store.EndFailed, err.Error())
	} else if ref != nil {
		s.destroy(ctx, prov, *ref)
	}
	rec.State = attemptFailed
	rec.Error = err.Error()
	if e, ok := provider.AsError(err); ok {
		rec.ErrorCode = string(e.Code)
	}
	// An attempt another one outran says so, whatever its calls answered when they were cut short
	if lostRun(ctx) {
		rec.ErrorCode = ErrorHedgeLost
	}
	if err := s.recordAttempt(ctx, rec); err != nil {
		s.d.Log.Error("record failed attempt", "placement", rec.PlacementID, "err", err)
	}
	s.release(ctx, rec.PlacementID, rec.Attempt)
}

// retireMember sets a member to retiring and queues the job that destroys its sandbox
func (s *Saga) retireMember(ctx context.Context, id uuid.UUID, reason, detail string) {
	ctx, cancel := step(context.WithoutCancel(ctx))
	defer cancel()
	err := s.d.Store.WithTx(ctx, func(tx pgx.Tx) error {
		return s.retireMemberTx(ctx, tx, id, reason, detail)
	})
	if err != nil {
		s.d.Log.Error("retire pool member", "member", id, "err", err)
	}
}

// retireMemberTx is retireMember inside tx, so a transaction that moves a placement on retires the
// members bound to it in the same commit
func (s *Saga) retireMemberTx(
	ctx context.Context, tx pgx.Tx, id uuid.UUID, reason, detail string,
) error {
	old, err := s.d.Store.Tx(tx).RetireMember(ctx, id, reason, truncate(detail, 500))
	if err != nil || old == "" {
		return err
	}
	_, err = s.d.River.InsertTx(ctx, tx, jobargs.PoolRetire{Member: id, Reason: reason}, nil)
	if err != nil {
		return fmt.Errorf("insert pool_retire: %w", err)
	}
	return nil
}

// truncate cuts a message to n bytes, so no provider text of any length lands in a row
func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
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
	ctx, span := s.span(ctx, "saga.fail", attr.Reason(reason))
	defer span.End()
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
		s.sessionEnded(ctx, p.ID)
	}
	s.sweep(ctx, p.ID, attempted, nil)
}

// FailStuck fails a placing placement whose saga went away as placing would, else does nothing
func (s *Saga) FailStuck(ctx context.Context, id uuid.UUID, reason string) error {
	ctx, span := s.orderSpanByID(ctx, id, "saga.fail_stuck", attr.Reason(reason))
	defer span.End()
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
		if _, err := ts.ReleaseAttemptSlots(ctx, id); err != nil {
			return err
		}
		// A member bound to a placement that failed has no owner left, so it goes with it
		members, err := ts.MembersOfPlacement(ctx, id)
		if err != nil {
			return err
		}
		for _, m := range members {
			if err := s.retireMemberTx(ctx, tx, m.ID, store.EndFailed, reason); err != nil {
				return err
			}
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
	s.sessionEnded(ctx, id)
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
	// The reservation an order awaited goes with it, as does its member
	if err := s.releaseAwaitedTx(ctx, tx, id); err != nil {
		return Event{}, err
	}
	cur, err := ts.PlacementByID(ctx, id)
	if err != nil {
		return Event{}, err
	}
	if err := s.endSession(ctx, tx, cur); err != nil {
		return Event{}, err
	}
	ev := newEvent(cur, reason)
	return ev, s.d.Store.InsertOutbox(ctx, tx, EventFailed, ev)
}

// releaseAwaitedTx releases the reservation a placement awaits and inserts pool_retire for its
// member, in the transaction that moves the placement out of placing (CP 18.5.5.1)
func (s *Saga) releaseAwaitedTx(ctx context.Context, tx pgx.Tx, id uuid.UUID) error {
	member, ok, err := s.d.Store.Tx(tx).ReleaseAwaitedOf(ctx, id, s.now())
	if err != nil || !ok {
		return err
	}
	return s.retireMemberTx(ctx, tx, member, store.EndUnused, "the order left placing")
}

// sweep destroys every sandbox labelled for the placement on provs except keep
func (s *Saga) sweep(
	ctx context.Context, id uuid.UUID, provs []provider.Provider, keep *provider.Ref,
) {
	ctx, span := s.span(ctx, "saga.sweep")
	destroyed := 0
	defer func() {
		span.SetAttributes(attr.Count(destroyed))
		span.End()
	}()
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
			destroyed++
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
	pkg, err := s.lookupPackage(c.PackageID)
	if err != nil {
		return store.Placement{}, err
	}
	p, err := s.d.Store.PlacementByHandle(ctx, sha)
	if err != nil {
		return store.Placement{}, err
	}
	ctx = s.d.Telemetry.JoinOrder(ctx, p)
	ctx, span := s.span(ctx, "saga.extend",
		attr.ExtendOrderHash(s.d.Telemetry.OrderHash(c.OrderID)), attr.PackageID(pkg.ID))
	defer span.End()
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
			AmountMicros: c.PackageMicros(), SecondsBought: int64(by / time.Second),
			Tx: c.Tx,
		}, by)
		if err != nil || !applied {
			return err
		}
		// Only the call that applies the order checks the price, so every retry answers as it did
		if !covers(c, x.price) {
			return underpaid(c, pkg, x.price)
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
	if s.d.Sessions != nil {
		s.d.Sessions.Placement(ctx, id, sessions.PhaseExtended, pkg, p.Deadline, "")
		amount, tx := paymentClaims(c)
		s.d.Sessions.Payment(ctx, id, string(ordertoken.KindExtend), amount, tx)
	}
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
	limit := budget.Limit{Placement: p.ID, Deadline: *p.Deadline, Payer: p.Payer}
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

// CheckExtend answers as Extend would for a fresh order now, with its price, changing nothing
func (s *Saga) CheckExtend(
	ctx context.Context, h, packageID string,
) (store.Placement, time.Time, int64, error) {
	sha, err := hashHandle(h)
	if err != nil {
		return store.Placement{}, time.Time{}, 0, err
	}
	pkg, err := s.lookupPackage(packageID)
	if err != nil {
		return store.Placement{}, time.Time{}, 0, err
	}
	p, err := s.d.Store.PlacementByHandle(ctx, sha)
	if err != nil {
		return store.Placement{}, time.Time{}, 0, err
	}
	x, err := s.checkExtend(p, pkg, nil)
	if err != nil {
		return store.Placement{}, time.Time{}, 0, err
	}
	return x.p, x.p.Deadline.Add(x.by), x.price, nil
}

// Extension is a package that can extend a placement, with the refusal extending by it gets now
type Extension struct {
	Package catalog.Package
	// PriceMicros is what the package costs on the placement's server, 0 for a live one not priced
	PriceMicros int64
	Refusal     error // nil, or one wrapping ErrNotReady or ErrExceedsLifetime
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
		x, err := s.checkExtend(p, pkg, nil)
		switch {
		case errors.Is(err, ErrNotExtension):
			continue
		case err != nil && !errors.Is(err, ErrNotReady) && !errors.Is(err, ErrExceedsLifetime):
			return nil, err
		}
		out = append(out, Extension{Package: pkg, PriceMicros: x.price, Refusal: err})
	}
	// The start package is left out when a dedicated extension offers the same minutes and price
	out = slices.DeleteFunc(out, func(x Extension) bool {
		return x.Package.ID == p.PackageID && slices.ContainsFunc(out, func(y Extension) bool {
			return y.Package.ExtensionOf == p.PackageID && y.Package.Minutes == x.Package.Minutes &&
				y.PriceMicros == x.PriceMicros
		})
	})
	slices.SortFunc(out, byListing)
	return out, nil
}

// byListing orders extensions by minutes, then price, then id
func byListing(a, b Extension) int {
	return cmp.Or(cmp.Compare(a.Package.Minutes, b.Package.Minutes),
		cmp.Compare(a.PriceMicros, b.PriceMicros), cmp.Compare(a.Package.ID, b.Package.ID))
}

// extension is a placement that passed the extend checks, with what extending it needs
type extension struct {
	p    store.Placement
	prov provider.Provider
	ref  provider.Ref
	by   time.Duration
	// price is what the order must pay, the only field a refusal may also set
	price  int64
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
	// A refusal keeps the price known by then, the fixed one before the server prices it
	known := extension{price: pkg.PriceMicros}
	if p.State != store.PlacementReady {
		return known, fmt.Errorf("%w: placement is %s", ErrNotReady, p.State)
	}
	if x.price, err = s.extensionPrice(p, pkg); err != nil {
		return known, err
	}
	known.price = x.price
	if x.prov, x.ref, err = s.home(p); err != nil {
		return known, err
	}
	if err := withinLifetime(x.prov, p, p.Deadline.Add(x.by)); err != nil {
		return known, err
	}
	return x, nil
}

// extensionPrice is pkg's price on p's server, a refusal as not ready once the server is gone
func (s *Saga) extensionPrice(p store.Placement, pkg catalog.Package) (int64, error) {
	price, err := s.d.Engine.ExtensionPrice(pkg, p.Provider, p.SizeName, s.d.Rule)
	if errors.Is(err, placement.ErrNoServer) {
		s.d.Log.Warn("refuse an extension on a server no longer configured", "placement", p.ID,
			"package", pkg.ID, "err", err)
		// The provider and size come from the settings file, so the refusal leaves them out
		return 0, fmt.Errorf("%w: the sandbox's server is no longer configured", ErrNotReady)
	}
	return price, err
}

// withinLifetime refuses a deadline whose sandbox would outlast the provider's maximum lifetime
func withinLifetime(prov provider.Provider, p store.Placement, deadline time.Time) error {
	limit := prov.Capabilities().MaxLifetime
	// A pooled sandbox is older than its placement, and the provider counts from its creation
	born := p.CreatedAt
	if p.SandboxCreatedAt != nil && p.SandboxCreatedAt.Before(born) {
		born = *p.SandboxCreatedAt
	}
	if life := deadline.Add(lifetimeSlack).Sub(born); limit > 0 && life > limit {
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
	ctx = s.d.Telemetry.JoinOrder(ctx, p)
	ctx, span := s.orderSpan(ctx, p, "saga.stop", attr.Reason(ReasonStop))
	defer span.End()
	reason := func() string { return ReasonStop }
	return s.terminate(ctx, p, reason, func(ctx context.Context) (bool, error) {
		return s.d.Store.MarkPlacementTerminating(ctx, p.ID, ReasonStop, s.now())
	})
}

// Terminate ends a ready placement whose deadline is not after due and finishes a terminating one
func (s *Saga) Terminate(ctx context.Context, placementID uuid.UUID, due time.Time) error {
	p, err := s.d.Store.PlacementByID(ctx, placementID)
	if err != nil {
		return err
	}
	ctx, span := s.orderSpan(ctx, p, "saga.terminate", attr.Reason(ReasonDeadline))
	defer span.End()
	reason := func() string { return ReasonDeadline }
	_, err = s.terminate(ctx, p, reason, func(ctx context.Context) (bool, error) {
		return s.d.Store.MarkPlacementDue(ctx, p.ID, ReasonDeadline, due, s.now())
	})
	return err
}

// endReasons maps a termination reason to the EndReason the edges get (CP 3.3)
var endReasons = map[string]routerv1.EndReason{
	ReasonStop:           routerv1.EndReason_END_REASON_STOPPED,
	ReasonDeadline:       routerv1.EndReason_END_REASON_EXPIRED,
	ReasonAbuse:          routerv1.EndReason_END_REASON_ABUSE,
	ReasonWalletBlocked:  routerv1.EndReason_END_REASON_ABUSE,
	ReasonControlLost:    routerv1.EndReason_END_REASON_CONTROL_LOST,
	ReasonProviderFailed: routerv1.EndReason_END_REASON_PROVIDER_FAILED,
	ReasonSandboxExited:  routerv1.EndReason_END_REASON_SANDBOX_EXITED,
	ReasonOperator:       routerv1.EndReason_END_REASON_OPERATOR,
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
	ctx context.Context, p store.Placement, reason func() string,
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
		s.placementEnded(ctx, p.ID, reason())
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
	shared, err, _ := s.destroys.Do(p.ID.String(), func() (any, error) {
		// The stats come first, bounded so they never hold the deletion up
		stats := s.readStats(ctx, p, prov, ref)
		err := destroyRef(ctx, prov, ref)
		if err == nil {
			s.markDestroyed(ctx, prov, ref)
		}
		return stats, err
	})
	if err != nil {
		return store.Placement{}, fmt.Errorf("destroy sandbox: %w", err)
	}
	rawStats, _ := shared.([]byte)

	edgeBytes, egressBytes := s.usedBytes(ctx, p.ID)
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
		spans, err := ts.PlacementSpans(ctx, p.ID)
		if err != nil {
			return err
		}
		cost := s.spansCost(spans, s.now())
		if err := ts.SetPlacementCost(ctx, p.ID, cost, edgeBytes, egressBytes); err != nil {
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
		if err := s.endSession(ctx, tx, ended); err != nil {
			return err
		}
		// The transition to terminating recorded why, whoever finishes the placement
		ev, err = s.terminatedEvent(ctx, ts, ended,
			cmp.Or(ended.TerminationReason, ReasonDeadline))
		if err != nil {
			return err
		}
		return s.d.Store.InsertOutbox(ctx, tx, EventTerminated, ev)
	})
	if err != nil {
		return store.Placement{}, fmt.Errorf("mark terminated: %w", err)
	}
	if !already {
		s.publish(ctx, ev)
		s.sessionEnded(ctx, p.ID)
		s.judgeMargin(ended)
		s.observe(ctx, ended, prov, rawStats)
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
	ctx, cancel := context.WithTimeout(ctx, destroyTimeout)
	defer cancel()
	if err := destroyRef(ctx, prov, ref); err != nil {
		s.d.Log.Error("destroy sandbox", "provider", prov.Name(), "sandbox", ref.ID, "err", err)
		return
	}
	s.markDestroyed(ctx, prov, ref)
}

// release frees the slot the placement holds, if any, so a repeat or a racing caller frees nothing
func (s *Saga) release(ctx context.Context, id uuid.UUID, attempt int) {
	ctx, cancel := step(ctx)
	defer cancel()
	if _, err := s.d.Store.ReleaseAttemptSlot(ctx, id, attempt); err != nil {
		s.d.Log.Error("release capacity", "placement", id, "attempt", attempt, "err", err)
	}
}

// step bounds one store or provider call so a stuck dependency cannot hold placing forever
func step(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, cleanupTimeout)
}

// endRoot ends the order root with the placement's state and, for a failure, its reason
func endRoot(root trace.Span, p store.Placement) {
	if root == nil {
		return
	}
	root.SetAttributes(attr.State(stateOf(p)))
	if p.FailureReason != "" {
		root.SetAttributes(attr.Reason(p.FailureReason))
	}
	root.End()
}

// endOrderRoot ends the order root once placing is over, with the state it ended in
func (s *Saga) endOrderRoot(ctx context.Context, root trace.Span, id uuid.UUID) {
	if root == nil {
		return
	}
	cctx, cancel := step(ctx)
	defer cancel()
	cur, err := s.d.Store.PlacementByID(cctx, id)
	if err != nil {
		root.End()
		return
	}
	endRoot(root, cur)
}
