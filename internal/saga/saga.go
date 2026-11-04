// Package saga places paid sandboxes on providers, extends their deadlines and tears them down.
//
// Create records the order and a placing placement, then places it in a goroutine. Each
// candidate gets a capacity slot, a create under an idempotency key derived from the order,
// attempt and provider, a wait for ready and, where needed, the runner upload. A create whose
// outcome is unknown is retried with the same key on the same provider, a known failure that
// allows it fails over to the next candidate, anything else fails the placement as refundable.
// Every state change commits together with its outbox event and, for a deadline, its River job,
// and is then published on Valkey for live readers.
package saga

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/valkey-io/valkey-go"

	"github.com/oynozan/bugnet-router/internal/catalog"
	"github.com/oynozan/bugnet-router/internal/handle"
	"github.com/oynozan/bugnet-router/internal/jobargs"
	"github.com/oynozan/bugnet-router/internal/ordertoken"
	"github.com/oynozan/bugnet-router/internal/placement"
	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/store"
)

// Errors the API maps to status codes
var (
	ErrBadToken       = errors.New("saga: bad order token")
	ErrUnderpaid      = errors.New("saga: amount below the package price")
	ErrUnknownPackage = errors.New("saga: unknown package")
	ErrNotReady       = errors.New("saga: placement is not ready")
	ErrHandleMismatch = errors.New("saga: order token is for another handle")
	ErrNotExtension   = errors.New("saga: package does not extend this placement")
)

// ReasonStop is the termination reason when the placement's owner stops it
const ReasonStop = "stop"

const (
	maxAttempts      = 3
	placingBudget    = 30 * time.Second
	lifetimeSlack    = 5 * time.Minute
	terminateTimeout = 60 * time.Second
)

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
	Store       *store.Store
	Engine      *placement.Engine
	Registry    *provider.Registry
	Catalog     *catalog.Catalog
	River       *river.Client[pgx.Tx] // insert only
	Valkey      valkey.Client
	Prefix      string
	OrderKey    ed25519.PublicKey
	HandleKey   []byte
	MarginFloor float64
	Tracker     *placement.Tracker
	Runners     map[string]string // architecture to the host path of its bugnet-run build
	Now         func() time.Time  // time.Now when nil
	Log         *slog.Logger      // slog.Default when nil
}

// Saga runs placements from order to teardown
type Saga struct {
	d  Deps
	wg sync.WaitGroup
}

// New builds a saga over deps
func New(deps Deps) *Saga {
	if deps.Now == nil {
		deps.Now = time.Now
	}
	if deps.Log == nil {
		deps.Log = slog.Default()
	}
	return &Saga{d: deps}
}

// Wait blocks until every placing goroutine has finished
func (s *Saga) Wait() { s.wg.Wait() }

func (s *Saga) now() time.Time { return s.d.Now().UTC() }

/* Create */

// Create verifies a start token, records the order and placement, and starts placing it
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
	h, sha := handle.Derive(s.d.HandleKey, c.OrderID)
	p, created, err := s.d.Store.CreatePlacement(ctx, store.NewPlacement{
		HandleSHA256: sha, OrderID: c.OrderID, PackageID: pkg.ID,
	})
	if err != nil {
		return store.Placement{}, "", fmt.Errorf("record placement: %w", err)
	}
	if !bytes.Equal(p.HandleSHA256, sha) {
		return store.Placement{}, "", fmt.Errorf(
			"%w: order %s belongs to another placement", ErrBadToken, c.OrderID)
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
	pkg, ok := s.d.Catalog.Get(c.PackageID)
	if !ok {
		return pkg, fmt.Errorf("%w %q", ErrUnknownPackage, c.PackageID)
	}
	if c.AmountMicros < pkg.PriceMicros {
		return pkg, fmt.Errorf("%w: paid %d, %s costs %d", ErrUnderpaid, c.AmountMicros, pkg.ID,
			pkg.PriceMicros)
	}
	return pkg, nil
}

/* Placing */

// outcome is how one attempt ended
type outcome struct {
	done     bool // the placement left placing, so placing stops
	failOver bool
	reason   string
}

// place tries candidates until one is ready or the attempt or time budget runs out
func (s *Saga) place(
	ctx context.Context, p store.Placement, pkg catalog.Package, req placement.Requirements,
) {
	budget, cancel := context.WithTimeout(ctx, placingBudget)
	defer cancel()
	tried := map[string]bool{}
	var attempted []provider.Provider
	reason := "no provider can host the package"
	for n := 1; n <= maxAttempts && budget.Err() == nil; {
		cands, err := s.d.Engine.Candidates(placement.Inputs{
			Package: pkg, Requirements: req, MarginFloor: s.d.MarginFloor, Exclude: tried,
		})
		if err != nil {
			break
		}
		c := cands[0]
		name := c.Provider.Name()
		tried[name] = true
		reserved, err := s.d.Store.ReserveCapacity(ctx, name)
		if err != nil {
			reason = err.Error()
			break
		}
		if !reserved {
			continue
		}
		attempted = append(attempted, c.Provider)
		out := s.attempt(ctx, budget, p, pkg, c, n)
		if out.done {
			return
		}
		reason = out.reason
		if !out.failOver {
			break
		}
		n++
	}
	s.fail(ctx, p, reason, attempted)
}

// attempt holds a capacity slot on c's provider and gives it back unless the sandbox is ready
func (s *Saga) attempt(
	ctx, budget context.Context, p store.Placement, pkg catalog.Package,
	c placement.Candidate, n int,
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
	if err := s.d.Store.RecordAttempt(ctx, rec); err != nil {
		s.release(ctx, name)
		return outcome{reason: err.Error()}
	}
	started := s.now()
	ref, err := s.create(budget, prov, provider.CreateRequest{
		IdempotencyKey: rec.IdempotencyKey,
		Size:           c.Size,
		Labels:         labels(p.ID),
		Lifetime:       time.Duration(pkg.Minutes)*time.Minute + lifetimeSlack,
	})
	if err != nil {
		s.d.Tracker.RecordFailure(name)
		s.abandon(ctx, prov, rec, nil, err)
		return outcome{
			failOver: canFailOver(err), reason: fmt.Sprintf("create on %s: %v", name, err),
		}
	}
	if err := s.boot(ctx, prov, ref); err != nil {
		noBuild := errors.Is(err, errNoRunnerBuild)
		if !noBuild {
			s.d.Tracker.RecordFailure(name)
		}
		s.abandon(ctx, prov, rec, &ref, err)
		return outcome{failOver: !noBuild, reason: fmt.Sprintf("boot on %s: %v", name, err)}
	}
	return s.commitReady(ctx, p, pkg, c, rec, ref, started)
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

// canFailOver never allows an unknown outcome, since the first sandbox may still boot and bill
func canFailOver(err error) bool {
	e, ok := provider.AsError(err)
	return ok && e.CanFailOver && e.Code != provider.CodeOutcomeUnknown
}

// commitReady starts the paid clock, or gives the sandbox back when the placement moved on
func (s *Saga) commitReady(
	ctx context.Context, p store.Placement, pkg catalog.Package, c placement.Candidate,
	rec store.Attempt, ref provider.Ref, started time.Time,
) outcome {
	prov := c.Provider
	readyAt := s.now()
	s.d.Tracker.RecordReady(prov.Name(), readyAt.Sub(started))
	deadline := readyAt.Add(time.Duration(pkg.Minutes) * time.Minute)
	if err := prov.SetLifetime(ctx, ref, deadline.Add(lifetimeSlack)); err != nil {
		// The lifetime set at create still outlasts the deadline unless ready took the full slack
		s.d.Log.Warn("set lifetime at ready", "placement", p.ID, "provider", prov.Name(),
			"err", err)
	}
	var ev Event
	err := s.d.Store.WithTx(ctx, func(tx pgx.Tx) error {
		ts := s.d.Store.Tx(tx)
		err := ts.MarkPlacementReady(ctx, p.ID, ref, c.Size.Name, readyAt, deadline)
		if err != nil {
			return err
		}
		ready := rec
		ready.State = attemptReady
		if err := ts.RecordAttempt(ctx, ready); err != nil {
			return err
		}
		if err := s.insertDeadline(ctx, tx, p.ID, deadline); err != nil {
			return err
		}
		cur, err := ts.PlacementByID(ctx, p.ID)
		if err != nil {
			return err
		}
		ev = newEvent(cur, "")
		return s.d.Store.InsertOutbox(ctx, tx, EventReady, ev)
	})
	if err != nil {
		s.abandon(ctx, prov, rec, &ref, err)
		if errors.Is(err, store.ErrStateConflict) {
			s.d.Log.Info("placement left placing before it was ready", "placement", p.ID)
			return outcome{done: true}
		}
		return outcome{reason: fmt.Sprintf("commit ready: %v", err)}
	}
	s.publish(ctx, ev)
	return outcome{done: true}
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
	if err := s.d.Store.RecordAttempt(ctx, rec); err != nil {
		s.d.Log.Error("record failed attempt", "placement", rec.PlacementID, "err", err)
	}
	s.release(ctx, prov.Name())
}

// fail records a refundable failure and destroys whatever this placement left on any provider
func (s *Saga) fail(
	ctx context.Context, p store.Placement, reason string, attempted []provider.Provider,
) {
	var ev Event
	err := s.d.Store.WithTx(ctx, func(tx pgx.Tx) error {
		ts := s.d.Store.Tx(tx)
		if err := ts.MarkPlacementFailed(ctx, p.ID, reason, true); err != nil {
			return err
		}
		cur, err := ts.PlacementByID(ctx, p.ID)
		if err != nil {
			return err
		}
		ev = newEvent(cur, reason)
		return s.d.Store.InsertOutbox(ctx, tx, EventFailed, ev)
	})
	if err != nil {
		s.d.Log.Error("mark placement failed", "placement", p.ID, "reason", reason, "err", err)
	}
	for _, prov := range attempted {
		owned, err := prov.ListOwned(ctx, labels(p.ID))
		if err != nil {
			s.d.Log.Error("list sandboxes to destroy", "placement", p.ID,
				"provider", prov.Name(), "err", err)
			continue
		}
		for _, o := range owned {
			s.destroy(ctx, prov, o.Ref)
		}
	}
	if err == nil {
		s.publish(ctx, ev)
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
	if pkg.ID != p.PackageID && pkg.ExtensionOf != p.PackageID {
		return store.Placement{}, fmt.Errorf("%w: %s on %s", ErrNotExtension, pkg.ID, p.PackageID)
	}
	if p.State != store.PlacementReady {
		return store.Placement{}, fmt.Errorf("%w: placement is %s", ErrNotReady, p.State)
	}

	var applied bool
	var ev Event
	id := p.ID
	err = s.d.Store.WithTx(ctx, func(tx pgx.Tx) error {
		var err error
		p, applied, err = s.d.Store.Tx(tx).ExtendDeadline(ctx, c.OrderID, id,
			time.Duration(pkg.Minutes)*time.Minute)
		if err != nil || !applied {
			return err
		}
		if err := s.insertDeadline(ctx, tx, id, *p.Deadline); err != nil {
			return err
		}
		ev = newEvent(p, "")
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
	if applied {
		s.publish(ctx, ev)
	}
	if p.State != store.PlacementReady {
		return p, nil
	}
	// A repeated order sets the lifetime again so a retry repairs a failed earlier call
	prov, ref, err := s.home(p)
	if err == nil {
		err = prov.SetLifetime(ctx, ref, p.Deadline.Add(lifetimeSlack))
	}
	if err != nil {
		return store.Placement{}, fmt.Errorf("set lifetime: %w", err)
	}
	return p, nil
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
	return s.terminate(ctx, p, ReasonStop)
}

// Terminate ends a placement for reason and does nothing when it has already ended
func (s *Saga) Terminate(ctx context.Context, placementID uuid.UUID, reason string) error {
	p, err := s.d.Store.PlacementByID(ctx, placementID)
	if err != nil {
		return err
	}
	_, err = s.terminate(ctx, p, reason)
	return err
}

// terminate destroys the sandbox, then marks the placement terminated and frees its slot once
func (s *Saga) terminate(
	ctx context.Context, p store.Placement, reason string,
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
	moved, err := s.d.Store.MarkPlacementTerminating(ctx, p.ID)
	if err != nil {
		return store.Placement{}, err
	}
	if !moved {
		if p, err = s.d.Store.PlacementByID(ctx, p.ID); err != nil {
			return store.Placement{}, err
		}
		if p.State == store.PlacementTerminated {
			return p, nil
		}
	}
	prov, ref, err := s.home(p)
	if err != nil {
		return store.Placement{}, err
	}
	if err := destroyRef(ctx, prov, ref); err != nil {
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
		if err := ts.MarkPlacementTerminated(ctx, p.ID); err != nil {
			return err
		}
		if ended, err = ts.PlacementByID(ctx, p.ID); err != nil {
			return err
		}
		ev = newEvent(ended, reason)
		return s.d.Store.InsertOutbox(ctx, tx, EventTerminated, ev)
	})
	if err != nil {
		return store.Placement{}, fmt.Errorf("mark terminated: %w", err)
	}
	if !already {
		s.release(ctx, p.Provider)
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

func labels(id uuid.UUID) map[string]string {
	return map[string]string{"bugnet": "1", "placement": id.String()}
}

// hashHandle reports a malformed handle as not found, like a well formed unknown one
func hashHandle(h string) ([]byte, error) {
	sha, err := handle.Hash(h)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", store.ErrNotFound, err)
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
	if err := destroyRef(ctx, prov, ref); err != nil {
		s.d.Log.Error("destroy sandbox", "provider", prov.Name(), "sandbox", ref.ID, "err", err)
	}
}

func (s *Saga) release(ctx context.Context, providerName string) {
	if err := s.d.Store.ReleaseCapacity(ctx, providerName); err != nil {
		s.d.Log.Error("release capacity", "provider", providerName, "err", err)
	}
}
