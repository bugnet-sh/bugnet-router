package saga

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/gateclient"
	"github.com/bugnet-sh/bugnet-router/internal/image"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/pool"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/ratelimit"
	"github.com/bugnet-sh/bugnet-router/internal/sessions"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// ErrQuoteInvalid means a quote id is not 1 to 128 printable ASCII bytes
var ErrQuoteInvalid = errors.New("saga: the quote id is not 1 to 128 printable ASCII bytes")

// BrakeKey is the Valkey key, under the prefix, that reserve_watch keeps set while the brake holds
const BrakeKey = "reserve:brake"

// reserveBucket is the router-wide bucket of new reservations
const reserveBucket = "rl:reserve:all"

const (
	// gateWait is how long Reserve waits for its one Valkey call, a silent Valkey answering BUSY
	gateWait = 200 * time.Millisecond
	// staleAfter is how old a reservation's heartbeat may get before it counts as dead
	staleAfter = 30 * time.Second
	// awaitPoll is how often an order waiting for a reservation reads its row
	awaitPoll = 250 * time.Millisecond
	// keepaliveTimeout bounds one gate check of a ready reservation
	keepaliveTimeout = 10 * time.Second
)

// ReserveInput is one Reserve call
type ReserveInput struct {
	QuoteID      string
	PackageID    string
	Requirements placement.Requirements
	// ClientIP is the agent client's address, the zero Addr when unknown or unparsable
	ClientIP netip.Addr
}

// Skip is why Reserve held nothing
type Skip string

// Reasons Reserve skips, as ReserveSkip names them
const (
	SkipDisabled    Skip = "disabled"
	SkipNoWarm      Skip = "no_warm"
	SkipBusy        Skip = "busy"
	SkipBraked      Skip = "braked"
	SkipQuoteSpent  Skip = "quote_spent"
	SkipNotPoolable Skip = "not_poolable"
)

// Outcome is what Reserve did, Skip empty when it holds a sandbox
type Outcome struct {
	Skip Skip
	// ExpiresAt is when the hold ends unless another Reserve renews it
	ExpiresAt time.Time
}

// validQuote reports whether q is 1 to 128 printable ASCII bytes
func validQuote(q string) bool {
	if len(q) < 1 || len(q) > 128 {
		return false
	}
	for i := range len(q) {
		if q[i] < 0x20 || q[i] > 0x7e {
			return false
		}
	}
	return true
}

// quoteSHA is what the router stores of a quote, never the quote itself
func quoteSHA(q string) []byte {
	sum := sha256.Sum256([]byte(q))
	return sum[:]
}

// reservedPrep is the part of Prepare's answer a reservation stores, never a ref or a credential
type reservedPrep struct {
	KeyID       string         `json:"key_id"`
	GateReadyAt *time.Time     `json:"gate_ready_at,omitempty"`
	SSHHost     *store.SSHHost `json:"ssh_host,omitempty"`
	CAVersion   int            `json:"ca_version,omitempty"`
	// Image is the sandbox's base image record, which the bind writes with the ready
	Image *image.Manifest `json:"image,omitempty"`
}

// reservationChannel is the Valkey channel that wakes the order awaiting a reservation
func (s *Saga) reservationChannel(id uuid.UUID) string {
	return s.d.Prefix + "reservation:" + id.String()
}

// reqJSON is the stored form of the requirements an order must repeat
func reqJSON(req placement.Requirements) []byte {
	raw, _ := json.Marshal(req)
	return raw
}

// sameRequirements reports whether the stored requirements equal req
func sameRequirements(raw []byte, req placement.Requirements) bool {
	var got placement.Requirements
	return json.Unmarshal(raw, &got) == nil && got == req
}

// fits reports whether the reservation is for this package and these requirements
func fits(r store.Reservation, pkg catalog.Package, req placement.Requirements) bool {
	return r.PackageID == pkg.ID && sameRequirements(r.Requirements, req)
}

/* Reserve */

// Reserve holds a prepared sandbox for a quote's order and never fails for want of one
//
// It costs one Valkey call and one short transaction. The preparation runs after the answer, in a
// goroutine of the saga's wait group, as placing does.
func (s *Saga) Reserve(ctx context.Context, in ReserveInput) (Outcome, error) {
	if !validQuote(in.QuoteID) {
		return Outcome{}, ErrQuoteInvalid
	}
	pkg, err := s.lookupPackage(in.PackageID)
	if err != nil {
		return Outcome{}, err
	}
	cfg := s.d.Reserve
	if !cfg.Enabled {
		return Outcome{Skip: SkipDisabled}, nil
	}
	if s.keyID == "" {
		return Outcome{Skip: SkipNoWarm}, nil
	}
	sha := quoteSHA(in.QuoteID)
	// A renewal takes no token, so what the call may spend depends on what the quote holds
	open, err := s.d.Store.OpenReservation(ctx, sha)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return Outcome{}, err
	}
	renewing := err == nil && fits(open, pkg, in.Requirements) &&
		(open.State == store.ReservationPreparing || open.State == store.ReservationReady)
	if skip := s.reserveGate(ctx, !renewing); skip != "" {
		return Outcome{Skip: skip}, nil
	}
	res, err := s.reserveTx(ctx, in, pkg, sha)
	if err != nil {
		return Outcome{}, err
	}
	if res.prepare != nil {
		s.startPreparing(ctx, *res.prepare, pkg, in.Requirements)
	}
	return res.out, nil
}

// reserveGate is Reserve's one Valkey call, which reads both brakes and takes the bucket's token
// when take is set, and answers the skip it means, empty to go on
func (s *Saga) reserveGate(ctx context.Context, take bool) Skip {
	cfg := s.d.Reserve
	vctx, cancel := context.WithTimeout(ctx, gateWait)
	defer cancel()
	g, err := s.limits.TakeUnless(vctx, ratelimit.Bucket{
		Key: reserveBucket, PerSecond: float64(cfg.PerSecond), Burst: cfg.Burst,
	}, take, BrakeKey, pool.SpendKey)
	switch {
	case err != nil:
		// Reserve is free and open to all, so a Valkey that does not answer fails it closed
		s.d.Log.Warn("reserve gate unread, answering busy", "err", err)
		return SkipBusy
	case g.Blocked != "":
		return SkipBraked
	case take && !g.Taken:
		return SkipBusy
	}
	return ""
}

// reserved is what the reserve transaction decided
type reserved struct {
	out Outcome
	// prepare is the preparation to start once the transaction committed, nil for none
	prepare *preparation
}

// preparation is what the goroutine that readies a reservation needs
type preparation struct {
	res    store.Reservation
	member store.Member
	cand   placement.Candidate
}

// reserveTx runs steps 2 to 6 of CP 18.5.3 in one transaction
func (s *Saga) reserveTx(
	ctx context.Context, in ReserveInput, pkg catalog.Package, sha []byte,
) (reserved, error) {
	cfg := s.d.Reserve
	now := s.now()
	hold := cfg.Hold + cfg.LapseGrace
	cands, candErr := s.d.Engine.Candidates(placement.Inputs{
		Package: pkg, Requirements: in.Requirements, MarginFloor: s.d.MarginFloor,
		WarmFirst: s.d.WarmFirst, Hold: hold, HoldMax: cfg.MaxHold + cfg.LapseGrace,
		ImageAccept: s.d.ImageAccept,
	})
	if candErr != nil && !errors.Is(candErr, placement.ErrNoCandidate) {
		return reserved{}, fmt.Errorf("find candidates: %w", candErr)
	}
	var ip *netip.Addr
	if a := in.ClientIP.Unmap(); a.IsValid() && !s.sharedAddr(a) {
		ip = &a
	}
	var out reserved
	err := s.d.Store.WithTx(ctx, func(tx pgx.Tx) error {
		ts := s.d.Store.Tx(tx)
		if err := ts.LockQuote(ctx, sha); err != nil {
			return err
		}
		cur, err := ts.OpenReservation(ctx, sha)
		switch {
		case err == nil && cur.State == store.ReservationAwaited:
			// An order already waits for it, so nothing is renewed or started
			out.out = Outcome{ExpiresAt: cur.ExpiresAt}
			return nil
		case err == nil && fits(cur, pkg, in.Requirements):
			out.out, err = s.renew(ctx, ts, cur, pkg, in.Requirements, now)
			return err
		case err == nil:
			// Another package or other requirements, so the old hold goes and the new one counts
			if err := s.endReservationTx(ctx, tx, cur, store.ReservationReleased, "",
				store.EndUnused); err != nil {
				return err
			}
		case !errors.Is(err, store.ErrNotFound):
			return err
		}
		skip, err := s.capsSkip(ctx, ts, sha, ip)
		if err != nil || skip != "" {
			out.out = Outcome{Skip: skip}
			return err
		}
		if candErr != nil {
			out.out = Outcome{Skip: noStart(pkg)}
			return nil
		}
		res, mem, cand, err := s.holdOne(ctx, ts, in, pkg, cands, sha, ip, now)
		if err != nil {
			return err
		}
		if mem.ID == uuid.Nil {
			out.out = Outcome{Skip: noStart(pkg)}
			return nil
		}
		if err := ts.InsertReservation(ctx, res); err != nil {
			return err
		}
		out.out = Outcome{ExpiresAt: res.ExpiresAt}
		out.prepare = &preparation{res: res, member: mem, cand: cand}
		return nil
	})
	if err != nil {
		return reserved{}, err
	}
	return out, nil
}

// noStart is the skip when no pool member and no start before payment fits
func noStart(pkg catalog.Package) Skip {
	if len(pkg.EgressAllow) > 0 {
		return SkipNotPoolable
	}
	return SkipNoWarm
}

// sharedAddr reports whether a is in a range many clients share
func (s *Saga) sharedAddr(a netip.Addr) bool {
	for _, p := range s.d.Reserve.SharedRanges {
		if p.Contains(a) {
			return true
		}
	}
	return false
}

// capsSkip applies the caps of a new reservation, in CP 18.5.3's order
func (s *Saga) capsSkip(
	ctx context.Context, ts *store.Store, sha []byte, ip *netip.Addr,
) (Skip, error) {
	cfg := s.d.Reserve
	n, err := ts.ReservationsOfQuote(ctx, sha)
	if err != nil {
		return "", err
	}
	if n >= cfg.PerQuote {
		return SkipQuoteSpent, nil
	}
	open, _, ofIP, err := ts.OpenCounts(ctx, ip)
	if err != nil {
		return "", err
	}
	if open >= cfg.MaxOpen || ip != nil && ofIP >= cfg.PerIPOpen {
		return SkipBusy, nil
	}
	return "", nil
}

// renew moves an open reservation's expiry out while the longer hold still clears the floor
//
// A hold that cannot grow, because it reached its limit or the floor, keeps its expiry and is
// answered as held.
func (s *Saga) renew(
	ctx context.Context, ts *store.Store, cur store.Reservation, pkg catalog.Package,
	req placement.Requirements, now time.Time,
) (Outcome, error) {
	cfg := s.d.Reserve
	next := now.Add(cfg.Hold)
	if next.After(cur.HoldUntil) {
		next = cur.HoldUntil
	}
	if !next.After(cur.ExpiresAt) {
		return Outcome{ExpiresAt: cur.ExpiresAt}, nil
	}
	mem, err := ts.MemberByID(ctx, cur.MemberID)
	if err != nil {
		return Outcome{}, err
	}
	cands, err := s.d.Engine.Candidates(placement.Inputs{
		Package: pkg, Requirements: req, MarginFloor: s.d.MarginFloor,
		WarmFirst: s.d.WarmFirst, Hold: next.Sub(cur.CreatedAt) + cfg.LapseGrace,
		HoldMax: cfg.MaxHold + cfg.LapseGrace, ImageAccept: s.d.ImageAccept,
	})
	fitsStill := false
	if err == nil {
		for _, c := range cands {
			fitsStill = fitsStill ||
				c.Provider.Name() == mem.Provider && c.Size.Name == mem.SizeName
		}
	}
	if !fitsStill {
		return Outcome{ExpiresAt: cur.ExpiresAt}, nil
	}
	if err := ts.RenewReservation(ctx, cur.ID, next); err != nil {
		return Outcome{}, err
	}
	return Outcome{ExpiresAt: next}, nil
}

// holdOne takes a pool member or starts a member of its own for the reservation, a zero member
// when nothing fits (CP 18.5.3 steps 4 and 5)
func (s *Saga) holdOne(
	ctx context.Context, ts *store.Store, in ReserveInput, pkg catalog.Package,
	cands []placement.Candidate, sha []byte, ip *netip.Addr, now time.Time,
) (store.Reservation, store.Member, placement.Candidate, error) {
	cfg := s.d.Reserve
	res := store.Reservation{
		ID: uuid.New(), QuoteSHA256: sha, PackageID: pkg.ID, Requirements: reqJSON(in.Requirements),
		PlacementID: uuid.New(), KeyID: s.keyID, ClientIP: ip, Owner: s.d.ReplicaID,
		CreatedAt: now, ExpiresAt: now.Add(cfg.Hold), HoldUntil: now.Add(cfg.MaxHold),
	}
	mem, cand, err := s.claimForReserve(ctx, ts, pkg, cands, res.ID, now)
	if err != nil {
		return res, store.Member{}, cand, err
	}
	res.Path = store.ReservePooled
	if mem.ID == uuid.Nil {
		if mem, cand, err = s.startForReserve(ctx, ts, cands, res.ID); err != nil {
			return res, store.Member{}, cand, err
		}
		res.Path = store.ReserveSpeculative
	}
	res.MemberID = mem.ID
	return res, mem, cand, nil
}

// shareOf is how many members of a pool open reservations may hold, at least one
func (s *Saga) shareOf(provider, size string) int {
	return max(1, int(math.Floor(s.d.ReserveShare*float64(s.d.Pools.Target(provider, size)))))
}

// claimForReserve is step 4, the first candidate in engine order whose pool has a warm member and
// room under the reservations' share
//
// A candidate with a pool and no claimable member records one miss for the Reserve, and one
// whose share is full is a refusal and no miss.
func (s *Saga) claimForReserve(
	ctx context.Context, ts *store.Store, pkg catalog.Package, cands []placement.Candidate,
	reservation uuid.UUID, now time.Time,
) (store.Member, placement.Candidate, error) {
	missed := false
	for _, c := range cands {
		name := c.Provider.Name()
		if !s.wantsPool(pkg, c) {
			continue
		}
		held, err := ts.HeldMembers(ctx, name, c.Size.Name)
		if err != nil {
			return store.Member{}, c, err
		}
		if held >= s.shareOf(name, c.Size.Name) {
			continue
		}
		if c.Warm {
			bornAfter := time.Time{}
			if life := c.Provider.Capabilities().MaxLifetime; life > 0 {
				need := time.Duration(pkg.Minutes)*time.Minute + s.d.PoolRoom + lifetimeSlack +
					s.d.Reserve.MaxHold + s.d.Reserve.LapseGrace
				bornAfter = now.Add(need - life)
			}
			m, ok, err := ts.ClaimWarm(ctx, name, c.Size.Name, bornAfter,
				store.Holder{Reservation: reservation})
			if err != nil || ok {
				return m, c, err
			}
		}
		if !missed {
			missed = true
			if err := ts.RecordMiss(ctx, name, c.Size.Name, now); err != nil {
				return store.Member{}, c, err
			}
		}
	}
	return store.Member{}, placement.Candidate{}, nil
}

// startForReserve is step 5, the first candidate that may start a sandbox before payment
//
// With cost first only the candidate a cold order would take is looked at, as for step 4.
func (s *Saga) startForReserve(
	ctx context.Context, ts *store.Store, cands []placement.Candidate, reservation uuid.UUID,
) (store.Member, placement.Candidate, error) {
	cfg := s.d.Reserve
	if !s.d.WarmFirst && len(cands) > 1 {
		cands = cands[:1]
	}
	_, specOpen, _, err := ts.OpenCounts(ctx, nil)
	if err != nil {
		return store.Member{}, placement.Candidate{}, err
	}
	if specOpen >= cfg.SpeculativeMaxOpen {
		return store.Member{}, placement.Candidate{}, nil
	}
	for _, c := range cands {
		name := c.Provider.Name()
		caps := c.Provider.Capabilities()
		switch {
		case !containsString(cfg.SpeculativeProviders, name),
			c.HourlyMicros > cfg.SpeculativeMaxHourlyMicros,
			caps.NeedsRunnerUpload && len(s.d.Runners[caps.RunnerArch()]) == 0,
			s.createsPaused(ctx, name):
			continue
		}
		if block, err := store.ProviderBlock(ctx, ts.Querier(), name); err != nil {
			return store.Member{}, c, err
		} else if block != "" {
			continue
		}
		if !s.takeCreateNow(ctx, name) {
			continue
		}
		m, ok, err := ts.StartReserveMember(ctx, uuid.New(), name, c.Size.Name, reservation)
		if err != nil {
			return store.Member{}, c, err
		}
		if ok {
			return m, c, nil
		}
	}
	return store.Member{}, placement.Candidate{}, nil
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// takeCreateNow spends a create token of name without waiting, false when none is there
func (s *Saga) takeCreateNow(ctx context.Context, name string) bool {
	b, ok := s.createBucket(name)
	if !ok {
		return true
	}
	taken, _, err := s.limits.TakeKey(ctx, b)
	if err != nil {
		s.d.Log.Warn("create bucket unreadable, no start before payment", "provider", name,
			"err", err)
		return false
	}
	return taken
}

// endReservationTx ends an open reservation inside tx and retires its member, so no path leaves
// a member held behind an ended reservation
func (s *Saga) endReservationTx(
	ctx context.Context, tx pgx.Tx, r store.Reservation, state, failure, endReason string,
) error {
	member, ended, err := s.d.Store.Tx(tx).EndReservation(ctx, r.ID, state, failure, s.now())
	if err != nil || !ended {
		return err
	}
	return s.retireMemberTx(ctx, tx, member, endReason, failure)
}

// ReleaseReservation gives back the quote's open reservation, nil when there is none or an order
// awaits it
func (s *Saga) ReleaseReservation(ctx context.Context, quoteID string) error {
	if !validQuote(quoteID) {
		return ErrQuoteInvalid
	}
	sha := quoteSHA(quoteID)
	var ended store.Reservation
	err := s.d.Store.WithTx(ctx, func(tx pgx.Tx) error {
		ts := s.d.Store.Tx(tx)
		if err := ts.LockQuote(ctx, sha); err != nil {
			return err
		}
		cur, err := ts.OpenReservation(ctx, sha)
		if errors.Is(err, store.ErrNotFound) ||
			err == nil && cur.State == store.ReservationAwaited {
			return nil
		}
		if err != nil {
			return err
		}
		ended = cur
		return s.endReservationTx(ctx, tx, cur, store.ReservationReleased, "", store.EndUnused)
	})
	if err != nil {
		return err
	}
	if ended.ID != uuid.Nil {
		s.publishReservation(ctx, ended.ID, store.ReservationReleased)
	}
	return nil
}

// failReservation fails an open reservation and retires its member
func (s *Saga) failReservation(ctx context.Context, id uuid.UUID, why string) {
	ctx, cancel := step(context.WithoutCancel(ctx))
	defer cancel()
	var failed bool
	err := s.d.Store.WithTx(ctx, func(tx pgx.Tx) error {
		r, err := s.d.Store.Tx(tx).ReservationForUpdate(ctx, id)
		if err != nil {
			return err
		}
		failed = r.State == store.ReservationPreparing || r.State == store.ReservationReady ||
			r.State == store.ReservationAwaited
		if !failed {
			return nil
		}
		return s.endReservationTx(ctx, tx, r, store.ReservationFailed, why, store.EndFailed)
	})
	if err != nil {
		s.d.Log.Error("fail a reservation", "reservation", id, "err", err)
		return
	}
	if failed {
		s.d.Log.Warn("reservation failed", "reservation", id, "why", why)
		s.publishReservation(ctx, id, store.ReservationFailed)
	}
}

// publishReservation wakes the order awaiting the reservation, a failure only logging since the
// order reads the row on a timer too
func (s *Saga) publishReservation(ctx context.Context, id uuid.UUID, state string) {
	if s.d.Valkey == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancel()
	cmd := s.d.Valkey.B().Publish().Channel(s.reservationChannel(id)).Message(state).Build()
	if err := s.d.Valkey.Do(ctx, cmd).Error(); err != nil {
		s.d.Log.Warn("publish a reservation", "reservation", id, "err", err)
	}
}

// watchReservation sends on the returned channel whenever the reservation is published, stop ends
// the subscription
func (s *Saga) watchReservation(
	ctx context.Context, id uuid.UUID,
) (wake <-chan struct{}, stop func()) {
	ch := make(chan struct{}, 1)
	if s.d.Valkey == nil {
		return ch, func() {}
	}
	rctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		cmd := s.d.Valkey.B().Subscribe().Channel(s.reservationChannel(id)).Build()
		_ = s.d.Valkey.Receive(rctx, cmd, func(valkey.PubSubMessage) {
			select {
			case ch <- struct{}{}:
			default:
			}
		})
	}()
	return ch, func() {
		cancel()
		<-done
	}
}

/* Preparing */

// startPreparing readies the reservation's sandbox on this replica, in the saga's wait group, and
// keeps its row alive until it ends
func (s *Saga) startPreparing(
	ctx context.Context, p preparation, pkg catalog.Package, req placement.Requirements,
) {
	ctx = context.WithoutCancel(ctx)
	s.keepReservation(p.res)
	s.wg.Go(func() { s.prepareReservation(ctx, p, pkg, req) })
}

// keepReservation beats the reservation's heartbeat every few seconds from preparing through
// ready until the row is bound, released, lapsed or failed, and checks a ready sandbox's gate
// every reserve.keepalive
func (s *Saga) keepReservation(r store.Reservation) {
	s.bg.Go(func() {
		beat := time.NewTicker(s.d.HeartbeatInterval)
		defer beat.Stop()
		alive := time.NewTicker(s.d.Reserve.Keepalive)
		defer alive.Stop()
		for {
			select {
			case <-s.bgCtx.Done():
				return
			case <-beat.C:
				ctx, cancel := step(s.bgCtx)
				open, err := s.d.Store.HeartbeatReservation(ctx, r.ID, s.d.ReplicaID, s.now())
				cancel()
				if err == nil && !open {
					return
				}
			case <-alive.C:
				if !s.keepalive(s.bgCtx, r.ID) {
					return
				}
			}
		}
	})
}

// keepalive checks a ready reservation's sandbox and fails the reservation when it does not
// answer, reporting false once the reservation is no longer open
func (s *Saga) keepalive(ctx context.Context, id uuid.UUID) bool {
	ctx, cancel := context.WithTimeout(ctx, keepaliveTimeout)
	defer cancel()
	r, err := s.d.Store.ReservationByID(ctx, id)
	if err != nil {
		return ctx.Err() == nil
	}
	switch r.State {
	case store.ReservationPreparing:
		return true
	case store.ReservationReady, store.ReservationAwaited:
	default:
		return false
	}
	if r.Prepared == nil {
		return true
	}
	if err := s.checkSandbox(ctx, r); err != nil {
		s.failReservation(ctx, id, "keepalive failed")
		s.d.Log.Warn("reserved sandbox failed its check", "reservation", id, "err", err)
		return false
	}
	return true
}

// checkSandbox reads a ready reservation's gate, or the provider's own state where the gate was
// never ready
func (s *Saga) checkSandbox(ctx context.Context, r store.Reservation) error {
	m, err := s.d.Store.MemberByID(ctx, r.MemberID)
	if err != nil || m.ProviderRef == nil {
		return fmt.Errorf("the member has no sandbox: %w", err)
	}
	prov, ok := s.d.Registry.Get(m.Provider)
	if !ok {
		return fmt.Errorf("provider %s is not configured", m.Provider)
	}
	var prep reservedPrep
	if err := json.Unmarshal(r.Prepared, &prep); err != nil {
		return fmt.Errorf("decode the prepared answer: %w", err)
	}
	if g, ok := provider.As[provider.Gater](prov); ok && prep.GateReadyAt != nil {
		route, err := g.GateRoute(*m.ProviderRef, s.d.Boot.gatePort())
		if err != nil {
			return fmt.Errorf("gate route: %w", err)
		}
		key, err := s.d.Keys.GateKey(r.KeyID, r.PlacementID)
		if err != nil {
			return fmt.Errorf("gate key: %w", err)
		}
		return gateclient.Health(ctx, route, key)
	}
	st, err := prov.Inspect(ctx, *m.ProviderRef)
	if err != nil {
		return fmt.Errorf("inspect: %w", err)
	}
	if st.State == provider.StateGone || st.State == provider.StateFailed {
		return fmt.Errorf("the sandbox is %s", st.State)
	}
	return nil
}

// live reports whether the reservation is still open, so a preparation whose row was released,
// lapsed or failed meanwhile stops
func (s *Saga) live(ctx context.Context, id uuid.UUID) bool {
	ctx, cancel := step(ctx)
	defer cancel()
	r, err := s.d.Store.ReservationByID(ctx, id)
	if err != nil {
		return false
	}
	return r.State == store.ReservationPreparing || r.State == store.ReservationReady ||
		r.State == store.ReservationAwaited
}

// memberLabels are the labels every pool member carries, never a placement label
func (s *Saga) memberLabels(m store.Member) map[string]string {
	l := s.OwnedLabels()
	l["bugnet_pool"] = m.SizeName
	l["pool_member"] = m.ID.String()
	return l
}

// prepareReservation runs CP 18.5.4's four steps, each boundary re-reading the row
func (s *Saga) prepareReservation(
	ctx context.Context, p preparation, pkg catalog.Package, req placement.Requirements,
) {
	cfg := s.d.Reserve
	ctx, cancel := context.WithTimeout(ctx, cfg.PrepBudget)
	defer cancel()
	r, m, prov, size := p.res, p.member, p.cand.Provider, p.cand.Size
	name := prov.Name()
	started := s.now()
	smp := s.sampleOf(store.Placement{}, prov, size, started)
	smp.Kind, smp.StartPath = store.SampleReserve, r.Path
	smp.MemberID, smp.ReservationID = m.ID, r.ID
	smp.WakeMS = store.NoStep
	stop := func(err error, record bool) {
		if record {
			s.recordSample(ctx, prov, size, smp, err)
		}
		if s.providerFailed(ctx, name, err, r.Path == store.ReserveSpeculative) {
			s.d.Tracker.RecordFailure(name)
		}
		s.failReservation(ctx, r.ID, truncate(err.Error(), 200))
	}
	var ref provider.Ref
	var img *image.Manifest
	switch r.Path {
	case store.ReservePooled:
		img = manifestOfRecord(m.Image)
		if m.ProviderRef == nil {
			stop(errors.New("the pool member has no sandbox"), false)
			return
		}
		ref = *m.ProviderRef
		if pk, ok := provider.As[provider.Parker](prov); ok {
			err := pk.Unpark(ctx, ref)
			smp.WakeMS = int(s.now().Sub(started).Milliseconds())
			if err != nil {
				stop(fmt.Errorf("unpark: %w", err), true)
				return
			}
		}
	default:
		var err error
		if ref, err = s.startMember(ctx, prov, m, size, r, pkg, &smp); err != nil {
			stop(err, true)
			return
		}
		if !s.live(ctx, r.ID) {
			s.retireMember(ctx, m.ID, store.EndUnused, "reservation ended while starting")
			return
		}
		var boot BootReport
		ref, boot, err = s.Boot(ctx, prov, ref, BootPlan{Pool: true, Restricted: pkg.EgressAllow})
		if boot.ReadyMS >= 0 {
			smp.ReadyMS = smp.CreateMS + boot.ReadyMS
		}
		smp.RunnerMS = boot.RunnerMS
		if boot.Image != nil {
			smp.ImageVersion = boot.Image.Version
		}
		smp.Reseed = boot.Reseed
		if err != nil {
			stop(fmt.Errorf("boot: %w", err), true)
			return
		}
		img = boot.Image
	}
	if !s.live(ctx, r.ID) {
		s.retireMember(ctx, m.ID, store.EndUnused, "reservation ended while preparing")
		return
	}
	prep, err := s.prepare(ctx, prov, ref, r.KeyID, r.PlacementID, req, true, 0, img)
	smp.GateMS = prep.GateMS
	if img != nil {
		smp.ImageVersion = img.Version
	}
	if prep.Reseed != "" {
		smp.Reseed = prep.Reseed
	}
	if err != nil {
		stop(fmt.Errorf("prepare: %w", err), true)
		return
	}
	if !s.live(ctx, r.ID) {
		s.retireMember(ctx, m.ID, store.EndUnused, "reservation ended while preparing")
		return
	}
	s.setReservedLifetime(ctx, prov, prep.Ref, m, r, pkg)
	raw, err := json.Marshal(reservedPrep{
		KeyID: r.KeyID, GateReadyAt: timePtr(prep.GateReadyAt), SSHHost: prep.SSHHost,
		CAVersion: prep.CAVersion, Image: prep.Image,
	})
	if err != nil {
		stop(err, true)
		return
	}
	wctx, wcancel := step(ctx)
	err = s.d.Store.MarkReservationReady(wctx, r.ID, prep.Ref, raw, s.now())
	wcancel()
	if err != nil {
		if errors.Is(err, store.ErrStateConflict) {
			s.retireMember(ctx, m.ID, store.EndUnused, "reservation ended before ready")
			return
		}
		stop(err, true)
		return
	}
	s.recordSample(ctx, prov, size, smp, nil)
	s.publishReservation(ctx, r.ID, store.ReservationReady)
}

// timePtr is nil for the zero time
func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// startMember creates the sandbox of a speculative reservation under its member's key and records
// it in the member row
func (s *Saga) startMember(
	ctx context.Context, prov provider.Provider, m store.Member, size provider.Size,
	r store.Reservation, pkg catalog.Package, smp *store.StartSample,
) (provider.Ref, error) {
	cfg := s.d.Reserve
	created := s.now()
	ref, err := s.create(ctx, prov, provider.CreateRequest{
		IdempotencyKey: m.IdempotencyKey, Size: size, Labels: s.memberLabels(m),
		Lifetime: r.HoldUntil.Sub(created) + cfg.LapseGrace +
			time.Duration(pkg.Minutes)*time.Minute + lifetimeSlack,
		Egress: s.d.Boot.egressAllow(pkg.EgressAllow),
	})
	smp.CreateMS = int(s.now().Sub(created).Milliseconds())
	smp.SandboxID = ref.ID
	if err != nil {
		return ref, fmt.Errorf("create: %w", err)
	}
	mctx, cancel := step(ctx)
	defer cancel()
	if err := s.d.Store.MarkMemberCreated(mctx, m.ID, ref, created); err != nil {
		return ref, err
	}
	return ref, nil
}

// lifetimeUntil is want, never past what the provider allows from the sandbox's creation
func lifetimeUntil(prov provider.Provider, born time.Time, want time.Time) time.Time {
	if life := prov.Capabilities().MaxLifetime; life > 0 && !born.IsZero() {
		if limit := born.Add(life); want.After(limit) {
			return limit
		}
	}
	return want
}

// setReservedLifetime sets the provider lifetime to the latest a bind could leave it needing, a
// failure only logging since the lifetime set at create or fill still holds the sandbox
func (s *Saga) setReservedLifetime(
	ctx context.Context, prov provider.Provider, ref provider.Ref, m store.Member,
	r store.Reservation, pkg catalog.Package,
) {
	born := s.now()
	if m.SandboxCreatedAt != nil {
		born = *m.SandboxCreatedAt
	}
	want := r.HoldUntil.Add(s.d.Reserve.LapseGrace + time.Duration(pkg.Minutes)*time.Minute +
		lifetimeSlack)
	lctx, cancel := step(ctx)
	defer cancel()
	if err := prov.SetLifetime(lctx, ref, lifetimeUntil(prov, born, want)); err != nil {
		s.d.Log.Warn("set the lifetime of a reserved sandbox", "reservation", r.ID, "err", err)
	}
}

/* Binding */

// bindRequest is the quote an order carries and what it ordered
type bindRequest struct {
	sha []byte
	pkg catalog.Package
	req placement.Requirements
}

// bindTo reads the quote an order carries, nil for none or a malformed one, which is logged and
// never an error since the order is paid
func (s *Saga) bindTo(
	in CreateInput, pkg catalog.Package,
) *bindRequest {
	if in.QuoteID == "" {
		return nil
	}
	if !validQuote(in.QuoteID) {
		s.d.Log.Info("an order carries a malformed quote id, it starts without a reservation")
		return nil
	}
	return &bindRequest{sha: quoteSHA(in.QuoteID), pkg: pkg, req: in.Requirements}
}

// readyFits reports whether the quote's reservation is ready and matches, which spares the order
// the no candidate refusal since a prepared sandbox exists
func (s *Saga) readyFits(ctx context.Context, b *bindRequest) bool {
	if b == nil {
		return false
	}
	r, err := s.d.Store.OpenReservation(ctx, b.sha)
	return err == nil && r.State == store.ReservationReady && fits(r, b.pkg, b.req)
}

// reservationFor locks the quote's open reservation inside the order's transaction and returns it
// when it matches the order, else releases it and retires its member, nil for none (CP 18.5.5)
func (s *Saga) reservationFor(
	ctx context.Context, tx pgx.Tx, b *bindRequest,
) (*store.Reservation, error) {
	ts := s.d.Store.Tx(tx)
	r, err := ts.OpenReservationForUpdate(ctx, b.sha)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	mem, err := ts.MemberByID(ctx, r.MemberID)
	if err != nil {
		return nil, err
	}
	block, err := store.ProviderBlock(ctx, ts.Querier(), mem.Provider)
	if err != nil {
		return nil, err
	}
	ok := fits(r, b.pkg, b.req) && !s.now().After(r.ExpiresAt.Add(s.d.Reserve.LapseGrace)) &&
		s.d.Engine.Healthy(mem.Provider) && block == ""
	if !ok {
		s.d.Log.Info("a reservation does not match its order, it is released",
			"reservation", r.ID)
		return nil, s.endReservationTx(ctx, tx, r, store.ReservationReleased, "mismatch",
			store.EndUnused)
	}
	return &r, nil
}

// boundStart is a reservation's sandbox that became the placement's, to finish after the commit
type boundStart struct {
	ev       Event
	deadline time.Time
	readyAt  time.Time
	prov     provider.Provider
	ref      provider.Ref
	member   store.Member
	res      store.Reservation
}

// bindInTx makes the reservation's prepared sandbox the placing placement's and runs every ready
// write in tx, the member's ref opened under its id and sealed again under the placement's
func (s *Saga) bindInTx(
	ctx context.Context, tx pgx.Tx, p store.Placement, pkg catalog.Package, r store.Reservation,
) (*boundStart, error) {
	ts := s.d.Store.Tx(tx)
	at := s.now()
	m, err := ts.BindReservation(ctx, r, p, at)
	if err != nil {
		return nil, err
	}
	prov, ok := s.d.Registry.Get(m.Provider)
	if !ok || m.ProviderRef == nil {
		return nil, fmt.Errorf("reserved sandbox of %s has no provider or ref", m.Provider)
	}
	var rp reservedPrep
	if err := json.Unmarshal(r.Prepared, &rp); err != nil {
		return nil, fmt.Errorf("decode the prepared answer: %w", err)
	}
	prep := Prepared{
		Ref: *m.ProviderRef, KeyID: rp.KeyID, SSHHost: rp.SSHHost, CAVersion: rp.CAVersion,
		GateMS: store.NoStep, Image: rp.Image,
	}
	if rp.GateReadyAt != nil {
		prep.GateReadyAt = *rp.GateReadyAt
	}
	size, _ := sizeNamed(prov, m.SizeName)
	hourly, fee := quote(prov, size)
	rec := store.Attempt{
		PlacementID: p.ID, Attempt: 1, Provider: m.Provider, IdempotencyKey: m.IdempotencyKey,
		State: attemptCreating, SizeName: m.SizeName, HourlyMicros: hourly,
		StartFeeMicros: fee, SandboxID: m.ProviderRef.ID, Path: store.PathReserved, MemberID: m.ID,
	}
	deadline := at.Add(time.Duration(pkg.Minutes) * time.Minute)
	ev, err := s.readyWrites(ctx, tx, p, pkg, prov, m.SizeName, rec, prep, &m,
		store.PathReserved, at, deadline)
	if err != nil {
		return nil, err
	}
	return &boundStart{
		ev: ev, deadline: deadline, readyAt: at, prov: prov, ref: *m.ProviderRef, member: m, res: r,
	}, nil
}

// sizeNamed finds a size of a provider by name
func sizeNamed(prov provider.Provider, name string) (provider.Size, bool) {
	sz, ok := provider.As[provider.Sizer](prov)
	if !ok {
		return provider.Size{}, false
	}
	for _, s := range sz.Sizes() {
		if s.Name == name {
			return s, true
		}
	}
	return provider.Size{}, false
}

// afterBind finishes a bind once its transaction committed, the ready event, the session page, the
// lifetime and the sample
func (s *Saga) afterBind(
	ctx context.Context, b *boundStart, p store.Placement, pkg catalog.Package, arrived time.Time,
) {
	s.publish(ctx, b.ev)
	s.publishReservation(ctx, b.res.ID, store.ReservationBound)
	if s.d.Sessions != nil {
		dl := b.deadline
		s.d.Sessions.Placement(ctx, p.ID, sessions.PhaseReady, pkg, &dl, "")
	}
	ctx = context.WithoutCancel(ctx)
	s.wg.Go(func() {
		born := b.readyAt
		if b.member.SandboxCreatedAt != nil {
			born = *b.member.SandboxCreatedAt
		}
		want := lifetimeUntil(b.prov, born, b.deadline.Add(lifetimeSlack))
		lctx, cancel := step(ctx)
		defer cancel()
		if err := b.prov.SetLifetime(lctx, b.ref, want); err != nil {
			s.d.Log.Warn("shorten the lifetime of a bound sandbox", "placement", p.ID, "err", err)
		}
	})
	size, _ := sizeNamed(b.prov, b.member.SizeName)
	smp := s.sampleOf(p, b.prov, size, arrived)
	smp.StartPath, smp.MemberID, smp.ReservationID = store.PathReserved, b.member.ID, b.res.ID
	smp.BindMS = max(1, int(b.readyAt.Sub(arrived).Milliseconds()))
	if p.PaidAt != nil {
		smp.PaidToReadyMS = max(1, int(b.readyAt.Sub(*p.PaidAt).Milliseconds()))
	}
	smp.SandboxID = b.ref.ID
	s.recordSample(ctx, b.prov, size, smp, nil)
}

/* Waiting */

// awaitReservation waits for the reservation an order found still preparing and binds it, true
// when the placement is bound or left placing, false when the order goes on cold
//
// It subscribes to the reservation's channel and reads the row every 250 ms. A failed
// reservation, one whose heartbeat is stale and one still not ready after reserve.await_max end
// the wait, the last releasing it.
func (s *Saga) awaitReservation(
	ctx context.Context, p store.Placement, pkg catalog.Package, r store.Reservation,
	arrived time.Time,
) bool {
	wake, stopWatch := s.watchReservation(ctx, r.ID)
	defer stopWatch()
	limit := time.NewTimer(s.d.Reserve.AwaitMax)
	defer limit.Stop()
	poll := time.NewTicker(awaitPoll)
	defer poll.Stop()
	for {
		rctx, cancel := step(ctx)
		cur, err := s.d.Store.ReservationByID(rctx, r.ID)
		cancel()
		switch {
		case err != nil:
			s.d.Log.Warn("read an awaited reservation", "reservation", r.ID, "err", err)
			return false
		case cur.State != store.ReservationAwaited:
			// Failed, lapsed or released by someone else, with its member retired already
			return false
		case cur.Prepared != nil && cur.ReadyAt != nil:
			return s.bindAwaited(ctx, p, pkg, cur, arrived)
		case s.now().Sub(heartbeatOf(cur)) > staleAfter:
			s.failReservation(ctx, cur.ID, "heartbeat stale")
			return false
		}
		select {
		case <-wake:
		case <-poll.C:
		case <-limit.C:
			s.releaseAwaited(ctx, p.ID)
			return false
		case <-ctx.Done():
			return false
		}
	}
}

func heartbeatOf(r store.Reservation) time.Time {
	if r.Heartbeat != nil {
		return *r.Heartbeat
	}
	return r.CreatedAt
}

// releaseAwaited releases the reservation an order waited for and retires its member, so the
// order goes on cold
func (s *Saga) releaseAwaited(ctx context.Context, placementID uuid.UUID) {
	ctx, cancel := step(context.WithoutCancel(ctx))
	defer cancel()
	err := s.d.Store.WithTx(ctx, func(tx pgx.Tx) error {
		return s.releaseAwaitedTx(ctx, tx, placementID)
	})
	if err != nil {
		s.d.Log.Error("release an awaited reservation", "placement", placementID, "err", err)
	}
}

// bindAwaited binds a prepared reservation to the placing placement that awaits it, in one
// transaction, true when the placement is ready or left placing and false when the bind failed and
// the order goes on cold
func (s *Saga) bindAwaited(
	ctx context.Context, p store.Placement, pkg catalog.Package, r store.Reservation,
	arrived time.Time,
) bool {
	var bound *boundStart
	var cur store.Placement
	tctx, cancel := step(ctx)
	defer cancel()
	err := s.d.Store.WithTx(tctx, func(tx pgx.Tx) error {
		ts := s.d.Store.Tx(tx)
		var err error
		if cur, err = ts.PlacementForUpdate(tctx, p.ID); err != nil {
			return err
		}
		if cur.State != store.PlacementPlacing {
			return store.ErrStateConflict
		}
		locked, err := ts.ReservationForUpdate(tctx, r.ID)
		if err != nil {
			return err
		}
		if locked.State != store.ReservationAwaited || locked.Prepared == nil {
			return store.ErrStateConflict
		}
		bound, err = s.bindInTx(tctx, tx, cur, pkg, locked)
		return err
	})
	switch {
	case err == nil:
		cur, _ = s.d.Store.PlacementByID(ctx, p.ID)
		s.afterBind(ctx, bound, cur, pkg, arrived)
		return true
	case errors.Is(err, store.ErrStateConflict):
		// The placement moved on or the reservation ended, and the moving transaction cleaned up
		s.d.Log.Info("awaited reservation not bound", "placement", p.ID, "reservation", r.ID)
		return cur.State != store.PlacementPlacing && cur.ID != uuid.Nil
	}
	s.d.Log.Warn("bind an awaited reservation", "placement", p.ID, "reservation", r.ID, "err", err)
	s.failReservation(ctx, r.ID, "bind failed")
	return false
}
