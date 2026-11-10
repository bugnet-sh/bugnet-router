package saga_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"log/slog"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/ordertoken"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/pool"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/x402"
)

// quoteHash is the SHA-256 the router stores of a quote
func quoteHash(q string) []byte {
	sum := sha256.Sum256([]byte(q))
	return sum[:]
}

// reserveSettings is the default Reserve table with the local provider named a allowed to start
// sandboxes before payment, since the real providers never run in these tests
func reserveSettings(speculative ...string) config.Reserve {
	r := config.DefaultReserve()
	r.SpeculativeProviders = speculative
	return r
}

// reserveEnv is an env with a clock the test can move and a Reserve table of its own
func reserveEnv(
	t *testing.T, r config.Reserve, adjust func(*saga.Deps), ps ...provider.Provider,
) (*env, *clock) {
	t.Helper()
	clk := &clock{}
	e := newEnvWith(t, func(d *saga.Deps) {
		d.Reserve, d.Now = r, clk.Now
		d.HeartbeatInterval = 50 * time.Millisecond
		if adjust != nil {
			adjust(d)
		}
	}, ps...)
	return e, clk
}

func (e *env) reserveReq(
	t *testing.T, quote, pkg string, req placement.Requirements, ip string,
) saga.Outcome {
	t.Helper()
	in := saga.ReserveInput{QuoteID: quote, PackageID: pkg, Requirements: req}
	if ip != "" {
		in.ClientIP = netip.MustParseAddr(ip)
	}
	out, err := e.saga.Reserve(t.Context(), in)
	if err != nil {
		t.Fatalf("reserve %s: %v", quote, err)
	}
	return out
}

func (e *env) reserve(t *testing.T, quote, pkg string) saga.Outcome {
	t.Helper()
	return e.reserveReq(t, quote, pkg, placement.Requirements{}, "")
}

// reservation returns the quote's newest reservation row
func (e *env) reservation(t *testing.T, quote string) store.Reservation {
	t.Helper()
	var id uuid.UUID
	err := e.pool.QueryRow(t.Context(), `select id from reservations where quote_sha256 = $1
		order by created_at desc limit 1`, quoteHash(quote)).Scan(&id)
	if err != nil {
		t.Fatalf("no reservation for %s: %v", quote, err)
	}
	r, err := e.store.ReservationByID(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func (e *env) reservationCount(t *testing.T, quote string) int {
	t.Helper()
	var n int
	err := e.pool.QueryRow(t.Context(), `select count(*) from reservations
		where quote_sha256 = $1`, quoteHash(quote)).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// waitReservation waits until the quote's newest reservation is in state
func (e *env) waitReservation(t *testing.T, quote, state string) store.Reservation {
	t.Helper()
	var r store.Reservation
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
		var id uuid.UUID
		err := e.pool.QueryRow(t.Context(), `select id from reservations
			where quote_sha256 = $1 order by created_at desc limit 1`, quoteHash(quote)).Scan(&id)
		if err == nil {
			if r, err = e.store.ReservationByID(t.Context(), id); err == nil && r.State == state {
				return r
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("reservation of %s is %q, want %q", quote, r.State, state)
	return r
}

func (e *env) member(t *testing.T, id uuid.UUID) store.Member {
	t.Helper()
	m, err := e.store.MemberByID(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// createWithQuote creates an order carrying the quote and waits for its placing to end
func (e *env) createWithQuote(
	t *testing.T, order, quote string, req placement.Requirements,
) store.Placement {
	t.Helper()
	p, _, err := e.saga.Create(t.Context(), saga.CreateInput{
		Token: e.startToken(t, order), Requirements: req, QuoteID: quote,
	})
	if err != nil {
		t.Fatalf("create %s: %v", order, err)
	}
	e.saga.Wait()
	return e.reload(t, p.ID)
}

func (e *env) recountOf(t *testing.T, name string) int {
	t.Helper()
	_, now, err := e.store.RecountCapacity(t.Context(), name, time.Hour, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return now
}

func (e *env) retireJobs(t *testing.T) int {
	t.Helper()
	var n int
	err := e.pool.QueryRow(t.Context(),
		`select count(*) from river_job where kind = 'pool_retire'`).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// poolReserveEnv is a pool of one local sandbox, filled, under a Reserve table
func poolReserveEnv(
	t *testing.T, r config.Reserve, adjust func(*saga.Deps), ps ...provider.Provider,
) (*poolEnv, *clock) {
	t.Helper()
	clk := &clock{}
	pe := newPoolEnv(t, func(d *saga.Deps) {
		d.Reserve, d.Now = r, clk.Now
		d.HeartbeatInterval = 50 * time.Millisecond
		if adjust != nil {
			adjust(d)
		}
	}, ps...)
	return pe, clk
}

func TestReserveThenBind(t *testing.T) {
	ca := testCA(t)
	// A sandbox named boxd gets the CA, so the bind has a CA version to store
	a := newLocal(t, local.Options{Name: "boxd"})
	e, _ := poolReserveEnv(t, reserveSettings(), func(d *saga.Deps) {
		d.Boot.CAPEM, d.Boot.CAVersion = ca, 3
	}, a)
	members := e.fill(t)
	eq(t, len(members), 1)

	out := e.reserve(t, "q2.a", smallPkg)
	eq(t, out.Skip, saga.Skip(""))
	eq(t, out.ExpiresAt.IsZero(), false)
	res := e.waitReservation(t, "q2.a", store.ReservationReady)
	eq(t, res.Path, store.ReservePooled)
	eq(t, e.member(t, members[0].ID).State, store.MemberHeld)
	// The reservation's row names no credential and no ref
	eq(t, strings.Contains(string(res.Prepared), "token"), false)

	p := e.createWithQuote(t, "order-1", "q2.a", placement.Requirements{})
	eq(t, p.State, store.PlacementReady)
	eq(t, p.StartPath, "reserved")
	eq(t, p.ID, res.PlacementID)
	// The paid clock starts at the bind
	bound := e.reservation(t, "q2.a")
	eq(t, bound.State, store.ReservationBound)
	eq(t, p.ReadyAt.Equal(*bound.EndedAt), true)
	eq(t, len(e.deadlineJobs(t)), 1)
	eq(t, len(e.events(t, saga.EventReady)), 1)
	eq(t, p.EgressCAVersion, 3)
	eq(t, p.KeyID, res.KeyID)
	m := e.member(t, members[0].ID)
	eq(t, m.State, store.MemberBound)
	eq(t, *m.PlacementID, p.ID)
	eq(t, m.HoldsSlot, false)
	// The member's slot became the placement's
	eq(t, e.inUse(t, "boxd"), 1)
	eq(t, e.recountOf(t, "boxd"), 1)
	attempts, err := e.store.Attempts(t.Context(), p.ID)
	eq(t, err, nil)
	eq(t, len(attempts), 1)
	eq(t, [2]string{attempts[0].Path, attempts[0].State}, [2]string{"reserved", "ready"})
	eq(t, p.GateReadyAt != nil, true)
	eq(t, p.ProviderRef.ID, m.ProviderRef.ID)
}

func TestReserveSkips(t *testing.T) {
	t.Run("disabled", func(t *testing.T) {
		r := reserveSettings("a")
		r.Enabled = false
		e, _ := reserveEnv(t, r, nil, newLocal(t, local.Options{Name: "a"}))
		eq(t, e.reserve(t, "q2.b", smallPkg).Skip, saga.SkipDisabled)
	})
	t.Run("braked", func(t *testing.T) {
		e, _ := reserveEnv(t, reserveSettings("a"), nil, newLocal(t, local.Options{Name: "a"}))
		eq(t, e.vk.Do(t.Context(), e.vk.B().Set().Key(e.prefix+saga.BrakeKey).Value("20 0").
			Build()).Error(), nil)
		eq(t, e.reserve(t, "q2.b", smallPkg).Skip, saga.SkipBraked)
	})
	t.Run("braked by the spend cap", func(t *testing.T) {
		e, _ := reserveEnv(t, reserveSettings("a"), nil, newLocal(t, local.Options{Name: "a"}))
		eq(t, e.vk.Do(t.Context(), e.vk.B().Set().Key(e.prefix+pool.SpendKey).Value("1").
			Build()).Error(), nil)
		eq(t, e.reserve(t, "q2.b", smallPkg).Skip, saga.SkipBraked)
	})
	t.Run("no warm member", func(t *testing.T) {
		// The provider may not start before payment and keeps no pool
		e, _ := reserveEnv(t, reserveSettings(), nil, newLocal(t, local.Options{Name: "a"}))
		eq(t, e.reserve(t, "q2.b", smallPkg).Skip, saga.SkipNoWarm)
		eq(t, e.reservationCount(t, "q2.b"), 0)
	})
	t.Run("max open", func(t *testing.T) {
		r := reserveSettings("a")
		r.MaxOpen = 2
		e, _ := reserveEnv(t, r, nil, newLocal(t, local.Options{Name: "a"}))
		eq(t, e.reserve(t, "q2.1", smallPkg).Skip, saga.Skip(""))
		eq(t, e.reserve(t, "q2.2", smallPkg).Skip, saga.Skip(""))
		eq(t, e.reserve(t, "q2.3", smallPkg).Skip, saga.SkipBusy)
	})
	t.Run("per address", func(t *testing.T) {
		r := reserveSettings("a")
		r.PerIPOpen = 1
		e, _ := reserveEnv(t, r, nil, newLocal(t, local.Options{Name: "a"}))
		eq(t, e.reserveReq(t, "q2.1", smallPkg, placement.Requirements{}, "203.0.113.9").Skip,
			saga.Skip(""))
		eq(t, e.reserveReq(t, "q2.2", smallPkg, placement.Requirements{}, "203.0.113.9").Skip,
			saga.SkipBusy)
		// Another address, and a client that sent none, are not held to it
		eq(t, e.reserveReq(t, "q2.3", smallPkg, placement.Requirements{}, "203.0.113.10").Skip,
			saga.Skip(""))
		eq(t, e.reserve(t, "q2.4", smallPkg).Skip, saga.Skip(""))
	})
	t.Run("speculative cap", func(t *testing.T) {
		r := reserveSettings("a")
		r.SpeculativeMaxOpen = 1
		e, _ := reserveEnv(t, r, nil, newLocal(t, local.Options{Name: "a"}))
		eq(t, e.reserve(t, "q2.1", smallPkg).Skip, saga.Skip(""))
		eq(t, e.reserve(t, "q2.2", smallPkg).Skip, saga.SkipNoWarm)
	})
	t.Run("bucket", func(t *testing.T) {
		r := reserveSettings("a")
		r.PerSecond, r.Burst = 1, 2
		e, _ := reserveEnv(t, r, nil, newLocal(t, local.Options{Name: "a"}))
		eq(t, e.reserve(t, "q2.1", smallPkg).Skip, saga.Skip(""))
		eq(t, e.reserve(t, "q2.2", smallPkg).Skip, saga.Skip(""))
		eq(t, e.reserve(t, "q2.3", smallPkg).Skip, saga.SkipBusy)
	})
	t.Run("slow valkey", func(t *testing.T) {
		vk := &slowValkey{}
		e, _ := reserveEnv(t, reserveSettings("a"), func(d *saga.Deps) {
			vk.Client = d.Valkey
			d.Valkey = vk
		}, newLocal(t, local.Options{Name: "a"}))
		vk.slow.Store(true)
		start := time.Now()
		eq(t, e.reserve(t, "q2.b", smallPkg).Skip, saga.SkipBusy)
		if d := time.Since(start); d > 2*time.Second {
			t.Fatalf("a silent Valkey held Reserve for %s", d)
		}
	})
}

// slowValkey answers nothing until its caller gives up once slow is set
type slowValkey struct {
	valkey.Client
	slow atomic.Bool
}

func (v *slowValkey) Do(ctx context.Context, cmd valkey.Completed) valkey.ValkeyResult {
	if v.slow.Load() {
		// Once the caller gave up, the client answers with the context's error
		<-ctx.Done()
	}
	return v.Client.Do(ctx, cmd)
}

func TestRenewalAfterAPackageSwitchIsNotQuoteSpent(t *testing.T) {
	e, _ := reserveEnv(t, reserveSettings("a"), nil, newLocal(t, local.Options{Name: "a"}))
	eq(t, e.reserve(t, "q2.s", smallPkg).Skip, saga.Skip(""))
	first := e.reservation(t, "q2.s")
	// Another package releases the first reservation, and the second one counts
	eq(t, e.reserve(t, "q2.s", longPkg).Skip, saga.Skip(""))
	eq(t, e.reservation(t, "q2.s").ID == first.ID, false)
	eq(t, e.member(t, first.MemberID).State, store.MemberRetiring)
	eq(t, e.reservationCount(t, "q2.s"), 2)
	// The same package again renews the second one, which is no new reservation
	eq(t, e.reserve(t, "q2.s", longPkg).Skip, saga.Skip(""))
	eq(t, e.reservationCount(t, "q2.s"), 2)
	// A package switch back would be a third
	eq(t, e.reserve(t, "q2.s", smallPkg).Skip, saga.SkipQuoteSpent)
	e.saga.Wait()
}

func TestRestrictedPackageIsNotPoolable(t *testing.T) {
	restricted := func() provider.Provider {
		a := newScripted(newLocal(t, local.Options{Name: "a"}), map[string]answer{})
		a.caps = func(c provider.Capabilities) provider.Capabilities {
			c.EgressControl = true
			return c
		}
		return a
	}
	t.Run("no start of its own", func(t *testing.T) {
		e, _ := reserveEnv(t, reserveSettings(), func(d *saga.Deps) {
			d.Boot.EgressHost = "egress.example"
		}, restricted())
		eq(t, e.reserve(t, "q2.r", restrictedPkg).Skip, saga.SkipNotPoolable)
	})
	t.Run("no provider with egress control", func(t *testing.T) {
		e, _ := reserveEnv(t, reserveSettings("a"), nil, newLocal(t, local.Options{Name: "a"}))
		eq(t, e.reserve(t, "q2.r", restrictedPkg).Skip, saga.SkipNotPoolable)
	})
	t.Run("a start of its own carries the list", func(t *testing.T) {
		a := restricted().(scripted)
		e, _ := reserveEnv(t, reserveSettings("a"), func(d *saga.Deps) {
			d.Boot.EgressHost = "egress.example"
		}, a)
		eq(t, e.reserve(t, "q2.r", restrictedPkg).Skip, saga.Skip(""))
		e.waitReservation(t, "q2.r", store.ReservationReady)
		reqs := *a.reqs
		eq(t, len(reqs), 1)
		eq(t, reqs[0].Egress != nil, true)
	})
}

func TestSharedRangesSkipThePerIPCap(t *testing.T) {
	e, _ := reserveEnv(t, reserveSettings("a"), nil, newLocal(t, local.Options{Name: "a"}))
	for i, q := range []string{"q2.1", "q2.2", "q2.3"} {
		out := e.reserveReq(t, q, smallPkg, placement.Requirements{}, "160.79.104.9")
		eq(t, out.Skip, saga.Skip(""))
		// An address in a shared range is never kept
		eq(t, e.reservation(t, q).ClientIP == nil, true)
		_ = i
	}
	// Outside the range the second per-address cap is 2
	eq(t, e.reserveReq(t, "q2.4", smallPkg, placement.Requirements{}, "203.0.113.9").Skip,
		saga.Skip(""))
	eq(t, e.reserveReq(t, "q2.5", smallPkg, placement.Requirements{}, "203.0.113.9").Skip,
		saga.Skip(""))
	eq(t, e.reserveReq(t, "q2.6", smallPkg, placement.Requirements{}, "203.0.113.9").Skip,
		saga.SkipBusy)
	e.saga.Wait()
}

func TestReserveMissRecordedOnce(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a"})
	e, _ := poolReserveEnv(t, reserveSettings(), nil, a)
	// The pool has no warm member yet, and the provider may not start before payment
	eq(t, e.reserve(t, "q2.m", smallPkg).Skip, saga.SkipNoWarm)
	eq(t, e.misses(t), 1)
	eq(t, e.reserve(t, "q2.n", smallPkg).Skip, saga.SkipNoWarm)
	eq(t, e.misses(t), 2)
}

func TestReserveShareIsNoMiss(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a"})
	e, _ := poolReserveEnv(t, reserveSettings(), nil, a)
	e.fill(t)
	eq(t, e.reserve(t, "q2.1", smallPkg).Skip, saga.Skip(""))
	e.waitReservation(t, "q2.1", store.ReservationReady)
	// The target is 1 and one member is held, which is the whole share of a pool of one
	eq(t, e.reserve(t, "q2.2", smallPkg).Skip, saga.SkipNoWarm)
	eq(t, e.misses(t), 0)
	e.saga.Wait()
}

func TestQuoteIDRules(t *testing.T) {
	e, _ := reserveEnv(t, reserveSettings("a"), nil, newLocal(t, local.Options{Name: "a"}))
	for _, q := range []string{"", strings.Repeat("q", 129), "q2.\x01", "q2.\x7f", "q2.é"} {
		_, err := e.saga.Reserve(t.Context(), saga.ReserveInput{QuoteID: q, PackageID: smallPkg})
		eq(t, err, saga.ErrQuoteInvalid)
		eq(t, e.saga.ReleaseReservation(t.Context(), q), saga.ErrQuoteInvalid)
	}
	_, err := e.saga.Reserve(t.Context(), saga.ReserveInput{
		QuoteID: strings.Repeat("q", 128), PackageID: "nope",
	})
	eq(t, err != nil && err != saga.ErrQuoteInvalid, true)
}

func TestReleaseReservationWithoutOneIsOK(t *testing.T) {
	e, _ := reserveEnv(t, reserveSettings("a"), nil, newLocal(t, local.Options{Name: "a"}))
	eq(t, e.saga.ReleaseReservation(t.Context(), "q2.none"), nil)
	eq(t, e.reserve(t, "q2.r", smallPkg).Skip, saga.Skip(""))
	r := e.waitReservation(t, "q2.r", store.ReservationReady)
	eq(t, e.saga.ReleaseReservation(t.Context(), "q2.r"), nil)
	eq(t, e.reservation(t, "q2.r").State, store.ReservationReleased)
	eq(t, e.member(t, r.MemberID).State, store.MemberRetiring)
	eq(t, e.retireJobs(t), 1)
	// Releasing again finds nothing open
	eq(t, e.saga.ReleaseReservation(t.Context(), "q2.r"), nil)
	eq(t, e.retireJobs(t), 1)
}

// rowCount counts the rows a query returns
func rowCount(t *testing.T, e *env, query string, args ...any) int {
	t.Helper()
	rows, err := e.pool.Query(t.Context(), query, args...)
	if err != nil {
		t.Fatal(err)
	}
	n, err := pgx.CollectRows(rows, pgx.RowTo[int])
	if err != nil {
		t.Fatal(err)
	}
	return len(n)
}

// pricey bills a different hour than the local provider's
type pricey struct {
	*local.Provider
	hourly int64
}

func (p pricey) HourlyCostMicros(provider.Size) int64 { return p.hourly }

func TestRenewalStopsAtMaxHold(t *testing.T) {
	e, clk := reserveEnv(t, reserveSettings("a"), nil, newLocal(t, local.Options{Name: "a"}))
	t0 := clk.Now()
	first := e.reserve(t, "q2.h", smallPkg)
	eq(t, first.ExpiresAt.Sub(t0) >= 119*time.Second && first.ExpiresAt.Sub(t0) <= 121*time.Second,
		true)
	clk.Advance(100 * time.Second)
	second := e.reserve(t, "q2.h", smallPkg)
	eq(t, second.ExpiresAt.After(first.ExpiresAt), true)
	clk.Advance(80 * time.Second)
	third := e.reserve(t, "q2.h", smallPkg)
	// The hold ends 300 s after the first Reserve, whatever the renewal asks
	hold := e.reservation(t, "q2.h").HoldUntil
	eq(t, third.ExpiresAt.Equal(hold), true)
	clk.Advance(100 * time.Second)
	fourth := e.reserve(t, "q2.h", smallPkg)
	eq(t, fourth.ExpiresAt.Equal(hold), true)
	eq(t, e.reservationCount(t, "q2.h"), 1)
	e.saga.Wait()
}

func TestRenewalRefusedPastTheFloor(t *testing.T) {
	// A 150 s hold takes this $0.05 package's margin to its floor of 50%, a longer one under it
	a := pricey{newLocal(t, local.Options{Name: "a"}), 120_000}
	e, clk := reserveEnv(t, reserveSettings("a"), nil, a)
	first := e.reserve(t, "q2.f", smallPkg)
	eq(t, first.Skip, saga.Skip(""))
	clk.Advance(60 * time.Second)
	// The renewal would hold 210 s, which no candidate clears, so the hold keeps its expiry
	again := e.reserve(t, "q2.f", smallPkg)
	eq(t, again.Skip, saga.Skip(""))
	// The held expiry comes back from Postgres, which keeps microseconds
	kept := first.ExpiresAt.Truncate(time.Microsecond)
	eq(t, again.ExpiresAt.Equal(kept), true)
	eq(t, e.reservation(t, "q2.f").ExpiresAt.Equal(kept), true)
	e.saga.Wait()
}

func TestSpeculativeE2BExampleRefused(t *testing.T) {
	// CP 18.5.3's example, E2B 2x2 at $0.1332 an hour for a $0.05 package of 10 minutes
	a := pricey{newLocal(t, local.Options{Name: "e2b"}), 133_200}
	e, _ := reserveEnv(t, reserveSettings("e2b"), nil, a)
	eq(t, e.reserve(t, "q2.e", smallPkg).Skip, saga.SkipNoWarm)
	eq(t, e.reservationCount(t, "q2.e"), 0)
	// The same package without a hold clears the floor, so the order itself still starts
	p, _ := e.placeReady(t, "order-1")
	eq(t, p.Provider, "e2b")
}

func TestSpeculativePathRunsPrepareOnce(t *testing.T) {
	a := newScripted(newLocal(t, local.Options{Name: "a"}), map[string]answer{})
	e, _ := reserveEnv(t, reserveSettings("a"), nil, a)
	eq(t, e.reserve(t, "q2.p", smallPkg).Skip, saga.Skip(""))
	e.waitReservation(t, "q2.p", store.ReservationReady)
	// Boot with Pool set ran the pool phase, and Prepare ran the full phase and the sampler once
	eq(t, a.calls(), []string{"setup pool", "setup sample=false", "setup sample=true"})
	p := e.createWithQuote(t, "order-1", "q2.p", placement.Requirements{})
	eq(t, p.StartPath, "reserved")
	eq(t, a.calls(), []string{"setup pool", "setup sample=false", "setup sample=true"})
	eq(t, len(*a.reqs), 1)
	eq(t, (*a.reqs)[0].IdempotencyKey, "reserve-"+e.reservation(t, "q2.p").MemberID.String())
}

func TestSetLifetimeCappedAtMaxLifetime(t *testing.T) {
	prov := newCapped(t, t.TempDir(), 21*time.Minute)
	born := time.Now()
	// A want past the limit is cut to the limit, one inside it stands
	eq(t, saga.LifetimeUntil(prov, born, born.Add(30*time.Minute)).Equal(born.Add(21*time.Minute)),
		true)
	eq(t, saga.LifetimeUntil(prov, born, born.Add(10*time.Minute)).Equal(born.Add(10*time.Minute)),
		true)
	unlimited := newLocal(t, local.Options{Name: "u"})
	eq(t, saga.LifetimeUntil(unlimited, born, born.Add(time.Hour)).Equal(born.Add(time.Hour)),
		true)

	// A reserved sandbox is asked for a lifetime the provider allows from its creation
	root := t.TempDir()
	c := newCapped(t, root, 21*time.Minute)
	e, _ := reserveEnv(t, reserveSettings("a"), nil, c)
	eq(t, e.reserve(t, "q2.l", smallPkg).Skip, saga.Skip(""))
	r := e.waitReservation(t, "q2.l", store.ReservationReady)
	m := e.member(t, r.MemberID)
	until := lifetime(t, root, m.ProviderRef)
	eq(t, until.After(m.SandboxCreatedAt.Add(21*time.Minute+time.Second)), false)
	eq(t, c.calls.Load() >= 1, true)
}

func TestPreparationHeartbeatsUntilBound(t *testing.T) {
	e, _ := reserveEnv(t, reserveSettings("a"), nil, newLocal(t, local.Options{Name: "a"}))
	eq(t, e.reserve(t, "q2.b", smallPkg).Skip, saga.Skip(""))
	r := e.waitReservation(t, "q2.b", store.ReservationReady)
	// A ready row beats too
	first := *e.reservation(t, "q2.b").Heartbeat
	time.Sleep(300 * time.Millisecond)
	later := *e.reservation(t, "q2.b").Heartbeat
	eq(t, later.After(first), true)
	eq(t, e.reservation(t, "q2.b").Owner, "replica-test")
	p := e.createWithQuote(t, "order-1", "q2.b", placement.Requirements{})
	eq(t, p.ID, r.PlacementID)
	// Once bound it beats no more
	bound := *e.reservation(t, "q2.b").Heartbeat
	time.Sleep(300 * time.Millisecond)
	eq(t, e.reservation(t, "q2.b").Heartbeat.Equal(bound), true)
}

// secretMeta puts a provider credential in the Meta of every sandbox it creates
type secretMeta struct{ *local.Provider }

func (s secretMeta) Create(ctx context.Context, req provider.CreateRequest) (provider.Ref, error) {
	ref, err := s.Provider.Create(ctx, req)
	if err == nil {
		if ref.Meta == nil {
			ref.Meta = map[string]string{}
		}
		ref.Meta["envd_token"] = "secret-envd-value"
	}
	return ref, err
}

func TestPreparedHoldsNoCredential(t *testing.T) {
	e, _ := reserveEnv(t, reserveSettings("a"), nil,
		secretMeta{newLocal(t, local.Options{Name: "a"})})
	eq(t, e.reserve(t, "q2.c", smallPkg).Skip, saga.Skip(""))
	r := e.waitReservation(t, "q2.c", store.ReservationReady)
	var prepared string
	eq(t, e.pool.QueryRow(t.Context(), `select prepared::text from reservations where id = $1`,
		r.ID).Scan(&prepared), nil)
	eq(t, strings.Contains(prepared, "secret-envd-value"), false)
	// The member's sealed ref opens under the member's id and no other
	var sealed string
	eq(t, e.pool.QueryRow(t.Context(),
		`select provider_ref->'meta'->>'envd_token' from pool_members where id = $1`,
		r.MemberID).Scan(&sealed), nil)
	eq(t, strings.HasPrefix(sealed, "v1."), true)
	opened, err := e.keys.OpenRef(r.MemberID, sealed)
	eq(t, err, nil)
	eq(t, opened, "secret-envd-value")
	_, err = e.keys.OpenRef(r.PlacementID, sealed)
	eq(t, err != nil, true)

	p := e.createWithQuote(t, "order-1", "q2.c", placement.Requirements{})
	eq(t, p.StartPath, "reserved")
	// The bind sealed it again under the placement's id
	eq(t, e.pool.QueryRow(t.Context(),
		`select provider_ref->'meta'->>'envd_token' from placements where id = $1`,
		p.ID).Scan(&sealed), nil)
	opened, err = e.keys.OpenRef(p.ID, sealed)
	eq(t, err, nil)
	eq(t, opened, "secret-envd-value")
	eq(t, e.pool.QueryRow(t.Context(),
		`select provider_ref->'meta'->>'envd_token' from pool_members where id = $1`,
		r.MemberID).Scan(&sealed), nil)
	_, err = e.keys.OpenRef(r.MemberID, sealed)
	eq(t, err != nil, true)
	opened, err = e.keys.OpenRef(p.ID, sealed)
	eq(t, err, nil)
	eq(t, opened, "secret-envd-value")
	eq(t, p.ProviderRef.Meta["envd_token"], "secret-envd-value")
}

func TestMismatchReleasesAndRetires(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a"})
	e, _ := reserveEnv(t, reserveSettings("a"), func(d *saga.Deps) {
		reg, err := provider.NewRegistry([]provider.Provider{a},
			provider.Features{Preview: []string{"a"}})
		if err != nil {
			t.Fatal(err)
		}
		d.Registry, d.Engine = reg, placement.New(reg, d.Tracker, nil)
	}, a)
	eq(t, e.reserve(t, "q2.x", smallPkg).Skip, saga.Skip(""))
	r := e.waitReservation(t, "q2.x", store.ReservationReady)
	// An order for another package never sees the reservation
	tok := e.token(t, tokenFor(longPkg, "order-1", 120_000))
	p, _, err := e.saga.Create(t.Context(), saga.CreateInput{Token: tok, QuoteID: "q2.x"})
	eq(t, err, nil)
	e.saga.Wait()
	p = e.reload(t, p.ID)
	eq(t, p.State, store.PlacementReady)
	eq(t, p.StartPath, "cold")
	eq(t, p.ID == r.PlacementID, false)
	eq(t, e.reservation(t, "q2.x").State, store.ReservationReleased)
	eq(t, e.member(t, r.MemberID).State, store.MemberRetiring)
	eq(t, e.retireJobs(t), 1)
	// The released reservation's slot is still held until its job runs, and the order took its own
	eq(t, e.inUse(t, "a"), 2)
	eq(t, e.recountOf(t, "a"), 2)

	// Other requirements are a mismatch too
	eq(t, e.reserve(t, "q2.y", smallPkg).Skip, saga.Skip(""))
	r = e.waitReservation(t, "q2.y", store.ReservationReady)
	p2 := e.createWithQuote(t, "order-2", "q2.y", placement.Requirements{Preview: true})
	eq(t, p2.StartPath, "cold")
	eq(t, e.reservation(t, "q2.y").State, store.ReservationReleased)
	eq(t, e.member(t, r.MemberID).State, store.MemberRetiring)
}

func TestWalletRefusalLeavesTheReservationOpen(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a"})
	e, _ := reserveEnv(t, reserveSettings("a"), func(d *saga.Deps) {
		d.Wallets = config.Wallets{AnonymousMaxConcurrent: 1}
	}, a)
	_, ha := e.placeReady(t, "order-a")
	eq(t, e.reserve(t, "q2.w", smallPkg).Skip, saga.Skip(""))
	r := e.waitReservation(t, "q2.w", store.ReservationReady)

	// The wallet is at its cap, so the order fails under its own fresh id and binds nothing
	_, _, err := e.saga.Create(t.Context(), saga.CreateInput{
		Token: e.startToken(t, "order-b"), QuoteID: "q2.w",
	})
	eq(t, err, saga.ErrWalletBusy)
	failed, err := e.store.PlacementByStartOrder(t.Context(), "order-b")
	eq(t, err, nil)
	eq(t, failed.State, store.PlacementFailed)
	eq(t, failed.ID == r.PlacementID, false)
	eq(t, e.reservation(t, "q2.w").State, store.ReservationReady)
	eq(t, e.member(t, r.MemberID).State, store.MemberHeld)

	// Once the wallet has room, the quote's next order binds it
	_, err = e.saga.Stop(t.Context(), ha)
	eq(t, err, nil)
	p := e.createWithQuote(t, "order-c", "q2.w", placement.Requirements{})
	eq(t, p.State, store.PlacementReady)
	eq(t, p.StartPath, "reserved")
	eq(t, p.ID, r.PlacementID)
}

func TestSecondOrderWithTheQuoteStartsCold(t *testing.T) {
	e, _ := reserveEnv(t, reserveSettings("a"), nil, newLocal(t, local.Options{Name: "a"}))
	eq(t, e.reserve(t, "q2.s", smallPkg).Skip, saga.Skip(""))
	e.waitReservation(t, "q2.s", store.ReservationReady)
	first := e.createWithQuote(t, "order-1", "q2.s", placement.Requirements{})
	eq(t, first.StartPath, "reserved")
	second := e.createWithQuote(t, "order-2", "q2.s", placement.Requirements{})
	eq(t, second.State, store.PlacementReady)
	eq(t, second.StartPath, "cold")
	eq(t, second.ID == first.ID, false)
}

func TestRetriedOrderReturnsTheBoundPlacement(t *testing.T) {
	e, _ := reserveEnv(t, reserveSettings("a"), nil, newLocal(t, local.Options{Name: "a"}))
	eq(t, e.reserve(t, "q2.t", smallPkg).Skip, saga.Skip(""))
	r := e.waitReservation(t, "q2.t", store.ReservationReady)
	tok := e.startToken(t, "order-1")
	first, h, err := e.saga.Create(t.Context(), saga.CreateInput{Token: tok, QuoteID: "q2.t"})
	eq(t, err, nil)
	e.saga.Wait()
	var named uuid.UUID
	eq(t, e.pool.QueryRow(t.Context(),
		`select placement_id from orders where order_id = 'order-1'`).Scan(&named), nil)
	// The order row names the reservation's placement id, and a retry reads it from there
	eq(t, named, r.PlacementID)
	again, h2, err := e.saga.Create(t.Context(), saga.CreateInput{Token: tok, QuoteID: "q2.t"})
	eq(t, err, nil)
	eq(t, again.ID, first.ID)
	eq(t, h2, h)
	eq(t, again.State, store.PlacementReady)
	eq(t, e.placementCount(t), 1)
	eq(t, len(e.deadlineJobs(t)), 1)
}

// tokenFor claims for a start order of a package
func tokenFor(pkg, order string, amount int64) ordertoken.Claims {
	return ordertoken.Claims{
		OrderID: order, Kind: ordertoken.KindStart, PackageID: pkg, AmountMicros: amount,
	}
}

func TestBindStoresNeedsSecrets(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a"})
	e, _ := reserveEnv(t, reserveSettings("a"), func(d *saga.Deps) {
		reg, err := provider.NewRegistry([]provider.Provider{a},
			provider.Features{Secrets: []string{"a"}})
		if err != nil {
			t.Fatal(err)
		}
		d.Registry, d.Engine = reg, placement.New(reg, d.Tracker, nil)
	}, a)
	req := placement.Requirements{Secrets: true}
	eq(t, e.reserveReq(t, "q2.n", smallPkg, req, "").Skip, saga.Skip(""))
	e.waitReservation(t, "q2.n", store.ReservationReady)
	p := e.createWithQuote(t, "order-1", "q2.n", req)
	eq(t, p.StartPath, "reserved")
	eq(t, p.NeedsSecrets, true)
}

func TestWaitCoversAPreparingReservation(t *testing.T) {
	e, _ := reserveEnv(t, reserveSettings("a"), nil, newLocal(t, local.Options{Name: "a"}))
	eq(t, e.reserve(t, "q2.w", smallPkg).Skip, saga.Skip(""))
	// Wait returns once the preparation is done, and never waits for the hold to end
	e.saga.Wait()
	eq(t, e.reservation(t, "q2.w").State, store.ReservationReady)
}

func TestMalformedQuoteOnCreateIsIgnored(t *testing.T) {
	var logs syncBuffer
	e, _ := reserveEnv(t, reserveSettings("a"), func(d *saga.Deps) {
		d.Log = slog.New(slog.NewTextHandler(&logs, nil))
	}, newLocal(t, local.Options{Name: "a"}))
	p := e.createWithQuote(t, "order-1", "bad\x01quote", placement.Requirements{})
	eq(t, p.State, store.PlacementReady)
	eq(t, p.StartPath, "cold")
	eq(t, strings.Count(logs.String(), "malformed quote id"), 1)
	// A quote nobody reserved is no error either
	p = e.createWithQuote(t, "order-2", "q2.unknown", placement.Requirements{})
	eq(t, p.StartPath, "cold")
}

// heldReady blocks the first WaitReady until the test opens hold, then fails it when failAfter is
// set, and answers every later one at once
type heldReady struct {
	*local.Provider
	hold      chan struct{}
	failAfter bool
	calls     *atomic.Int64
	entered   chan struct{}
}

func newHeldReady(p *local.Provider) heldReady {
	return heldReady{
		Provider: p, hold: make(chan struct{}), calls: &atomic.Int64{},
		entered: make(chan struct{}, 8),
	}
}

func (h heldReady) WaitReady(ctx context.Context, ref provider.Ref) error {
	if h.calls.Add(1) > 1 {
		return h.Provider.WaitReady(ctx, ref)
	}
	h.entered <- struct{}{}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-h.hold:
	}
	if h.failAfter {
		return &provider.Error{Code: provider.CodeInternal, Err: errors.New("boot failed")}
	}
	return h.Provider.WaitReady(ctx, ref)
}

// awaitEnv is an env with a speculative provider whose first boot is held
func awaitEnv(
	t *testing.T, r config.Reserve, h heldReady, adjust func(*saga.Deps),
) (*env, *clock) {
	t.Helper()
	return reserveEnv(t, r, adjust, h)
}

// startAwaiting reserves the quote, waits until its sandbox is booting, then creates the order
// without waiting for its placing to end
func startAwaiting(
	t *testing.T, e *env, h heldReady, quote, order string, req placement.Requirements,
) store.Placement {
	t.Helper()
	eq(t, e.reserveReq(t, quote, smallPkg, req, "").Skip, saga.Skip(""))
	<-h.entered
	p, _, err := e.saga.Create(t.Context(), saga.CreateInput{
		Token: e.startToken(t, order), Requirements: req, QuoteID: quote,
	})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, p.State, store.PlacementPlacing)
	return p
}

func TestOrderWaitsForAPreparingReservation(t *testing.T) {
	t.Run("binds once prepared", func(t *testing.T) {
		h := newHeldReady(newLocal(t, local.Options{Name: "a"}))
		e, _ := awaitEnv(t, reserveSettings("a"), h, nil)
		p := startAwaiting(t, e, h, "q2.a", "order-1", placement.Requirements{})
		res := e.reservation(t, "q2.a")
		eq(t, res.State, store.ReservationAwaited)
		eq(t, p.ID, res.PlacementID)
		close(h.hold)
		e.saga.Wait()
		got := e.reload(t, p.ID)
		eq(t, got.State, store.PlacementReady)
		eq(t, got.StartPath, "reserved")
		eq(t, e.reservation(t, "q2.a").State, store.ReservationBound)
		eq(t, e.member(t, res.MemberID).State, store.MemberBound)
		eq(t, e.inUse(t, "a"), 1)
		eq(t, e.recountOf(t, "a"), 1)
	})
	t.Run("a failed reservation goes cold", func(t *testing.T) {
		h := newHeldReady(newLocal(t, local.Options{Name: "a"}))
		h.failAfter = true
		e, _ := awaitEnv(t, reserveSettings("a"), h, nil)
		p := startAwaiting(t, e, h, "q2.a", "order-1", placement.Requirements{})
		res := e.reservation(t, "q2.a")
		close(h.hold)
		e.saga.Wait()
		got := e.reload(t, p.ID)
		eq(t, got.State, store.PlacementReady)
		eq(t, got.StartPath, "cold")
		eq(t, e.reservation(t, "q2.a").State, store.ReservationFailed)
		eq(t, e.member(t, res.MemberID).State, store.MemberRetiring)
		eq(t, e.retireJobs(t), 1)
	})
}

func TestAwaitMaxReleasesAndGoesCold(t *testing.T) {
	h := newHeldReady(newLocal(t, local.Options{Name: "a"}))
	r := reserveSettings("a")
	r.AwaitMax = 300 * time.Millisecond
	e, _ := awaitEnv(t, r, h, nil)
	p := startAwaiting(t, e, h, "q2.a", "order-1", placement.Requirements{})
	res := e.reservation(t, "q2.a")
	start := time.Now()
	for e.reload(t, p.ID).State == store.PlacementPlacing && time.Since(start) < 10*time.Second {
		time.Sleep(20 * time.Millisecond)
	}
	got := e.reload(t, p.ID)
	eq(t, got.State, store.PlacementReady)
	eq(t, got.StartPath, "cold")
	eq(t, e.reservation(t, "q2.a").State, store.ReservationReleased)
	eq(t, e.member(t, res.MemberID).State, store.MemberRetiring)
	// The preparation sees its reservation gone and stops
	close(h.hold)
	e.saga.Wait()
	eq(t, e.reservation(t, "q2.a").State, store.ReservationReleased)
}

func TestAwaitedReservationFreedWhenPlacingDies(t *testing.T) {
	h := newHeldReady(newLocal(t, local.Options{Name: "a"}))
	e := newPoolEnv(t, func(d *saga.Deps) {
		d.Reserve = reserveSettings("a")
		d.HeartbeatInterval = 50 * time.Millisecond
	}, h)
	p := startAwaiting(t, e.env, h, "q2.d", "order-1", placement.Requirements{})
	r := e.reservation(t, "q2.d")
	eq(t, r.State, store.ReservationAwaited)
	// The placing replica died, so the watch fails the placement
	eq(t, e.saga.FailStuck(t.Context(), p.ID, saga.ReasonPlacingStalled), nil)
	eq(t, e.reservation(t, "q2.d").State, store.ReservationReleased)
	eq(t, e.member(t, r.MemberID).State, store.MemberRetiring)
	eq(t, e.retireJobs(t), 1)
	close(h.hold)
	e.saga.Wait()
	eq(t, e.mgr.Retire(t.Context(), r.MemberID, store.EndUnused), nil)
	eq(t, e.member(t, r.MemberID).State, store.MemberGone)
	eq(t, e.recount(t, "a"), e.inUse(t, "a"))
	eq(t, e.inUse(t, "a"), 0)
	eq(t, e.reload(t, p.ID).State, store.PlacementFailed)
}

func TestKeepaliveFailureFailsTheReservation(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a"})
	r := reserveSettings("a")
	r.Keepalive = 50 * time.Millisecond
	e, _ := reserveEnv(t, r, nil, a)
	eq(t, e.reserve(t, "q2.k", smallPkg).Skip, saga.Skip(""))
	res := e.waitReservation(t, "q2.k", store.ReservationReady)
	m := e.member(t, res.MemberID)
	// The sandbox dies before any order binds it
	eq(t, a.Destroy(t.Context(), *m.ProviderRef), nil)
	e.waitReservation(t, "q2.k", store.ReservationFailed)
	eq(t, e.member(t, res.MemberID).State, store.MemberRetiring)
	eq(t, e.retireJobs(t), 1)
	// An order with the quote starts cold
	p := e.createWithQuote(t, "order-1", "q2.k", placement.Requirements{})
	eq(t, p.State, store.PlacementReady)
	eq(t, p.StartPath, "cold")
}

func TestBindAppliesX402Rows(t *testing.T) {
	h := newHeldReady(newLocal(t, local.Options{Name: "a"}))
	e, _ := awaitEnv(t, reserveSettings("a"), h, func(d *saga.Deps) {
		reg, err := provider.NewRegistry([]provider.Provider{h},
			provider.Features{Secrets: []string{"a"}})
		if err != nil {
			t.Fatal(err)
		}
		d.Registry, d.Engine = reg, placement.New(reg, d.Tracker, nil)
	})
	req := placement.Requirements{Secrets: true}
	p := startAwaiting(t, e, h, "q2.x", "order-1", req)
	err := e.store.PutX402Service(t.Context(), store.X402Service{
		PlacementID: p.ID, ServiceID: x402.ServiceID("GET", "api.example.com", "/v1/"),
		Spec: x402.Spec{
			Host: "api.example.com", Prefix: "/v1/", Method: "GET", MaxPriceMicros: 10_000,
			PayTo: x402PayTo, Indexed: true,
		},
	})
	eq(t, err, nil)
	eq(t, applyJobs(t, e, p.ID), 0)
	close(h.hold)
	e.saga.Wait()
	got := e.reload(t, p.ID)
	eq(t, got.State, store.PlacementReady)
	eq(t, got.StartPath, "reserved")
	eq(t, applyJobs(t, e, p.ID), 1)
}
