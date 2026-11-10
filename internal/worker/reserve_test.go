package worker_test

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// heldMember inserts a member a reservation holds, with no slot so the counts stay out of it
func heldMember(t *testing.T, e *env) uuid.UUID {
	t.Helper()
	id := uuid.New()
	sqlRun(t, e, `insert into pool_members (id, provider, size_name, origin, state,
			idempotency_key, reservation_id, claimed_at)
		values ($1, 'local', 'small', 'reserve', 'held', $2, $1, now())`,
		id, "reserve-"+id.String())
	return id
}

// reservationRow inserts a reservation in state whose expiry and heartbeat are the given seconds
// from now and returns its id and member's
func reservationRow(
	t *testing.T, e *env, state string, expiresIn, beatAge time.Duration, placement uuid.UUID,
) (uuid.UUID, uuid.UUID) {
	t.Helper()
	member := heldMember(t, e)
	id := uuid.New()
	if placement == uuid.Nil {
		placement = uuid.New()
	}
	sqlRun(t, e, `insert into reservations (id, quote_sha256, package_id, requirements,
			placement_id, key_id, member_id, path, state, heartbeat, created_at, expires_at,
			hold_until)
		values ($1, $2, 'small-10m', '{}', $3, 'k1', $4, 'speculative', $5,
			now() - make_interval(secs => $6), now() - interval '5 minutes',
			now() + make_interval(secs => $7), now() + interval '10 minutes')`,
		id, id[:], placement, member, state, beatAge.Seconds(), expiresIn.Seconds())
	return id, member
}

func (e *env) reservationState(t *testing.T, id uuid.UUID) string {
	t.Helper()
	var s string
	if err := e.pool.QueryRow(t.Context(), `select state from reservations where id = $1`, id).
		Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

func (e *env) memberState(t *testing.T, id uuid.UUID) string {
	t.Helper()
	m, err := e.store.MemberByID(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	return m.State
}

func (e *env) runReserveWatch(t *testing.T) saga.WatchReport {
	t.Helper()
	rep, err := e.saga.WatchReservations(t.Context())
	if err != nil {
		t.Fatalf("reserve watch: %v", err)
	}
	return rep
}

func TestReserveWatchLapsesAndFails(t *testing.T) {
	e := newEnv(t, options{})
	placing := e.placing(t)
	left := e.placing(t)
	if err := e.store.MarkPlacementFailed(t.Context(), left.ID, "boom", true); err != nil {
		t.Fatal(err)
	}
	// Past its expiry and the 30 s of grace, within the grace, and a fresh and a stale heartbeat
	lapsed, lapsedMember := reservationRow(t, e, "ready", -time.Minute, 0, uuid.Nil)
	graced, _ := reservationRow(t, e, "ready", -10*time.Second, 0, uuid.Nil)
	stale, staleMember := reservationRow(t, e, "preparing", time.Minute, 31*time.Second, uuid.Nil)
	fresh, _ := reservationRow(t, e, "preparing", time.Minute, 5*time.Second, uuid.Nil)
	// An awaited reservation is kept while its placement is placing and failed once it left
	kept, _ := reservationRow(t, e, "awaited", -time.Hour, time.Hour, placing.ID)
	orphan, orphanMember := reservationRow(t, e, "awaited", time.Minute, 0, left.ID)
	// A held member whose reservation is gone is retired
	loose := heldMember(t, e)

	rep := e.runReserveWatch(t)
	eq(t, rep.Lapsed, 1)
	eq(t, rep.Failed, 2)
	eq(t, rep.Retired, 1)
	eq(t, e.reservationState(t, lapsed), "lapsed")
	eq(t, e.reservationState(t, graced), "ready")
	eq(t, e.reservationState(t, stale), "failed")
	eq(t, e.reservationState(t, fresh), "preparing")
	eq(t, e.reservationState(t, kept), "awaited")
	eq(t, e.reservationState(t, orphan), "failed")
	for _, id := range []uuid.UUID{lapsedMember, staleMember, orphanMember, loose} {
		eq(t, e.memberState(t, id), store.MemberRetiring)
	}
	eq(t, count(t, e, `select count(*) from river_job where kind = 'pool_retire'`), 4)
	eq(t, count(t, e, `select count(*) from reservations where client_ip is not null`), 0)
	// A second run finds nothing more to do
	rep = e.runReserveWatch(t)
	eq(t, rep.Lapsed+rep.Failed+rep.Retired, 0)
	eq(t, count(t, e, `select count(*) from river_job where kind = 'pool_retire'`), 4)
}

// endUnused inserts n reservations that ended unused a minute ago
func endUnused(t *testing.T, e *env, n int) {
	t.Helper()
	for range n {
		id, _ := reservationRow(t, e, "lapsed", -time.Hour, 0, uuid.Nil)
		sqlRun(t, e, `update reservations set ended_at = now() - interval '1 minute' where id = $1`,
			id)
	}
}

// bound inserts n reservations that bound an order a minute ago
func bound(t *testing.T, e *env, n int) {
	t.Helper()
	for range n {
		id, _ := reservationRow(t, e, "bound", -time.Hour, 0, uuid.Nil)
		sqlRun(t, e, `update reservations set ended_at = now() - interval '1 minute' where id = $1`,
			id)
	}
}

func (e *env) brakeSet(t *testing.T) bool {
	t.Helper()
	n, err := e.vk.Do(t.Context(), e.vk.B().Exists().Key(e.prefix+saga.BrakeKey).Build()).AsInt64()
	if err != nil {
		t.Fatal(err)
	}
	return n > 0
}

func (e *env) clearBrake(t *testing.T) {
	t.Helper()
	if err := e.vk.Do(t.Context(), e.vk.B().Del().Key(e.prefix+saga.BrakeKey).Build()).
		Error(); err != nil {
		t.Fatal(err)
	}
}

func TestBrake(t *testing.T) {
	e := newEnv(t, options{})
	endUnused(t, e, 20)
	bound(t, e, 3)
	rep := e.runReserveWatch(t)
	// 20 unused, and at least 5 times 3 bound
	eq(t, rep.Braked, true)
	eq(t, e.brakeSet(t), true)
	e.clearBrake(t)
	bound(t, e, 2)
	// Now 5 bound, and 20 is under 5 times 5
	rep = e.runReserveWatch(t)
	eq(t, rep.Braked, false)
	eq(t, e.brakeSet(t), false)
	// Under the count of unused it never sets, whatever the ratio
	e2 := newEnv(t, options{})
	endUnused(t, e2, 19)
	e2.runReserveWatch(t)
	eq(t, e2.brakeSet(t), false)
}

func TestBrakeSetsAgainWhileTheHourHoldsTheUnused(t *testing.T) {
	r := config.DefaultReserve()
	r.BrakeFor = time.Second
	e := newEnv(t, options{reserve: r})
	endUnused(t, e, 20)
	e.runReserveWatch(t)
	eq(t, e.brakeSet(t), true)
	// A brake that is set keeps its expiry, and ends after brake_for
	e.runReserveWatch(t)
	time.Sleep(1200 * time.Millisecond)
	eq(t, e.brakeSet(t), false)
	// The hour still holds the 20 unused, so the next run sets it again at once
	e.runReserveWatch(t)
	eq(t, e.brakeSet(t), true)
	// The brake clears once the unused ones age out of the hour
	sqlRun(t, e, `update reservations set ended_at = now() - interval '2 hours'`)
	e.runReserveWatch(t)
	eq(t, e.brakeSet(t), false)
}

func TestRetentionReservations(t *testing.T) {
	e := newEnv(t, options{})
	day := 24 * time.Hour
	ended := func(ago time.Duration) uuid.UUID {
		id, _ := reservationRow(t, e, "released", -time.Hour, 0, uuid.Nil)
		sqlRun(t, e, `update reservations set ended_at = now() - make_interval(secs => $2)
			where id = $1`, id, ago.Seconds())
		return id
	}
	old, recent := ended(8*day), ended(6*day)
	open, _ := reservationRow(t, e, "ready", time.Minute, 0, uuid.Nil)
	eq(t, e.worker.Retain(t.Context()), false)
	has := func(id uuid.UUID) bool {
		return count(t, e, `select count(*) from reservations where id = $1`, id) == 1
	}
	eq(t, has(old), false)
	eq(t, has(recent), true)
	eq(t, has(open), true)
}
