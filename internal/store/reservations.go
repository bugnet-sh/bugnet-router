package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// Reservation states, CP 18.12
const (
	ReservationPreparing = "preparing"
	ReservationReady     = "ready"
	ReservationAwaited   = "awaited"
	ReservationBound     = "bound"
	ReservationLapsed    = "lapsed"
	ReservationReleased  = "released"
	ReservationFailed    = "failed"
)

// Reservation paths, how its sandbox is found
const (
	// ReservePooled takes a warm pool member
	ReservePooled = "pooled"
	// ReserveSpeculative starts a sandbox before payment
	ReserveSpeculative = "speculative"
)

// openStates are the states of a reservation that still holds its sandbox for a quote
const openStates = `('preparing', 'ready', 'awaited')`

// Reservation is one sandbox held for a quote's order, never holding a ref or a credential
type Reservation struct {
	ID          uuid.UUID
	QuoteSHA256 []byte
	PackageID   string
	// Requirements is the JSON of the requirements the order must repeat
	Requirements []byte
	// PlacementID is minted at Reserve and becomes the bound placement's id
	PlacementID uuid.UUID
	// KeyID names the data-plane root that derived the placement's gate key
	KeyID    string
	MemberID uuid.UUID
	Path     string
	State    string
	// Prepared is Prepare's answer without the ref, nil until the sandbox is ready
	Prepared []byte
	// ClientIP is the client's address outside the shared ranges, nil for none and after the end
	ClientIP  *netip.Addr
	Owner     string
	Heartbeat *time.Time
	CreatedAt time.Time
	ExpiresAt time.Time
	HoldUntil time.Time
	ReadyAt   *time.Time
	EndedAt   *time.Time
	Failure   string
}

const reservationColumns = `id, quote_sha256, package_id, requirements, placement_id, key_id,
	member_id, path, state, prepared, client_ip, coalesce(owner, ''), heartbeat, created_at,
	expires_at, hold_until, ready_at, ended_at, coalesce(failure, '')`

func scanReservation(row pgx.Row) (Reservation, error) {
	var r Reservation
	err := row.Scan(&r.ID, &r.QuoteSHA256, &r.PackageID, &r.Requirements, &r.PlacementID,
		&r.KeyID, &r.MemberID, &r.Path, &r.State, &r.Prepared, &r.ClientIP, &r.Owner,
		&r.Heartbeat, &r.CreatedAt, &r.ExpiresAt, &r.HoldUntil, &r.ReadyAt, &r.EndedAt,
		&r.Failure)
	if errors.Is(err, pgx.ErrNoRows) {
		return Reservation{}, ErrNotFound
	}
	if err != nil {
		return Reservation{}, err
	}
	r.CreatedAt, r.ExpiresAt, r.HoldUntil = utc(r.CreatedAt), utc(r.ExpiresAt), utc(r.HoldUntil)
	r.Heartbeat, r.ReadyAt, r.EndedAt = utcPtr(r.Heartbeat), utcPtr(r.ReadyAt), utcPtr(r.EndedAt)
	return r, nil
}

// ReservationByID returns one reservation or ErrNotFound
func (s *Store) ReservationByID(ctx context.Context, id uuid.UUID) (Reservation, error) {
	r, err := scanReservation(s.q.QueryRow(ctx,
		`select `+reservationColumns+` from reservations where id = $1`, id))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return Reservation{}, fmt.Errorf("select reservation: %w", err)
	}
	return r, err
}

// OpenReservation returns the quote's open reservation without a lock, ErrNotFound for none
func (s *Store) OpenReservation(ctx context.Context, quote []byte) (Reservation, error) {
	r, err := scanReservation(s.q.QueryRow(ctx, `select `+reservationColumns+`
		from reservations where quote_sha256 = $1 and state in `+openStates, quote))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return Reservation{}, fmt.Errorf("select open reservation: %w", err)
	}
	return r, err
}

// OpenReservationForUpdate locks and returns the quote's open reservation that no order awaits
// yet, ErrNotFound for none
func (s *Store) OpenReservationForUpdate(
	ctx context.Context, quote []byte,
) (Reservation, error) {
	r, err := scanReservation(s.q.QueryRow(ctx, `select `+reservationColumns+`
		from reservations where quote_sha256 = $1 and state in ('preparing', 'ready')
		for update`, quote))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return Reservation{}, fmt.Errorf("lock open reservation: %w", err)
	}
	return r, err
}

// LockQuote serialises the writers of one quote's reservations until the transaction ends
func (s *Store) LockQuote(ctx context.Context, quote []byte) error {
	_, err := s.q.Exec(ctx,
		`select pg_advisory_xact_lock(hashtextextended(encode($1::bytea, 'hex'), 0))`, quote)
	if err != nil {
		return fmt.Errorf("lock quote: %w", err)
	}
	return nil
}

// ReservationsOfQuote counts every reservation the quote ever had, whatever its state
func (s *Store) ReservationsOfQuote(ctx context.Context, quote []byte) (int, error) {
	var n int
	err := s.q.QueryRow(ctx, `select count(*) from reservations where quote_sha256 = $1`,
		quote).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count reservations of quote: %w", err)
	}
	return n, nil
}

// OpenCounts counts the open reservations, those that started their own sandbox, and those of
// one client address, a nil address counting none
func (s *Store) OpenCounts(
	ctx context.Context, ip *netip.Addr,
) (open, speculative, ofIP int, err error) {
	err = s.q.QueryRow(ctx, `
		select count(*)::int, (count(*) filter (where path = 'speculative'))::int,
			(count(*) filter (where $1::inet is not null and client_ip = $1::inet))::int
		from reservations where state in `+openStates, ip).Scan(&open, &speculative, &ofIP)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("count open reservations: %w", err)
	}
	return open, speculative, ofIP, nil
}

// HeldMembers counts the members of one pool that open reservations hold
func (s *Store) HeldMembers(ctx context.Context, provider, size string) (int, error) {
	var n int
	err := s.q.QueryRow(ctx, `
		select count(*) from pool_members
		where provider = $1 and size_name = $2 and state = 'held'`, provider, size).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count held members: %w", err)
	}
	return n, nil
}

// InsertReservation stores a new reservation, its member held already
func (s *Store) InsertReservation(ctx context.Context, r Reservation) error {
	_, err := s.q.Exec(ctx, `
		insert into reservations (id, quote_sha256, package_id, requirements, placement_id,
			key_id, member_id, path, state, client_ip, owner, heartbeat, created_at, expires_at,
			hold_until)
		values ($1, $2, $3, $4, $5, $6, $7, $8, 'preparing', $9, $10, $11, $11, $12, $13)`,
		r.ID, r.QuoteSHA256, r.PackageID, r.Requirements, r.PlacementID, r.KeyID, r.MemberID,
		r.Path, r.ClientIP, nullIfEmpty(r.Owner), utc(r.CreatedAt), utc(r.ExpiresAt),
		utc(r.HoldUntil))
	if err != nil {
		return fmt.Errorf("insert reservation: %w", err)
	}
	return nil
}

// RenewReservation moves the expiry of an open reservation no order awaits
func (s *Store) RenewReservation(
	ctx context.Context, id uuid.UUID, expiresAt time.Time,
) error {
	tag, err := s.q.Exec(ctx, `
		update reservations set expires_at = $2
		where id = $1 and state in ('preparing', 'ready')`, id, utc(expiresAt))
	if err != nil {
		return fmt.Errorf("renew reservation: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("renew reservation %s: %w", id, ErrStateConflict)
	}
	return nil
}

// EndReservation moves an open reservation to an ending state and returns its member, false when
// it was not open or, for want, not in state from
func (s *Store) EndReservation(
	ctx context.Context, id uuid.UUID, state, failure string, at time.Time,
) (uuid.UUID, bool, error) {
	var member uuid.UUID
	err := s.q.QueryRow(ctx, `
		update reservations set state = $2, failure = nullif($3, ''), ended_at = $4,
			client_ip = null
		where id = $1 and state in `+openStates+`
		returning member_id`, id, state, failure, utc(at)).Scan(&member)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, false, nil
	}
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("end reservation: %w", err)
	}
	return member, true, nil
}

// AwaitReservation marks a reservation an order now waits for
func (s *Store) AwaitReservation(ctx context.Context, id uuid.UUID) error {
	tag, err := s.q.Exec(ctx, `
		update reservations set state = 'awaited'
		where id = $1 and state in ('preparing', 'ready')`, id)
	if err != nil {
		return fmt.Errorf("await reservation: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("await reservation %s: %w", id, ErrStateConflict)
	}
	return nil
}

// HeartbeatReservation shows the replica still prepares or keeps the reservation, false once it
// is no longer open
func (s *Store) HeartbeatReservation(
	ctx context.Context, id uuid.UUID, replica string, at time.Time,
) (bool, error) {
	tag, err := s.q.Exec(ctx, `
		update reservations set owner = $2, heartbeat = $3
		where id = $1 and state in `+openStates, id, nullIfEmpty(replica), utc(at))
	if err != nil {
		return false, fmt.Errorf("heartbeat reservation: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// MarkReservationReady stores a prepared sandbox, its ref sealed under the member's id in the
// member row and the rest of Prepare's answer in the reservation
//
// A reservation an order awaits keeps its state, which that order reads as prepared by ready_at.
func (s *Store) MarkReservationReady(
	ctx context.Context, id uuid.UUID, ref provider.Ref, prepared []byte, at time.Time,
) error {
	return s.WithTx(ctx, func(tx pgx.Tx) error {
		var member uuid.UUID
		err := tx.QueryRow(ctx, `
			update reservations set prepared = $2, ready_at = $3,
				state = case when state = 'preparing' then 'ready' else state end
			where id = $1 and state in ('preparing', 'awaited')
			returning member_id`, id, prepared, utc(at)).Scan(&member)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("mark reservation %s ready: %w", id, ErrStateConflict)
		}
		if err != nil {
			return fmt.Errorf("mark reservation ready: %w", err)
		}
		sealed, err := s.sealRef(member, ref)
		if err != nil {
			return fmt.Errorf("mark reservation %s ready: %w", id, err)
		}
		raw, err := json.Marshal(sealed)
		if err != nil {
			return fmt.Errorf("encode member ref: %w", err)
		}
		tag, err := tx.Exec(ctx, `
			update pool_members set provider_ref = $2 where id = $1 and state = 'held'`,
			member, raw)
		if err != nil {
			return fmt.Errorf("store prepared member ref: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("mark reservation %s ready: member: %w", id, ErrStateConflict)
		}
		return nil
	})
}

// StartReserveMember takes a slot with the placements' margin and inserts a held member whose
// sandbox a reservation is about to start, false when the provider has no free slot
func (s *Store) StartReserveMember(
	ctx context.Context, id uuid.UUID, provider, size string, reservation uuid.UUID,
) (Member, bool, error) {
	ok, err := s.ReserveCapacity(ctx, provider, s.reserveSlots)
	if err != nil || !ok {
		return Member{}, false, err
	}
	m, err := s.scanMember(s.q.QueryRow(ctx, `
		insert into pool_members (id, provider, size_name, origin, state, idempotency_key,
			holds_slot, reservation_id, claimed_at)
		values ($1, $2, $3, 'reserve', 'held', $4, true, $5, now())
		returning `+memberColumns, id, provider, size, "reserve-"+id.String(), reservation))
	if err != nil {
		return Member{}, false, fmt.Errorf("insert reserve member: %w", err)
	}
	return m, true, nil
}

// BindReservation makes a prepared reservation's sandbox the placing placement's, in the order's
// transaction, and returns the member with its ref opened
//
// The reservation and its member become bound and the member's slot moves to the placement. The
// ref is opened under the member's id and sealed again under the placement's.
func (s *Store) BindReservation(
	ctx context.Context, r Reservation, p Placement, at time.Time,
) (Member, error) {
	tag, err := s.q.Exec(ctx, `
		update reservations set state = 'bound', ended_at = $2, client_ip = null
		where id = $1 and state in ('ready', 'awaited') and prepared is not null`,
		r.ID, utc(at))
	if err != nil {
		return Member{}, fmt.Errorf("bind reservation: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return Member{}, fmt.Errorf("bind reservation %s: %w", r.ID, ErrStateConflict)
	}
	m, err := s.scanMember(s.q.QueryRow(ctx, `
		with cur as (select id, provider_ref from pool_members where id = $1 for update),
		bound as (
			update pool_members m set state = 'bound', placement_id = $2, bound_at = $3
			from cur where m.id = cur.id and m.state = 'held' and cur.provider_ref is not null
			returning m.*)
		select `+memberColumns+` from bound`, r.MemberID, p.ID, utc(at)))
	if errors.Is(err, ErrNotFound) {
		return Member{}, fmt.Errorf("bind reservation %s: member: %w", r.ID, ErrStateConflict)
	}
	if err != nil {
		return Member{}, fmt.Errorf("bind member: %w", err)
	}
	if err := s.moveSlot(ctx, &m, Holder{Placement: p.ID, Attempt: 1}); err != nil {
		return Member{}, err
	}
	if err := s.resealMember(ctx, &m, p.ID); err != nil {
		return Member{}, err
	}
	return m, nil
}

// RepointOrder makes an order name another placement, the reservation's, in the order's
// transaction
func (s *Store) RepointOrder(ctx context.Context, order string, placement uuid.UUID) error {
	tag, err := s.q.Exec(ctx, `update orders set placement_id = $2 where order_id = $1`,
		order, placement)
	if err != nil {
		return fmt.Errorf("repoint order: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("repoint order %s: %w", order, ErrNotFound)
	}
	return nil
}

// AwaitedReservationOf returns the reservation an order's placement awaits, ErrNotFound for none
func (s *Store) AwaitedReservationOf(
	ctx context.Context, placement uuid.UUID,
) (Reservation, error) {
	r, err := scanReservation(s.q.QueryRow(ctx, `select `+reservationColumns+`
		from reservations where placement_id = $1 and state = 'awaited'`, placement))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return Reservation{}, fmt.Errorf("select awaited reservation: %w", err)
	}
	return r, err
}

// ReservationForUpdate locks and returns one reservation
func (s *Store) ReservationForUpdate(ctx context.Context, id uuid.UUID) (Reservation, error) {
	r, err := scanReservation(s.q.QueryRow(ctx,
		`select `+reservationColumns+` from reservations where id = $1 for update`, id))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return Reservation{}, fmt.Errorf("lock reservation: %w", err)
	}
	return r, err
}

// EndedMember is a reservation that ended with the member its sandbox is retired through
type EndedMember struct {
	Reservation uuid.UUID
	Member      uuid.UUID
}

// endWhere ends every open reservation where picks and returns what it ended
func (s *Store) endWhere(
	ctx context.Context, state, failure, where string, at time.Time, args ...any,
) ([]EndedMember, error) {
	args = append([]any{state, failure, utc(at)}, args...)
	rows, err := s.q.Query(ctx, `
		update reservations set state = $1, failure = nullif($2, ''), ended_at = $3,
			client_ip = null
		where state in `+openStates+` and `+where+`
		returning id, member_id`, args...)
	if err != nil {
		return nil, fmt.Errorf("end reservations: %w", err)
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (EndedMember, error) {
		var e EndedMember
		return e, r.Scan(&e.Reservation, &e.Member)
	})
}

// LapseReservations lapses every preparing or ready reservation past its expiry and grace
func (s *Store) LapseReservations(
	ctx context.Context, now time.Time, grace time.Duration,
) ([]EndedMember, error) {
	return s.endWhere(ctx, ReservationLapsed, "", `state in ('preparing', 'ready')
		and expires_at + make_interval(secs => $4) < $3`, now, grace.Seconds())
}

// FailStaleReservations fails every preparing or ready reservation whose heartbeat is older than
// the cutoff
func (s *Store) FailStaleReservations(
	ctx context.Context, cutoff, now time.Time,
) ([]EndedMember, error) {
	return s.endWhere(ctx, ReservationFailed, "heartbeat stale", `state in ('preparing', 'ready')
		and coalesce(heartbeat, created_at) < $4`, now, utc(cutoff))
}

// FailOrphanedAwaited fails every awaited reservation whose placement is no longer placing
func (s *Store) FailOrphanedAwaited(ctx context.Context, now time.Time) ([]EndedMember, error) {
	return s.endWhere(ctx, ReservationFailed, "placement left placing", `state = 'awaited'
		and not exists (select 1 from placements p
			where p.id = reservations.placement_id and p.state = 'placing')`, now)
}

// ReleaseAwaitedOf releases the reservation a placement awaits and returns its member, false when
// it awaited none, in the transaction that moves the placement out of placing
func (s *Store) ReleaseAwaitedOf(
	ctx context.Context, placement uuid.UUID, at time.Time,
) (uuid.UUID, bool, error) {
	var member uuid.UUID
	err := s.q.QueryRow(ctx, `
		update reservations set state = 'released', ended_at = $2, client_ip = null
		where placement_id = $1 and state = 'awaited'
		returning member_id`, placement, utc(at)).Scan(&member)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, false, nil
	}
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("release awaited reservation: %w", err)
	}
	return member, true, nil
}

// UnkeptHeldMembers returns the held members no open reservation keeps, which still need a
// retirement
func (s *Store) UnkeptHeldMembers(ctx context.Context) ([]uuid.UUID, error) {
	rows, err := s.q.Query(ctx, `
		select m.id from pool_members m
		where m.state = 'held' and not exists (select 1 from reservations r
			where r.member_id = m.id and (r.state in ('preparing', 'ready')
				or r.state = 'awaited' and exists (select 1 from placements p
					where p.id = r.placement_id and p.state = 'placing')))`)
	if err != nil {
		return nil, fmt.Errorf("select unkept held members: %w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
	if err != nil {
		return nil, fmt.Errorf("select unkept held members: %w", err)
	}
	return ids, nil
}

// BrakeCounts counts the reservations that ended unused and those bound since a time
func (s *Store) BrakeCounts(ctx context.Context, since time.Time) (unused, bound int, err error) {
	err = s.q.QueryRow(ctx, `
		select (count(*) filter (where state in ('lapsed', 'released')))::int,
			(count(*) filter (where state = 'bound'))::int
		from reservations where ended_at >= $1`, utc(since)).Scan(&unused, &bound)
	if err != nil {
		return 0, 0, fmt.Errorf("count reservations for the brake: %w", err)
	}
	return unused, bound, nil
}

// FirstCommandP90 counts the placements that became ready since a time on the reserved or pooled
// path with a measured first frame and gives the p90 of payment to first frame in milliseconds
func (s *Store) FirstCommandP90(
	ctx context.Context, since time.Time,
) (n int, p90 time.Duration, err error) {
	var ms *float64
	err = s.q.QueryRow(ctx, `
		with firsts as (
			select (extract(epoch from p.ready_at - p.paid_at) * 1000)::bigint + j.first_frame_ms
				as total
			from placements p
			join lateral (select first_frame_ms from jobs where placement_id = p.id
				order by created_at, id limit 1) j on true
			where p.ready_at >= $1 and p.start_path in ('reserved', 'pooled')
				and p.paid_at is not null and j.first_frame_ms is not null)
		select count(*)::int, percentile_disc(0.9) within group (order by total) from firsts`,
		utc(since)).Scan(&n, &ms)
	if err != nil {
		return 0, 0, fmt.Errorf("read the first command times: %w", err)
	}
	if ms != nil {
		p90 = time.Duration(*ms * float64(time.Millisecond))
	}
	return n, p90, nil
}
