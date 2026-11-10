package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// Pool member states, a member only moves forward through them
const (
	MemberFilling  = "filling"
	MemberWarm     = "warm"
	MemberHeld     = "held"
	MemberBound    = "bound"
	MemberRetiring = "retiring"
	MemberGone     = "gone"
)

// Pool member origins
const (
	// OriginPool is a member the router filled and parked
	OriginPool = "pool"
	// OriginProvider is a member a provider-kept pool handed out
	OriginProvider = "provider"
	// OriginReserve is a member started for one reservation
	OriginReserve = "reserve"
)

// Why a member ended, the values of pool_members.end_reason
const (
	EndAged    = "aged"
	EndUnused  = "unused"
	EndLapsed  = "lapsed"
	EndFailed  = "failed"
	EndDrained = "drained"
	EndGone    = "gone"
)

// Member is one sandbox a pool keeps, claimed by an order or a reservation in time
type Member struct {
	ID             uuid.UUID
	Provider       string
	SizeName       string
	Origin         string
	State          string
	IdempotencyKey string
	// ProviderRef is the member's sandbox with its credentials opened, nil before Create returned
	ProviderRef      *provider.Ref
	Image            string
	SandboxCreatedAt *time.Time
	HoldsSlot        bool
	ReservationID    *uuid.UUID
	PlacementID      *uuid.UUID
	CreatedAt        time.Time
	WarmAt           *time.Time
	ParkedAt         *time.Time
	ClaimedAt        *time.Time
	BoundAt          *time.Time
	EndedAt          *time.Time
	EndReason        string
	Error            string
}

// Holder is who takes a member, a reservation or a placement and one of its attempts
type Holder struct {
	Reservation uuid.UUID
	Placement   uuid.UUID
	Attempt     int
}

const memberColumns = `id, provider, size_name, origin, state, idempotency_key, provider_ref,
	coalesce(image, ''), sandbox_created_at, holds_slot, reservation_id, placement_id, created_at,
	warm_at, parked_at, claimed_at, bound_at, ended_at, coalesce(end_reason, ''),
	coalesce(error, '')`

func (s *Store) scanMember(row pgx.Row) (Member, error) {
	var m Member
	var ref []byte
	err := row.Scan(&m.ID, &m.Provider, &m.SizeName, &m.Origin, &m.State, &m.IdempotencyKey,
		&ref, &m.Image, &m.SandboxCreatedAt, &m.HoldsSlot, &m.ReservationID, &m.PlacementID,
		&m.CreatedAt, &m.WarmAt, &m.ParkedAt, &m.ClaimedAt, &m.BoundAt, &m.EndedAt,
		&m.EndReason, &m.Error)
	if errors.Is(err, pgx.ErrNoRows) {
		return Member{}, ErrNotFound
	}
	if err != nil {
		return Member{}, err
	}
	m.CreatedAt = utc(m.CreatedAt)
	for _, t := range []**time.Time{&m.SandboxCreatedAt, &m.WarmAt, &m.ParkedAt, &m.ClaimedAt,
		&m.BoundAt, &m.EndedAt} {
		*t = utcPtr(*t)
	}
	if ref != nil {
		m.ProviderRef = &provider.Ref{}
		if err := json.Unmarshal(ref, m.ProviderRef); err != nil {
			return Member{}, fmt.Errorf("decode member ref: %w", err)
		}
		s.openMemberRef(&m)
	}
	return m, nil
}

// openMemberRef opens the credentials of m's ref under the member id, or under its placement's
// id once a claim sealed them again, and drops any it cannot open
func (s *Store) openMemberRef(m *Member) {
	owners := []uuid.UUID{m.ID}
	if m.PlacementID != nil {
		owners = []uuid.UUID{*m.PlacementID, m.ID}
	}
	for _, field := range credentialFields {
		v, ok := m.ProviderRef.Meta[field]
		if !ok || len(v) < len(sealedPrefix) || v[:len(sealedPrefix)] != sealedPrefix {
			continue
		}
		var plain string
		var err error
		for _, owner := range owners {
			if plain, err = s.open(owner, v); err == nil {
				break
			}
		}
		if err != nil {
			delete(m.ProviderRef.Meta, field)
			s.log.Warn("pool member credential does not open, it reads as absent",
				"member", m.ID, "field", field, "err", err)
			continue
		}
		m.ProviderRef.Meta[field] = plain
	}
}

// MemberByID returns one member or ErrNotFound
func (s *Store) MemberByID(ctx context.Context, id uuid.UUID) (Member, error) {
	m, err := s.scanMember(s.q.QueryRow(ctx,
		`select `+memberColumns+` from pool_members where id = $1`, id))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return Member{}, fmt.Errorf("select member: %w", err)
	}
	return m, err
}

// MemberOfPlacement returns the member bound to a placement, ErrNotFound when it has none
func (s *Store) MemberOfPlacement(ctx context.Context, id uuid.UUID) (Member, error) {
	m, err := s.scanMember(s.q.QueryRow(ctx, `select `+memberColumns+`
		from pool_members where placement_id = $1 and state <> 'gone'
		order by claimed_at desc nulls last limit 1`, id))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return Member{}, fmt.Errorf("select member of placement: %w", err)
	}
	return m, err
}

// MembersOfPlacement returns every member bound to a placement that is not gone yet
func (s *Store) MembersOfPlacement(ctx context.Context, id uuid.UUID) ([]Member, error) {
	return s.members(ctx, `where placement_id = $1 and state not in ('gone', 'retiring')`, id)
}

func (s *Store) members(ctx context.Context, where string, args ...any) ([]Member, error) {
	rows, err := s.q.Query(ctx, `select `+memberColumns+` from pool_members `+where, args...)
	if err != nil {
		return nil, fmt.Errorf("select members: %w", err)
	}
	defer rows.Close()
	var out []Member
	for rows.Next() {
		m, err := s.scanMember(rows)
		if err != nil {
			return nil, fmt.Errorf("select members: %w", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// LiveMembers returns every member still filling, warm, held, bound or retiring, oldest first
func (s *Store) LiveMembers(ctx context.Context) ([]Member, error) {
	return s.members(ctx, `where state <> 'gone' order by created_at`)
}

// MembersSince returns the members that existed at any time since a time, for the cost checks
func (s *Store) MembersSince(ctx context.Context, since time.Time) ([]Member, error) {
	return s.members(ctx, `where ended_at is null or ended_at >= $1 order by created_at`,
		utc(since))
}

// WarmMembers returns the warm members of one pool, oldest warm first
func (s *Store) WarmMembers(ctx context.Context, provider, size string) ([]Member, error) {
	return s.members(ctx, `where provider = $1 and size_name = $2 and state = 'warm'
		order by warm_at`, provider, size)
}

// ErrPoolFull means a fill found no slot, or the pool's provider already holds its share
var ErrPoolFull = errors.New("store: pool provider has no room for another member")

// StartFill reserves one slot with the placements' margin and inserts a filling member, in one
// transaction, so no member exists without its slot
//
// capacityShare bounds the members a provider holds to that share of its max_concurrent. then,
// when not nil, runs in the same transaction after the row exists, as an insert of the fill job
// must, and an error from it rolls the member and its slot back.
func (s *Store) StartFill(
	ctx context.Context, provider, size string, capacityShare float64,
	then func(ctx context.Context, tx pgx.Tx, m Member) error,
) (Member, error) {
	var m Member
	err := s.WithTx(ctx, func(tx pgx.Tx) error {
		var maxConcurrent, members int
		err := tx.QueryRow(ctx, `
			select coalesce((select max_concurrent from provider_capacity where provider = $1), $2),
				(select count(*) from pool_members
					where provider = $1 and state in ('filling', 'warm', 'held'))`,
			provider, DefaultMaxConcurrent).Scan(&maxConcurrent, &members)
		if err != nil {
			return fmt.Errorf("count members: %w", err)
		}
		if float64(members+1) > capacityShare*float64(maxConcurrent) {
			return ErrPoolFull
		}
		ok, err := s.Tx(tx).ReserveCapacity(ctx, provider, s.reserveSlots)
		if err != nil {
			return err
		}
		if !ok {
			return ErrPoolFull
		}
		id := uuid.New()
		m, err = s.Tx(tx).scanMember(tx.QueryRow(ctx, `
			insert into pool_members (id, provider, size_name, origin, state, idempotency_key,
				holds_slot)
			values ($1, $2, $3, 'pool', 'filling', $4, true)
			returning `+memberColumns, id, provider, size, "pool-"+id.String()))
		if err != nil || then == nil {
			return err
		}
		return then(ctx, tx, m)
	})
	return m, err
}

// MarkMemberCreated records the sandbox a fill created, so a crash leaves its id behind
func (s *Store) MarkMemberCreated(
	ctx context.Context, id uuid.UUID, ref provider.Ref, createdAt time.Time,
) error {
	sealed, err := s.sealRef(id, ref)
	if err != nil {
		return fmt.Errorf("mark member created %s: %w", id, err)
	}
	raw, err := json.Marshal(sealed)
	if err != nil {
		return fmt.Errorf("encode member ref: %w", err)
	}
	tag, err := s.q.Exec(ctx, `
		update pool_members set provider_ref = $2, sandbox_created_at = $3
		where id = $1 and state = 'filling'`, id, raw, utc(createdAt))
	if err != nil {
		return fmt.Errorf("mark member created: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("mark member created %s: %w", id, ErrStateConflict)
	}
	return nil
}

// MarkMemberWarm makes a filled member claimable, its ref sealed under its own id
func (s *Store) MarkMemberWarm(
	ctx context.Context, id uuid.UUID, ref provider.Ref, image string, at time.Time,
) error {
	sealed, err := s.sealRef(id, ref)
	if err != nil {
		return fmt.Errorf("mark member warm %s: %w", id, err)
	}
	raw, err := json.Marshal(sealed)
	if err != nil {
		return fmt.Errorf("encode member ref: %w", err)
	}
	tag, err := s.q.Exec(ctx, `
		update pool_members set state = 'warm', provider_ref = $2, image = $3, warm_at = $4,
			parked_at = $4
		where id = $1 and state = 'filling'`, id, raw, nullIfEmpty(image), utc(at))
	if err != nil {
		return fmt.Errorf("mark member warm: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("mark member warm %s: %w", id, ErrStateConflict)
	}
	return nil
}

// RetireMember sets a member that is not retiring or gone to retiring with its reason and returns
// the state it left, empty when it was already retiring or gone
func (s *Store) RetireMember(
	ctx context.Context, id uuid.UUID, reason, detail string,
) (string, error) {
	var old string
	err := s.q.QueryRow(ctx, `
		with cur as (select id, state from pool_members where id = $1 for update)
		update pool_members m set state = 'retiring', end_reason = $2, error = nullif($3, '')
		from cur where m.id = cur.id and cur.state not in ('retiring', 'gone')
		returning cur.state`, id, reason, detail).Scan(&old)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("retire member: %w", err)
	}
	return old, nil
}

// MemberGone marks a member whose sandbox is destroyed and frees its slot when it still holds
// one, a repeat changing nothing
func (s *Store) MemberGone(ctx context.Context, id uuid.UUID, reason string, at time.Time) error {
	return s.WithTx(ctx, func(tx pgx.Tx) error {
		var provider string
		var holds bool
		err := tx.QueryRow(ctx, `
			with cur as (select id, provider, holds_slot from pool_members
				where id = $1 and state <> 'gone' for update)
			update pool_members m set state = 'gone', ended_at = $3, holds_slot = false,
				end_reason = coalesce(m.end_reason, $2)
			from cur where m.id = cur.id
			returning cur.provider, cur.holds_slot`, id, reason, utc(at)).Scan(&provider, &holds)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("mark member gone: %w", err)
		}
		if holds {
			return s.Tx(tx).ReleaseCapacity(ctx, provider)
		}
		return nil
	})
}

// ClaimWarm takes the oldest warm member of provider and size made after bornAfter, in tx
//
// A placement claims it as bound and takes over its slot in the same statement, so in_use stays
// as it was, and the member's credentials are sealed again under the placement's id. A zero
// bornAfter takes any age. It reports false when no member is claimable.
func (s *Store) ClaimWarm(
	ctx context.Context, provider, size string, bornAfter time.Time, h Holder,
) (Member, bool, error) {
	state := MemberBound
	if h.Placement == uuid.Nil {
		state = MemberHeld
	}
	var born any
	if !bornAfter.IsZero() {
		born = utc(bornAfter)
	}
	m, err := s.scanMember(s.q.QueryRow(ctx, `
		with pick as (
			select id from pool_members
			where provider = $1 and size_name = $2 and state = 'warm'
				and ($3::timestamptz is null or sandbox_created_at > $3)
			order by warm_at
			limit 1
			for update skip locked)
		, claimed as (
			update pool_members m
			set state = $4, reservation_id = $5, placement_id = $6, claimed_at = now(),
				bound_at = case when $4 = 'bound' then now() end
			from pick
			where m.id = pick.id
			returning m.*)
		select `+memberColumns+` from claimed`, provider, size, born, state,
		nilUUID(h.Reservation), nilUUID(h.Placement)))
	if errors.Is(err, ErrNotFound) {
		return Member{}, false, nil
	}
	if err != nil {
		return Member{}, false, fmt.Errorf("claim warm member: %w", err)
	}
	if h.Placement == uuid.Nil {
		return m, true, nil
	}
	if err := s.moveSlot(ctx, &m, h); err != nil {
		return Member{}, false, err
	}
	if err := s.resealMember(ctx, &m, h.Placement); err != nil {
		return Member{}, false, err
	}
	return m, true, nil
}

// moveSlot hands the claimed member's slot to the placement row, or to the attempt row when the
// placement row holds a slot already, and frees it when neither can take it
func (s *Store) moveSlot(ctx context.Context, m *Member, h Holder) error {
	if !m.HoldsSlot {
		return nil
	}
	tag, err := s.q.Exec(ctx, `
		update placements set capacity_provider = $2, capacity_attempt = $3
		where id = $1 and capacity_provider is null`, h.Placement, m.Provider, h.Attempt)
	if err != nil {
		return fmt.Errorf("move slot to placement: %w", err)
	}
	if tag.RowsAffected() == 0 {
		tag, err = s.q.Exec(ctx, `
			update placement_attempts set slot_provider = $3
			where placement_id = $1 and attempt = $2 and slot_provider is null`,
			h.Placement, h.Attempt, m.Provider)
		if err != nil {
			return fmt.Errorf("move slot to attempt: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("move slot of member %s: no holder row for attempt %d of %s: %w",
				m.ID, h.Attempt, h.Placement, ErrStateConflict)
		}
	}
	if _, err := s.q.Exec(ctx, `update pool_members set holds_slot = false where id = $1`,
		m.ID); err != nil {
		return fmt.Errorf("clear member slot: %w", err)
	}
	m.HoldsSlot = false
	return nil
}

// resealMember seals the member's credentials again under the placement that claimed it
func (s *Store) resealMember(ctx context.Context, m *Member, placement uuid.UUID) error {
	if m.ProviderRef == nil {
		return nil
	}
	sealed, err := s.sealRef(placement, *m.ProviderRef)
	if err != nil {
		return fmt.Errorf("seal member ref for %s: %w", placement, err)
	}
	raw, err := json.Marshal(sealed)
	if err != nil {
		return fmt.Errorf("encode member ref: %w", err)
	}
	if _, err := s.q.Exec(ctx, `update pool_members set provider_ref = $2 where id = $1`,
		m.ID, raw); err != nil {
		return fmt.Errorf("reseal member ref: %w", err)
	}
	return nil
}

// RetireWarm sets every warm member of one pool to retiring and returns their ids
func (s *Store) RetireWarm(
	ctx context.Context, provider, size, reason string,
) ([]uuid.UUID, error) {
	rows, err := s.q.Query(ctx, `
		with warm as (select id from pool_members
			where provider = $1 and size_name = $2 and state = 'warm' for update skip locked)
		update pool_members m set state = 'retiring', end_reason = $3
		from warm where m.id = warm.id returning m.id`, provider, size, reason)
	if err != nil {
		return nil, fmt.Errorf("retire warm members: %w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
	if err != nil {
		return nil, fmt.Errorf("retire warm members: %w", err)
	}
	return ids, nil
}

// WarmCounts counts the claimable members of every pool, by provider and size
func (s *Store) WarmCounts(ctx context.Context) (map[[2]string]int, error) {
	rows, err := s.q.Query(ctx, `
		select provider, size_name, count(*)::int from pool_members where state = 'warm'
		group by provider, size_name`)
	if err != nil {
		return nil, fmt.Errorf("count warm members: %w", err)
	}
	defer rows.Close()
	out := map[[2]string]int{}
	for rows.Next() {
		var provider, size string
		var n int
		if err := rows.Scan(&provider, &size, &n); err != nil {
			return nil, fmt.Errorf("count warm members: %w", err)
		}
		out[[2]string{provider, size}] = n
	}
	return out, rows.Err()
}

// ReservationKeepsMember reports whether a held member's reservation is still open, and, once an
// order awaits it, whether that order's placement is still placing
func (s *Store) ReservationKeepsMember(ctx context.Context, member uuid.UUID) (bool, error) {
	var keeps bool
	err := s.q.QueryRow(ctx, `
		select coalesce(bool_or(r.state in ('preparing', 'ready')
				or r.state = 'awaited' and exists (select 1 from placements p
					where p.id = r.placement_id and p.state = 'placing')), false)
		from reservations r where r.member_id = $1`, member).Scan(&keeps)
	if err != nil {
		return false, fmt.Errorf("select reservations of member: %w", err)
	}
	return keeps, nil
}
