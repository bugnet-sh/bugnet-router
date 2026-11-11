package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ErrTooManyKeys means the placement already has its most live SSH keys
var ErrTooManyKeys = errors.New("store: the placement has its most SSH keys")

// Transports an ssh_hosts row names
const (
	// SSHTransportGate is our sshd behind the bugnet-run gate
	SSHTransportGate = "gate"
	// SSHTransportBoatNative is Boat's own sshd
	SSHTransportBoatNative = "boat_native"
	// SSHTransportLiumNative is Lium's own sshd on the pod's published port
	SSHTransportLiumNative = "lium_native"
)

// SSHTransportNative names a provider's own sshd, as SSHTransportBoatNative names Boat's
func SSHTransportNative(provider string) string {
	return provider + "_native"
}

// SSHKey is one user key of a placement
type SSHKey struct {
	PlacementID uuid.UUID
	Fingerprint string
	// PublicKey is the key in SSH wire format
	PublicKey []byte
	CreatedAt time.Time
	RemovedAt *time.Time
}

// SSHKeyRef names one key of one placement
type SSHKeyRef struct {
	Placement   uuid.UUID
	Fingerprint string
}

// SSHHost is how a placement's SSH server is reached, as the router recorded it
type SSHHost struct {
	PlacementID uuid.UUID
	Transport   string
	SSHDPort    int
	// SSHUser is Capabilities.ExecUser or the native sshd's user, never what the sandbox printed
	SSHUser string
	// HostKey is the server's public key in authorized_keys format
	HostKey    string
	PreparedAt time.Time
}

// AddSSHKey stores a key while the placement has fewer than most live ones, a live one as it is
func (s *Store) AddSSHKey(
	ctx context.Context, placement uuid.UUID, fingerprint string, wire []byte, most int,
) error {
	err := s.WithLock(ctx, "ssh-keys:"+placement.String(), func(ts *Store) error {
		var live bool
		err := ts.q.QueryRow(ctx, `select removed_at is null from ssh_keys
			where placement_id = $1 and fingerprint = $2`, placement, fingerprint).Scan(&live)
		switch {
		case err == nil && live:
			return nil
		case err != nil && !errors.Is(err, pgx.ErrNoRows):
			return err
		}
		var n int
		if err := ts.q.QueryRow(ctx, `select count(*) from ssh_keys
			where placement_id = $1 and removed_at is null`, placement).Scan(&n); err != nil {
			return err
		}
		if n >= most {
			return ErrTooManyKeys
		}
		_, err = ts.q.Exec(ctx, `
			insert into ssh_keys (placement_id, fingerprint, public_key) values ($1, $2, $3)
			on conflict (placement_id, fingerprint) do update
				set public_key = excluded.public_key, removed_at = null, created_at = now()`,
			placement, fingerprint, wire)
		return err
	})
	switch {
	case errors.Is(err, ErrTooManyKeys):
		return err
	case err != nil:
		return fmt.Errorf("add ssh key: %w", err)
	}
	return nil
}

// RemoveSSHKey marks a live key removed, reporting whether it was live
func (s *Store) RemoveSSHKey(
	ctx context.Context, placement uuid.UUID, fingerprint string,
) (bool, error) {
	tag, err := s.q.Exec(ctx, `update ssh_keys set removed_at = now()
		where placement_id = $1 and fingerprint = $2 and removed_at is null`,
		placement, fingerprint)
	if err != nil {
		return false, fmt.Errorf("remove ssh key: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// LiveSSHKey returns a key the placement has and has not removed, or ErrNotFound
func (s *Store) LiveSSHKey(
	ctx context.Context, placement uuid.UUID, fingerprint string,
) (SSHKey, error) {
	k := SSHKey{PlacementID: placement, Fingerprint: fingerprint}
	err := s.q.QueryRow(ctx, `select public_key, created_at, removed_at from ssh_keys
		where placement_id = $1 and fingerprint = $2 and removed_at is null`,
		placement, fingerprint).Scan(&k.PublicKey, &k.CreatedAt, &k.RemovedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return SSHKey{}, ErrNotFound
	}
	if err != nil {
		return SSHKey{}, fmt.Errorf("select ssh key: %w", err)
	}
	k.CreatedAt = utc(k.CreatedAt)
	return k, nil
}

// LiveSSHKeys answers which of refs name a key not removed, in one read
func (s *Store) LiveSSHKeys(ctx context.Context, refs []SSHKeyRef) (map[SSHKeyRef]bool, error) {
	out := make(map[SSHKeyRef]bool, len(refs))
	if len(refs) == 0 {
		return out, nil
	}
	ids := make([]uuid.UUID, len(refs))
	fps := make([]string, len(refs))
	for i, r := range refs {
		ids[i], fps[i] = r.Placement, r.Fingerprint
	}
	rows, err := s.q.Query(ctx, `select k.placement_id, k.fingerprint from ssh_keys k
		join unnest($1::uuid[], $2::text[]) as w(placement_id, fingerprint)
			on k.placement_id = w.placement_id and k.fingerprint = w.fingerprint
		where k.removed_at is null`, ids, fps)
	if err != nil {
		return nil, fmt.Errorf("select live ssh keys: %w", err)
	}
	live, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (SSHKeyRef, error) {
		var ref SSHKeyRef
		err := r.Scan(&ref.Placement, &ref.Fingerprint)
		return ref, err
	})
	if err != nil {
		return nil, fmt.Errorf("select live ssh keys: %w", err)
	}
	for _, ref := range live {
		out[ref] = true
	}
	return out, nil
}

// SetSSHAlias gives a placement without an alias this one, false when that cannot be
func (s *Store) SetSSHAlias(ctx context.Context, id uuid.UUID, alias string) (bool, error) {
	tag, err := s.q.Exec(ctx, `update placements set ssh_alias = $2
		where id = $1 and ssh_alias is null
			and not exists (select 1 from placements where ssh_alias = $2)`, id, alias)
	if err != nil {
		return false, fmt.Errorf("set ssh alias: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// PlacementBySSHAlias returns the placement holding alias, or ErrNotFound
func (s *Store) PlacementBySSHAlias(ctx context.Context, alias string) (Placement, error) {
	return s.placementWhere(ctx, "ssh_alias = $1", alias)
}

// PutSSHHost records how a placement's SSH server is reached, replacing an earlier record
func (s *Store) PutSSHHost(ctx context.Context, h SSHHost) error {
	_, err := s.q.Exec(ctx, `
		insert into ssh_hosts (placement_id, transport, sshd_port, ssh_user, host_key)
		values ($1, $2, $3, $4, $5)
		on conflict (placement_id) do update set transport = excluded.transport,
			sshd_port = excluded.sshd_port, ssh_user = excluded.ssh_user,
			host_key = excluded.host_key, prepared_at = now()`,
		h.PlacementID, h.Transport, h.SSHDPort, h.SSHUser, h.HostKey)
	if err != nil {
		return fmt.Errorf("put ssh host: %w", err)
	}
	return nil
}

// SSHHost returns the placement's SSH server record, or ErrNotFound before it has one
func (s *Store) SSHHost(ctx context.Context, id uuid.UUID) (SSHHost, error) {
	h := SSHHost{PlacementID: id}
	err := s.q.QueryRow(ctx, `select transport, sshd_port, ssh_user, host_key, prepared_at
		from ssh_hosts where placement_id = $1`, id).
		Scan(&h.Transport, &h.SSHDPort, &h.SSHUser, &h.HostKey, &h.PreparedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return SSHHost{}, ErrNotFound
	}
	if err != nil {
		return SSHHost{}, fmt.Errorf("select ssh host: %w", err)
	}
	h.PreparedAt = utc(h.PreparedAt)
	return h, nil
}

// AddGatewayKey records that the gateway's key of this fingerprint is authorised in the sandbox
func (s *Store) AddGatewayKey(ctx context.Context, id uuid.UUID, fingerprint string) error {
	_, err := s.q.Exec(ctx, `insert into ssh_gateway_keys (placement_id, gateway_key_fp)
		values ($1, $2) on conflict do nothing`, id, fingerprint)
	if err != nil {
		return fmt.Errorf("add gateway key: %w", err)
	}
	return nil
}

// HasGatewayKey reports whether the gateway's key of this fingerprint is authorised already
func (s *Store) HasGatewayKey(
	ctx context.Context, id uuid.UUID, fingerprint string,
) (bool, error) {
	var ok bool
	err := s.q.QueryRow(ctx, `select exists (select 1 from ssh_gateway_keys
		where placement_id = $1 and gateway_key_fp = $2)`, id, fingerprint).Scan(&ok)
	if err != nil {
		return false, fmt.Errorf("select gateway key: %w", err)
	}
	return ok, nil
}

// DeleteGatewayKeys forgets every gateway key of a placement, as its end does
func (s *Store) DeleteGatewayKeys(ctx context.Context, id uuid.UUID) error {
	if _, err := s.q.Exec(ctx, `delete from ssh_gateway_keys where placement_id = $1`,
		id); err != nil {
		return fmt.Errorf("delete gateway keys: %w", err)
	}
	return nil
}
