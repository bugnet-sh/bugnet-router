package store

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// PlacementByEgressToken returns the placement holding the tunnel token of this digest, or
// ErrNotFound
func (s *Store) PlacementByEgressToken(ctx context.Context, sha []byte) (Placement, error) {
	return s.placementWhere(ctx, "egress_token_sha256 = $1", sha)
}

// DeleteSecretsOf deletes the placement's secrets and links and clears its tunnel token
func (s *Store) DeleteSecretsOf(ctx context.Context, id uuid.UUID) error {
	if _, err := s.q.Exec(ctx, `delete from secrets where placement_id = $1`, id); err != nil {
		return fmt.Errorf("delete secrets: %w", err)
	}
	if _, err := s.q.Exec(ctx, `delete from secret_links where placement_id = $1`, id); err != nil {
		return fmt.Errorf("delete secret links: %w", err)
	}
	if _, err := s.q.Exec(ctx, `update placements set egress_token_sha256 = null
		where id = $1`, id); err != nil {
		return fmt.Errorf("clear egress token: %w", err)
	}
	return nil
}

// DeleteSecretLeftovers deletes the secrets and links, and clears the tunnel tokens, of every
// placement that ended or whose deadline is not after cutoff
func (s *Store) DeleteSecretLeftovers(ctx context.Context, cutoff time.Time) (int64, error) {
	const stale = `select id from placements where state in ($1, $2) or deadline <= $3`
	var n int64
	for _, q := range []string{
		`delete from secrets where placement_id in (` + stale + `)`,
		`delete from secret_links where placement_id in (` + stale + `)`,
		`update placements set egress_token_sha256 = null where egress_token_sha256 is not null
			and id in (` + stale + `)`,
	} {
		tag, err := s.q.Exec(ctx, q, PlacementTerminated, PlacementFailed, utc(cutoff))
		if err != nil {
			return n, fmt.Errorf("delete secret leftovers: %w", err)
		}
		n += tag.RowsAffected()
	}
	return n, nil
}

// DeleteTrafficLeftovers deletes the previews, SSH keys and SSH hosts of every placement that
// ended at or before cutoff
func (s *Store) DeleteTrafficLeftovers(ctx context.Context, cutoff time.Time) (int64, error) {
	var n int64
	for _, table := range []string{"previews", "ssh_keys", "ssh_hosts"} {
		tag, err := s.q.Exec(ctx, `delete from `+table+` where placement_id in (
			select id from placements where state in ($1, $2) and terminated_at <= $3)`,
			PlacementTerminated, PlacementFailed, utc(cutoff))
		if err != nil {
			return n, fmt.Errorf("delete %s leftovers: %w", table, err)
		}
		n += tag.RowsAffected()
	}
	return n, nil
}
