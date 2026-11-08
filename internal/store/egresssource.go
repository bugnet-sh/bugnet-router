package store

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// EgressSource is a placement's stored source, with a zero Prefix when none was ever stored
type EgressSource struct {
	Prefix netip.Prefix
	Bind   netip.Addr
	ReadAt time.Time
}

// SetEgressSource stores src and reports whether it changed, moving secrets_version if so
func (s *Store) SetEgressSource(
	ctx context.Context, id uuid.UUID, src provider.EgressSource,
) (bool, error) {
	var changed bool
	err := s.q.QueryRow(ctx, `
		with old as (
			select egress_source, egress_source_bind from placements where id = $1 for update
		)
		update placements p
		set egress_source = $2::cidr, egress_source_bind = $3::inet, egress_source_read_at = now(),
			secrets_version = p.secrets_version
				+ case when (old.egress_source, old.egress_source_bind) is distinct from ($2, $3)
					then 1 else 0 end
		from old
		where p.id = $1
		returning (old.egress_source, old.egress_source_bind) is distinct from ($2, $3)`,
		id, src.Prefix.Masked(), src.Bind).Scan(&changed)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrNotFound
	}
	if err != nil {
		return false, fmt.Errorf("set egress source: %w", err)
	}
	return changed, nil
}

// SecretsWithoutSourceCount counts ready placements of a provider that hold secrets and have no
// stored egress source
func (s *Store) SecretsWithoutSourceCount(ctx context.Context, providerName string) (int, error) {
	var n int
	err := s.q.QueryRow(ctx, `
		select count(*) from placements p
		where p.state = $1 and p.provider = $2 and p.egress_source is null
			and exists (select 1 from secrets x where x.placement_id = p.id and not x.fetch_only)`,
		PlacementReady, providerName).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count placements without an egress source: %w", err)
	}
	return n, nil
}
