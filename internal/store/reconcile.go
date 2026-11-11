package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// MarkDegraded records when a sandbox was first seen degraded, keeping an earlier time
func (s *Store) MarkDegraded(ctx context.Context, id uuid.UUID, at time.Time) error {
	_, err := s.q.Exec(ctx, `
		update placements set degraded_since = $2 where id = $1 and degraded_since is null`,
		id, utc(at))
	if err != nil {
		return fmt.Errorf("mark degraded: %w", err)
	}
	return nil
}

// ClearDegraded forgets a degraded spell, since a healthy reading ends it
func (s *Store) ClearDegraded(ctx context.Context, id uuid.UUID) error {
	_, err := s.q.Exec(ctx, `
		update placements set degraded_since = null where id = $1 and degraded_since is not null`,
		id)
	if err != nil {
		return fmt.Errorf("clear degraded: %w", err)
	}
	return nil
}

// PlacementBySandbox finds the placement a provider's sandbox belongs to, or ErrNotFound. It
// reads the kept sandbox first and then an attempt's, for a sandbox that was not kept.
func (s *Store) PlacementBySandbox(
	ctx context.Context, provider, sandboxID string,
) (Placement, error) {
	for _, where := range []string{
		`provider = $1 and provider_ref ->> 'id' = $2`,
		`id in (select placement_id from placement_attempts where provider = $1
			and sandbox_id = $2)`,
	} {
		p, err := s.scanPlacement(s.q.QueryRow(ctx, `select `+placementColumns+
			` from placements where `+where+` order by created_at desc limit 1`,
			provider, sandboxID))
		if !errors.Is(err, ErrNotFound) {
			if err != nil {
				return Placement{}, fmt.Errorf("select placement: %w", err)
			}
			return p, nil
		}
	}
	return Placement{}, ErrNotFound
}

// ReadySandbox is a ready placement and the sandbox it holds
type ReadySandbox struct {
	PlacementID uuid.UUID
	SandboxID   string
	Deadline    *time.Time
}

// ReadySandboxes lists the placements ready on a provider since before the given time
func (s *Store) ReadySandboxes(
	ctx context.Context, provider string, readyBefore time.Time,
) ([]ReadySandbox, error) {
	rows, err := s.q.Query(ctx, `
		select id, provider_ref ->> 'id', deadline from placements
		where provider = $1 and state = $2 and ready_at < $3 and provider_ref is not null
		order by ready_at, id`, provider, PlacementReady, utc(readyBefore))
	if err != nil {
		return nil, fmt.Errorf("select ready sandboxes: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (ReadySandbox, error) {
		var rs ReadySandbox
		return rs, r.Scan(&rs.PlacementID, &rs.SandboxID, &rs.Deadline)
	})
	if err != nil {
		return nil, fmt.Errorf("select ready sandboxes: %w", err)
	}
	return out, nil
}

// LockProviderSignals takes the advisory lock that orders the correlation guard's readers of one
// provider until the transaction ends
func LockProviderSignals(ctx context.Context, q Querier, provider string) error {
	_, err := q.Exec(ctx,
		`select pg_advisory_xact_lock(hashtextextended('bugnet:signals:' || $1, 0))`, provider)
	if err != nil {
		return fmt.Errorf("lock provider signals: %w", err)
	}
	return nil
}

// LostCount counts the placements of a provider that got one of classes since then, an
// operator's end, a held end and a bare alert left out
func LostCount(
	ctx context.Context, q Querier, provider string, classes []string, since time.Time,
) (int, error) {
	var n int
	err := q.QueryRow(ctx, `
		select count(distinct placement_id) from provider_signals
		where provider = $1 and class = any($2) and created_at > $3 and source <> 'operator'
			and action <> 'held' and action <> $4
			and placement_id is not null`, provider, classes, utc(since), SignalAlerted).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count provider signals: %w", err)
	}
	return n, nil
}

// CanaryFailedSince reports whether the provider's newest canary since then failed
func CanaryFailedSince(
	ctx context.Context, q Querier, provider string, since time.Time,
) (bool, error) {
	var outcome string
	err := q.QueryRow(ctx, `
		select outcome from start_samples
		where provider = $1 and kind = 'canary' and started_at > $2
		order by started_at desc limit 1`, provider, utc(since)).Scan(&outcome)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("select newest canary: %w", err)
	}
	return outcome == OutcomeFailed, nil
}

// ProviderBlock is the account block on a provider, empty while the account works
func ProviderBlock(ctx context.Context, q Querier, provider string) (string, error) {
	var blocked *string
	err := q.QueryRow(ctx, `select blocked from provider_health where provider = $1`,
		provider).Scan(&blocked)
	if errors.Is(err, pgx.ErrNoRows) || blocked == nil {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("select provider block: %w", err)
	}
	return *blocked, nil
}

// LostEnds counts each provider's provider_failed ends since then
func (s *Store) LostEnds(ctx context.Context, since time.Time) (map[string]int, error) {
	rows, err := s.q.Query(ctx, `
		select provider, count(*) from placements
		where termination_reason = 'provider_failed' and terminating_at > $1
		group by provider`, utc(since))
	if err != nil {
		return nil, fmt.Errorf("count provider failures: %w", err)
	}
	return collectCounts(rows)
}

// UnaskedKills counts each provider's placements killed that no router asked for since then
func (s *Store) UnaskedKills(ctx context.Context, since time.Time) (map[string]int, error) {
	rows, err := s.q.Query(ctx, `
		select provider, count(distinct placement_id) from provider_signals
		where created_at > $1 and observed ->> 'alert' = $2 group by provider`,
		utc(since), AlertUnaskedKill)
	if err != nil {
		return nil, fmt.Errorf("count unasked kills: %w", err)
	}
	return collectCounts(rows)
}

// HasAlert reports whether a placement holds a signal whose evidence carries the alert
func (s *Store) HasAlert(ctx context.Context, placementID uuid.UUID, alert string) (bool, error) {
	var has bool
	err := s.q.QueryRow(ctx, `
		select exists (select 1 from provider_signals
			where placement_id = $1 and observed ->> 'alert' = $2)`, placementID, alert).Scan(&has)
	if err != nil {
		return false, fmt.Errorf("select placement alert: %w", err)
	}
	return has, nil
}

// AlertUnaskedKill is the alert a signal's evidence carries for a kill no router asked for
const AlertUnaskedKill = "unasked_kill"

// SignalAlerted is the action of a signal that raised an alert and left its placement as it was
const SignalAlerted = "alerted"

func collectCounts(rows pgx.Rows) (map[string]int, error) {
	out := map[string]int{}
	var name string
	var n int
	_, err := pgx.ForEachRow(rows, []any{&name, &n}, func() error {
		out[name] = n
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("count rows: %w", err)
	}
	return out, nil
}
