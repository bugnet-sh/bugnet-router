package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Limit sources of provider_capacity.limit_source
const (
	LimitFromProvider = "provider"
	LimitFromSettings = "settings"
	LimitFromDefault  = "default"
)

// Account signal sources, classes and actions of provider_signals
const (
	SignalClassAccount  = "account"
	SignalSourceAccount = "account_check"
	SignalProviderBlock = "provider_blocked"
	SignalProviderClear = "provider_cleared"
)

// WriteLimits stores one poll, the sandboxes this router holds being read in the same statement
func (s *Store) WriteLimits(
	ctx context.Context, provider string, limit int, source string, active int, at time.Time,
) error {
	_, err := s.q.Exec(ctx, `
		insert into provider_capacity (provider, max_concurrent, in_use, external_in_use,
			limit_source, provider_active, polled_at)
		values ($1, $2, 0, greatest($4::int, 0), $3, $4::int, $5)
		on conflict (provider) do update set
			max_concurrent = excluded.max_concurrent, limit_source = excluded.limit_source,
			provider_active = excluded.provider_active,
			external_in_use = greatest(0, excluded.provider_active - provider_capacity.in_use),
			polled_at = excluded.polled_at`,
		provider, limit, source, active, utc(at))
	if err != nil {
		return fmt.Errorf("write limits: %w", err)
	}
	return nil
}

// Capacity is one provider's slot count as the last poll left it
type Capacity struct {
	Provider      string
	MaxConcurrent int
	InUse         int
	ExternalInUse int
	LimitSource   string
	// ProviderActive is the provider's own count at the last poll
	ProviderActive int
	// PolledAt is zero for a row no poll has written
	PolledAt time.Time
}

// Capacities returns every provider's capacity row, by provider
func (s *Store) Capacities(ctx context.Context) ([]Capacity, error) {
	rows, err := s.q.Query(ctx, `
		select provider, max_concurrent, in_use, external_in_use, limit_source, provider_active,
			polled_at
		from provider_capacity order by provider`)
	if err != nil {
		return nil, fmt.Errorf("select capacity: %w", err)
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Capacity, error) {
		var c Capacity
		var polled *time.Time
		err := r.Scan(&c.Provider, &c.MaxConcurrent, &c.InUse, &c.ExternalInUse, &c.LimitSource,
			&c.ProviderActive, &polled)
		if polled != nil {
			c.PolledAt = utc(*polled)
		}
		return c, err
	})
}

// ProviderHealth is one provider's shared health row
type ProviderHealth struct {
	Provider string
	// State is ok, slow or down, from the start samples
	State      string
	StateSince time.Time
	// Samples and ReadyP95 are the rollup window's count and ready p95
	Samples  int
	ReadyP95 time.Duration
	// ColdTotalP95 and PooledTotalP95 are the whole-attempt p95 figures of the cold and pooled
	// paths, zero for none
	ColdTotalP95, PooledTotalP95 time.Duration
	// Blocked is banned, unfunded or auth, empty while the account works
	Blocked       string
	BlockedDetail string
	BlockedSince  time.Time
	// AccountCheckedAt is zero before the first check
	AccountCheckedAt time.Time
	// SuccessLB is the Wilson lower bound of the window's success rate
	SuccessLB float64
	// ForeignSandboxes is the last count of sandboxes no known deployment owns
	ForeignSandboxes int
	UpdatedAt        time.Time
}

// ProviderHealths returns every provider's shared health row
func (s *Store) ProviderHealths(ctx context.Context) ([]ProviderHealth, error) {
	rows, err := s.q.Query(ctx, `
		select provider, state, state_since, samples, ready_p95_ms, coalesce(blocked, ''),
			blocked_detail, blocked_since, account_checked_at, success_lb, foreign_sandboxes,
			updated_at, coalesce(cold_total_p95_ms, 0), coalesce(pooled_total_p95_ms, 0)
		from provider_health order by provider`)
	if err != nil {
		return nil, fmt.Errorf("select provider health: %w", err)
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (ProviderHealth, error) {
		var h ProviderHealth
		var since, checked *time.Time
		var p95, cold, pooled int
		err := r.Scan(&h.Provider, &h.State, &h.StateSince, &h.Samples, &p95, &h.Blocked,
			&h.BlockedDetail, &since, &checked, &h.SuccessLB, &h.ForeignSandboxes,
			&h.UpdatedAt, &cold, &pooled)
		h.StateSince = utc(h.StateSince)
		h.ReadyP95 = time.Duration(p95) * time.Millisecond
		h.ColdTotalP95 = time.Duration(cold) * time.Millisecond
		h.PooledTotalP95 = time.Duration(pooled) * time.Millisecond
		if since != nil {
			h.BlockedSince = utc(*since)
		}
		if checked != nil {
			h.AccountCheckedAt = utc(*checked)
		}
		h.UpdatedAt = utc(h.UpdatedAt)
		return h, err
	})
}

// AccountChange says what one account check did to the provider's row
type AccountChange int

// Changes a check can make
const (
	AccountSame AccountChange = iota
	AccountBlocked
	AccountCleared
)

// RecordAccount stores one reading of an account and signals a change of its block, empty if none
func (s *Store) RecordAccount(
	ctx context.Context, provider, block, detail string, at time.Time,
) (AccountChange, error) {
	// Postgres refuses a NUL or a byte that is not UTF-8 in text, which a provider's words may hold
	detail = strings.ReplaceAll(strings.ToValidUTF8(detail, ""), "\x00", "")
	change := AccountSame
	err := s.WithTx(ctx, func(tx pgx.Tx) error {
		ts := s.Tx(tx)
		if _, err := tx.Exec(ctx, `
			insert into provider_health (provider) values ($1) on conflict do nothing`,
			provider); err != nil {
			return fmt.Errorf("record account: %w", err)
		}
		// The row is locked, so two checks cannot both see the same block as the one before
		var before string
		err := tx.QueryRow(ctx, `
			select coalesce(blocked, '') from provider_health where provider = $1 for update`,
			provider).Scan(&before)
		if err != nil {
			return fmt.Errorf("record account: %w", err)
		}
		_, err = tx.Exec(ctx, `
			update provider_health set blocked = nullif($2::text, ''), blocked_detail = $3,
				blocked_since = case when $2::text = '' then null
					when blocked is distinct from nullif($2::text, '') then $4::timestamptz
					else blocked_since end,
				account_checked_at = $4::timestamptz, updated_at = $4::timestamptz
			where provider = $1`, provider, block, detail, utc(at))
		if err != nil {
			return fmt.Errorf("record account: %w", err)
		}
		change = accountChange(before, block)
		if change == AccountSame {
			return nil
		}
		action := SignalProviderBlock
		if change == AccountCleared {
			action = SignalProviderClear
		}
		observed, err := json.Marshal(map[string]string{"block": block, "detail": detail})
		if err != nil {
			return err
		}
		return ts.InsertSignal(ctx, Signal{
			Class: SignalClassAccount, Provider: provider, Source: SignalSourceAccount,
			Observed: observed, Action: action, CreatedAt: at,
		})
	})
	if err != nil {
		return AccountSame, err
	}
	return change, nil
}

func accountChange(before, after string) AccountChange {
	switch {
	case before == after:
		return AccountSame
	case after == "":
		return AccountCleared
	}
	return AccountBlocked
}
