package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Quote is the price an estimate showed for one start package
type Quote struct {
	PackageID, Provider, SizeName string
	CostMicros, PriceMicros       int64
	QuotedAt                      time.Time
}

// PutQuotes stores the quotes of an estimate and stamps every row it writes with now
func (s *Store) PutQuotes(ctx context.Context, estimateID uuid.UUID, qs []Quote) error {
	for _, q := range qs {
		_, err := s.q.Exec(ctx, `
			insert into price_quotes (estimate_id, package_id, provider, size_name, cost_micros,
				price_micros, quoted_at)
			values ($1, $2, $3, $4, $5, $6, now())
			on conflict (estimate_id, package_id) do update set provider = excluded.provider,
				size_name = excluded.size_name, cost_micros = excluded.cost_micros,
				price_micros = excluded.price_micros, quoted_at = now()`,
			estimateID, q.PackageID, q.Provider, q.SizeName, q.CostMicros, q.PriceMicros)
		if err != nil {
			return fmt.Errorf("put quote %s: %w", q.PackageID, err)
		}
	}
	return nil
}

// QuoteFor reads the quote of a package of an estimate and reports false when there is none
// or it is older than maxAge
func (s *Store) QuoteFor(
	ctx context.Context, estimateID uuid.UUID, packageID string, maxAge time.Duration,
) (Quote, bool, error) {
	var q Quote
	err := s.q.QueryRow(ctx, `
		select package_id, provider, size_name, cost_micros, price_micros, quoted_at
		from price_quotes
		where estimate_id = $1 and package_id = $2 and quoted_at >= now() - $3::interval`,
		estimateID, packageID, maxAge).Scan(&q.PackageID, &q.Provider, &q.SizeName,
		&q.CostMicros, &q.PriceMicros, &q.QuotedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Quote{}, false, nil
	}
	if err != nil {
		return Quote{}, false, fmt.Errorf("quote for %s: %w", packageID, err)
	}
	q.QuotedAt = q.QuotedAt.UTC()
	return q, true, nil
}
