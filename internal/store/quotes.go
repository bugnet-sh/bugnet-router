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
	// OrderID is the order that claimed the quote, empty for none
	OrderID string
}

// PutQuotes stores the quotes of an estimate, stamping every row it writes with now and leaving it
// unclaimed
func (s *Store) PutQuotes(ctx context.Context, estimateID uuid.UUID, qs []Quote) error {
	for _, q := range qs {
		_, err := s.q.Exec(ctx, `
			insert into price_quotes (estimate_id, package_id, provider, size_name, cost_micros,
				price_micros, quoted_at)
			values ($1, $2, $3, $4, $5, $6, now())
			on conflict (estimate_id, package_id) do update set provider = excluded.provider,
				size_name = excluded.size_name, cost_micros = excluded.cost_micros,
				price_micros = excluded.price_micros, quoted_at = now(), order_id = null`,
			estimateID, q.PackageID, q.Provider, q.SizeName, q.CostMicros, q.PriceMicros)
		if err != nil {
			return fmt.Errorf("put quote %s: %w", q.PackageID, err)
		}
	}
	return nil
}

// quoteColumns are the columns scanQuote reads, in its order
const quoteColumns = `package_id, provider, size_name, cost_micros, price_micros, quoted_at,
	coalesce(order_id, '')`

// QuoteFor reads the quote of a package of an estimate and reports false when there is none
// or it is older than maxAge
func (s *Store) QuoteFor(
	ctx context.Context, estimateID uuid.UUID, packageID string, maxAge time.Duration,
) (Quote, bool, error) {
	q, ok, err := scanQuote(s.q.QueryRow(ctx, `
		select `+quoteColumns+` from price_quotes
		where estimate_id = $1 and package_id = $2 and quoted_at >= now() - $3::interval`,
		estimateID, packageID, maxAge))
	if err != nil {
		return Quote{}, false, fmt.Errorf("quote for %s: %w", packageID, err)
	}
	return q, ok, nil
}

// ClaimQuote claims for the order the quote of a package of an estimate and reads it, false when
// the quote is older than maxAge, paid is short of its price or another order holds it
func (s *Store) ClaimQuote(
	ctx context.Context, estimateID uuid.UUID, packageID, orderID string, paid int64,
	maxAge time.Duration,
) (Quote, bool, error) {
	q, ok, err := scanQuote(s.q.QueryRow(ctx, `
		update price_quotes set order_id = $3
		where estimate_id = $1 and package_id = $2 and quoted_at >= now() - $5::interval
			and (order_id is null or order_id = $3) and price_micros <= $4
		returning `+quoteColumns,
		estimateID, packageID, orderID, paid, maxAge))
	if err != nil {
		return Quote{}, false, fmt.Errorf("claim quote %s: %w", packageID, err)
	}
	return q, ok, nil
}

// scanQuote reads one row of quoteColumns, false for no row
func scanQuote(row pgx.Row) (Quote, bool, error) {
	var q Quote
	err := row.Scan(&q.PackageID, &q.Provider, &q.SizeName, &q.CostMicros, &q.PriceMicros,
		&q.QuotedAt, &q.OrderID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Quote{}, false, nil
	}
	if err != nil {
		return Quote{}, false, err
	}
	q.QuotedAt = q.QuotedAt.UTC()
	return q, true, nil
}
