package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/store"
)

func quote(pkg string, price int64) store.Quote {
	return store.Quote{
		PackageID: pkg, Provider: "boat", SizeName: "small", CostMicros: price - 10,
		PriceMicros: price,
	}
}

func TestPutQuotesUpsertsAndRefreshes(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	id := uuid.New()
	if err := s.PutQuotes(ctx, id, []store.Quote{quote("a", 100), quote("b", 200)}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		update price_quotes set quoted_at = now() - interval '1 hour' where estimate_id = $1`,
		id); err != nil {
		t.Fatal(err)
	}
	old, ok, err := s.QuoteFor(ctx, id, "a", 24*time.Hour)
	if err != nil || !ok {
		t.Fatalf("QuoteFor = %v, %v", ok, err)
	}
	if err := s.PutQuotes(ctx, id, []store.Quote{quote("a", 150)}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.QuoteFor(ctx, id, "a", 24*time.Hour)
	if err != nil || !ok {
		t.Fatalf("QuoteFor = %v, %v", ok, err)
	}
	if got.PriceMicros != 150 || got.CostMicros != 140 || got.Provider != "boat" ||
		got.SizeName != "small" {
		t.Fatalf("quote = %+v", got)
	}
	if !got.QuotedAt.After(old.QuotedAt) {
		t.Fatalf("quoted_at did not move: %v then %v", old.QuotedAt, got.QuotedAt)
	}
	other, ok, err := s.QuoteFor(ctx, id, "b", 24*time.Hour)
	if err != nil || !ok || other.PriceMicros != 200 {
		t.Fatalf("b = %+v, %v, %v", other, ok, err)
	}
}

func TestQuoteForAge(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	id := uuid.New()
	if err := s.PutQuotes(ctx, id, []store.Quote{quote("a", 100)}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		update price_quotes set quoted_at = now() - interval '2 hours' where estimate_id = $1`,
		id); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.QuoteFor(ctx, id, "a", time.Hour); err != nil || ok {
		t.Fatalf("old quote = %v, %v", ok, err)
	}
	if _, ok, err := s.QuoteFor(ctx, id, "a", 3*time.Hour); err != nil || !ok {
		t.Fatalf("fresh enough quote = %v, %v", ok, err)
	}
	if _, ok, err := s.QuoteFor(ctx, uuid.New(), "a", time.Hour); err != nil || ok {
		t.Fatalf("unknown estimate = %v, %v", ok, err)
	}
}

func TestRetainPriceQuotes(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	oldID, freshID := uuid.New(), uuid.New()
	for _, id := range []uuid.UUID{oldID, freshID} {
		if err := s.PutQuotes(ctx, id, []store.Quote{quote("a", 100)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `
		update price_quotes set quoted_at = now() - interval '25 hours' where estimate_id = $1`,
		oldID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `delete from price_quotes where estimate_id = any($1)`,
			[]uuid.UUID{oldID, freshID})
	})
	// other sessions' rows share the table, so only these two ids are checked
	cutoff := time.Now().Add(-24 * time.Hour)
	if _, err := s.Purge(ctx, store.RetainPriceQuotes, cutoff, 10000); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.QuoteFor(ctx, oldID, "a", 48*time.Hour); ok {
		t.Fatal("old quote survived")
	}
	if _, ok, _ := s.QuoteFor(ctx, freshID, "a", 48*time.Hour); !ok {
		t.Fatal("fresh quote was deleted")
	}
}
