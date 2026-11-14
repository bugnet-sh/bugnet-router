package store_test

import (
	"context"
	"fmt"
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

func TestClaimQuote(t *testing.T) {
	s, pool := newStoreWithPool(t)
	ctx := context.Background()
	id := uuid.New()
	if err := s.PutQuotes(ctx, id, []store.Quote{quote("a", 100)}); err != nil {
		t.Fatal(err)
	}
	// A payment short of the price claims nothing
	if _, ok, err := s.ClaimQuote(ctx, id, "a", "order-1", 99, time.Hour); err != nil || ok {
		t.Fatalf("short claim = %v, %v", ok, err)
	}
	got, ok, err := s.ClaimQuote(ctx, id, "a", "order-1", 100, time.Hour)
	if err != nil || !ok || got.OrderID != "order-1" || got.PriceMicros != 100 ||
		got.Provider != "boat" || got.SizeName != "small" {
		t.Fatalf("claim = %+v, %v, %v", got, ok, err)
	}
	// A retry of the same order keeps its claim, and another order gets none
	if _, ok, err := s.ClaimQuote(ctx, id, "a", "order-1", 100, time.Hour); err != nil || !ok {
		t.Fatalf("retried claim = %v, %v", ok, err)
	}
	if _, ok, err := s.ClaimQuote(ctx, id, "a", "order-2", 500, time.Hour); err != nil || ok {
		t.Fatalf("second order's claim = %v, %v", ok, err)
	}
	read, ok, err := s.QuoteFor(ctx, id, "a", time.Hour)
	if err != nil || !ok || read.OrderID != "order-1" {
		t.Fatalf("QuoteFor after the claim = %+v, %v, %v", read, ok, err)
	}
	for _, c := range []struct {
		estimate uuid.UUID
		pkg      string
	}{{uuid.New(), "a"}, {id, "b"}} {
		_, ok, err := s.ClaimQuote(ctx, c.estimate, c.pkg, "order-3", 100, time.Hour)
		if err != nil || ok {
			t.Fatalf("claim of no quote = %v, %v", ok, err)
		}
	}
	// A quote past the hold is claimed by no order
	other := uuid.New()
	if err := s.PutQuotes(ctx, other, []store.Quote{quote("a", 100)}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		update price_quotes set quoted_at = now() - interval '2 hours' where estimate_id = $1`,
		other); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.ClaimQuote(ctx, other, "a", "order-4", 100, time.Hour); err != nil || ok {
		t.Fatalf("claim of an old quote = %v, %v", ok, err)
	}
}

func TestClaimQuoteOneOrderWins(t *testing.T) {
	s, _ := newStoreWithPool(t)
	ctx := context.Background()
	id := uuid.New()
	if err := s.PutQuotes(ctx, id, []store.Quote{quote("a", 100)}); err != nil {
		t.Fatal(err)
	}
	const orders = 8
	won := make(chan string, orders)
	errs := make(chan error, orders)
	for i := range orders {
		go func() {
			order := fmt.Sprintf("order-%d", i)
			_, ok, err := s.ClaimQuote(ctx, id, "a", order, 100, time.Hour)
			if ok {
				won <- order
			}
			errs <- err
		}()
	}
	for range orders {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	close(won)
	var winners []string
	for w := range won {
		winners = append(winners, w)
	}
	if len(winners) != 1 {
		t.Fatalf("winners %v, want exactly one", winners)
	}
}

func TestPutQuotesClearsTheClaim(t *testing.T) {
	s, _ := newStoreWithPool(t)
	ctx := context.Background()
	id := uuid.New()
	if err := s.PutQuotes(ctx, id, []store.Quote{quote("a", 100)}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.ClaimQuote(ctx, id, "a", "order-1", 100, time.Hour); err != nil || !ok {
		t.Fatalf("claim = %v, %v", ok, err)
	}
	// The estimate is served again, so its quote is a new one that the next paid order claims
	if err := s.PutQuotes(ctx, id, []store.Quote{quote("a", 120)}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.QuoteFor(ctx, id, "a", time.Hour)
	if err != nil || !ok || got.OrderID != "" || got.PriceMicros != 120 {
		t.Fatalf("QuoteFor after a new serve = %+v, %v, %v", got, ok, err)
	}
	if _, ok, err := s.ClaimQuote(ctx, id, "a", "order-2", 120, time.Hour); err != nil || !ok {
		t.Fatalf("claim after a new serve = %v, %v", ok, err)
	}
}
