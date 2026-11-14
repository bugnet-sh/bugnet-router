package pricing_test

import (
	"math"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/pricing"
)

func TestPrice(t *testing.T) {
	r := pricing.Rule{MarkupBP: 1000}
	for _, c := range []struct{ cost, want int64 }{
		{0, 0}, {1, 2}, {3050, 3355}, {22755, 25031}, {34540, 37994}, {130557, 143613},
	} {
		if got := r.Price(c.cost); got != c.want {
			t.Errorf("Price(%d) = %d, want %d", c.cost, got, c.want)
		}
	}
	if !r.Fits(3050, 3355) || r.Fits(3050, 3354) {
		t.Fatal("Fits disagrees with Price")
	}
	if got := r.Price(math.MaxInt64 / 2); got != math.MaxInt64 {
		t.Fatalf("an overflowing price is %d, want MaxInt64", got)
	}
	o := pricing.Rule{Overhead: map[string]time.Duration{"boat": 10 * time.Second}}
	if got := o.Billed("boat", 10*time.Minute); got != 610*time.Second {
		t.Fatalf("Billed = %v", got)
	}
	if got := o.Billed("e2b", 10*time.Minute); got != 10*time.Minute {
		t.Fatalf("an unlisted provider has no overhead, got %v", got)
	}
}

func TestPriceEdges(t *testing.T) {
	r := pricing.Rule{MarkupBP: 1000}
	if got := r.Price(-5); got != 0 {
		t.Errorf("a negative cost prices %d, want 0", got)
	}
	if got := r.Price(math.MaxInt64); got != math.MaxInt64 {
		t.Errorf("the largest cost prices %d, want MaxInt64", got)
	}
	if got := (pricing.Rule{}).Price(7); got != 7 {
		t.Errorf("no markup prices %d, want the cost", got)
	}
}
