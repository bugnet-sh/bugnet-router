package placement

import (
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/pricing"
)

// hedgeCand is a candidate priced by the hour with a start fee, billed by the second
func hedgeCand(t *testing.T, name string, hourly, fee int64) Candidate {
	t.Helper()
	return Candidate{
		Provider: newLocal(t, name, hourly, time.Second), HourlyMicros: hourly,
		StartFeeMicros: fee,
	}
}

func TestHedgeFitsExamples(t *testing.T) {
	life, run := 90*time.Second, 10*time.Minute
	none := pricing.Rule{}
	small := hedgeCand(t, "small", 24_000, 0)
	medium := hedgeCand(t, "medium", 12_000, 700)
	// 4000 to run the package, 700 of fee, 600 for the loser's 90 s
	eq(t, HedgeFits(small, medium, run, 0, life, 25_000, none), true)
	// 20000 to run, 3300 of fees, 3000 for the loser, over 25000
	big := hedgeCand(t, "big", 120_000, 0)
	pricey := hedgeCand(t, "pricey", 12_000, 3300)
	eq(t, HedgeFits(big, pricey, run, 0, life, 25_000, none), false)
	// the dearer one winning costs the most, whichever side it is on
	eq(t, HedgeFits(pricey, big, run, 0, life, 25_000, none), false)
	eq(t, HedgeFits(small, medium, run, 0, life, 500, none), false)
}

func TestHedgeFitsUsesBudget(t *testing.T) {
	// The worst case of 5,300 is priced at 5,830 under a 10% markup
	life, run := 90*time.Second, 10*time.Minute
	r := pricing.Rule{MarkupBP: 1000}
	small := hedgeCand(t, "small", 24_000, 0)
	medium := hedgeCand(t, "medium", 12_000, 700)
	eq(t, HedgeFits(small, medium, run, 0, life, 5830, r), true)
	eq(t, HedgeFits(small, medium, run, 0, life, 5829, r), false)
	eq(t, HedgeFits(small, medium, run, 0, life, 5300, pricing.Rule{}), true)
}

func TestHedgeFitsFreePackage(t *testing.T) {
	life, run := 90*time.Second, 10*time.Minute
	r := pricing.Rule{MarkupBP: 1000}
	zero := hedgeCand(t, "zero", 0, 0)
	eq(t, HedgeFits(zero, zero, run, 0, life, 0, r), true)
	eq(t, HedgeFits(zero, hedgeCand(t, "fee", 0, 1), run, 0, life, 0, r), false)
	eq(t, HedgeFits(hedgeCand(t, "paid", 1000, 0), zero, run, 0, life, 0, r), false)
}

// a reservation's pre of 55 s counts on both sides and can tip a close fit
func TestHedgeFitsCountsAReservationsPre(t *testing.T) {
	run := 10 * time.Minute
	a := hedgeCand(t, "a", 120_000, 0)
	b := hedgeCand(t, "b", 6_000, 1000)
	eq(t, HedgeFits(a, b, run, 0, 90*time.Second, 25_000, pricing.Rule{}), true)
	eq(t, HedgeFits(a, b, run, 55*time.Second, 90*time.Second, 25_000, pricing.Rule{}), false)
}
