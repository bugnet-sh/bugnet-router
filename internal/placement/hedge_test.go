package placement

import (
	"testing"
	"time"
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
	life := 90 * time.Second
	p := pkg(10, 50_000) // $0.05 for 10 minutes, so a budget of $0.025 at a floor of 0.5
	small := hedgeCand(t, "small", 24_000, 0)
	medium := hedgeCand(t, "medium", 12_000, 700)
	// 4000 to run the package, 700 of fee, 600 for the loser's 90 s
	eq(t, HedgeFits(p, small, medium, 0, life, 0.5), true)
	// 20000 to run, 3300 of fees, 3000 for the loser, over 25000
	big := hedgeCand(t, "big", 120_000, 0)
	pricey := hedgeCand(t, "pricey", 12_000, 3300)
	eq(t, HedgeFits(p, big, pricey, 0, life, 0.5), false)
	// the dearer one winning costs the most, whichever side it is on
	eq(t, HedgeFits(p, pricey, big, 0, life, 0.5), false)
	eq(t, HedgeFits(p, small, medium, 0, life, 0.99), false)
}

func TestHedgeFitsFreePackage(t *testing.T) {
	free := pkg(10, 0)
	zero := hedgeCand(t, "zero", 0, 0)
	eq(t, HedgeFits(free, zero, zero, 0, 90*time.Second, 0.5), true)
	eq(t, HedgeFits(free, zero, hedgeCand(t, "fee", 0, 1), 0, 90*time.Second, 0.5), false)
	eq(t, HedgeFits(free, hedgeCand(t, "paid", 1000, 0), zero, 0, 90*time.Second, 0.5), false)
}

// a reservation's pre of 55 s counts on both sides and can tip a close fit
func TestHedgeFitsCountsAReservationsPre(t *testing.T) {
	p := pkg(10, 50_000)
	a := hedgeCand(t, "a", 120_000, 0)
	b := hedgeCand(t, "b", 6_000, 1000)
	eq(t, HedgeFits(p, a, b, 0, 90*time.Second, 0.5), true)
	eq(t, HedgeFits(p, a, b, 55*time.Second, 90*time.Second, 0.5), false)
}
