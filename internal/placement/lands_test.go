package placement

import (
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/pricing"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// landed names each landing's provider, size and whether it is open
func landed(ls []Landing) []string {
	var out []string
	for _, l := range ls {
		word := "closed"
		if l.Open {
			word = "open"
		}
		out = append(out, l.Provider.Name()+" "+l.Size.Name+" "+word)
	}
	return out
}

func TestLandsKeepsTheProvidersOrderWhateverTheirHealth(t *testing.T) {
	dear := newLocal(t, "dear", 18000, time.Second)
	down := newLocal(t, "down", 6000, time.Second)
	cheap := newLocal(t, "cheap", 1000, time.Second)
	e := engine(fakeHealth{down: map[string]bool{"down": true}}, dear, down, cheap)
	in := Inputs{Package: pkg(10, 50000), Rule: pricing.Rule{MarkupBP: 1000}}
	got := e.Lands(in)
	eq(t, landed(got), []string{"dear small open", "down small closed", "cheap small open"})
	eq(t, []int64{got[0].CostMicros, got[0].PriceMicros}, []int64{3000, 50000})
	eq(t, got[2].Warm, false)
	cands, err := e.Candidates(in)
	eq(t, err, nil)
	eq(t, names(cands), []string{"cheap", "dear"})
}

func TestLandsLeavesOutWhatTheRuleRefuses(t *testing.T) {
	big := provider.Size{Name: "big", Resources: provider.Resources{
		VCPU: 8, MemoryMiB: 16384, DiskGiB: 100,
	}}
	sized := newLocal(t, "sized", 1000, time.Second, smallSize, big)
	dear := newLocal(t, "dear", 200000, time.Second)
	small := newLocal(t, "small", 1000, time.Second)
	e := engine(fakeHealth{}, sized, dear, small)
	large := pkg(10, 50000)
	large.Resources.VCPU = 4
	double, plus40 := pricing.Rule{MarkupBP: 10000}, pricing.Rule{MarkupBP: 4000}
	eq(t, landed(e.Lands(Inputs{Package: large, Rule: double})), []string{"sized big open"})
	// 10 minutes at 200,000 an hour cost 33,334, over 50,000 at 100% and under it at 40%
	eq(t, landed(e.Lands(Inputs{Package: pkg(10, 50000), Rule: double})),
		[]string{"sized small open", "small small open"})
	eq(t, landed(e.Lands(Inputs{Package: pkg(10, 50000), Rule: plus40})),
		[]string{"sized small open", "dear small open", "small small open"})
	eq(t, len(e.Lands(Inputs{Package: pkg(10, 50000), Exclude: map[string]bool{
		"sized": true, "dear": true, "small": true,
	}})), 0)
}

func TestLandsCountsTheStartFeeAndClosesARefusedStart(t *testing.T) {
	// A 30,000 start fee takes 10 minutes at 18,000 an hour to 33,000 of a 50,000 price
	fee := startFee{Provider: newLocal(t, "fee", 18000, time.Second), fee: 30000}
	refused := startFee{Provider: newLocal(t, "refused", 18000, time.Second), refused: true,
		fee: 30000}
	e := engine(fakeHealth{}, fee, refused)
	double := Inputs{Package: pkg(10, 50000), Rule: pricing.Rule{MarkupBP: 10000}}
	got := e.Lands(double)
	eq(t, landed(got), []string{"refused small closed"})
	eq(t, []int64{got[0].CostMicros, got[0].StartFeeMicros}, []int64{3000, 0})
	got = e.Lands(Inputs{Package: pkg(10, 50000), Rule: pricing.Rule{MarkupBP: 5000}})
	eq(t, landed(got), []string{"fee small open", "refused small closed"})
	eq(t, got[0].CostMicros, int64(33000))
	_, err := e.Candidates(double)
	eq(t, err, ErrNoCandidate)
}

func TestLandsFollowsImageAccept(t *testing.T) {
	e, _ := imageEngine(t, "own")
	got := e.Lands(Inputs{Package: pkg(10, 1000000), ImageAccept: []string{"2026.11.1"}})
	eq(t, landed(got), []string{"fresh small open", "own small open"})
}
