package placement

import (
	"errors"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/pricing"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

type fakeWarm map[string]bool

func (f fakeWarm) WarmAvailable(name, size string) bool { return f[name+"/"+size] }

func warmEngine(h Health, w Warm, ps ...provider.Provider) *Engine {
	e := engine(h, ps...)
	e.warm = w
	return e
}

func TestCandidatesMarkWarmMembers(t *testing.T) {
	a := newLocal(t, "a", 300000, time.Second)
	b := newLocal(t, "b", 600000, time.Second)
	e := warmEngine(fakeHealth{}, fakeWarm{"a/small": true, "b/small": true}, a, b)
	got, err := e.Candidates(Inputs{Package: pkg(10, 1000000)})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, names(got), []string{"a", "b"})
	eq(t, []bool{got[0].Warm, got[1].Warm}, []bool{true, false})
	eq(t, got[0].HourlyMicros, int64(300000))
	eq(t, got[0].StartFeeMicros, int64(0))
	got, err = e.Candidates(Inputs{Package: pkg(10, 1000000), WarmFirst: true})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, []bool{got[0].Warm, got[1].Warm}, []bool{true, true})
}

func TestCostFirstLooksAtTheFirstOnly(t *testing.T) {
	a := newLocal(t, "a", 300000, time.Second)
	b := newLocal(t, "b", 600000, time.Second)
	e := warmEngine(fakeHealth{}, fakeWarm{"b/small": true}, a, b)
	got, err := e.Candidates(Inputs{Package: pkg(10, 1000000)})
	if err != nil {
		t.Fatal(err)
	}
	// The dearer provider has the warm member, but a cold order takes the cheaper one
	eq(t, names(got), []string{"a", "b"})
	eq(t, []bool{got[0].Warm, got[1].Warm}, []bool{false, false})
	// Once the first is tried, the next one is the first
	got, err = e.Candidates(Inputs{Package: pkg(10, 1000000), Exclude: map[string]bool{"a": true}})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, got[0].Warm, true)
}

func TestWarmFirstOrder(t *testing.T) {
	cheap := newLocal(t, "cheap", 100000, time.Second)
	warm := newLocal(t, "warm", 600000, time.Second)
	slow := newLocal(t, "slow", 300000, time.Second)
	h := fakeHealth{p95: map[string]time.Duration{"slow": 20 * time.Second,
		"warm": 20 * time.Second}}
	e := warmEngine(h, fakeWarm{"warm/small": true}, cheap, warm, slow)
	got, err := e.Candidates(Inputs{Package: pkg(10, 1000000), WarmFirst: true})
	if err != nil {
		t.Fatal(err)
	}
	// Warm before cheap, and warm before not slow, even where its creates are slow
	eq(t, names(got), []string{"warm", "cheap", "slow"})
	got, err = e.Candidates(Inputs{Package: pkg(10, 1000000)})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, names(got), []string{"cheap", "slow", "warm"})
}

func TestRestrictedPackageIsNeverWarm(t *testing.T) {
	p := capsProvider{
		Provider: newLocal(t, "r", 100000, time.Second),
		caps:     provider.Capabilities{EgressControl: true},
	}
	e := warmEngine(fakeHealth{}, fakeWarm{"r/small": true}, p)
	in := Inputs{Package: pkg(10, 1000000)}
	got, err := e.Candidates(in)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, got[0].Warm, true)
	in.Package.EgressAllow = []string{"example.com"}
	got, err = e.Candidates(in)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, got[0].Warm, false)
}

func TestHoldRaisesCostAndLifetime(t *testing.T) {
	// 10 minutes at $0.60 an hour is $0.10, and a hold of 150 s adds $0.025
	p := capsProvider{
		Provider: newLocal(t, "a", 600000, time.Second),
		caps:     provider.Capabilities{MaxLifetime: 20 * time.Minute},
	}
	e := engine(fakeHealth{}, p)
	base, err := e.Candidates(Inputs{Package: pkg(10, 200000)})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, base[0].CostMicros, int64(100000))
	held, err := e.Candidates(Inputs{Package: pkg(10, 200000), Hold: 150 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, held[0].CostMicros, int64(125000))
	eq(t, held[0].PriceMicros, int64(200000))
	// The hold bills into the cost, so a markup the bare cost clears can refuse it
	double := pricing.Rule{MarkupBP: 10000}
	if _, err = e.Candidates(Inputs{Package: pkg(10, 200000), Rule: double}); err != nil {
		t.Fatalf("the bare cost was refused: %v", err)
	}
	_, err = e.Candidates(Inputs{
		Package: pkg(10, 200000), Hold: 150 * time.Second, Rule: double,
	})
	eq(t, errors.Is(err, ErrNoCandidate), true)
	// The longest hold must fit the lifetime: 10 + 6 + 5 minutes needs more than 20
	_, err = e.Candidates(Inputs{Package: pkg(10, 200000), HoldMax: 6 * time.Minute})
	if err == nil {
		t.Fatal("a hold that outlasts the provider's lifetime was a candidate")
	}
	fits := Inputs{Package: pkg(10, 200000), HoldMax: 5 * time.Minute}
	if _, err = e.Candidates(fits); err != nil {
		t.Fatalf("a hold that fits was refused: %v", err)
	}
}
