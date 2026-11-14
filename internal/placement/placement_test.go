package placement

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/pricing"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/boat"
	"github.com/bugnet-sh/bugnet-router/internal/provider/boxd"
	"github.com/bugnet-sh/bugnet-router/internal/provider/e2b"
	"github.com/bugnet-sh/bugnet-router/internal/provider/koyeb"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
)

type fakeHealth struct {
	down map[string]bool
	p95  map[string]time.Duration
}

func (f fakeHealth) Healthy(name string) bool           { return !f.down[name] }
func (f fakeHealth) ReadyP95(name string) time.Duration { return f.p95[name] }

// capsProvider is a local provider with fixed capabilities and a gate route
type capsProvider struct {
	*local.Provider
	caps provider.Capabilities
}

func (c capsProvider) Capabilities() provider.Capabilities { return c.caps }

func (capsProvider) PrepareGate(context.Context, provider.Ref, int) (map[string]string, error) {
	return nil, nil
}

func (capsProvider) GateRoute(provider.Ref, int) (provider.GateRoute, error) {
	return provider.GateRoute{}, nil
}

var smallSize = provider.Size{
	Name: "small", Resources: provider.Resources{VCPU: 2, MemoryMiB: 2048, DiskGiB: 10},
}

func newLocal(
	t *testing.T, name string, cost int64, inc time.Duration, sizes ...provider.Size,
) *local.Provider {
	t.Helper()
	if len(sizes) == 0 {
		sizes = []provider.Size{smallSize}
	}
	return local.New(local.Options{
		Name: name, Root: t.TempDir(), Sizes: sizes, HourlyCostMicros: cost, Increment: inc,
	})
}

func pkg(minutes int, price int64) catalog.Package {
	return catalog.Package{
		ID: "p", Resources: provider.Resources{VCPU: 1, MemoryMiB: 1024, DiskGiB: 5},
		Minutes: minutes, PriceMicros: price, FixedPrice: true,
	}
}

func names(cs []Candidate) []string {
	var out []string
	for _, c := range cs {
		out = append(out, c.Provider.Name())
	}
	return out
}

// engine lists each provider for the data-plane features its own capabilities claim
func engine(h Health, ps ...provider.Provider) *Engine {
	var f provider.Features
	for _, p := range ps {
		c := p.Capabilities()
		for _, l := range []struct {
			on   bool
			into *[]string
		}{{c.Preview, &f.Preview}, {c.SSH, &f.SSH}, {c.Secrets, &f.Secrets}} {
			if l.on {
				*l.into = append(*l.into, p.Name())
			}
		}
	}
	reg, err := provider.NewRegistry(ps, f)
	if err != nil {
		panic(err)
	}
	return New(reg, h, nil)
}

func TestCandidatesCheapestFirst(t *testing.T) {
	a := newLocal(t, "a", 600000, time.Second)
	b := newLocal(t, "b", 300000, time.Second)
	c := newLocal(t, "c", 120000, time.Second)
	e := engine(fakeHealth{}, a, b, c)
	got, err := e.Candidates(Inputs{Package: pkg(10, 1000000)})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(names(got), []string{"c", "b", "a"}) {
		t.Fatalf("order = %v", names(got))
	}
	if got[0].CostMicros != 20000 || got[0].Size.Name != "small" {
		t.Fatalf("candidate = %+v", got[0])
	}
}

func TestCandidatesEqualCostKeepListOrder(t *testing.T) {
	dear := newLocal(t, "dear", 600000, time.Second)
	b := newLocal(t, "b", 1000, time.Second)
	a := newLocal(t, "a", 1000, time.Second)
	c := newLocal(t, "c", 1000, time.Second)
	got, err := engine(fakeHealth{}, dear, b, a, c).Candidates(Inputs{Package: pkg(10, 1000000)})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(names(got), []string{"b", "a", "c", "dear"}) {
		t.Fatalf("order = %v", names(got))
	}
}

func TestCandidatesEqualCostKeepListOrderPastTwelve(t *testing.T) {
	// Go sorts 12 or fewer elements by insertion, which is stable whatever sort is asked for
	var ps []provider.Provider
	groups := map[int64][]string{}
	for i := range 24 {
		name := fmt.Sprintf("p%02d", 23-i)
		cost := int64(1000 * (1 + i%3))
		ps = append(ps, newLocal(t, name, cost, time.Second))
		groups[cost] = append(groups[cost], name)
	}
	got, err := engine(fakeHealth{}, ps...).Candidates(Inputs{Package: pkg(10, 1000000)})
	if err != nil {
		t.Fatal(err)
	}
	want := slices.Concat(groups[1000], groups[2000], groups[3000])
	if !slices.Equal(names(got), want) {
		t.Fatalf("order = %v, want each cost in list order %v", names(got), want)
	}
}

func TestCandidatesSlowCheapAfterFastDearer(t *testing.T) {
	slowDear := newLocal(t, "slow-dear", 300000, time.Second)
	slowCheap := newLocal(t, "slow-cheap", 1000, time.Second)
	fastDear := newLocal(t, "fast-dear", 600000, time.Second)
	h := fakeHealth{p95: map[string]time.Duration{
		"slow-dear": 11 * time.Second, "slow-cheap": 11 * time.Second,
	}}
	e := engine(h, slowDear, slowCheap, fastDear)
	got, err := e.Candidates(Inputs{Package: pkg(10, 1000000)})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(names(got), []string{"fast-dear", "slow-cheap", "slow-dear"}) {
		t.Fatalf("order = %v", names(got))
	}
}

// readCounter reports every provider fast and counts the ready p95 reads of each
type readCounter struct{ reads map[string]int }

func (r readCounter) Healthy(string) bool { return true }

func (r readCounter) ReadyP95(name string) time.Duration {
	r.reads[name]++
	return 0
}

func TestCandidatesReadReadyP95OncePerCandidate(t *testing.T) {
	h := readCounter{reads: map[string]int{}}
	ps := []provider.Provider{
		newLocal(t, "a", 4000, time.Second), newLocal(t, "b", 3000, time.Second),
		newLocal(t, "c", 2000, time.Second), newLocal(t, "d", 1000, time.Second),
	}
	got, err := engine(h, ps...).Candidates(Inputs{Package: pkg(10, 1000000)})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(names(got), []string{"d", "c", "b", "a"}) {
		t.Fatalf("order = %v", names(got))
	}
	for _, n := range names(got) {
		if h.reads[n] != 1 {
			t.Fatalf("ready p95 of %s read %d times, want 1", n, h.reads[n])
		}
	}
}

func TestCandidatesOrderByCostWithStartFee(t *testing.T) {
	// 10 minutes cost 1,000 plus a 5,000 start fee against 3,000 with no fee
	fee := startFee{Provider: newLocal(t, "fee", 6000, time.Second), fee: 5000}
	plain := newLocal(t, "plain", 18000, time.Second)
	got, err := engine(fakeHealth{}, fee, plain).Candidates(Inputs{Package: pkg(10, 1000000)})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(names(got), []string{"plain", "fee"}) {
		t.Fatalf("order = %v", names(got))
	}
	if got[1].CostMicros != 6000 {
		t.Fatalf("fee costs %d, want 6000", got[1].CostMicros)
	}
}

func TestCandidatesDemoteSlowReadyStably(t *testing.T) {
	a := newLocal(t, "a", 1, time.Second)
	b := newLocal(t, "b", 1, time.Second)
	c := newLocal(t, "c", 1, time.Second)
	d := newLocal(t, "d", 1, time.Second)
	h := fakeHealth{p95: map[string]time.Duration{"a": 11 * time.Second, "c": 10 * time.Second}}
	got, err := engine(h, a, b, c, d).Candidates(Inputs{Package: pkg(10, 1000000)})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(names(got), []string{"b", "c", "d", "a"}) {
		t.Fatalf("order = %v", names(got))
	}
}

func TestCandidatesDropWhereTheFixedPriceMissesTheMarkup(t *testing.T) {
	// A 100% markup prices cheap's hour at 40,000 and dear's at 60,000
	cheap := newLocal(t, "cheap", 20000, time.Hour)
	dear := newLocal(t, "dear", 30000, time.Hour)
	e := engine(fakeHealth{}, cheap, dear)
	in := Inputs{Package: pkg(60, 50000), Rule: pricing.Rule{MarkupBP: 10000}}
	got, err := e.Candidates(in)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(names(got), []string{"cheap"}) {
		t.Fatalf("got %v", names(got))
	}
	eq(t, []int64{got[0].CostMicros, got[0].PriceMicros}, []int64{20000, 50000})
	in.Rule.MarkupBP = 5000
	if got, _ = e.Candidates(in); len(got) != 2 {
		t.Fatalf("want both, got %v", names(got))
	}
}

// startFee charges a fixed fee per start, or refuses every start when refused is set
type startFee struct {
	*local.Provider
	fee     int64
	refused bool
}

func (s startFee) StartCostMicros(provider.Size) (int64, bool) { return s.fee, !s.refused }

func TestCandidatesCountTheStartFee(t *testing.T) {
	// 10 minutes of a small machine cost 3,000, a 30,000 start fee takes them to 33,000
	fee := startFee{Provider: newLocal(t, "fee", 18000, time.Second), fee: 30000}
	free := startFee{Provider: newLocal(t, "free", 18000, time.Second)}
	refused := startFee{Provider: newLocal(t, "refused", 1, time.Second), refused: true}
	e := engine(fakeHealth{}, fee, free, refused)
	got, err := e.Candidates(Inputs{
		Package: pkg(10, 50000), Rule: pricing.Rule{MarkupBP: 10000},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(names(got), []string{"free"}) || got[0].CostMicros != 3000 {
		t.Fatalf("got %v costing %d, want free alone at 3000", names(got), got[0].CostMicros)
	}
	got, err = e.Candidates(Inputs{Package: pkg(10, 50000), Rule: pricing.Rule{MarkupBP: 5000}})
	if err != nil || !slices.Equal(names(got), []string{"free", "fee"}) ||
		got[1].CostMicros != 33000 {
		t.Fatalf("got %v, %v, want free then fee at 33000", got, err)
	}
}

func TestCandidatesLivePackageLandsAtAnyCost(t *testing.T) {
	dear := newLocal(t, "dear", 100000, time.Hour)
	got, err := engine(fakeHealth{}, dear).Candidates(Inputs{
		Package: live(60), Rule: pricing.Rule{MarkupBP: 10000},
	})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, names(got), []string{"dear"})
	eq(t, []int64{got[0].CostMicros, got[0].PriceMicros}, []int64{100000, 200000})
}

func TestCandidatesZeroPrice(t *testing.T) {
	free := newLocal(t, "free", 0, time.Second)
	paid := newLocal(t, "paid", 100, time.Second)
	got, err := engine(fakeHealth{}, free, paid).Candidates(Inputs{
		Package: pkg(10, 0), Rule: pricing.Rule{MarkupBP: 1000},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(names(got), []string{"free"}) || got[0].PriceMicros != 0 {
		t.Fatalf("got %v", names(got))
	}
}

func TestCandidatesNeedGPU(t *testing.T) {
	plain := newLocal(t, "plain", 1000, time.Second)
	e := engine(fakeHealth{}, plain)
	_, err := e.Candidates(Inputs{Package: pkg(10, 100000), Requirements: Requirements{GPU: true}})
	if !errors.Is(err, ErrNoCandidate) {
		t.Fatalf("err = %v", err)
	}

	gpuSize := provider.Size{
		Name: "gpu",
		Resources: provider.Resources{
			VCPU: 4, MemoryMiB: 8192, DiskGiB: 20, GPUCount: 1, GPUMemoryGiB: 16,
		},
	}
	gp := capsProvider{
		Provider: newLocal(t, "gpu", 1000, time.Second, gpuSize),
		caps:     provider.Capabilities{GPU: true, MaxLifetime: time.Hour},
	}
	e = engine(fakeHealth{}, plain, gp)
	got, err := e.Candidates(Inputs{
		Package: pkg(10, 100000), Requirements: Requirements{GPU: true},
	})
	if err != nil || !slices.Equal(names(got), []string{"gpu"}) {
		t.Fatalf("got %v, %v", names(got), err)
	}

	gpuPkg := pkg(10, 100000)
	gpuPkg.Resources.GPUCount = 1
	got, err = e.Candidates(Inputs{Package: gpuPkg})
	if err != nil || !slices.Equal(names(got), []string{"gpu"}) {
		t.Fatalf("got %v, %v", names(got), err)
	}
}

func TestCandidatesCapabilitiesAndLifetime(t *testing.T) {
	ssh := capsProvider{
		Provider: newLocal(t, "ssh", 1000, time.Second),
		caps:     provider.Capabilities{SSH: true, Preview: true, MaxLifetime: time.Hour},
	}
	plain := newLocal(t, "plain", 1000, time.Second)
	e := engine(fakeHealth{}, plain, ssh)
	got, err := e.Candidates(Inputs{
		Package: pkg(30, 100000), Requirements: Requirements{SSH: true, Preview: true},
	})
	if err != nil || !slices.Equal(names(got), []string{"ssh"}) {
		t.Fatalf("got %v, %v", names(got), err)
	}
	// 56 minutes plus the 5 minute margin exceeds the one hour cap
	if _, err := e.Candidates(Inputs{
		Package: pkg(56, 100000), Requirements: Requirements{SSH: true},
	}); !errors.Is(err, ErrNoCandidate) {
		t.Fatalf("err = %v", err)
	}
	if _, err := e.Candidates(Inputs{
		Package: pkg(55, 100000), Requirements: Requirements{SSH: true},
	}); err != nil {
		t.Fatal(err)
	}
	unlimited := capsProvider{Provider: newLocal(t, "u", 1000, time.Second)}
	got, err = engine(fakeHealth{}, unlimited).Candidates(Inputs{Package: pkg(5000, 100000)})
	if err != nil || len(got) != 1 {
		t.Fatalf("got %v, %v", names(got), err)
	}
}

// adapters builds every provider adapter offline, each pointed at a closed loopback port
func adapters(t *testing.T) []provider.Provider {
	t.Helper()
	const closed = "127.0.0.1:1"
	bx, err := boxd.New(boxd.Options{APIKey: "bxd_test", Endpoint: closed, Insecure: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bx.Close() })
	eb, err := e2b.New(e2b.Options{
		APIKey: "e2b_test", BaseURL: "http://" + closed, EnvdURL: "http://" + closed,
		Templates: []e2b.Template{
			{Name: "2x2", TemplateID: "bugnet-2x2", VCPU: 2, MemoryMiB: 2048, DiskGiB: 10},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ky, err := koyeb.New(koyeb.Options{APIToken: "koyeb_test", BaseURL: "http://" + closed})
	if err != nil {
		t.Fatal(err)
	}
	bt, err := boat.New(boat.Options{APIKey: "boat_test", BaseURL: "http://" + closed})
	if err != nil {
		t.Fatal(err)
	}
	return []provider.Provider{newLocal(t, "local", 0, time.Second), bx, eb, ky, bt}
}

func TestCandidatesSkipProviderWithoutSecrets(t *testing.T) {
	claims := func(name string) capsProvider {
		return capsProvider{
			Provider: newLocal(t, name, 1000, time.Second),
			caps:     provider.Capabilities{Secrets: true, MaxLifetime: time.Hour},
		}
	}
	reg, err := provider.NewRegistry([]provider.Provider{claims("unlisted"), claims("listed")},
		provider.Features{Secrets: []string{"listed"}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := New(reg, fakeHealth{}, nil).Candidates(Inputs{
		Package: pkg(10, 100000), Requirements: Requirements{Secrets: true},
	})
	eq(t, err, nil)
	eq(t, names(got), []string{"listed"})
}

func TestCandidatesReadCapabilitiesThroughTheRegistry(t *testing.T) {
	// The gated provider's adapter claims nothing, the other claims everything but is unlisted
	gated := capsProvider{
		Provider: newLocal(t, "gated", 1000, time.Second),
		caps:     provider.Capabilities{MaxLifetime: time.Hour},
	}
	claiming := capsProvider{
		Provider: newLocal(t, "claiming", 1000, time.Second),
		caps: provider.Capabilities{
			SSH: true, Preview: true, Secrets: true, MaxLifetime: time.Hour,
		},
	}
	reg, err := provider.NewRegistry([]provider.Provider{claiming, gated},
		provider.Features{Preview: []string{"gated"}, SSH: []string{"gated"}})
	if err != nil {
		t.Fatal(err)
	}
	e := New(reg, fakeHealth{}, nil)
	got, err := e.Candidates(Inputs{
		Package: pkg(10, 100000), Requirements: Requirements{SSH: true, Preview: true},
	})
	eq(t, err, nil)
	eq(t, names(got), []string{"gated"})
	_, err = e.Candidates(Inputs{
		Package: pkg(10, 100000), Requirements: Requirements{Secrets: true},
	})
	eq(t, errors.Is(err, ErrNoCapableProvider), true)
}

func TestCandidatesForSSHPreviewOrSecretsFindNone(t *testing.T) {
	e := engine(fakeHealth{}, adapters(t)...)
	in := func(r Requirements) Inputs {
		return Inputs{Package: pkg(10, 10_000_000), Requirements: r}
	}
	// Boat prices a start from its API, which this test never calls
	plain := in(Requirements{})
	plain.Exclude = map[string]bool{"boat": true}
	got, err := e.Candidates(plain)
	eq(t, err, nil)
	eq(t, len(got), 4)
	for _, r := range []Requirements{{SSH: true}, {Preview: true}, {Secrets: true}} {
		_, err = e.Candidates(in(r))
		eq(t, errors.Is(err, ErrNoCapableProvider), true)
		eq(t, errors.Is(err, ErrNoCandidate), true)
	}
}

func TestNoCapableProviderLooksOnlyAtCapabilities(t *testing.T) {
	ssh := capsProvider{
		Provider: newLocal(t, "ssh", 1000, time.Second),
		caps:     provider.Capabilities{SSH: true, MaxLifetime: time.Hour},
	}
	secrets := capsProvider{
		Provider: newLocal(t, "secrets", 1000, time.Second),
		caps:     provider.Capabilities{Secrets: true, MaxLifetime: time.Hour},
	}
	plain := newLocal(t, "plain", 1000, time.Second)
	gpuPkg := pkg(10, 100000)
	gpuPkg.Resources.GPUCount = 1
	big := pkg(10, 100000)
	big.Resources.VCPU = 64
	sshOnly := Requirements{SSH: true}
	for _, c := range []struct {
		name    string
		e       *Engine
		in      Inputs
		capable bool
	}{
		{"ssh unhealthy", engine(fakeHealth{down: map[string]bool{"ssh": true}}, plain, ssh),
			Inputs{Package: pkg(10, 100000), Requirements: sshOnly}, true},
		{"ssh excluded", engine(fakeHealth{}, plain, ssh), Inputs{
			Package: pkg(10, 100000), Requirements: sshOnly, Exclude: map[string]bool{"ssh": true},
		}, true},
		{"ssh past its lifetime", engine(fakeHealth{}, plain, ssh),
			Inputs{Package: pkg(56, 100000), Requirements: sshOnly}, true},
		{"over the fixed price", engine(fakeHealth{}, plain),
			Inputs{Package: pkg(10, 200), Rule: pricing.Rule{MarkupBP: 10000}}, true},
		{"no size", engine(fakeHealth{}, plain), Inputs{Package: big}, true},
		{"every provider unhealthy", engine(fakeHealth{down: map[string]bool{"plain": true}},
			plain), Inputs{Package: pkg(10, 100000)}, true},
		{"no ssh", engine(fakeHealth{}, plain), Inputs{
			Package: pkg(10, 100000), Requirements: sshOnly,
		}, false},
		{"no preview", engine(fakeHealth{}, plain, ssh), Inputs{
			Package: pkg(10, 100000), Requirements: Requirements{Preview: true},
		}, false},
		{"no secrets", engine(fakeHealth{}, plain, ssh), Inputs{
			Package: pkg(10, 100000), Requirements: Requirements{Secrets: true},
		}, false},
		{"secrets unhealthy", engine(fakeHealth{down: map[string]bool{"secrets": true}},
			plain, secrets), Inputs{
			Package: pkg(10, 100000), Requirements: Requirements{Secrets: true},
		}, true},
		{"no gpu for a gpu package", engine(fakeHealth{}, plain, ssh),
			Inputs{Package: gpuPkg}, false},
		{"no gpu asked for", engine(fakeHealth{}, plain, ssh),
			Inputs{Package: pkg(10, 100000), Requirements: Requirements{GPU: true}}, false},
		{"no providers and no need", engine(fakeHealth{}), Inputs{Package: pkg(10, 100000)}, true},
		{"no providers", engine(fakeHealth{}), Inputs{
			Package: pkg(10, 100000), Requirements: sshOnly,
		}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := c.e.Candidates(c.in)
			eq(t, errors.Is(err, ErrNoCandidate), true)
			eq(t, errors.Is(err, ErrNoCapableProvider), !c.capable)
		})
	}
}

func TestCandidatesSkipUnhealthyAndExcluded(t *testing.T) {
	a := newLocal(t, "a", 1000, time.Second)
	b := newLocal(t, "b", 1000, time.Second)
	c := newLocal(t, "c", 1000, time.Second)
	h := fakeHealth{down: map[string]bool{"a": true}}
	got, err := engine(h, a, b, c).Candidates(Inputs{
		Package: pkg(10, 1000000), Exclude: map[string]bool{"b": true},
	})
	if err != nil || !slices.Equal(names(got), []string{"c"}) {
		t.Fatalf("got %v, %v", names(got), err)
	}
	_, err = engine(h, a, b).Candidates(Inputs{
		Package: pkg(10, 1000000), Exclude: map[string]bool{"b": true},
	})
	if !errors.Is(err, ErrNoCandidate) {
		t.Fatalf("err = %v", err)
	}
}

func TestCandidatesSizeTooSmall(t *testing.T) {
	p := newLocal(t, "p", 1000, time.Second)
	big := pkg(10, 100000)
	big.Resources.VCPU = 64
	_, err := engine(fakeHealth{}, p).Candidates(Inputs{Package: big})
	if !errors.Is(err, ErrNoCandidate) {
		t.Fatalf("err = %v", err)
	}
}

func TestCostRoundsUpToIncrement(t *testing.T) {
	hour := newLocal(t, "hour", 120000, time.Hour)
	got, err := engine(fakeHealth{}, hour).Candidates(Inputs{Package: pkg(10, 1000000)})
	if err != nil {
		t.Fatal(err)
	}
	if got[0].CostMicros != 120000 {
		t.Fatalf("cost = %d", got[0].CostMicros)
	}
}

func TestCostPerSecondRoundsUpToMicro(t *testing.T) {
	// 1 micro per hour for 1 minute is 1/60 micro, rounded up to 1
	p := newLocal(t, "p", 1, 0)
	got, err := engine(fakeHealth{}, p).Candidates(Inputs{Package: pkg(1, 1000)})
	if err != nil {
		t.Fatal(err)
	}
	if got[0].CostMicros != 1 {
		t.Fatalf("cost = %d", got[0].CostMicros)
	}
	q := newLocal(t, "q", 360000, 0)
	got, _ = engine(fakeHealth{}, q).Candidates(Inputs{Package: pkg(10, 100000)})
	if got[0].CostMicros != 60000 {
		t.Fatalf("cost = %d", got[0].CostMicros)
	}
}

func TestCostMinutesAcrossIncrement(t *testing.T) {
	p := newLocal(t, "p", 600000, 15*time.Minute)
	got, err := engine(fakeHealth{}, p).Candidates(Inputs{Package: pkg(16, 1000000)})
	if err != nil {
		t.Fatal(err)
	}
	if got[0].CostMicros != 300000 {
		t.Fatalf("cost = %d", got[0].CostMicros)
	}
}

func TestRestrictedPackageNeverOnBoatOrKoyeb(t *testing.T) {
	e := engine(fakeHealth{}, adapters(t)...)
	open := pkg(10, 1000000)
	got, err := e.Candidates(Inputs{Package: open})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"koyeb", "boat", "local"} {
		if !slices.Contains(names(got), name) {
			t.Fatalf("an open package's candidates %v lack %s", names(got), name)
		}
	}
	restricted := open
	restricted.EgressAllow = []string{"huggingface.co"}
	got, err = e.Candidates(Inputs{Package: restricted})
	if err != nil {
		t.Fatal(err)
	}
	have := names(got)
	slices.Sort(have)
	eq(t, have, []string{"boxd", "e2b"})
}

func imageEngine(t *testing.T, exempt ...string) (*Engine, []provider.Provider) {
	t.Helper()
	ps := []provider.Provider{
		newLocal(t, "old", 1000, time.Second), newLocal(t, "fresh", 2000, time.Second),
		newLocal(t, "own", 3000, time.Second),
	}
	reg, err := provider.NewRegistry(ps, provider.Features{
		Images:      map[string]string{"old": "2026.10.1", "fresh": "2026.11.1"},
		ImageExempt: exempt,
	})
	eq(t, err, nil)
	return New(reg, fakeHealth{}, nil), ps
}

func TestCandidatesDropProviderOffAccept(t *testing.T) {
	e, _ := imageEngine(t)
	got, err := e.Candidates(Inputs{Package: pkg(10, 1000000), ImageAccept: []string{"2026.11.1"}})
	eq(t, err, nil)
	eq(t, names(got), []string{"fresh"})
	_, err = e.Candidates(Inputs{Package: pkg(10, 1000000), ImageAccept: []string{"2025.1.1"}})
	eq(t, errors.Is(err, ErrNoCandidate), true)
}

func TestExemptProviderStaysWithoutImage(t *testing.T) {
	e, _ := imageEngine(t, "own")
	got, err := e.Candidates(Inputs{Package: pkg(10, 1000000), ImageAccept: []string{"2026.11.1"}})
	eq(t, err, nil)
	eq(t, names(got), []string{"fresh", "own"})
}

func TestEmptyAcceptFiltersNothing(t *testing.T) {
	e, _ := imageEngine(t)
	got, err := e.Candidates(Inputs{Package: pkg(10, 1000000)})
	eq(t, err, nil)
	eq(t, names(got), []string{"old", "fresh", "own"})
}

// boatRate is Boat small's hourly rate, billed by the second
const boatRate = 18_000

// boatRule is a 10% markup with Boat's 10 s and E2B's 15 s of overhead
var boatRule = pricing.Rule{MarkupBP: 1000, Overhead: map[string]time.Duration{
	"boat": 10 * time.Second, "e2b": 15 * time.Second,
}}

// live is a package quoted from its cost
func live(minutes int) catalog.Package {
	p := pkg(minutes, 0)
	p.FixedPrice = false
	return p
}

func TestCandidatePriceLive(t *testing.T) {
	// 610 s at 18,000 an hour cost 3,050, which 10% takes to 3,355
	e := engine(fakeHealth{}, newLocal(t, "boat", boatRate, time.Second))
	got, err := e.Candidates(Inputs{Package: live(10), Rule: boatRule})
	eq(t, err, nil)
	eq(t, []int64{got[0].CostMicros, got[0].PriceMicros}, []int64{3050, 3355})
	// A hold bills beside the overhead, 660 s cost 3,300
	got, err = e.Candidates(Inputs{Package: live(10), Rule: boatRule, Hold: 50 * time.Second})
	eq(t, err, nil)
	eq(t, []int64{got[0].CostMicros, got[0].PriceMicros}, []int64{3300, 3630})
}

func TestCandidateFixedFloor(t *testing.T) {
	e := engine(fakeHealth{}, newLocal(t, "boat", boatRate, time.Second))
	_, err := e.Candidates(Inputs{Package: pkg(10, 3354), Rule: boatRule})
	eq(t, errors.Is(err, ErrNoCandidate), true)
	got, err := e.Candidates(Inputs{Package: pkg(10, 3355), Rule: boatRule})
	eq(t, err, nil)
	eq(t, names(got), []string{"boat"})
	eq(t, []int64{got[0].CostMicros, got[0].PriceMicros}, []int64{3050, 3355})
}

func TestBudgetFilters(t *testing.T) {
	e := engine(fakeHealth{}, newLocal(t, "boat", boatRate, time.Second))
	_, err := e.Candidates(Inputs{Package: live(10), Rule: boatRule, BudgetMicros: 3354})
	eq(t, errors.Is(err, ErrNoCandidate), true)
	got, err := e.Candidates(Inputs{Package: live(10), Rule: boatRule, BudgetMicros: 3355})
	eq(t, err, nil)
	eq(t, names(got), []string{"boat"})
}

func TestPreferExemptFromBudget(t *testing.T) {
	// e2b bills 615 s at 36,000 an hour, 6,150 priced at 6,765
	boat := newLocal(t, "boat", boatRate, time.Second)
	dear := newLocal(t, "e2b", 2*boatRate, time.Second)
	e := engine(fakeHealth{}, boat, dear)
	in := Inputs{Package: live(10), Rule: boatRule, BudgetMicros: 3355}
	got, err := e.Candidates(in)
	eq(t, err, nil)
	eq(t, names(got), []string{"boat"})
	in.Prefer = &Server{Provider: "e2b", Size: "small"}
	got, err = e.Candidates(in)
	eq(t, err, nil)
	eq(t, names(got), []string{"e2b", "boat"})
	eq(t, got[0].PriceMicros, int64(6765))
	// Without a budget the preferred server still comes ahead of a cheaper one
	in.BudgetMicros = 0
	got, err = e.Candidates(in)
	eq(t, err, nil)
	eq(t, names(got), []string{"e2b", "boat"})
	// A preferred size the provider does not land the package on is no exemption
	in.BudgetMicros, in.Prefer = 3355, &Server{Provider: "e2b", Size: "big"}
	got, err = e.Candidates(in)
	eq(t, err, nil)
	eq(t, names(got), []string{"boat"})
}

func TestQuoteFallsBackToCheapestLanding(t *testing.T) {
	dear := newLocal(t, "e2b", 2*boatRate, time.Second)
	boat := newLocal(t, "boat", boatRate, time.Second)
	twin := newLocal(t, "twin", 18_300, time.Second)
	up := engine(fakeHealth{}, dear, boat, twin)
	c, ok := up.Quote(Inputs{Package: live(10), Rule: boatRule})
	eq(t, ok, true)
	eq(t, []any{c.Provider.Name(), c.PriceMicros}, []any{"boat", int64(3355)})
	// boat and twin both cost 3,050, so the registry's order keeps boat
	down := engine(fakeHealth{down: map[string]bool{"e2b": true, "boat": true, "twin": true}},
		dear, boat, twin)
	c, ok = down.Quote(Inputs{Package: live(10), Rule: boatRule})
	eq(t, ok, false)
	eq(t, []any{c.Provider.Name(), c.PriceMicros}, []any{"boat", int64(3355)})
	ties := engine(fakeHealth{down: map[string]bool{"twin": true, "boat": true}}, twin, boat)
	c, ok = ties.Quote(Inputs{Package: live(10), Rule: boatRule})
	eq(t, ok, false)
	eq(t, c.Provider.Name(), "twin")
	big := live(10)
	big.Resources.VCPU = 64
	c, ok = up.Quote(Inputs{Package: big, Rule: boatRule})
	eq(t, ok, false)
	eq(t, c, Candidate{})
}

func TestCandidateExtensionPrice(t *testing.T) {
	// 10 minutes at 18,000 an hour cost 3,000, which 10% takes to 3,300 with no overhead
	boat := engine(fakeHealth{}, newLocal(t, "boat", boatRate, time.Second))
	c, ok := boat.Quote(Inputs{Package: live(10), Rule: boatRule})
	eq(t, ok, true)
	price, err := c.ExtensionPrice(live(10), boatRule)
	eq(t, []any{price, err}, []any{int64(3300), nil})
	// 10 minutes at 133,200 an hour cost 22,200, priced at 24,420
	dear := engine(fakeHealth{}, newLocal(t, "e2b", 133_200, time.Second))
	c, _ = dear.Quote(Inputs{Package: live(10), Rule: boatRule})
	price, err = c.ExtensionPrice(live(10), boatRule)
	eq(t, []any{price, err}, []any{int64(24_420), nil})
	// A 7-minute increment bills the 10 minutes as 14, 4,200 priced at 4,620
	slow := engine(fakeHealth{}, newLocal(t, "slow", boatRate, 7*time.Minute))
	c, _ = slow.Quote(Inputs{Package: live(10), Rule: boatRule})
	price, err = c.ExtensionPrice(live(10), boatRule)
	eq(t, []any{price, err}, []any{int64(4620), nil})
	price, err = c.ExtensionPrice(pkg(10, 45_000), boatRule)
	eq(t, []any{price, err}, []any{int64(45_000), nil})
	// A start no server lands quotes a zero Candidate, which has no server to price on
	for _, ext := range []catalog.Package{live(10), pkg(10, 45_000)} {
		price, err = Candidate{}.ExtensionPrice(ext, boatRule)
		eq(t, []any{price, errors.Is(err, ErrNoServer)}, []any{int64(0), true})
	}
}

func TestEngineExtensionPrice(t *testing.T) {
	// Health aside, the start's overhead aside, each server bills the extension's 10 minutes
	e := engine(fakeHealth{down: map[string]bool{"e2b": true}},
		newLocal(t, "boat", boatRate, time.Second), newLocal(t, "e2b", 133_200, time.Second))
	for _, c := range []struct {
		server string
		want   int64
	}{{"boat", 3300}, {"e2b", 24_420}} {
		price, err := e.ExtensionPrice(live(10), c.server, smallSize.Name, boatRule)
		eq(t, []any{c.server, price, err}, []any{c.server, c.want, nil})
	}
	price, err := e.ExtensionPrice(pkg(10, 45_000), "e2b", smallSize.Name, boatRule)
	eq(t, []any{price, err}, []any{int64(45_000), nil})
	// A provider or size that left the config prices nothing, fixed or live
	for _, gone := range []Server{{"boxd", smallSize.Name}, {"boat", "large"}} {
		for _, ext := range []catalog.Package{live(10), pkg(10, 45_000)} {
			price, err := e.ExtensionPrice(ext, gone.Provider, gone.Size, boatRule)
			eq(t, []any{gone, price, errors.Is(err, ErrNoServer)}, []any{gone, int64(0), true})
		}
	}
}
