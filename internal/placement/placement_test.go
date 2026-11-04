package placement

import (
	"errors"
	"testing"
	"time"

	"github.com/oynozan/bugnet-router/internal/catalog"
	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/provider/local"
)

type fakeHealth struct {
	down map[string]bool
	p95  map[string]time.Duration
}

func (f fakeHealth) Healthy(name string) bool           { return !f.down[name] }
func (f fakeHealth) ReadyP95(name string) time.Duration { return f.p95[name] }

type capsProvider struct {
	*local.Provider
	caps provider.Capabilities
}

func (c capsProvider) Capabilities() provider.Capabilities { return c.caps }

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
		Minutes: minutes, PriceMicros: price,
	}
}

func names(cs []Candidate) []string {
	var out []string
	for _, c := range cs {
		out = append(out, c.Provider.Name())
	}
	return out
}

func eq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func engine(h Health, ps ...provider.Provider) *Engine {
	return New(provider.NewRegistry(ps), h)
}

func TestCandidatesRespectPreferenceOrder(t *testing.T) {
	a := newLocal(t, "a", 600000, time.Second)
	b := newLocal(t, "b", 300000, time.Second)
	c := newLocal(t, "c", 100000, time.Second)
	e := engine(fakeHealth{}, a, b, c)
	got, err := e.Candidates(Inputs{Package: pkg(10, 1000000)})
	if err != nil {
		t.Fatal(err)
	}
	if !eq(names(got), []string{"a", "b", "c"}) {
		t.Fatalf("order = %v", names(got))
	}
	if got[0].CostMicros != 100000 || got[0].Size.Name != "small" {
		t.Fatalf("candidate = %+v", got[0])
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
	if !eq(names(got), []string{"b", "c", "d", "a"}) {
		t.Fatalf("order = %v", names(got))
	}
}

func TestCandidatesDropBelowMarginFloor(t *testing.T) {
	cheap := newLocal(t, "cheap", 20000, time.Hour)
	dear := newLocal(t, "dear", 30000, time.Hour)
	e := engine(fakeHealth{}, cheap, dear)
	in := Inputs{Package: pkg(60, 50000), MarginFloor: 0.5}
	got, err := e.Candidates(in)
	if err != nil {
		t.Fatal(err)
	}
	if !eq(names(got), []string{"cheap"}) {
		t.Fatalf("got %v", names(got))
	}
	if m := got[0].MarginFraction; m < 0.59 || m > 0.61 {
		t.Fatalf("margin = %v", m)
	}
	in.Package = pkg(60, 50000)
	in.MarginFloor = 0.3
	if got, _ = e.Candidates(in); len(got) != 2 {
		t.Fatalf("want both, got %v", names(got))
	}
}

func TestCandidatesZeroPrice(t *testing.T) {
	free := newLocal(t, "free", 0, time.Second)
	paid := newLocal(t, "paid", 100, time.Second)
	got, err := engine(fakeHealth{}, free, paid).Candidates(Inputs{Package: pkg(10, 0)})
	if err != nil {
		t.Fatal(err)
	}
	if !eq(names(got), []string{"free"}) || got[0].MarginFraction != 0 {
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
	if err != nil || !eq(names(got), []string{"gpu"}) {
		t.Fatalf("got %v, %v", names(got), err)
	}

	gpuPkg := pkg(10, 100000)
	gpuPkg.Resources.GPUCount = 1
	got, err = e.Candidates(Inputs{Package: gpuPkg})
	if err != nil || !eq(names(got), []string{"gpu"}) {
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
	if err != nil || !eq(names(got), []string{"ssh"}) {
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

func TestCandidatesSkipUnhealthyAndExcluded(t *testing.T) {
	a := newLocal(t, "a", 1000, time.Second)
	b := newLocal(t, "b", 1000, time.Second)
	c := newLocal(t, "c", 1000, time.Second)
	h := fakeHealth{down: map[string]bool{"a": true}}
	got, err := engine(h, a, b, c).Candidates(Inputs{
		Package: pkg(10, 1000000), Exclude: map[string]bool{"b": true},
	})
	if err != nil || !eq(names(got), []string{"c"}) {
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
