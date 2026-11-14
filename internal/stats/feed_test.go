package stats

import (
	"encoding/json"
	"regexp"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/pricing"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// liveCatalog has one live package that two servers land on and one no server fits
const liveCatalog = `{"packages": [
	{"id": "live-10m", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10},
	{"id": "huge-10m", "resources": {"vcpu": 64, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10}
]}`

// liveRule marks costs up 10% and adds each provider's overhead
var liveRule = pricing.Rule{
	MarkupBP: 1000,
	Overhead: map[string]time.Duration{"boat": 10 * time.Second, "e2b": 15 * time.Second},
}

// liveEngine has boat at 18,000 an hour and e2b at 133,200, each down when named in down
func liveEngine(t *testing.T, down downHealth) (*placement.Engine, *provider.Registry) {
	t.Helper()
	boat := newShaped(t, "boat", 18000, provider.Capabilities{}, size("b", 2, 4, 51))
	e2b := newShaped(t, "e2b", 133200, provider.Capabilities{}, size("e", 2, 4, 51))
	reg, err := provider.NewRegistry([]provider.Provider{boat, e2b}, provider.Features{})
	if err != nil {
		t.Fatal(err)
	}
	return placement.New(reg, down, nil), reg
}

func TestFeedPricesPerServer(t *testing.T) {
	e, reg := liveEngine(t, downHealth{})
	got := Servers(e, reg, loadPackages(t, liveCatalog), placement.Inputs{Rule: liveRule})
	if len(got) != 2 {
		t.Fatalf("servers = %d, want 2", len(got))
	}
	for i, want := range []int64{3355, 25031} {
		if p := got[i].Packages; len(p) != 1 || p[0].PriceMicros != want {
			t.Errorf("%s packages = %+v, want price %d", got[i].Provider, p, want)
		}
	}
}

func TestFeedPackagePriceIsTheQuote(t *testing.T) {
	e, _ := liveEngine(t, downHealth{})
	got := Packages(loadPackages(t, liveCatalog), Quote(e, placement.Inputs{Rule: liveRule}))
	if got[0].PriceMicros != 3355 {
		t.Errorf("price = %d, want 3355", got[0].PriceMicros)
	}
	// A package no server lands on has no price to show
	if got[1].PriceMicros != 0 {
		t.Errorf("huge price = %d, want 0", got[1].PriceMicros)
	}
}

func TestFeedPriceWhenNothingFits(t *testing.T) {
	e, _ := liveEngine(t, downHealth{"boat": true, "e2b": true})
	got := Packages(loadPackages(t, liveCatalog), Quote(e, placement.Inputs{Rule: liveRule}))
	if got[0].PriceMicros != 3355 {
		t.Errorf("price = %d, want the cheapest landing's 3355", got[0].PriceMicros)
	}
	negative := Packages(loadPackages(t, liveCatalog), func(_ catalog.Package) int64 { return -5 })
	if negative[0].PriceMicros != 0 {
		t.Errorf("negative quote = %d, want 0", negative[0].PriceMicros)
	}
}

func TestFeedCarriesNoCost(t *testing.T) {
	e, reg := liveEngine(t, downHealth{})
	pkgs := loadPackages(t, liveCatalog)
	in := placement.Inputs{Rule: liveRule}
	s := Merge(store.StatsFigures{At: t0}, PaymentsPart{}, t0, pkgs, Quote(e, in),
		Servers(e, reg, pkgs, in), nil)
	body, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	// A word starts a key or a camel hump, so generatedAt is not a rate
	words := `(Cost|Rate|Hourly|Fee|Margin|Markup|cost|rate|hourly|fee|margin|markup)`
	banned := regexp.MustCompile(`"(` + words + `|[^"]*[A-Z]` + words + `)[^"]*":`)
	if m := banned.FindString(string(body)); m != "" {
		t.Errorf("feed carries %s", m)
	}
}
