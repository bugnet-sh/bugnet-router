package stats

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/pricing"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
)

// shaped is a local provider with fixed capabilities
type shaped struct {
	*local.Provider
	caps provider.Capabilities
}

func (s shaped) Capabilities() provider.Capabilities { return s.caps }

// unsized hides the sizes of the provider it holds
type unsized struct{ provider.Provider }

// unlisted places packages over 4 vCPUs on a size its Sizes leaves out
type unlisted struct{ shaped }

func (u unlisted) SizeFor(r provider.Resources) (provider.Size, bool) {
	if r.VCPU > 4 {
		return size("hidden", 16, 64, 200), true
	}
	return u.shaped.SizeFor(r)
}

// downHealth reports the providers it names as down
type downHealth map[string]bool

func (d downHealth) Healthy(name string) bool    { return !d[name] }
func (downHealth) ReadyP95(string) time.Duration { return 0 }

// doubled holds each fixed price to twice its cost
var doubled = placement.Inputs{Rule: pricing.Rule{MarkupBP: 10000}}

func size(name string, vcpu, memGiB, diskGiB int) provider.Size {
	return provider.Size{Name: name, Resources: provider.Resources{
		VCPU: vcpu, MemoryMiB: memGiB << 10, DiskGiB: diskGiB,
	}}
}

func newShaped(
	t *testing.T, name string, hourly int64, caps provider.Capabilities, sizes ...provider.Size,
) shaped {
	t.Helper()
	return shaped{local.New(local.Options{
		Name: name, Root: t.TempDir(), Sizes: sizes, HourlyCostMicros: hourly,
		Increment: time.Second,
	}), caps}
}

// serverCatalog lists a large package first, since the catalog orders by price
const serverCatalog = `{"packages": [
	{"id": "small-10m", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10, "price_micros": 50000},
	{"id": "small-10m-more", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10, "price_micros": 50000, "extension_of": "small-10m"},
	{"id": "medium-10m", "resources": {"vcpu": 2, "memory_mib": 4096, "disk_gib": 40},
		"minutes": 10, "price_micros": 80000},
	{"id": "xl-10m", "resources": {"vcpu": 8, "memory_mib": 16384, "disk_gib": 100},
		"minutes": 10, "price_micros": 40000},
	{"id": "gpu-30m", "resources": {"vcpu": 8, "memory_mib": 32768, "disk_gib": 100,
		"gpu_count": 1, "gpu_memory_gib": 24, "gpu_model": "NVIDIA L4"},
		"minutes": 30, "price_micros": 1500000}
]}`

func serverPackages(t *testing.T) []catalog.Package {
	t.Helper()
	return loadPackages(t, serverCatalog)
}

// loadPackages is the catalog body's packages in catalog order
func loadPackages(t *testing.T, body string) []catalog.Package {
	t.Helper()
	path := filepath.Join(t.TempDir(), "packages.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := catalog.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	return c.All()
}

func TestServersListEachSizeAPackageLandsOn(t *testing.T) {
	gpu := size("g1", 8, 32, 100)
	gpu.Resources.GPUCount, gpu.Resources.GPUMemoryGiB = 1, 24
	// a sells three sizes at 18,000 an hour and keeps a sandbox 30 days
	a := newShaped(t, "a", 18000, provider.Capabilities{
		MaxLifetime: 30 * 24 * time.Hour, ExecUser: "user",
	}, size("s2x4", 2, 4, 51), size("s4x8", 4, 8, 51), size("s8x16", 8, 16, 111))
	// b runs as root for at most an hour and is down
	b := newShaped(t, "b", 1000, provider.Capabilities{MaxLifetime: time.Hour},
		size("2x2", 2, 2, 10), size("4x8", 4, 8, 10))
	// c costs 40,000 for 10 minutes, so doubled leaves it only packages at 80,000 or more
	c := newShaped(t, "c", 240000, provider.Capabilities{GPU: true, ExecUser: "boxd"},
		size("1x4", 1, 4, 100), size("2x8", 2, 8, 100), gpu)
	// d lists no sizes, so its sizes follow the order packages land on them, smallest package first
	d := unsized{newShaped(t, "d", 1000, provider.Capabilities{
		MaxLifetime: 2 * time.Hour, ExecUser: "dev",
	}, size("small", 2, 2, 10), size("big", 8, 16, 100))}
	reg, err := provider.NewRegistry([]provider.Provider{a, b, c, d}, provider.Features{})
	if err != nil {
		t.Fatal(err)
	}
	e := placement.New(reg, downHealth{"b": true}, nil)
	got := Servers(e, reg, serverPackages(t), doubled)
	small := ServerPackage{ID: "small-10m", Minutes: 10, PriceMicros: 50000}
	medium := ServerPackage{ID: "medium-10m", Minutes: 10, PriceMicros: 80000}
	xl := ServerPackage{ID: "xl-10m", Minutes: 10, PriceMicros: 40000}
	gpuPkg := ServerPackage{ID: "gpu-30m", Minutes: 30, PriceMicros: 1500000}
	month, hour, twoHours := ptr(int64(43190)), ptr(int64(50)), ptr(int64(110))
	eq(t, got, []Server{
		{Provider: "a", Size: "s2x4", VCPU: 2, MemoryMiB: 4096, DiskGiB: 51,
			LongestRunMinutes: month, User: "user", Available: true,
			Packages: []ServerPackage{small, medium}},
		{Provider: "a", Size: "s8x16", VCPU: 8, MemoryMiB: 16384, DiskGiB: 111,
			LongestRunMinutes: month, User: "user", Available: true,
			Packages: []ServerPackage{xl}},
		{Provider: "b", Size: "2x2", VCPU: 2, MemoryMiB: 2048, DiskGiB: 10,
			LongestRunMinutes: hour, User: "root", Packages: []ServerPackage{small}},
		{Provider: "c", Size: "2x8", VCPU: 2, MemoryMiB: 8192, DiskGiB: 100, User: "boxd",
			Available: true, Packages: []ServerPackage{medium}},
		{Provider: "c", Size: "g1", VCPU: 8, MemoryMiB: 32768, DiskGiB: 100,
			GPU: &GPU{Count: 1, MemoryGiB: 24, Model: "NVIDIA L4"}, User: "boxd",
			Available: true, Packages: []ServerPackage{gpuPkg}},
		{Provider: "d", Size: "small", VCPU: 2, MemoryMiB: 2048, DiskGiB: 10,
			LongestRunMinutes: twoHours, User: "dev", Available: true,
			Packages: []ServerPackage{small}},
		{Provider: "d", Size: "big", VCPU: 8, MemoryMiB: 16384, DiskGiB: 100,
			LongestRunMinutes: twoHours, User: "dev", Available: true,
			Packages: []ServerPackage{medium, xl}},
	})

	// The servers carry no cost, margin or rate of any provider
	body, err := Encode(got)
	if err != nil {
		t.Fatal(err)
	}
	for _, word := range []string{"cost", "margin", "hourly", "fee", "18000", "240000"} {
		if strings.Contains(strings.ToLower(string(body)), word) {
			t.Errorf("the servers carry %q: %s", word, body)
		}
	}
}

func TestServersLongestRunIsTheLargestOverTheSizesPackages(t *testing.T) {
	// a keeps a sandbox 30 days, which 10-minute runs fill to 43,190 and hourly ones to 43,140
	a := newShaped(t, "a", 1000, provider.Capabilities{MaxLifetime: 30 * 24 * time.Hour},
		size("s2x4", 2, 4, 51))
	reg, err := provider.NewRegistry([]provider.Provider{a}, provider.Features{})
	if err != nil {
		t.Fatal(err)
	}
	// The hourly package costs more, so it comes after the 10-minute one in catalog order
	pkgs := loadPackages(t, `{"packages": [
		{"id": "small-60m", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
			"minutes": 60, "price_micros": 300000},
		{"id": "small-10m", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
			"minutes": 10, "price_micros": 50000}
	]}`)
	e := placement.New(reg, downHealth{}, nil)
	got := Servers(e, reg, pkgs, doubled)
	runs := []int64{}
	for _, s := range got {
		if s.LongestRunMinutes != nil {
			runs = append(runs, *s.LongestRunMinutes)
		}
	}
	eq(t, runs, []int64{43190})
	eq(t, got, []Server{{
		Provider: "a", Size: "s2x4", VCPU: 2, MemoryMiB: 4096, DiskGiB: 51,
		LongestRunMinutes: ptr(int64(43190)), User: "root", Available: true,
		Packages: []ServerPackage{
			{ID: "small-10m", Minutes: 10, PriceMicros: 50000},
			{ID: "small-60m", Minutes: 60, PriceMicros: 300000},
		},
	}})
}

func TestServersListASizeTheProviderDoesNotListLast(t *testing.T) {
	a := unlisted{newShaped(t, "a", 1000, provider.Capabilities{}, size("s2x4", 2, 4, 51))}
	reg, err := provider.NewRegistry([]provider.Provider{a}, provider.Features{})
	if err != nil {
		t.Fatal(err)
	}
	// xl-10m comes first in catalog order and lands on the hidden size
	e := placement.New(reg, downHealth{}, nil)
	got := Servers(e, reg, serverPackages(t), doubled)
	names := []string{}
	for _, s := range got {
		names = append(names, s.Size)
	}
	eq(t, names, []string{"s2x4", "hidden"})
	eq(t, got[1], Server{
		Provider: "a", Size: "hidden", VCPU: 16, MemoryMiB: 65536, DiskGiB: 200, User: "root",
		Available: true, Packages: []ServerPackage{{ID: "xl-10m", Minutes: 10, PriceMicros: 40000}},
	})
}

func TestServersEmptyWhenNothingLands(t *testing.T) {
	big := newShaped(t, "big", 1000, provider.Capabilities{}, size("1x1", 1, 1, 1))
	reg, err := provider.NewRegistry([]provider.Provider{big}, provider.Features{})
	if err != nil {
		t.Fatal(err)
	}
	e := placement.New(reg, downHealth{}, nil)
	eq(t, Servers(e, reg, serverPackages(t), placement.Inputs{}), []Server{})
}

func TestLongestRunUsesTheExtensionSlack(t *testing.T) {
	eq(t, placement.LifetimeSlack, saga.LifetimeSlack)
}
