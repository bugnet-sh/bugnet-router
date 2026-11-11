package lium

import (
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/gpu"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

func TestLiumNewRefusesBadOptions(t *testing.T) {
	cases := map[string]struct {
		change func(*Options)
		want   string
	}{
		"no key":       {func(o *Options) { o.APIKey = "" }, "an API key is required"},
		"no sizes":     {func(o *Options) { o.Sizes = nil }, "at least one size"},
		"no countries": {func(o *Options) { o.Countries = nil }, "at least one country"},
		"unnamed size": {func(o *Options) { o.Sizes[0].Name = "" }, "needs a name"},
		"repeated size": {func(o *Options) { o.Sizes[1].Name = o.Sizes[0].Name },
			"repeats the size"},
		"unknown model": {func(o *Options) { o.Sizes[0].Model = "NVIDIA RTX 9090" },
			"canonical GPU model"},
		"no GPUs":    {func(o *Options) { o.Sizes[0].GPUs = 0 }, "gpus must be 1 or more"},
		"no cap":     {func(o *Options) { o.Sizes[0].MaxHourlyMicros = 0 }, "above 0"},
		"no match":   {func(o *Options) { o.Sizes[0].Match = nil }, "match must name"},
		"empty name": {func(o *Options) { o.Sizes[0].Match = []string{""} }, "match must name"},
		// A 3090 size renting 4090 nodes would sell one model as another
		"another model's name": {
			func(o *Options) { o.Sizes[2].Match = []string{"NVIDIA GeForce RTX 4090"} },
			`"NVIDIA GeForce RTX 4090" is a NVIDIA RTX 4090`,
		},
		"negative start": {func(o *Options) { o.StartBilled = -time.Second },
			"start billed must not be negative"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			opts := testOptions()
			tc.change(&opts)
			_, err := newProvider(opts, time.Now)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("newProvider = %v, want an error naming %q", err, tc.want)
			}
		})
	}
	// A name Lium may add later is taken as the configured model
	opts := testOptions()
	opts.Sizes[0].Match = append(opts.Sizes[0].Match, "NVIDIA GeForce RTX 4090 D")
	if _, err := newProvider(opts, time.Now); err != nil {
		t.Fatalf("an unknown match name: %v", err)
	}
}

func TestLiumSizesLowestCapFirst(t *testing.T) {
	p, err := newProvider(testOptions(), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, p.Sizes(), []provider.Size{
		{Name: "lium-rtx3090", Resources: provider.Resources{
			VCPU: 8, MemoryMiB: 28672, DiskGiB: 200, GPUCount: 1, GPUMemoryGiB: 24,
			GPUModel: gpu.RTX3090,
		}},
		{Name: "lium-rtx4090", Resources: provider.Resources{
			VCPU: 8, MemoryMiB: 28672, DiskGiB: 200, GPUCount: 1, GPUMemoryGiB: 24,
			GPUModel: gpu.RTX4090,
		}},
		{Name: "lium-rtxpro6000", Resources: provider.Resources{
			VCPU: 6, MemoryMiB: 57344, DiskGiB: 180, GPUCount: 1, GPUMemoryGiB: 96,
			GPUModel: gpu.RTXPro6000,
		}},
		{Name: "lium-h200", Resources: provider.Resources{
			VCPU: 16, MemoryMiB: 245760, DiskGiB: 900, GPUCount: 1, GPUMemoryGiB: 141,
			GPUModel: gpu.H200,
		}},
	})
	var _ provider.Sizer = p
	var _ provider.StartCoster = p
}

func TestLiumOptionsAreCopied(t *testing.T) {
	opts := testOptions()
	p, err := newProvider(opts, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	opts.Sizes[2].Match[0] = "changed"
	opts.Sizes[2].MaxHourlyMicros = 1
	s, ok := p.size("lium-rtx3090")
	if !ok || s.Match[0] != "NVIDIA GeForce RTX 3090" || s.MaxHourlyMicros != 350_000 {
		t.Fatalf("the provider shares its sizes with the caller: %+v", s)
	}
}

func TestLiumCapabilities(t *testing.T) {
	p, err := newProvider(testOptions(), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, p.Name(), "lium")
	eq(t, p.Capabilities(), provider.Capabilities{
		GPU: true, Arch: "amd64", NeedsRunnerUpload: true, MaxLifetime: 2 * time.Hour,
	})
	opts := testOptions()
	opts.MaxLifetime = 0
	if p, err = newProvider(opts, time.Now); err != nil {
		t.Fatal(err)
	}
	eq(t, p.Capabilities().MaxLifetime, 2*time.Hour)
}

func TestLiumCosts(t *testing.T) {
	p, err := newProvider(testOptions(), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	rtx3090 := provider.Size{Name: "lium-rtx3090"}
	// The cap, whatever node is rented below it
	eq(t, p.HourlyCostMicros(rtx3090), int64(350_000))
	eq(t, p.HourlyCostMicros(provider.Size{Name: "lium-h200"}), int64(4_900_000))
	eq(t, p.HourlyCostMicros(provider.Size{Name: "lium-b200"}), int64(0))
	eq(t, p.BillingIncrement(), time.Second)

	// 7 minutes at $0.35 an hour, billed per second and rounded up: 350000 x 420 / 3600
	cost, ok := p.StartCostMicros(rtx3090)
	eq(t, cost, int64(40_834))
	eq(t, ok, true)
	cost, ok = p.StartCostMicros(provider.Size{Name: "lium-h200"})
	eq(t, cost, int64(571_667))
	eq(t, ok, true)
	cost, ok = p.StartCostMicros(provider.Size{Name: "lium-b200"})
	eq(t, cost, int64(0))
	eq(t, ok, false)

	for start, want := range map[time.Duration]int64{
		0: 0, time.Second: 98, 1500 * time.Millisecond: 195, time.Hour: 350_000,
	} {
		opts := testOptions()
		opts.StartBilled = start
		p, err := newProvider(opts, time.Now)
		if err != nil {
			t.Fatal(err)
		}
		// A part of a second bills as a whole one
		if cost, ok := p.StartCostMicros(rtx3090); cost != want || !ok {
			t.Errorf("start billed %s costs %d, %v, want %d", start, cost, ok, want)
		}
	}
}
