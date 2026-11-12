package lium

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/gpu"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/contract"
)

func TestLiumNewRefusesBadOptions(t *testing.T) {
	cases := map[string]struct {
		change func(*Options)
		want   string
	}{
		"no key":       {func(o *Options) { o.APIKey = "" }, "an API key is required"},
		"no seed":      {func(o *Options) { o.SSHSeed = nil }, "an SSH seed of 32 bytes"},
		"short seed":   {func(o *Options) { o.SSHSeed = o.SSHSeed[:31] }, "an SSH seed of 32"},
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
		"image by tag": {func(o *Options) { o.Image = "ghcr.io/bugnet-sh/bugnet-base:2026.11.3" },
			"not named by digest"},
		"relative volume": {func(o *Options) { o.TemplateVolume = "workspace" },
			"template volume must be an absolute path"},
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

	// 7 minutes at $0.35 an hour, billed per second and rounded up, is 350000 x 420 / 3600
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

// listed builds a provider on the fake and reads its listing once at clk's time
func listed(t *testing.T, f *fakeLium, clk *clock, nodes ...fakeNode) *Provider {
	t.Helper()
	f.setNodes(nodes...)
	p := f.provider(t, testOptions(), clk.now)
	if err := p.nodes.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLiumSizeFor(t *testing.T) {
	rtx4090 := euNode("n4090")
	rtx4090.gpuClass, rtx4090.machine, rtx4090.price = "NVIDIA GeForce RTX 4090",
		"NVIDIA GeForce RTX 4090", 0.5
	pro := euNode("npro")
	pro.gpuClass = "NVIDIA RTX PRO 6000 Blackwell Server Edition"
	pro.machine = "NVIDIA RTX PRO 6000 Blackwell Workstation Edition"
	pro.price, pro.ramGiB = 1.75, 94
	f := newFake(t)
	p := listed(t, f, newClock(), euNode("n3090"), rtx4090, pro)

	want := func(name string) provider.Size {
		s, _ := p.size(name)
		return provider.Size{Name: name, Resources: s.resources}
	}
	rows := []struct {
		name  string
		floor provider.Resources
		size  string
	}{
		{"any model takes the lowest cap", provider.Resources{GPUCount: 1}, "lium-rtx3090"},
		{"the model asked", provider.Resources{GPUCount: 1, GPUModel: gpu.RTX4090},
			"lium-rtx4090"},
		{"the model alone asks for a GPU", provider.Resources{GPUModel: gpu.RTX4090},
			"lium-rtx4090"},
		{"GPU memory past the 24 GiB cards", provider.Resources{GPUCount: 1,
			GPUMemoryGiB: 32}, "lium-rtxpro6000"},
		{"a floor every size covers", provider.Resources{
			VCPU: 6, MemoryMiB: 28672, DiskGiB: 180, GPUCount: 1, GPUMemoryGiB: 24,
		}, "lium-rtx3090"},
		{"a model with no eligible node", provider.Resources{GPUCount: 1,
			GPUModel: gpu.H200}, ""},
		{"a model not sold", provider.Resources{GPUCount: 1, GPUModel: gpu.B200}, ""},
		{"more vCPUs than a size with a node", provider.Resources{GPUCount: 1, VCPU: 9}, ""},
		{"more GPUs than any size", provider.Resources{GPUCount: 2}, ""},
		{"more disk than any size", provider.Resources{GPUCount: 1, DiskGiB: 901}, ""},
	}
	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := p.SizeFor(tc.floor)
			if tc.size == "" {
				if ok {
					t.Fatalf("SizeFor(%+v) = %+v, want none", tc.floor, got)
				}
				return
			}
			if !ok {
				t.Fatalf("SizeFor(%+v) found none, want %s", tc.floor, tc.size)
			}
			eq(t, got, want(tc.size))
		})
	}

	// With no 3090 node left the next size by cap serves
	p = listed(t, f, newClock(), rtx4090)
	got, ok := p.SizeFor(provider.Resources{GPUCount: 1})
	eq(t, ok, true)
	eq(t, got.Name, "lium-rtx4090")
}

// TestLiumSizeForRefusesFloorsWithoutGPU keeps Lium out of the worker's CPU-only canary
func TestLiumSizeForRefusesFloorsWithoutGPU(t *testing.T) {
	f := newFake(t)
	p := listed(t, f, newClock(), euNode("n3090"))
	for _, floor := range []provider.Resources{
		{}, {VCPU: 1, MemoryMiB: 512}, {VCPU: 8, MemoryMiB: 28672, DiskGiB: 200},
	} {
		if got, ok := p.SizeFor(floor); ok {
			t.Errorf("SizeFor(%+v) = %+v, want none for a floor without a GPU", floor, got)
		}
	}
	if _, ok := p.SizeFor(provider.Resources{GPUCount: 1}); !ok {
		t.Fatal("the listing holds no node, so the refusal above proves nothing")
	}
	for _, s := range p.Sizes() {
		if s.Resources.GPUCount == 0 {
			t.Errorf("size %s has no GPU, so canaries would not skip Lium", s.Name)
		}
	}
}

func TestLiumSizeForNeedsAFreshListing(t *testing.T) {
	f := newFake(t)
	f.setNodes(euNode("n3090"))
	clk := newClock()
	p := f.provider(t, testOptions(), clk.now)
	floor := provider.Resources{GPUCount: 1}
	if _, ok := p.SizeFor(floor); ok {
		t.Fatal("SizeFor answered before any listing was read")
	}
	if err := p.nodes.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := p.SizeFor(floor); !ok {
		t.Fatal("SizeFor refused with a fresh listing")
	}
	clk.add(2 * time.Minute)
	if _, ok := p.SizeFor(floor); !ok {
		t.Fatal("a listing 2 minutes old is still fresh")
	}
	clk.add(time.Second)
	if _, ok := p.SizeFor(floor); ok {
		t.Fatal("SizeFor answered from a listing over 2 minutes old")
	}
	// Sizes and prices answer from memory whatever the listing's age
	eq(t, len(p.Sizes()), 4)
	eq(t, p.HourlyCostMicros(provider.Size{Name: "lium-rtx3090"}), int64(350_000))

	// A failed refresh keeps the old listing, so its age still counts
	f.inject("GET /executors", fault{status: 502, code: "upstream_unavailable", message: "x"})
	if err := p.nodes.refresh(context.Background()); err == nil {
		t.Fatal("a refused listing refreshed")
	}
	if _, ok := p.SizeFor(floor); ok {
		t.Fatal("a failed refresh made the old listing fresh")
	}
	f.clearFaults()
	if err := p.nodes.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := p.SizeFor(floor); !ok {
		t.Fatal("a fresh listing after an outage did not restore the size")
	}
}

// liumReadyTimeout is lium.ready_timeout's default, the time the router gives a pod to boot
const liumReadyTimeout = 6 * time.Minute

func TestLiumContract(t *testing.T) {
	f := newFake(t)
	f.startStatus = "RUNNING"
	f.setNodes(euNode("node-a"))
	// The contract's files step writes under the host's /tmp, which the fake pod shares
	t.Cleanup(func() {
		for _, dir := range f.sftpMkdirs() {
			if strings.HasPrefix(dir, "/tmp/bugnet-contract-") {
				_ = os.RemoveAll(dir)
			}
		}
	})
	contract.Run(t, func(t *testing.T) provider.Provider {
		p := f.provider(t, testOptions(), time.Now)
		p.pollEvery = time.Millisecond
		relist(t, p)
		return p
	}, contract.Floor(provider.Resources{GPUCount: 1, GPUModel: gpu.RTX3090}),
		contract.ReadyTimeout(liumReadyTimeout))
}
