package hf

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/gpu"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// designRow is one row of design 4.5's table, its two dollar columns as the design prints them
type designRow struct {
	name                                   string
	model                                  string
	gpus, gpuGiB, vcpu, memoryMiB, diskGiB int
	hourly                                 int64
	tenMinutes, withStart                  string
}

// design45 is design 4.5's table, cheapest first
var design45 = []designRow{
	{"t4-small", gpu.T4, 1, 16, 4, 14_305, 46, 410_020, "0.068", "0.089"},
	{"t4-medium", gpu.T4, 1, 16, 8, 28_610, 93, 610_000, "0.102", "0.132"},
	{"l4x1", gpu.L4, 1, 24, 8, 28_610, 372, 809_980, "0.135", "0.175"},
	{"a10g-small", gpu.A10G, 1, 24, 4, 14_305, 102, 1_010_020, "0.168", "0.219"},
	{"a10g-large", gpu.A10G, 1, 24, 12, 43_869, 186, 1_510_000, "0.252", "0.327"},
	{"l40sx1", gpu.L40S, 1, 48, 8, 59_127, 353, 1_810_000, "0.302", "0.392"},
	{"a100-large", gpu.A100, 1, 80, 12, 135_421, 931, 2_510_020, "0.418", "0.544"},
	{"rtx-pro-6000", gpu.RTXPro6000, 1, 96, 23, 244_140, 442, 2_759_980, "0.460", "0.598"},
	{"a10g-largex2", gpu.A10G, 2, 48, 24, 87_738, 931, 3_010_000, "0.502", "0.652"},
	{"l4x4", gpu.L4, 4, 96, 48, 177_383, 2_980, 3_809_980, "0.635", "0.825"},
	{"h200", gpu.H200, 1, 141, 23, 244_140, 2_793, 5_009_980, "0.835", "1.085"},
	{"a10g-largex4", gpu.A10G, 4, 96, 48, 175_476, 1_862, 5_010_040, "0.835", "1.086"},
	{"rtx-pro-6000x2", gpu.RTXPro6000, 2, 192, 46, 488_281, 884, 5_510_020, "0.918", "1.194"},
	{"l40sx4", gpu.L40S, 4, 192, 48, 364_303, 2_980, 8_309_980, "1.385", "1.800"},
	// Equal prices keep the order of HF's list
	{"a100x4", gpu.A100, 4, 320, 48, 541_687, 3_725, 10_010_020, "1.668", "2.169"},
	{"h200x2", gpu.H200, 2, 282, 46, 488_281, 5_587, 10_010_020, "1.668", "2.169"},
	{"rtx-pro-6000x4", gpu.RTXPro6000, 4, 384, 92, 976_562, 1_769, 11_009_980, "1.835", "2.385"},
	{"a100x8", gpu.A100, 8, 640, 96, 1_083_374, 7_450, 20_009_980, "3.335", "4.335"},
	{"h200x4", gpu.H200, 4, 564, 92, 976_562, 11_175, 20_009_980, "3.335", "4.335"},
	{"rtx-pro-6000x8", gpu.RTXPro6000, 8, 768, 184, 1_953_125, 3_539, 22_010_020, "3.668",
		"4.769"},
	{"l40sx8", gpu.L40S, 8, 384, 192, 1_462_936, 6_053, 23_509_960, "3.918", "5.094"},
	{"h200x8", gpu.H200, 8, 1_128, 184, 1_953_125, 22_351, 40_010_020, "6.668", "8.669"},
}

func (r designRow) size() provider.Size {
	return provider.Size{Name: r.name, Resources: provider.Resources{
		VCPU: r.vcpu, MemoryMiB: r.memoryMiB, DiskGiB: r.diskGiB, GPUCount: r.gpus,
		GPUMemoryGiB: r.gpuGiB, GPUModel: r.model,
	}}
}

func TestHFCompiledTableIsTheDesigns(t *testing.T) {
	p, err := newProvider(testOptions())
	if err != nil {
		t.Fatal(err)
	}
	var want []provider.Size
	for _, r := range design45 {
		want = append(want, r.size())
	}
	eq(t, p.Sizes(), want)
	dollars := func(micros int64) string { return fmt.Sprintf("%.3f", float64(micros)/1e6) }
	for _, r := range design45 {
		sz := r.size()
		hourly := p.HourlyCostMicros(sz)
		ten := placement.CostMicros(hourly, 10*time.Minute, p.BillingIncrement())
		start, ok := p.StartCostMicros(sz)
		if hourly != r.hourly || !ok || dollars(ten) != r.tenMinutes ||
			dollars(ten+start) != r.withStart {
			t.Errorf("%s costs %d an hour, $%s for 10 minutes and $%s with the start, want "+
				"%d, $%s and $%s", r.name, hourly, dollars(ten), dollars(ten+start), r.hourly,
				r.tenMinutes, r.withStart)
		}
	}
	var _ provider.Sizer = p
	var _ provider.StartCoster = p
}

func TestHFCompiledFlavorsAreTheGPUFlavors(t *testing.T) {
	var names []string
	for name := range compiled {
		names = append(names, name)
	}
	eq(t, slices.Sorted(slices.Values(names)), slices.Sorted(slices.Values(gpu.HFFlavors())))
}

func TestHFFlavorsSoldAreConfigured(t *testing.T) {
	opts := testOptions()
	opts.Flavors = []string{"h200", "t4-small", "h200"}
	p, err := newProvider(opts)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, p.Sizes(), []provider.Size{design45[0].size(), design45[10].size()})
	if sz, ok := p.SizeFor(provider.Resources{GPUCount: 1, GPUModel: gpu.A100}); ok {
		t.Fatalf("an A100 floor got %s, which is not sold", sz.Name)
	}
	eq(t, p.HourlyCostMicros(design45[6].size()), 0)
	if _, ok := p.StartCostMicros(design45[6].size()); ok {
		t.Fatal("a flavor not sold has a start cost")
	}
	opts.Flavors[0] = "a100x8"
	eq(t, len(p.Sizes()), 2)
}

func TestHFSizeFor(t *testing.T) {
	p, err := newProvider(testOptions())
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name  string
		floor provider.Resources
		want  string
	}{
		{"zero floor", provider.Resources{}, ""},
		{"no GPU", provider.Resources{VCPU: 2, MemoryMiB: 4096, DiskGiB: 10}, ""},
		{"one T4", provider.Resources{GPUCount: 1, GPUModel: gpu.T4}, "t4-small"},
		{"a T4 with more memory", provider.Resources{
			GPUCount: 1, GPUModel: gpu.T4, MemoryMiB: 20_000,
		}, "t4-medium"},
		{"a T4 with more disk than any", provider.Resources{
			GPUCount: 1, GPUModel: gpu.T4, DiskGiB: 100,
		}, ""},
		{"80 GiB of any model", provider.Resources{GPUCount: 1, GPUMemoryGiB: 80}, "a100-large"},
		{"100 GiB of any model", provider.Resources{GPUMemoryGiB: 100}, "h200"},
		{"two H200", provider.Resources{GPUCount: 2, GPUModel: gpu.H200}, "h200x2"},
		{"an A100 with more CPUs", provider.Resources{
			GPUCount: 1, GPUModel: gpu.A100, VCPU: 16,
		}, "a100x4"},
		{"a model alone", provider.Resources{GPUModel: gpu.L4}, "l4x1"},
		{"a model HF lacks", provider.Resources{GPUCount: 1, GPUModel: gpu.H100}, ""},
		{"more GPUs than any", provider.Resources{GPUCount: 9, GPUModel: gpu.H200}, ""},
		{"exactly the largest", design45[len(design45)-1].size().Resources, "h200x8"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sz, ok := p.SizeFor(tc.floor)
			if tc.want == "" {
				if ok {
					t.Fatalf("SizeFor gave %s, want none", sz.Name)
				}
				return
			}
			i := slices.IndexFunc(design45, func(r designRow) bool { return r.name == tc.want })
			if !ok || sz != design45[i].size() {
				t.Fatalf("SizeFor = %+v, %v, want %+v", sz, ok, design45[i].size())
			}
		})
	}
}

func TestHFCosts(t *testing.T) {
	opts := testOptions()
	p, err := newProvider(opts)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, p.BillingIncrement(), time.Minute)
	small := design45[0].size()
	unknown := provider.Size{Name: "cpu-basic"}
	eq(t, p.HourlyCostMicros(unknown), 0)
	if _, ok := p.StartCostMicros(unknown); ok {
		t.Fatal("a size not sold has a start cost")
	}
	for _, tc := range []struct {
		billed time.Duration
		want   int64
	}{
		{0, 0},
		{time.Minute, 6_834},
		// HF bills whole minutes, so part of a minute is priced as one
		{90 * time.Second, 13_668},
		{3 * time.Minute, 20_501},
	} {
		opts.StartBilled = tc.billed
		if p, err = newProvider(opts); err != nil {
			t.Fatal(err)
		}
		got, ok := p.StartCostMicros(small)
		if !ok || got != tc.want {
			t.Errorf("start billed %s costs %d, %v, want %d", tc.billed, got, ok, tc.want)
		}
	}
}
