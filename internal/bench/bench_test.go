package bench_test

import (
	"bytes"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/bench"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

func TestNearestRank(t *testing.T) {
	ten := []int{10, 9, 8, 7, 6, 5, 4, 3, 2, 1}
	eq(t, bench.Percentile(ten, 0.9), 9)
	eq(t, bench.Percentile(ten, 0.5), 5)
	eq(t, bench.Percentile(ten, 0.99), 10)
	eq(t, bench.Percentile(ten, 0), 1)
	eq(t, bench.Percentile([]int{7}, 0.99), 7)
	eq(t, bench.Percentile(nil, 0.5), 0)
	// The input keeps its order
	eq(t, ten[0], 10)
}

func newLocal(hourly int64) provider.Provider {
	return local.New(local.Options{
		Name: "local", Root: "/nonexistent", HourlyCostMicros: hourly, Increment: time.Second,
		Sizes: []provider.Size{
			{Name: "1x1", Resources: provider.Resources{VCPU: 1, MemoryMiB: 1024, DiskGiB: 10}},
			{Name: "2x2", Resources: provider.Resources{VCPU: 2, MemoryMiB: 2048, DiskGiB: 10}},
		},
	})
}

func TestEstimateRoundsToTheIncrement(t *testing.T) {
	// 30 s of 3,600,000 micros an hour is 30,000 micros a run
	prov := newLocal(3_600_000)
	size, _ := bench.SizeNamed(prov, "")
	eq(t, bench.Estimate(prov, size, 10), int64(300_000))
	eq(t, bench.Estimate(prov, size, 0), int64(0))
}

func TestSizeNamed(t *testing.T) {
	prov := newLocal(1)
	s, ok := bench.SizeNamed(prov, "")
	eq(t, ok, true)
	eq(t, s.Name, "1x1")
	s, ok = bench.SizeNamed(prov, "2x2")
	eq(t, ok, true)
	eq(t, s.Resources.VCPU, 2)
	_, ok = bench.SizeNamed(prov, "9x9")
	eq(t, ok, false)
}

func TestNewIDHasTheDocumentedShape(t *testing.T) {
	id := bench.NewID(time.Date(2026, 11, 5, 14, 12, 9, 0, time.UTC))
	if !regexp.MustCompile(`^b_20261105_1412_[a-z2-7]{4}$`).MatchString(id) {
		t.Fatalf("id %q", id)
	}
}

func sample(kind store.SampleKind, size, outcome string, ready, first int) store.StartSample {
	return store.StartSample{
		Kind: kind, Provider: "e2b", SizeName: size, Outcome: outcome, CreateMS: ready / 2,
		ReadyMS: ready, RunnerMS: store.NoStep, GateMS: 100, FirstCmdMS: first,
		WarmCmdMS: store.NoStep, DestroyMS: store.NoStep, CostMicros: 10,
	}
}

func TestBuildGroupsByProviderSizeAndKind(t *testing.T) {
	var all []store.StartSample
	for i := 1; i <= 10; i++ {
		all = append(all, sample(store.SampleBench, "2x2", store.OutcomeOK, i*100, 50))
	}
	failed := sample(store.SampleBench, "2x2", store.OutcomeFailed, 0, store.NoStep)
	failed.ErrorCode = "timeout"
	all = append(all, failed, sample(store.SampleBench, "2x2", store.OutcomeCapacity, 0, 0),
		sample(store.SampleCanary, "1x1", store.OutcomeOK, 200, 20))
	reports := bench.Build(all)
	eq(t, len(reports), 2)
	r := reports[1]
	eq(t, r.Size, "2x2")
	eq(t, r.Kind, store.SampleBench)
	eq(t, [3]int{r.OK, r.Failed, r.Refused}, [3]int{10, 1, 1})
	eq(t, r.Errors, map[string]int{"timeout": 1})
	eq(t, r.Ready, bench.Steps{Samples: 10, P50: 500, P90: 900, P99: 1000})
	// The runner did not run on any sample, so it has none
	eq(t, r.Runner.Samples, 0)
	// Total is ready, gate and first command, 600 ms at the median
	eq(t, r.Total.P50, 500+100+50)
	eq(t, r.CostMicros, int64(120))
	eq(t, reports[0].Kind, store.SampleCanary)
}

func TestFormatPrintsTheTable(t *testing.T) {
	r := bench.Build([]store.StartSample{
		sample(store.SampleBench, "2x2", store.OutcomeOK, 660, 90),
	})[0]
	var b bytes.Buffer
	eq(t, bench.Format(&b, "bench b_1 e2b 2x2", r, 25000), nil)
	out := b.String()
	for _, want := range []string{"bench b_1 e2b 2x2", "ok 1   failed 0", "p50", "ready", "0.66 s",
		"$0.000 measured", "($0.025 estimated)"} {
		if !strings.Contains(out, want) {
			t.Errorf("table lacks %q:\n%s", want, out)
		}
	}
}
