package lium

import (
	"log/slog"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/gpu"
)

// eq fails the test when got and want differ
func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

// euCountries are the 27 EU member states, lium.countries by default
var euCountries = []string{
	"AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR", "HU", "IE", "IT", "LV",
	"LT", "LU", "MT", "NL", "PL", "PT", "RO", "SK", "SI", "ES", "SE",
}

// sanctioned are lium.blocked_countries by default
var sanctioned = []string{"BY", "CU", "IR", "KP", "RU", "SY"}

// Starting sizes of design 3.5, listed out of cap order on purpose
var (
	sizeRTX4090 = SizeSpec{
		Name: "lium-rtx4090", Model: gpu.RTX4090, Match: []string{"NVIDIA GeForce RTX 4090"},
		GPUs: 1, GPUMemoryGiB: 24, VCPU: 8, MemoryMiB: 28672, DiskGiB: 200,
		MaxHourlyMicros: 700_000,
	}
	sizeRTX3090 = SizeSpec{
		Name: "lium-rtx3090", Model: gpu.RTX3090, Match: []string{"NVIDIA GeForce RTX 3090"},
		GPUs: 1, GPUMemoryGiB: 24, VCPU: 8, MemoryMiB: 28672, DiskGiB: 200,
		MaxHourlyMicros: 350_000,
	}
	sizeH200 = SizeSpec{
		Name: "lium-h200", Model: gpu.H200, Match: []string{"NVIDIA H200", "NVIDIA H200 NVL"},
		GPUs: 1, GPUMemoryGiB: 141, VCPU: 16, MemoryMiB: 245760, DiskGiB: 900,
		MaxHourlyMicros: 4_900_000,
	}
	sizeRTXPro6000 = SizeSpec{
		Name: "lium-rtxpro6000", Model: gpu.RTXPro6000,
		Match: []string{"NVIDIA RTX PRO 6000 Blackwell Server Edition",
			"NVIDIA RTX PRO 6000 Blackwell Workstation Edition",
			"NVIDIA RTX PRO 6000 Blackwell Max-Q Workstation Edition"},
		GPUs: 1, GPUMemoryGiB: 96, VCPU: 6, MemoryMiB: 57344, DiskGiB: 180,
		MaxHourlyMicros: 1_800_000,
	}
)

// testSizes clones the test sizes so a test may change its own copy
func testSizes() []SizeSpec {
	out := []SizeSpec{sizeRTX4090, sizeH200, sizeRTX3090, sizeRTXPro6000}
	for i := range out {
		out[i].Match = slices.Clone(out[i].Match)
	}
	return out
}

// testOptions are design 3.12's settings with the test sizes, BaseURL left for the caller
func testOptions() Options {
	return Options{
		APIKey:           fakeKey,
		Log:              slog.New(slog.DiscardHandler),
		Sizes:            testSizes(),
		Countries:        slices.Clone(euCountries),
		OtherCountries:   true,
		BlockedCountries: slices.Clone(sanctioned),
		MaxLifetime:      2 * time.Hour,
		StartBilled:      7 * time.Minute,
	}
}

// clock is a test clock that moves only when told to
type clock struct {
	mu sync.Mutex
	at time.Time
}

func newClock() *clock {
	return &clock{at: time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)}
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = c.at.Add(d)
}
