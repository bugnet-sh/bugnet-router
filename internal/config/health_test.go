package config

import (
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

var healthDefaults = Health{
	CanaryEvery: 5 * time.Minute, CanaryEveryDown: time.Minute, BoatCanaryEvery: 30 * time.Minute,
	PassiveWindow: 5 * time.Minute, ProbeWorkers: 4, CanaryVCPU: 1, CanaryMemoryMiB: 512,
	Window: 15 * time.Minute, MinSamples: 5, DownBelow: 0.5, SlowReadyP95: 10 * time.Second,
	DownAlertAfter: 10 * time.Minute, SamplesKeep: 720 * time.Hour,
}

func TestHealthDefaults(t *testing.T) {
	c, err := LoadFile(writeFile(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.Health, healthDefaults)
	eq(t, DefaultHealth(), healthDefaults)
}

func TestHealthSettings(t *testing.T) {
	c, err := LoadFile(writeFile(t, `[health]
canary_every = "2m"
canary_every_down = "10s"
boat_canary_every = "1h"
passive_window = "1m"
probe_workers = 2
canary_vcpu = 2
canary_memory_mib = 1024
window = "30m"
min_samples = 3
down_below = 0.6
slow_ready_p95 = "20s"
down_alert_after = "5m"
samples_keep = "24h"
`))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.Health, Health{
		CanaryEvery: 2 * time.Minute, CanaryEveryDown: 10 * time.Second,
		BoatCanaryEvery: time.Hour, PassiveWindow: time.Minute, ProbeWorkers: 2, CanaryVCPU: 2,
		CanaryMemoryMiB: 1024, Window: 30 * time.Minute, MinSamples: 3, DownBelow: 0.6,
		SlowReadyP95: 20 * time.Second, DownAlertAfter: 5 * time.Minute,
		SamplesKeep: 24 * time.Hour,
	})
}

func TestHealthRefusals(t *testing.T) {
	for _, tc := range []struct{ toml, want string }{
		{"[health]\ncanary_every = \"soon\"", "health.canary_every must be a Go duration"},
		{"[health]\nwindow = \"0s\"", "health.window must be a Go duration"},
		{"[health]\nprobe_workers = 0", "health.probe_workers must be 1 to 64"},
		{"[health]\ncanary_vcpu = 0", "health.canary_vcpu must be 1 to"},
		{"[health]\nmin_samples = 0", "health.min_samples must be 1 to"},
		{"[health]\ndown_below = 1", "health.down_below must be above 0 and under 1"},
		{"[health]\ndown_below = 0", "health.down_below must be above 0 and under 1"},
		{"[health]\nsurprise = 1", "surprise"},
	} {
		msg := problem(t, tc.toml+"\n")
		if !strings.Contains(msg, tc.want) {
			t.Errorf("%s gave %q, want %q", tc.toml, msg, tc.want)
		}
	}
}

func TestExampleTomlHoldsHealthDefaults(t *testing.T) {
	eq(t, loadExample(t).Health, healthDefaults)
	doc, err := outline([]byte(committed(t, "example.toml")))
	if err != nil {
		t.Fatal(err)
	}
	var written, keys []string
	for _, s := range doc {
		if len(s.path) == 2 && s.path[0] == "health" {
			written = append(written, s.path[1])
		}
	}
	schema := reflect.TypeFor[healthFile]()
	for i := range schema.NumField() {
		keys = append(keys, schema.Field(i).Tag.Get("toml"))
	}
	slices.Sort(written)
	slices.Sort(keys)
	eq(t, written, keys)
}
