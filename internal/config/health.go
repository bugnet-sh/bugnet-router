package config

import (
	"cmp"
	"fmt"
	"time"
)

// Health holds the canary and shared health settings
type Health struct {
	// CanaryEvery is the gap between canaries of a provider, with 20 percent jitter
	CanaryEvery time.Duration
	// CanaryEveryDown is the gap while the provider is down
	CanaryEveryDown time.Duration
	// BoatCanaryEvery is Boat's gap, since every create spends one of its included starts
	BoatCanaryEvery time.Duration
	// PassiveWindow is how far back a ready placement stands in for a canary
	PassiveWindow time.Duration
	// ProbeWorkers works the probes queue
	ProbeWorkers int
	// CanaryVCPU and CanaryMemoryMiB are what the canary's size must cover
	CanaryVCPU, CanaryMemoryMiB int
	// Window is how far back the rollup counts samples
	Window time.Duration
	// MinSamples is the fewest samples that can mark a provider down or slow
	MinSamples int
	// DownBelow is the Wilson lower bound under which a provider is down
	DownBelow float64
	// SlowReadyP95 is the ready time over which a provider is slow
	SlowReadyP95 time.Duration
	// DownAlertAfter is how long a provider is down before /monitor reports it
	DownAlertAfter time.Duration
	// SamplesKeep is how long a start sample is kept
	SamplesKeep time.Duration
}

type healthFile struct {
	CanaryEvery     string  `toml:"canary_every"`
	CanaryEveryDown string  `toml:"canary_every_down"`
	BoatCanaryEvery string  `toml:"boat_canary_every"`
	PassiveWindow   string  `toml:"passive_window"`
	ProbeWorkers    int64   `toml:"probe_workers"`
	CanaryVCPU      int64   `toml:"canary_vcpu"`
	CanaryMemoryMiB int64   `toml:"canary_memory_mib"`
	Window          string  `toml:"window"`
	MinSamples      int64   `toml:"min_samples"`
	DownBelow       float64 `toml:"down_below"`
	SlowReadyP95    string  `toml:"slow_ready_p95"`
	DownAlertAfter  string  `toml:"down_alert_after"`
	SamplesKeep     string  `toml:"samples_keep"`
}

func defaultHealth() healthFile {
	return healthFile{
		CanaryEvery: "5m", CanaryEveryDown: "1m", BoatCanaryEvery: "30m", PassiveWindow: "5m",
		ProbeWorkers: 4, CanaryVCPU: 1, CanaryMemoryMiB: 512, Window: "15m", MinSamples: 5,
		DownBelow: 0.5, SlowReadyP95: "10s", DownAlertAfter: "10m", SamplesKeep: "720h",
	}
}

// DefaultHealth is the health table of an empty settings file
func DefaultHealth() Health {
	h, _ := defaultHealth().read(defaultHealth())
	return h
}

// read checks the health table against its bounds, d holding the defaults
func (f healthFile) read(d healthFile) (Health, error) {
	var h Health
	for _, p := range []struct {
		key, raw, def string
		into          *time.Duration
	}{
		{"canary_every", f.CanaryEvery, d.CanaryEvery, &h.CanaryEvery},
		{"canary_every_down", f.CanaryEveryDown, d.CanaryEveryDown, &h.CanaryEveryDown},
		{"boat_canary_every", f.BoatCanaryEvery, d.BoatCanaryEvery, &h.BoatCanaryEvery},
		{"passive_window", f.PassiveWindow, d.PassiveWindow, &h.PassiveWindow},
		{"window", f.Window, d.Window, &h.Window},
		{"slow_ready_p95", f.SlowReadyP95, d.SlowReadyP95, &h.SlowReadyP95},
		{"down_alert_after", f.DownAlertAfter, d.DownAlertAfter, &h.DownAlertAfter},
		{"samples_keep", f.SamplesKeep, d.SamplesKeep, &h.SamplesKeep},
	} {
		v, err := time.ParseDuration(cmp.Or(p.raw, p.def))
		if err != nil || v < time.Second || v > 90*24*time.Hour {
			return Health{}, fmt.Errorf(
				"health.%s must be a Go duration from 1s to 2160h such as 5m", p.key)
		}
		*p.into = v
	}
	for _, p := range []struct {
		key      string
		v        int64
		min, max int64
		into     *int
	}{
		{"probe_workers", f.ProbeWorkers, 1, 64, &h.ProbeWorkers},
		{"canary_vcpu", f.CanaryVCPU, 1, 64, &h.CanaryVCPU},
		{"canary_memory_mib", f.CanaryMemoryMiB, 1, 1 << 20, &h.CanaryMemoryMiB},
		{"min_samples", f.MinSamples, 1, 10000, &h.MinSamples},
	} {
		if p.v < p.min || p.v > p.max {
			return Health{}, fmt.Errorf("health.%s must be %d to %d", p.key, p.min, p.max)
		}
		*p.into = int(p.v)
	}
	if f.DownBelow <= 0 || f.DownBelow >= 1 {
		return Health{}, fmt.Errorf("health.down_below must be above 0 and under 1")
	}
	h.DownBelow = f.DownBelow
	return h, nil
}
