package config

import (
	"cmp"
	"fmt"
	"net/netip"
	"slices"
	"time"
)

// Reserve holds the Reserve settings, CP 18.15
type Reserve struct {
	// Enabled turns Reserve off for every quote when false
	Enabled bool
	// Hold is the expires_at Reserve answers
	Hold time.Duration
	// MaxHold is how far repeated Reserve calls renew a hold from the first
	MaxHold time.Duration
	// LapseGrace is how late an order may still bind a hold that ran out
	LapseGrace time.Duration
	// Keepalive is the gap between gate checks of a ready reservation
	Keepalive time.Duration
	// PrepBudget bounds the whole preparation of one reservation
	PrepBudget time.Duration
	// AwaitMax is how long an order waits for a reservation still preparing
	AwaitMax time.Duration
	// PerQuote is the most reservations one quote ever gets
	PerQuote int
	// MaxOpen is the most reservations open at once
	MaxOpen int
	// PerSecond and Burst are the router-wide bucket of new reservations
	PerSecond, Burst int
	// PerIPOpen is the most open reservations of one client address outside SharedRanges
	PerIPOpen int
	// SharedRanges are the addresses many clients share, which never count toward PerIPOpen
	SharedRanges []netip.Prefix
	// SpeculativeProviders may start a sandbox before payment, never boat
	SpeculativeProviders []string
	// SpeculativeMaxOpen is the most open reservations that started their own sandbox
	SpeculativeMaxOpen int
	// SpeculativeMaxHourlyMicros is the dearest size started before payment, by its hour
	SpeculativeMaxHourlyMicros int64
	// BrakeUnused is how many reservations must end unused in an hour before the brake can set
	BrakeUnused int
	// BrakeRatio is how many times the bound ones the unused must reach for the brake to set
	BrakeRatio int
	// BrakeFor is how long a brake lasts
	BrakeFor time.Duration
}

// Targets holds the 5 s target's settings, CP 18.15
type Targets struct {
	// FirstCommandP90 is the payment to first command p90 that /monitor judges
	FirstCommandP90 time.Duration
	// MinPlacements is the fewest reserved and pooled placements in an hour that /monitor judges
	MinPlacements int
}

type reserveFile struct {
	Enabled                    bool     `toml:"enabled"`
	Hold                       string   `toml:"hold"`
	MaxHold                    string   `toml:"max_hold"`
	LapseGrace                 string   `toml:"lapse_grace"`
	Keepalive                  string   `toml:"keepalive"`
	PrepBudget                 string   `toml:"prep_budget"`
	AwaitMax                   string   `toml:"await_max"`
	PerQuote                   int64    `toml:"per_quote"`
	MaxOpen                    int64    `toml:"max_open"`
	PerSecond                  int64    `toml:"per_second"`
	Burst                      int64    `toml:"burst"`
	PerIPOpen                  int64    `toml:"per_ip_open"`
	SharedRanges               []string `toml:"shared_ranges"`
	SpeculativeProviders       []string `toml:"speculative_providers"`
	SpeculativeMaxOpen         int64    `toml:"speculative_max_open"`
	SpeculativeMaxHourlyMicros int64    `toml:"speculative_max_hourly_micros"`
	BrakeUnused                int64    `toml:"brake_unused"`
	BrakeRatio                 int64    `toml:"brake_ratio"`
	BrakeFor                   string   `toml:"brake_for"`
}

type targetsFile struct {
	FirstCommandP90 string `toml:"first_command_p90"`
	MinPlacements   int64  `toml:"min_placements"`
}

func defaultReserve() reserveFile {
	return reserveFile{
		Enabled: true, Hold: "120s", MaxHold: "300s", LapseGrace: "30s", Keepalive: "30s",
		PrepBudget: "45s", AwaitMax: "10s", PerQuote: 2, MaxOpen: 10, PerSecond: 1, Burst: 10,
		PerIPOpen: 2, SharedRanges: []string{"160.79.104.0/21"},
		SpeculativeProviders: []string{"e2b", "koyeb", "boxd"}, SpeculativeMaxOpen: 5,
		SpeculativeMaxHourlyMicros: 200_000, BrakeUnused: 20, BrakeRatio: 5, BrakeFor: "30m",
	}
}

func defaultTargets() targetsFile {
	return targetsFile{FirstCommandP90: "5s", MinPlacements: 20}
}

// DefaultReserve is the reserve table of an empty settings file
func DefaultReserve() Reserve {
	r, _ := defaultReserve().read(defaultReserve())
	return r
}

// DefaultTargets is the targets table of an empty settings file
func DefaultTargets() Targets {
	t, _ := defaultTargets().read(defaultTargets())
	return t
}

// read checks the reserve table against its bounds, d holding the defaults
func (f reserveFile) read(d reserveFile) (Reserve, error) {
	r := Reserve{Enabled: f.Enabled}
	for _, v := range []struct {
		key, raw, def string
		min, max      time.Duration
		into          *time.Duration
	}{
		{"hold", f.Hold, d.Hold, time.Second, time.Hour, &r.Hold},
		{"max_hold", f.MaxHold, d.MaxHold, time.Second, time.Hour, &r.MaxHold},
		{"lapse_grace", f.LapseGrace, d.LapseGrace, 0, time.Hour, &r.LapseGrace},
		{"keepalive", f.Keepalive, d.Keepalive, time.Millisecond, time.Hour, &r.Keepalive},
		{"prep_budget", f.PrepBudget, d.PrepBudget, time.Second, 150 * time.Second,
			&r.PrepBudget},
		{"await_max", f.AwaitMax, d.AwaitMax, 0, 30 * time.Second, &r.AwaitMax},
		{"brake_for", f.BrakeFor, d.BrakeFor, time.Second, 24 * time.Hour, &r.BrakeFor},
	} {
		got, err := time.ParseDuration(cmp.Or(v.raw, v.def))
		if err != nil || got < v.min || got > v.max {
			return Reserve{}, fmt.Errorf("reserve.%s must be a Go duration from %s to %s such as %s",
				v.key, v.min, v.max, v.def)
		}
		*v.into = got
	}
	if r.MaxHold < r.Hold {
		return Reserve{}, fmt.Errorf("reserve.max_hold must be at least reserve.hold")
	}
	for _, n := range []struct {
		key      string
		v        int64
		min, max int64
		into     *int
	}{
		{"per_quote", f.PerQuote, 1, 100, &r.PerQuote},
		{"max_open", f.MaxOpen, 1, 100_000, &r.MaxOpen},
		{"per_second", f.PerSecond, 1, 10_000, &r.PerSecond},
		{"burst", f.Burst, 1, 100_000, &r.Burst},
		{"per_ip_open", f.PerIPOpen, 1, 100_000, &r.PerIPOpen},
		{"speculative_max_open", f.SpeculativeMaxOpen, 0, 100_000, &r.SpeculativeMaxOpen},
		{"brake_unused", f.BrakeUnused, 1, 1_000_000, &r.BrakeUnused},
		{"brake_ratio", f.BrakeRatio, 1, 1_000_000, &r.BrakeRatio},
	} {
		if n.v < n.min || n.v > n.max {
			return Reserve{}, fmt.Errorf("reserve.%s must be %d to %d", n.key, n.min, n.max)
		}
		*n.into = int(n.v)
	}
	if f.SpeculativeMaxHourlyMicros < 0 || f.SpeculativeMaxHourlyMicros > 1_000_000_000 {
		return Reserve{}, fmt.Errorf("reserve.speculative_max_hourly_micros must be 0 to 1000000000")
	}
	r.SpeculativeMaxHourlyMicros = f.SpeculativeMaxHourlyMicros
	for _, s := range f.SharedRanges {
		p, err := netip.ParsePrefix(s)
		if err != nil {
			return Reserve{}, fmt.Errorf("reserve.shared_ranges %q is not a CIDR range", s)
		}
		r.SharedRanges = append(r.SharedRanges, p.Masked())
	}
	for _, name := range f.SpeculativeProviders {
		switch {
		case name == "boat":
			// Every Boat create counts as a start against its windows
			return Reserve{}, fmt.Errorf("reserve.speculative_providers cannot list boat")
		case !knownProviders[name]:
			return Reserve{}, fmt.Errorf(
				"reserve.speculative_providers lists %q, which is not a provider, use one of %s",
				name, knownList())
		case slices.Contains(r.SpeculativeProviders, name):
			return Reserve{}, fmt.Errorf("reserve.speculative_providers lists %s twice", name)
		}
		r.SpeculativeProviders = append(r.SpeculativeProviders, name)
	}
	return r, nil
}

// read checks the targets table against its bounds, d holding the defaults
func (f targetsFile) read(d targetsFile) (Targets, error) {
	p90, err := time.ParseDuration(cmp.Or(f.FirstCommandP90, d.FirstCommandP90))
	if err != nil || p90 < time.Millisecond || p90 > time.Hour {
		return Targets{}, fmt.Errorf(
			"targets.first_command_p90 must be a Go duration from 1ms to 1h such as 5s")
	}
	if f.MinPlacements < 1 || f.MinPlacements > 1_000_000 {
		return Targets{}, fmt.Errorf("targets.min_placements must be 1 to 1000000")
	}
	return Targets{FirstCommandP90: p90, MinPlacements: int(f.MinPlacements)}, nil
}
