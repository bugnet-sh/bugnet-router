package config

import (
	"net/netip"
	"strings"
	"testing"
	"time"
)

var reserveDefaults = Reserve{
	Enabled: true, Hold: 120 * time.Second, MaxHold: 300 * time.Second,
	LapseGrace: 30 * time.Second, Keepalive: 30 * time.Second, PrepBudget: 45 * time.Second,
	AwaitMax: 10 * time.Second, PerQuote: 2, MaxOpen: 10, PerSecond: 1, Burst: 10, PerIPOpen: 2,
	SharedRanges:         []netip.Prefix{netip.MustParsePrefix("160.79.104.0/21")},
	SpeculativeProviders: []string{"e2b", "koyeb", "boxd"}, SpeculativeMaxOpen: 5,
	SpeculativeMaxHourlyMicros: 200_000, BrakeUnused: 20, BrakeRatio: 5,
	BrakeFor: 30 * time.Minute,
}

func TestReserveAndTargetsDefaults(t *testing.T) {
	c, err := LoadFile(writeFile(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.Reserve, reserveDefaults)
	eq(t, DefaultReserve(), reserveDefaults)
	eq(t, c.Targets, Targets{FirstCommandP90: 5 * time.Second, MinPlacements: 20})
	eq(t, DefaultTargets(), c.Targets)
}

func TestReserveSettings(t *testing.T) {
	c, err := LoadFile(writeFile(t, `[reserve]
enabled = false
hold = "60s"
max_hold = "90s"
lapse_grace = "5s"
keepalive = "10s"
prep_budget = "30s"
await_max = "4s"
per_quote = 3
max_open = 4
per_second = 2
burst = 5
per_ip_open = 1
shared_ranges = ["10.0.0.0/8", "2001:db8::/32"]
speculative_providers = ["e2b"]
speculative_max_open = 1
speculative_max_hourly_micros = 100000
brake_unused = 10
brake_ratio = 3
brake_for = "10m"

[targets]
first_command_p90 = "3s"
min_placements = 5
`))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.Reserve, Reserve{
		Hold: time.Minute, MaxHold: 90 * time.Second, LapseGrace: 5 * time.Second,
		Keepalive: 10 * time.Second, PrepBudget: 30 * time.Second, AwaitMax: 4 * time.Second,
		PerQuote: 3, MaxOpen: 4, PerSecond: 2, Burst: 5, PerIPOpen: 1,
		SharedRanges: []netip.Prefix{
			netip.MustParsePrefix("10.0.0.0/8"), netip.MustParsePrefix("2001:db8::/32"),
		},
		SpeculativeProviders: []string{"e2b"}, SpeculativeMaxOpen: 1,
		SpeculativeMaxHourlyMicros: 100_000, BrakeUnused: 10, BrakeRatio: 3,
		BrakeFor: 10 * time.Minute,
	})
	eq(t, c.Targets, Targets{FirstCommandP90: 3 * time.Second, MinPlacements: 5})
}

func TestReserveRefusals(t *testing.T) {
	for name, c := range map[string]struct{ doc, want string }{
		"boat":         {"[reserve]\nspeculative_providers = [\"boat\"]\n", "boat"},
		"unknown":      {"[reserve]\nspeculative_providers = [\"x\"]\n", "not a provider"},
		"twice":        {"[reserve]\nspeculative_providers = [\"e2b\", \"e2b\"]\n", "twice"},
		"hold":         {"[reserve]\nhold = \"soon\"\n", "reserve.hold"},
		"max under":    {"[reserve]\nhold = \"60s\"\nmax_hold = \"30s\"\n", "max_hold"},
		"per quote":    {"[reserve]\nper_quote = 0\n", "reserve.per_quote"},
		"range":        {"[reserve]\nshared_ranges = [\"nope\"]\n", "shared_ranges"},
		"ratio":        {"[reserve]\nbrake_ratio = 0\n", "reserve.brake_ratio"},
		"hourly":       {"[reserve]\nspeculative_max_hourly_micros = -1\n", "hourly"},
		"unknown key":  {"[reserve]\nfast = true\n", "reserve.fast"},
		"p90":          {"[targets]\nfirst_command_p90 = \"0s\"\n", "targets.first_command_p90"},
		"placements":   {"[targets]\nmin_placements = 0\n", "targets.min_placements"},
		"targets keys": {"[targets]\nfast = 1\n", "targets.fast"},
	} {
		_, err := LoadFile(writeFile(t, c.doc))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: got %v, want an error naming %q", name, err, c.want)
		}
	}
}
