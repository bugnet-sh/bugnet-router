package config

import (
	"strings"
	"testing"
	"time"
)

var poolsDefaults = Pools{
	Enabled: true, Order: OrderCostFirst, Tick: 15 * time.Second, Workers: 4, ReserveShare: 0.5,
	CapacityShare: 0.25, Headroom: 2, RateWindow: 15 * time.Minute, ExtensionRoom: time.Hour,
	InspectEvery: 10 * time.Minute, MaxFillsPerTick: 2, FailPause: 10 * time.Minute,
	MaxDailyMicros: 2_000_000,
}

func TestPoolsDefaults(t *testing.T) {
	c, err := LoadFile(writeFile(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.Pools, poolsDefaults)
	eq(t, DefaultPools(), poolsDefaults)
	eq(t, c.Boxd.ParkedGiB, 5)
}

func TestPoolsSettings(t *testing.T) {
	c, err := LoadFile(writeFile(t, `[pools]
enabled = false
order = "warm_first"
tick = "5s"
workers = 2
reserve_share = 0.4
capacity_share = 0.1
headroom = 3
rate_window = "30m"
extension_room = "2h"
inspect_every = "5m"
max_fills_per_tick = 3
fail_pause = "20m"
max_daily_micros = 5000000

[[pools.pool]]
provider = "boxd"
size = "1x4"
min = 2
max = 5
max_age = "12h"

[[pools.pool]]
provider = "local"
size = "small"
max = 2

[boxd]
parked_gib = 8
`))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.Pools, Pools{
		Order: OrderWarmFirst, Tick: 5 * time.Second, Workers: 2, ReserveShare: 0.4,
		CapacityShare: 0.1, Headroom: 3, RateWindow: 30 * time.Minute,
		ExtensionRoom: 2 * time.Hour, InspectEvery: 5 * time.Minute, MaxFillsPerTick: 3,
		FailPause: 20 * time.Minute, MaxDailyMicros: 5_000_000,
		Pool: []Pool{
			{Provider: "boxd", Size: "1x4", Min: 2, Max: 5, MaxAge: 12 * time.Hour},
			{Provider: "local", Size: "small", Max: 2, MaxAge: 24 * time.Hour},
		},
	})
	eq(t, c.Boxd.ParkedGiB, 8)
}

func TestPoolsRefusals(t *testing.T) {
	table := `[[pools.pool]]
provider = "boxd"
size = "1x4"
max = 2
`
	for name, c := range map[string]struct{ doc, want string }{
		"order":       {"[pools]\norder = \"fast\"\n", "pools.order"},
		"tick":        {"[pools]\ntick = \"soon\"\n", "pools.tick"},
		"workers":     {"[pools]\nworkers = 0\n", "pools.workers"},
		"share":       {"[pools]\nreserve_share = 1.5\n", "pools.reserve_share"},
		"cap share":   {"[pools]\ncapacity_share = 0\n", "pools.capacity_share"},
		"boat":        {"[[pools.pool]]\nprovider = \"boat\"\nsize = \"s\"\nmax = 1\n", "boat"},
		"unknown":     {"[[pools.pool]]\nprovider = \"x\"\nsize = \"s\"\nmax = 1\n", "not a provider"},
		"no size":     {"[[pools.pool]]\nprovider = \"boxd\"\nmax = 1\n", "pools.pool[0].size"},
		"min over":    {"[[pools.pool]]\nprovider = \"boxd\"\nsize = \"s\"\nmin = 3\nmax = 2\n", "min"},
		"no max":      {"[[pools.pool]]\nprovider = \"boxd\"\nsize = \"s\"\n", "max"},
		"age":         {table + "max_age = \"1s\"\n", "max_age"},
		"twice":       {table + table, "twice"},
		"parked":      {"[boxd]\nparked_gib = 0\n", "boxd.parked_gib"},
		"secret-ish":  {"[pools]\nunknown = 1\n", "pools.unknown"},
		"daily micro": {"[pools]\nmax_daily_micros = -1\n", "max_daily_micros"},
	} {
		_, err := LoadFile(writeFile(t, c.doc))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: got %v, want an error naming %q", name, err, c.want)
		}
	}
}
