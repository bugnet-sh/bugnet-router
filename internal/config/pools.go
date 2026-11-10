package config

import (
	"cmp"
	"fmt"
	"time"
)

// Pool orders, which candidate a start looks at first
const (
	// OrderCostFirst keeps the cheapest candidate first, the founder's standing rule
	OrderCostFirst = "cost_first"
	// OrderWarmFirst puts a candidate with a warm member ahead of every cold one
	OrderWarmFirst = "warm_first"
)

// Pools holds the warm pool settings
type Pools struct {
	// Enabled turns the pools off for every provider when false
	Enabled bool
	// Order is cost_first or warm_first
	Order string
	// Tick is the gap between pool_tick runs
	Tick time.Duration
	// Workers works the pools queue
	Workers int
	// ReserveShare is the share of a pool's target that open reservations may hold
	ReserveShare float64
	// CapacityShare is the share of a provider's max_concurrent that pool members may take
	CapacityShare float64
	// Headroom is added to the demand a target is sized for
	Headroom int
	// RateWindow is how far back claims and misses count as demand
	RateWindow time.Duration
	// ExtensionRoom is the lifetime a claimed member must still have past its package
	ExtensionRoom time.Duration
	// InspectEvery is the gap between inspections of one warm member
	InspectEvery time.Duration
	// MaxFillsPerTick is the most fills one tick starts for one pool
	MaxFillsPerTick int
	// FailPause is how long three failed fills in a row pause a pool
	FailPause time.Duration
	// MaxDailyMicros is the waste and idle cost of a day that drops every target to its min
	MaxDailyMicros int64
	// Pool lists the pools, one per provider and size
	Pool []Pool
}

// Pool is one [[pools.pool]] table
type Pool struct {
	Provider string
	Size     string
	Min, Max int
	// MaxAge is how old a warm member gets before it is retired and replaced
	MaxAge time.Duration
}

type poolsFile struct {
	Enabled         bool       `toml:"enabled"`
	Order           string     `toml:"order"`
	Tick            string     `toml:"tick"`
	Workers         int64      `toml:"workers"`
	ReserveShare    float64    `toml:"reserve_share"`
	CapacityShare   float64    `toml:"capacity_share"`
	Headroom        int64      `toml:"headroom"`
	RateWindow      string     `toml:"rate_window"`
	ExtensionRoom   string     `toml:"extension_room"`
	InspectEvery    string     `toml:"inspect_every"`
	MaxFillsPerTick int64      `toml:"max_fills_per_tick"`
	FailPause       string     `toml:"fail_pause"`
	MaxDailyMicros  int64      `toml:"max_daily_micros"`
	Pool            []poolFile `toml:"pool"`
}

type poolFile struct {
	Provider string `toml:"provider"`
	Size     string `toml:"size"`
	Min      int64  `toml:"min"`
	Max      int64  `toml:"max"`
	MaxAge   string `toml:"max_age"`
}

func defaultPools() poolsFile {
	return poolsFile{
		Enabled: true, Order: OrderCostFirst, Tick: "15s", Workers: 4, ReserveShare: 0.5,
		CapacityShare: 0.25, Headroom: 2, RateWindow: "15m", ExtensionRoom: "1h",
		InspectEvery: "10m", MaxFillsPerTick: 2, FailPause: "10m", MaxDailyMicros: 2_000_000,
	}
}

// DefaultPools is the pools table of an empty settings file, which lists no pool
func DefaultPools() Pools {
	p, _ := defaultPools().read(defaultPools())
	return p
}

// defaultPoolAge is a router-kept member's longest life when a pool table gives none
const defaultPoolAge = "24h"

// read checks the pools table against its bounds, d holding the defaults
func (f poolsFile) read(d poolsFile) (Pools, error) {
	p := Pools{Enabled: f.Enabled}
	if f.Order = cmp.Or(f.Order, d.Order); f.Order != OrderCostFirst &&
		f.Order != OrderWarmFirst {
		return Pools{}, fmt.Errorf("pools.order must be %s or %s", OrderCostFirst, OrderWarmFirst)
	}
	p.Order = f.Order
	for _, d := range []struct {
		key, raw, def string
		min, max      time.Duration
		into          *time.Duration
	}{
		{"tick", f.Tick, d.Tick, time.Second, time.Hour, &p.Tick},
		{"rate_window", f.RateWindow, d.RateWindow, time.Minute, 24 * time.Hour, &p.RateWindow},
		{"extension_room", f.ExtensionRoom, d.ExtensionRoom, 0, 24 * time.Hour,
			&p.ExtensionRoom},
		{"inspect_every", f.InspectEvery, d.InspectEvery, time.Second, 24 * time.Hour,
			&p.InspectEvery},
		{"fail_pause", f.FailPause, d.FailPause, time.Second, 24 * time.Hour, &p.FailPause},
	} {
		v, err := time.ParseDuration(cmp.Or(d.raw, d.def))
		if err != nil || v < d.min || v > d.max {
			return Pools{}, fmt.Errorf("pools.%s must be a Go duration from %s to %s such as %s",
				d.key, d.min, d.max, d.def)
		}
		*d.into = v
	}
	for _, n := range []struct {
		key      string
		v        int64
		min, max int64
		into     *int
	}{
		{"workers", f.Workers, 1, 64, &p.Workers},
		{"headroom", f.Headroom, 0, 1000, &p.Headroom},
		{"max_fills_per_tick", f.MaxFillsPerTick, 1, 100, &p.MaxFillsPerTick},
	} {
		if n.v < n.min || n.v > n.max {
			return Pools{}, fmt.Errorf("pools.%s must be %d to %d", n.key, n.min, n.max)
		}
		*n.into = int(n.v)
	}
	for _, s := range []struct {
		key  string
		v    float64
		into *float64
	}{
		{"reserve_share", f.ReserveShare, &p.ReserveShare},
		{"capacity_share", f.CapacityShare, &p.CapacityShare},
	} {
		if s.v <= 0 || s.v > 1 {
			return Pools{}, fmt.Errorf("pools.%s must be above 0 and at most 1", s.key)
		}
		*s.into = s.v
	}
	if f.MaxDailyMicros < 0 || f.MaxDailyMicros > 1_000_000_000_000 {
		return Pools{}, fmt.Errorf("pools.max_daily_micros must be 0 to 1000000000000")
	}
	p.MaxDailyMicros = f.MaxDailyMicros
	seen := map[[2]string]bool{}
	for i, t := range f.Pool {
		pool, err := t.read(i)
		if err != nil {
			return Pools{}, err
		}
		key := [2]string{pool.Provider, pool.Size}
		if seen[key] {
			return Pools{}, fmt.Errorf("pools.pool lists %s %s twice", pool.Provider, pool.Size)
		}
		seen[key] = true
		p.Pool = append(p.Pool, pool)
	}
	return p, nil
}

func (t poolFile) read(i int) (Pool, error) {
	where := fmt.Sprintf("pools.pool[%d]", i)
	switch {
	case !knownProviders[t.Provider]:
		return Pool{}, fmt.Errorf("%s.provider %q is not a provider, use one of %s", where,
			t.Provider, knownList())
	case t.Provider == "boat":
		// Every Boat create counts as a start against its windows, so it keeps no pool
		return Pool{}, fmt.Errorf("%s.provider boat cannot keep a pool", where)
	case t.Size == "":
		return Pool{}, fmt.Errorf("%s.size must name a size the provider sells", where)
	case t.Min < 0 || t.Max < 1 || t.Max > 1000 || t.Min > t.Max:
		return Pool{}, fmt.Errorf("%s needs 0 <= min <= max and 1 <= max <= 1000", where)
	}
	age, err := time.ParseDuration(cmp.Or(t.MaxAge, defaultPoolAge))
	if err != nil || age < time.Minute || age > 30*24*time.Hour {
		return Pool{}, fmt.Errorf(
			"%s.max_age must be a Go duration from 1m to 720h such as 24h", where)
	}
	return Pool{Provider: t.Provider, Size: t.Size, Min: int(t.Min), Max: int(t.Max), MaxAge: age}, nil
}
