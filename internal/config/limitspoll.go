package config

import (
	"cmp"
	"fmt"
	"time"
)

// LimitsPoll holds the limit polling and account check settings
type LimitsPoll struct {
	// Every is how often the limits of E2B, boxd and Koyeb are read
	Every time.Duration
	// BoatEvery is how often Boat's limits are read
	BoatEvery time.Duration
	// AccountEvery is how often a provider's account is checked
	AccountEvery time.Duration
	// AccountEveryBlocked is how often a blocked provider's account is checked
	AccountEveryBlocked time.Duration
	// ReserveSlots is how many slots placements leave free for probes
	ReserveSlots int
	// StaleAfter is the missed polls before /monitor reports old limits
	StaleAfter int
}

type limitsPollFile struct {
	Every               string `toml:"every"`
	BoatEvery           string `toml:"boat_every"`
	AccountEvery        string `toml:"account_every"`
	AccountEveryBlocked string `toml:"account_every_blocked"`
	ReserveSlots        int64  `toml:"reserve_slots"`
	StaleAfter          int64  `toml:"stale_after"`
}

func defaultLimitsPoll() limitsPollFile {
	return limitsPollFile{
		Every: "60s", BoatEvery: "15s", AccountEvery: "5m", AccountEveryBlocked: "1m",
		ReserveSlots: 1, StaleAfter: 5,
	}
}

// DefaultLimitsPoll is the limits_poll table of an empty settings file
func DefaultLimitsPoll() LimitsPoll {
	l, _ := defaultLimitsPoll().read(defaultLimitsPoll())
	return l
}

// maxProviderCount keeps a provider's cap and create rate far inside what Postgres holds
const maxProviderCount = 1_000_000

// DefaultMaxConcurrent is the slots of a provider with neither a cap nor a max_concurrent
const DefaultMaxConcurrent = 50

// read checks the limits_poll table against its bounds, d holding the defaults
func (f limitsPollFile) read(d limitsPollFile) (LimitsPoll, error) {
	var l LimitsPoll
	for _, p := range []struct {
		key, raw, def string
		into          *time.Duration
	}{
		{"every", f.Every, d.Every, &l.Every},
		{"boat_every", f.BoatEvery, d.BoatEvery, &l.BoatEvery},
		{"account_every", f.AccountEvery, d.AccountEvery, &l.AccountEvery},
		{"account_every_blocked", f.AccountEveryBlocked, d.AccountEveryBlocked,
			&l.AccountEveryBlocked},
	} {
		v, err := time.ParseDuration(cmp.Or(p.raw, p.def))
		if err != nil || v < time.Second || v > 24*time.Hour {
			return LimitsPoll{}, fmt.Errorf(
				"limits_poll.%s must be a Go duration from 1s to 24h such as 60s", p.key)
		}
		*p.into = v
	}
	if f.ReserveSlots < 0 || f.ReserveSlots > 1000 {
		return LimitsPoll{}, fmt.Errorf("limits_poll.reserve_slots must be 0 to 1000")
	}
	l.ReserveSlots = int(f.ReserveSlots)
	if f.StaleAfter < 1 || f.StaleAfter > 1000 {
		return LimitsPoll{}, fmt.Errorf("limits_poll.stale_after must be 1 to 1000")
	}
	l.StaleAfter = int(f.StaleAfter)
	return l, nil
}
