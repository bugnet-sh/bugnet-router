package config

import (
	"cmp"
	"fmt"
	"time"
)

// Hedge holds the hedged start settings, CP 18.15
type Hedge struct {
	// Enabled turns hedges off for every start when false
	Enabled bool
	// DefaultAfter is when a hedge starts for a provider without min_samples whole attempts
	DefaultAfter time.Duration
	// MinAfter is the earliest a hedge ever starts
	MinAfter time.Duration
	// MinSamples is the fewest whole attempts whose p95 sets a hedge's time
	MinSamples int
	// LoserLife is the longest a loser lives, which the margin check charges
	LoserLife time.Duration
	// MinLeft is the placing budget a hedge needs left
	MinLeft time.Duration
	// MinBucketShare is how full the next candidate's create bucket must be
	MinBucketShare float64
	// PerMinute and Burst are the router-wide bucket of hedges
	PerMinute, Burst int
	// OntoBoat lets a hedge start on Boat, whose every create counts as a start
	OntoBoat bool
	// OntoGPU lets a GPU package hedge, which pays for a second GPU sandbox beside a slow one
	OntoGPU bool
}

type hedgeFile struct {
	Enabled        bool    `toml:"enabled"`
	DefaultAfter   string  `toml:"default_after"`
	MinAfter       string  `toml:"min_after"`
	MinSamples     int64   `toml:"min_samples"`
	LoserLife      string  `toml:"loser_life"`
	MinLeft        string  `toml:"min_left"`
	MinBucketShare float64 `toml:"min_bucket_share"`
	PerMinute      int64   `toml:"per_minute"`
	Burst          int64   `toml:"burst"`
	OntoBoat       bool    `toml:"onto_boat"`
	OntoGPU        bool    `toml:"onto_gpu"`
}

func defaultHedge() hedgeFile {
	return hedgeFile{
		Enabled: true, DefaultAfter: "5s", MinAfter: "1500ms", MinSamples: 5, LoserLife: "90s",
		MinLeft: "10s", MinBucketShare: 0.5, PerMinute: 30, Burst: 5,
	}
}

// DefaultHedge is the hedge table of an empty settings file
func DefaultHedge() Hedge {
	h, _ := defaultHedge().read(defaultHedge())
	return h
}

// read checks the hedge table against its bounds, d holding the defaults
func (f hedgeFile) read(d hedgeFile) (Hedge, error) {
	h := Hedge{Enabled: f.Enabled, OntoBoat: f.OntoBoat, OntoGPU: f.OntoGPU}
	for _, v := range []struct {
		key, raw, def string
		min, max      time.Duration
		into          *time.Duration
	}{
		{"default_after", f.DefaultAfter, d.DefaultAfter, time.Millisecond, time.Minute,
			&h.DefaultAfter},
		{"min_after", f.MinAfter, d.MinAfter, time.Millisecond, time.Minute, &h.MinAfter},
		{"loser_life", f.LoserLife, d.LoserLife, time.Second, 10 * time.Minute, &h.LoserLife},
		{"min_left", f.MinLeft, d.MinLeft, 0, time.Minute, &h.MinLeft},
	} {
		got, err := time.ParseDuration(cmp.Or(v.raw, v.def))
		if err != nil || got < v.min || got > v.max {
			return Hedge{}, fmt.Errorf("hedge.%s must be a Go duration from %s to %s such as %s",
				v.key, v.min, v.max, v.def)
		}
		*v.into = got
	}
	for _, n := range []struct {
		key      string
		v        int64
		min, max int64
		into     *int
	}{
		{"min_samples", f.MinSamples, 1, 1_000_000, &h.MinSamples},
		{"per_minute", f.PerMinute, 1, 100_000, &h.PerMinute},
		{"burst", f.Burst, 1, 100_000, &h.Burst},
	} {
		if n.v < n.min || n.v > n.max {
			return Hedge{}, fmt.Errorf("hedge.%s must be %d to %d", n.key, n.min, n.max)
		}
		*n.into = int(n.v)
	}
	if !(f.MinBucketShare >= 0 && f.MinBucketShare <= 1) {
		return Hedge{}, fmt.Errorf("hedge.min_bucket_share must be 0 to 1")
	}
	h.MinBucketShare = f.MinBucketShare
	return h, nil
}
