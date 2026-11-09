package config

import "fmt"

// Bench holds the benchmark harness settings
type Bench struct {
	// ConfirmAboveMicros is the estimate over which StartBench needs the operator's confirmation
	ConfirmAboveMicros int64
	// MaxRuns is the most runs one benchmark may ask for
	MaxRuns int
}

type benchFile struct {
	ConfirmAboveMicros int64 `toml:"confirm_above_micros"`
	MaxRuns            int64 `toml:"max_runs"`
}

func defaultBench() benchFile {
	return benchFile{ConfirmAboveMicros: 500000, MaxRuns: 200}
}

// DefaultBench is the bench table of an empty settings file
func DefaultBench() Bench {
	b, _ := defaultBench().read()
	return b
}

// read checks the bench table against its bounds
func (f benchFile) read() (Bench, error) {
	if f.ConfirmAboveMicros < 0 || f.ConfirmAboveMicros > 1_000_000_000 {
		return Bench{}, fmt.Errorf("bench.confirm_above_micros must be 0 to 1000000000")
	}
	if f.MaxRuns < 1 || f.MaxRuns > 10000 {
		return Bench{}, fmt.Errorf("bench.max_runs must be 1 to 10000")
	}
	return Bench{ConfirmAboveMicros: f.ConfirmAboveMicros, MaxRuns: int(f.MaxRuns)}, nil
}
