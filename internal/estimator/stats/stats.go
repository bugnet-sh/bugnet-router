// Package stats checks the sampler's stats file, which a sandbox's user writes as root
//
// Nothing in the file is trusted. A strict decoder refuses unknown fields and any number that is
// not a whole number, then every value must be inside what the sandbox could have reached.
package stats

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// Path is where the sampler keeps its readings
const Path = "/tmp/bugnet/stats.json"

// DefaultMaxBytes is the largest file Check accepts when the setting is zero
const DefaultMaxBytes = 4 << 10

// maxCount bounds oom_kills and samples
const maxCount = 1_000_000

// Stats is a checked stats file, every value whole and inside the bounds
type Stats struct {
	MemPeakMiB, SwapPeakMiB, DiskPeakMiB, GPUMemPeakMiB int
	CPUSeconds                                          int64
	BusyP95Millicores, OOMKills, Samples                int
}

type file struct {
	V                 int64  `json:"v"`
	MemPeakMiB        int64  `json:"mem_peak_mib"`
	SwapPeakMiB       int64  `json:"swap_peak_mib"`
	DiskPeakMiB       int64  `json:"disk_peak_mib"`
	CPUSeconds        int64  `json:"cpu_seconds"`
	BusyP95Millicores int64  `json:"busy_p95_millicores"`
	GPUMemPeakMiB     int64  `json:"gpu_mem_peak_mib"`
	OOMKills          int64  `json:"oom_kills"`
	Samples           int64  `json:"samples"`
	UpdatedAt         string `json:"updated_at"`
}

// ErrInvalid means the file is dropped
var ErrInvalid = errors.New("stats: the file is not valid")

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrInvalid}, args...)...)
}

// satMul multiplies two non-negative numbers, stopping at the largest int64
func satMul(a, b int64) int64 {
	if a > 0 && b > math.MaxInt64/a {
		return math.MaxInt64
	}
	return a * b
}

// Check decodes raw strictly and bounds every value by the size it ran on and the time paid
func Check(raw []byte, size provider.Size, paid time.Duration) (Stats, error) {
	return CheckMax(raw, DefaultMaxBytes, size, paid)
}

// CheckMax is Check with the file size limit of the stats_max_bytes setting
func CheckMax(raw []byte, maxBytes int, size provider.Size, paid time.Duration) (Stats, error) {
	if len(raw) > maxBytes {
		return Stats{}, invalid("%d bytes", len(raw))
	}
	var f file
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return Stats{}, invalid("%v", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return Stats{}, invalid("data after the object")
	}
	if f.V != 1 {
		return Stats{}, invalid("version %d", f.V)
	}
	if len(f.UpdatedAt) > 64 {
		return Stats{}, invalid("updated_at is too long")
	}
	r := size.Resources
	memory := int64(r.MemoryMiB)
	bounds := []struct {
		name     string
		v, limit int64
	}{
		{"mem_peak_mib", f.MemPeakMiB, memory},
		{"swap_peak_mib", f.SwapPeakMiB, memory},
		{"disk_peak_mib", f.DiskPeakMiB, int64(r.DiskGiB) << 10},
		{"gpu_mem_peak_mib", f.GPUMemPeakMiB, satMul(satMul(int64(r.GPUMemoryGiB), int64(r.GPUCount)), 1<<10)},
		{"cpu_seconds", f.CPUSeconds, satMul(int64(r.VCPU), int64(paid/time.Second))},
		{"busy_p95_millicores", f.BusyP95Millicores, int64(r.VCPU) * 1000},
		{"oom_kills", f.OOMKills, maxCount},
		{"samples", f.Samples, maxCount},
	}
	for _, b := range bounds {
		if b.v < 0 || b.v > b.limit {
			return Stats{}, invalid("%s is out of range", b.name)
		}
	}
	return Stats{
		MemPeakMiB: int(f.MemPeakMiB), SwapPeakMiB: int(f.SwapPeakMiB),
		DiskPeakMiB: int(f.DiskPeakMiB), GPUMemPeakMiB: int(f.GPUMemPeakMiB),
		CPUSeconds: f.CPUSeconds, BusyP95Millicores: int(f.BusyP95Millicores),
		OOMKills: int(f.OOMKills), Samples: int(f.Samples),
	}, nil
}
