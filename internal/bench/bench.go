// Package bench holds the pure parts of the benchmark harness, which are nearest-rank
// percentiles, the cost estimate of a benchmark, its ids, the size lookup and the start reports.
// The waves run in the worker and the commands in bugnet-dev.
package bench

import (
	"crypto/rand"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/health"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// ExpectedRun is how long a probe is taken to keep its sandbox, for the cost estimate
const ExpectedRun = 30 * time.Second

// Percentile is the nearest-rank p-th percentile of ms, 0 for none. p is a fraction from 0 to 1.
func Percentile(ms []int, p float64) int {
	if len(ms) == 0 {
		return 0
	}
	s := slices.Clone(ms)
	slices.Sort(s)
	rank := int(math.Ceil(p * float64(len(s))))
	return s[min(max(rank, 1), len(s))-1]
}

// Estimate is what runs probes of size on prov cost, each kept ExpectedRun at the hourly rate
// rounded up to the billing increment, plus the provider's fee for a start
func Estimate(prov provider.Provider, size provider.Size, runs int) int64 {
	return health.StartCost(prov, size, ExpectedRun) * int64(runs)
}

// NewID makes b_<yyyymmdd>_<hhmm>_<4 random> for a benchmark started at t
func NewID(t time.Time) string {
	t = t.UTC()
	return fmt.Sprintf("b_%s_%s_%s", t.Format("20060102"), t.Format("1504"),
		strings.ToLower(rand.Text()[:4]))
}

// SizeNamed returns the size of prov called name, or its smallest size when name is empty
func SizeNamed(prov provider.Provider, name string) (provider.Size, bool) {
	if name == "" {
		return prov.SizeFor(provider.Resources{})
	}
	if s, ok := provider.As[provider.Sizer](prov); ok {
		for _, size := range s.Sizes() {
			if size.Name == name {
				return size, true
			}
		}
	}
	return provider.Size{}, false
}
