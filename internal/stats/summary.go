// Package stats builds the public stats feed that bugnet.sh/stats and the landing's pricing read.
//
// router-worker's snapshot job reads the router's figures, calls Payments' totals, and merges both
// with the catalog's start packages and the live limits into one summary, which router-api serves
// as stored. Session pages are read from Postgres on request. Every value that a later task builds
// comes from one source function in sources.go or internal/store/stats.go, which answers the form
// the website's contract keeps for an unknown value until that task lands.
package stats

import (
	"encoding/json"
	"maps"
	"slices"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// Version is the feed's shape version
const Version = 1

// dayLayout is how a UTC day goes out
const dayLayout = "2006-01-02"

// Time marshals as RFC 3339 UTC to the second
type Time time.Time

// MarshalJSON writes the time in UTC with no fraction of a second
func (t Time) MarshalJSON() ([]byte, error) {
	return []byte(`"` + time.Time(t).UTC().Format(time.RFC3339) + `"`), nil
}

// Summary is GET /api/stats's body, field for field as SS 7
type Summary struct {
	Version     int              `json:"version"`
	GeneratedAt Time             `json:"generatedAt"`
	Now         Now              `json:"now"`
	Totals      Totals           `json:"totals"`
	Daily       []Day            `json:"daily"`
	Providers   []Provider       `json:"providers"`
	Payments    Breakdown        `json:"payments"`
	Packages    []Package        `json:"packages"`
	Limits      map[string]int64 `json:"limits"`
}

// Now counts the sandboxes running when the router part was read
type Now struct {
	Active           int64            `json:"active"`
	ActiveByProvider []ProviderActive `json:"activeByProvider"`
}

// ProviderActive is one provider's running sandboxes
type ProviderActive struct {
	Provider string `json:"provider"`
	Active   int64  `json:"active"`
}

// Money is Payments' four sums, in micros of USDC
type Money struct {
	EarnedMicros            int64 `json:"earnedMicros"`
	RefundedMicros          int64 `json:"refundedMicros"`
	AllowanceReturnedMicros int64 `json:"allowanceReturnedMicros"`
	X402SpentMicros         int64 `json:"x402SpentMicros"`
}

// Totals is the all-time figures, SpentMicros nil while the provider cost meter is unknown
type Totals struct {
	Sandboxes      int64 `json:"sandboxes"`
	ComputeSeconds int64 `json:"computeSeconds"`
	Money
	SpentMicros *int64 `json:"spentMicros"`
}

// Day is one UTC day, SpentMicros nil while the provider cost meter is unknown
type Day struct {
	Day string `json:"day"`
	Money
	SpentMicros    *int64 `json:"spentMicros"`
	Sandboxes      int64  `json:"sandboxes"`
	ComputeSeconds int64  `json:"computeSeconds"`
	PeakActive     int64  `json:"peakActive"`
	MedianSeconds  int64  `json:"medianSeconds"`
}

// Provider is one provider's last 7 days, each pointer nil while its source is unknown
type Provider struct {
	Provider    string `json:"provider"`
	Sandboxes7d int64  `json:"sandboxes7d"`
	Active      int64  `json:"active"`
	StartP50Ms  *int64 `json:"startP50Ms"`
	StartP95Ms  *int64 `json:"startP95Ms"`
	Failed7d    *int64 `json:"failed7d"`
}

// Breakdown is Payments' counts by kind and method, and its refunds
type Breakdown struct {
	ByKind   []KindCount   `json:"byKind"`
	ByMethod []MethodCount `json:"byMethod"`
	Refunds  Count         `json:"refunds"`
}

// KindCount is the payments of one kind
type KindCount struct {
	Kind   string `json:"kind"`
	Count  int64  `json:"count"`
	Micros int64  `json:"micros"`
}

// MethodCount is the payments made one way
type MethodCount struct {
	Method string `json:"method"`
	Count  int64  `json:"count"`
	Micros int64  `json:"micros"`
}

// Count is a number of payments and their sum
type Count struct {
	Count  int64 `json:"count"`
	Micros int64 `json:"micros"`
}

// Package is one start package on sale
type Package struct {
	ID          string `json:"id"`
	VCPU        int64  `json:"vcpu"`
	MemoryMiB   int64  `json:"memoryMiB"`
	DiskGiB     int64  `json:"diskGiB"`
	GPU         *GPU   `json:"gpu"`
	Minutes     int64  `json:"minutes"`
	PriceMicros int64  `json:"priceMicros"`
}

// GPU is a package's GPUs, MemoryGiB each one's own memory
type GPU struct {
	Count     int64  `json:"count"`
	MemoryGiB int64  `json:"memoryGiB"`
	Model     string `json:"model"`
}

// Encode writes a summary or a page as the routes serve it
func Encode(v any) ([]byte, error) { return json.Marshal(v) }

// Merge builds CP 20.6.1's summary, generatedAt the older of f.At and paidAt
func Merge(
	f store.StatsFigures, p PaymentsPart, paidAt time.Time, pkgs []catalog.Package,
	limits map[string]int64,
) Summary {
	at := f.At
	if paidAt.Before(at) {
		at = paidAt
	}
	s := Summary{
		Version:     Version,
		GeneratedAt: Time(at.UTC().Truncate(time.Second)),
		Now:         Now{ActiveByProvider: []ProviderActive{}},
		Totals: Totals{
			Sandboxes: f.Sandboxes, ComputeSeconds: f.ComputeSeconds, Money: p.Totals,
			SpentMicros: f.SpentMicros,
		},
		Daily:     days(f, p),
		Providers: []Provider{},
		Payments: Breakdown{
			ByKind: orEmpty(p.ByKind), ByMethod: orEmpty(p.ByMethod), Refunds: p.Refunds,
		},
		Packages: Packages(pkgs),
		Limits:   map[string]int64{},
	}
	for _, name := range slices.Sorted(maps.Keys(f.Active)) {
		if n := f.Active[name]; n > 0 {
			s.Now.Active += n
			s.Now.ActiveByProvider = append(s.Now.ActiveByProvider,
				ProviderActive{Provider: name, Active: n})
		}
	}
	for _, r := range f.Providers {
		s.Providers = append(s.Providers, Provider{
			Provider: r.Provider, Sandboxes7d: r.Sandboxes7d, Active: r.Active,
			StartP50Ms: r.StartP50Ms, StartP95Ms: r.StartP95Ms, Failed7d: r.Failed7d,
		})
	}
	maps.Copy(s.Limits, limits)
	maps.Copy(s.Limits, p.Limits)
	return s
}

// days runs from the earlier part's first day through f.At's day, a day neither has at zero
func days(f store.StatsFigures, p PaymentsPart) []Day {
	last := f.At.UTC().Truncate(24 * time.Hour)
	router := map[string]store.StatsDay{}
	var first time.Time
	for _, d := range f.Days {
		key := d.Day.UTC().Format(dayLayout)
		router[key] = d
		if first.IsZero() || d.Day.Before(first) {
			first = d.Day.UTC()
		}
	}
	money := map[string]Money{}
	for _, d := range p.Daily {
		money[d.Day] = d.Money
		day, err := time.Parse(dayLayout, d.Day)
		if err == nil && (first.IsZero() || day.Before(first)) {
			first = day
		}
	}
	out := []Day{}
	if first.IsZero() {
		return out
	}
	for day := first; !day.After(last); day = day.AddDate(0, 0, 1) {
		key := day.Format(dayLayout)
		r, ok := router[key]
		d := Day{
			Day: key, Money: money[key], SpentMicros: r.SpentMicros, Sandboxes: r.Sandboxes,
			ComputeSeconds: r.ComputeSeconds, PeakActive: r.PeakActive,
			MedianSeconds: r.MedianSeconds,
		}
		// A day before the router's first one had no placements, so it spent nothing
		if !ok && f.SpentMicros != nil {
			d.SpentMicros = new(int64)
		}
		out = append(out, d)
	}
	return out
}

// paymentsLookback is how many days back Payments' days reach while the router has no order
const paymentsLookback = 90

// PaymentsFloor is the first day Payments' daily rows may fill from: the day before the router's
// first order, or 90 days before f.At's day while it has none. It keeps one stray early day from
// zero-filling every day since
func PaymentsFloor(f store.StatsFigures) time.Time {
	if f.FirstOrderAt != nil {
		// A payment confirmed just before midnight reaches the router on the next day
		return f.FirstOrderAt.UTC().Truncate(24*time.Hour).AddDate(0, 0, -1)
	}
	return f.At.UTC().Truncate(24*time.Hour).AddDate(0, 0, -paymentsLookback)
}

// From returns p without its days before floor, and how many it left out. The totals keep them
func (p PaymentsPart) From(floor time.Time) (PaymentsPart, int) {
	kept := []PaymentsDay{}
	for _, d := range p.Daily {
		if day, err := time.Parse(dayLayout, d.Day); err == nil && !day.Before(floor) {
			kept = append(kept, d)
		}
	}
	dropped := len(p.Daily) - len(kept)
	p.Daily = kept
	return p, dropped
}

// Packages returns the catalog's start packages in catalog order
func Packages(pkgs []catalog.Package) []Package {
	out := []Package{}
	for _, p := range pkgs {
		if p.ExtensionOf != "" {
			continue
		}
		r := p.Resources
		out = append(out, Package{
			ID: p.ID, VCPU: int64(r.VCPU), MemoryMiB: int64(r.MemoryMiB),
			DiskGiB: int64(r.DiskGiB), GPU: gpu(r.GPUCount, r.GPUMemoryGiB, p.GPUModel),
			Minutes: int64(p.Minutes), PriceMicros: p.PriceMicros,
		})
	}
	return out
}

// gpu is nil without a GPU
func gpu(count, memoryGiB int, model string) *GPU {
	if count == 0 {
		return nil
	}
	return &GPU{Count: int64(count), MemoryGiB: int64(memoryGiB), Model: model}
}

// orEmpty keeps a list out of the JSON as [] rather than null
func orEmpty[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}
