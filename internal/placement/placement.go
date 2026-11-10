// Package placement filters the providers that may host a package and orders them cheapest first
package placement

import (
	"cmp"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// ErrNoCandidate means no provider can host the package under the given constraints
var ErrNoCandidate = errors.New("placement: no candidate provider")

// ErrNoCapableProvider is an ErrNoCandidate where no configured provider has the capabilities
var ErrNoCapableProvider = fmt.Errorf("%w, none has the capabilities", ErrNoCandidate)

const (
	slowReadyThreshold = 10 * time.Second
	lifetimeSlack      = 5 * time.Minute
)

// Candidate is one provider and size that may host a package
type Candidate struct {
	Provider       provider.Provider
	Size           provider.Size
	CostMicros     int64 // the package's minutes in whole billing increments plus any start fee
	MarginFraction float64
	// Warm means a pool member is claimed before any create is tried
	Warm bool
	// HourlyMicros and StartFeeMicros are the parts of CostMicros that hedging needs
	HourlyMicros, StartFeeMicros int64
}

// Requirements are the capabilities a placement needs beyond the package's resources
type Requirements struct{ SSH, Preview, GPU, Secrets bool }

// Inputs describe one placement request
type Inputs struct {
	Package      catalog.Package
	Requirements Requirements
	MarginFloor  float64
	Exclude      map[string]bool // providers already tried
	// WarmFirst puts candidates with a warm member ahead of every cold one
	WarmFirst bool
	// Hold is provider time before the paid clock starts, billed and checked against the floor
	Hold time.Duration
	// HoldMax is the longest a renewed hold can run, which the candidate's lifetime must cover
	HoldMax time.Duration
	// ImageAccept lists the base image versions creates may start from, nil for any
	ImageAccept []string
}

// Health reports how well each provider has been behaving
type Health interface {
	Healthy(provider string) bool
	ReadyP95(provider string) time.Duration
}

// Warm reports whether a provider has a warm member of a size to claim now
type Warm interface {
	WarmAvailable(provider, size string) bool
}

// Engine filters and orders providers for a package, capacity is left to the saga
type Engine struct {
	reg    *provider.Registry
	health Health
	warm   Warm
}

// New builds an engine over a registry, a health source and a warm source, which may be nil
func New(reg *provider.Registry, health Health, warm Warm) *Engine {
	return &Engine{reg: reg, health: health, warm: warm}
}

// Healthy reports whether the engine's health source lets the provider be tried now
func (e *Engine) Healthy(name string) bool { return e.health.Healthy(name) }

// Candidates returns usable providers cheapest first, slow to get ready last, or ErrNoCandidate
func (e *Engine) Candidates(in Inputs) ([]Candidate, error) {
	pk := in.Package
	req := in.Requirements
	req.GPU = req.GPU || pk.Resources.GPUCount > 0
	need := time.Duration(pk.Minutes)*time.Minute + max(in.HoldMax, 0) + lifetimeSlack
	billed := time.Duration(pk.Minutes)*time.Minute + max(in.Hold, 0)
	var rs []ranked
	for _, p := range e.reg.Ordered() {
		name := p.Name()
		if in.Exclude[name] || !e.health.Healthy(name) {
			continue
		}
		if !e.reg.ImageAccepted(p, in.ImageAccept) {
			continue
		}
		caps := e.reg.Capabilities(p)
		if !capable(caps, req) {
			continue
		}
		// A restricted package needs rules outside the VM, which Koyeb, Boat and local lack
		if len(pk.EgressAllow) > 0 && !caps.EgressControl {
			continue
		}
		if caps.MaxLifetime != 0 && caps.MaxLifetime < need {
			continue
		}
		size, ok := p.SizeFor(pk.Resources)
		if !ok {
			continue
		}
		hourly := p.HourlyCostMicros(size)
		cost := costMicros(hourly, billed, p.BillingIncrement())
		var fee int64
		if sc, ok := p.(provider.StartCoster); ok {
			var ok bool
			if fee, ok = sc.StartCostMicros(size); !ok {
				continue
			}
			cost += fee
		}
		margin := 0.0
		if pk.PriceMicros == 0 {
			if cost != 0 {
				continue
			}
		} else {
			margin = float64(pk.PriceMicros-cost) / float64(pk.PriceMicros)
			if margin < in.MarginFloor {
				continue
			}
		}
		c := Candidate{
			Provider: p, Size: size, CostMicros: cost, MarginFraction: margin,
			HourlyMicros: hourly, StartFeeMicros: fee,
			// A pool member carries no egress restriction, so a restricted package creates cold
			Warm: e.warm != nil && len(pk.EgressAllow) == 0 &&
				e.warm.WarmAvailable(name, size.Name),
		}
		rs = append(rs, ranked{Candidate: c, slow: e.slowRank(name)})
	}
	if len(rs) == 0 {
		return nil, e.noCandidate(req)
	}
	/* Ordering */
	// A stable sort leaves equal costs in the order the providers setting lists them
	// With WarmFirst a warm member is quick to claim even where creates are slow, so it comes first
	slices.SortStableFunc(rs, func(a, b ranked) int {
		warm := 0
		if in.WarmFirst {
			warm = cmp.Compare(b2i(b.Warm), b2i(a.Warm))
		}
		return cmp.Or(warm, cmp.Compare(a.slow, b.slow), cmp.Compare(a.CostMicros, b.CostMicros))
	})
	out := make([]Candidate, len(rs))
	for i, r := range rs {
		out[i] = r.Candidate
		// Cost first looks at the candidate a cold order would take, and no other
		out[i].Warm = r.Warm && (in.WarmFirst || i == 0)
	}
	return out, nil
}

// capable reports whether caps covers every capability req asks for
func capable(caps provider.Capabilities, req Requirements) bool {
	return (!req.SSH || caps.SSH) && (!req.Preview || caps.Preview) && (!req.GPU || caps.GPU) &&
		(!req.Secrets || caps.Secrets)
}

// noCandidate is ErrNoCapableProvider when req asks for a capability no provider has at all
func (e *Engine) noCandidate(req Requirements) error {
	if req == (Requirements{}) {
		return ErrNoCandidate
	}
	for _, p := range e.reg.Ordered() {
		if capable(e.reg.Capabilities(p), req) {
			return ErrNoCandidate
		}
	}
	return ErrNoCapableProvider
}

// ranked keeps a slow rank read once, a ready time recorded mid-sort would break the comparator
type ranked struct {
	Candidate
	slow int
}

// slowRank puts a provider whose ready p95 is over the threshold behind the fast ones
func (e *Engine) slowRank(name string) int {
	if e.health.ReadyP95(name) > slowReadyThreshold {
		return 1
	}
	return 0
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

// costMicros bills whole increments and rounds the result up to a whole micro
func costMicros(hourly int64, used, increment time.Duration) int64 {
	billed := used
	if increment > 0 {
		billed = (used + increment - 1) / increment * increment
	}
	num := new(big.Int).Mul(big.NewInt(hourly), big.NewInt(int64(billed)))
	den := big.NewInt(int64(time.Hour))
	num.Add(num, den.Sub(den, big.NewInt(1)))
	return num.Div(num, big.NewInt(int64(time.Hour))).Int64()
}

// HedgeFits reports whether b beside a stays above the floor, a having run pre before the order
//
// Whichever attempt wins runs the whole package and the loser runs until it is destroyed, so the
// worst case is the dearer one winning, both start fees paid, and the loser living loserLife.
// pre counts on both sides, which keeps the check an upper bound. A free package fits only when
// both candidates cost nothing.
func HedgeFits(
	pkg catalog.Package, a, b Candidate, pre, loserLife time.Duration, floor float64,
) bool {
	run := time.Duration(pkg.Minutes) * time.Minute
	incA, incB := a.Provider.BillingIncrement(), b.Provider.BillingIncrement()
	winner := max(costMicros(a.HourlyMicros, pre+run, incA), costMicros(b.HourlyMicros, run, incB))
	loser := max(costMicros(a.HourlyMicros, pre+loserLife, incA),
		costMicros(b.HourlyMicros, pre+loserLife, incB))
	worst := winner + a.StartFeeMicros + b.StartFeeMicros + loser
	return float64(worst) <= float64(pkg.PriceMicros)*(1-floor)
}
