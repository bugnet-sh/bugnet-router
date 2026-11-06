// Package placement filters the providers that may host a package and orders them cheapest first
package placement

import (
	"cmp"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"time"

	"github.com/oynozan/bugnet-router/internal/catalog"
	"github.com/oynozan/bugnet-router/internal/provider"
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
}

// Requirements are the capabilities a placement needs beyond the package's resources
type Requirements struct{ SSH, Preview, GPU, Secrets bool }

// Inputs describe one placement request
type Inputs struct {
	Package      catalog.Package
	Requirements Requirements
	MarginFloor  float64
	Exclude      map[string]bool // providers already tried
}

// Health reports how well each provider has been behaving
type Health interface {
	Healthy(provider string) bool
	ReadyP95(provider string) time.Duration
}

// Engine filters and orders providers for a package, capacity is left to the saga
type Engine struct {
	reg    *provider.Registry
	health Health
}

// New builds an engine over a registry and a health source
func New(reg *provider.Registry, health Health) *Engine {
	return &Engine{reg: reg, health: health}
}

// Candidates returns usable providers cheapest first, slow to get ready last, or ErrNoCandidate
func (e *Engine) Candidates(in Inputs) ([]Candidate, error) {
	pk := in.Package
	req := in.Requirements
	req.GPU = req.GPU || pk.Resources.GPUCount > 0
	need := time.Duration(pk.Minutes)*time.Minute + lifetimeSlack
	var rs []ranked
	for _, p := range e.reg.Ordered() {
		name := p.Name()
		if in.Exclude[name] || !e.health.Healthy(name) {
			continue
		}
		caps := p.Capabilities()
		if !capable(caps, req) {
			continue
		}
		if caps.MaxLifetime != 0 && caps.MaxLifetime < need {
			continue
		}
		size, ok := p.SizeFor(pk.Resources)
		if !ok {
			continue
		}
		cost := costMicros(p.HourlyCostMicros(size), pk.Minutes, p.BillingIncrement())
		if sc, ok := p.(provider.StartCoster); ok {
			fee, ok := sc.StartCostMicros(size)
			if !ok {
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
		c := Candidate{Provider: p, Size: size, CostMicros: cost, MarginFraction: margin}
		rs = append(rs, ranked{Candidate: c, slow: e.slowRank(name)})
	}
	if len(rs) == 0 {
		return nil, e.noCandidate(req)
	}
	/* Ordering */
	// A stable sort leaves equal costs in the order the providers setting lists them
	slices.SortStableFunc(rs, func(a, b ranked) int {
		return cmp.Or(cmp.Compare(a.slow, b.slow), cmp.Compare(a.CostMicros, b.CostMicros))
	})
	out := make([]Candidate, len(rs))
	for i, r := range rs {
		out[i] = r.Candidate
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
		if capable(p.Capabilities(), req) {
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

// costMicros bills whole increments and rounds the result up to a whole micro
func costMicros(hourly int64, minutes int, increment time.Duration) int64 {
	used := time.Duration(minutes) * time.Minute
	billed := used
	if increment > 0 {
		billed = (used + increment - 1) / increment * increment
	}
	num := new(big.Int).Mul(big.NewInt(hourly), big.NewInt(int64(billed)))
	den := big.NewInt(int64(time.Hour))
	num.Add(num, den.Sub(den, big.NewInt(1)))
	return num.Div(num, big.NewInt(int64(time.Hour))).Int64()
}
