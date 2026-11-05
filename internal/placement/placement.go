// Package placement picks which providers may host a package and tries the cheapest first
package placement

import (
	"cmp"
	"errors"
	"math/big"
	"slices"
	"time"

	"github.com/oynozan/bugnet-router/internal/catalog"
	"github.com/oynozan/bugnet-router/internal/provider"
)

// ErrNoCandidate means no provider can host the package under the given constraints
var ErrNoCandidate = errors.New("placement: no candidate provider")

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
type Requirements struct{ SSH, Preview, GPU bool }

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
	needGPU := in.Requirements.GPU || pk.Resources.GPUCount > 0
	need := time.Duration(pk.Minutes)*time.Minute + lifetimeSlack
	var out []Candidate
	for _, p := range e.reg.Ordered() {
		name := p.Name()
		if in.Exclude[name] || !e.health.Healthy(name) {
			continue
		}
		caps := p.Capabilities()
		if (in.Requirements.SSH && !caps.SSH) ||
			(in.Requirements.Preview && !caps.Preview) ||
			(needGPU && !caps.GPU) {
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
		out = append(out, Candidate{
			Provider: p, Size: size, CostMicros: cost, MarginFraction: margin,
		})
	}
	if len(out) == 0 {
		return nil, ErrNoCandidate
	}
	/* Ordering */
	// A stable sort leaves equal costs in the order the providers setting lists them
	slices.SortStableFunc(out, func(a, b Candidate) int {
		return cmp.Or(
			cmp.Compare(e.slowRank(a), e.slowRank(b)),
			cmp.Compare(a.CostMicros, b.CostMicros),
		)
	})
	return out, nil
}

// slowRank puts a provider whose ready p95 is over the threshold behind the fast ones
func (e *Engine) slowRank(c Candidate) int {
	if e.health.ReadyP95(c.Provider.Name()) > slowReadyThreshold {
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
