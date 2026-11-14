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
	"github.com/bugnet-sh/bugnet-router/internal/pricing"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// ErrNoCandidate means no provider can host the package under the given constraints
var ErrNoCandidate = errors.New("placement: no candidate provider")

// ErrNoCapableProvider is an ErrNoCandidate where no configured provider has the capabilities
var ErrNoCapableProvider = fmt.Errorf("%w, none has the capabilities", ErrNoCandidate)

// ErrNoServer means a provider or size to price on is not configured
var ErrNoServer = errors.New("placement: no such server")

const (
	slowReadyThreshold = 10 * time.Second
	// LifetimeSlack is how far a provider's lifetime limit must reach past a package's minutes
	LifetimeSlack = 5 * time.Minute
)

// Candidate is one provider and size that may host a package
type Candidate struct {
	Provider   provider.Provider
	Size       provider.Size
	CostMicros int64 // the billed time in whole billing increments plus any start fee
	// PriceMicros is what a run costs the user here, a fixed package's price or the marked up cost
	PriceMicros int64
	// Warm means a pool member is claimed before any create is tried
	Warm bool
	// HourlyMicros and StartFeeMicros are the parts of CostMicros that hedging needs
	HourlyMicros, StartFeeMicros int64
}

// Server is one provider and one of its sizes
type Server struct{ Provider, Size string }

// Requirements are the capabilities a placement needs beyond the package's resources
type Requirements struct{ SSH, Preview, GPU, Secrets bool }

// Inputs describe one placement request
type Inputs struct {
	Package      catalog.Package
	Requirements Requirements
	// Rule prices a live package and holds a fixed package's price over its marked up cost
	Rule pricing.Rule
	// BudgetMicros is the most a candidate's price may be, 0 for no budget
	BudgetMicros int64
	// Prefer is the server that sorts first and is exempt from the budget, nil for none
	Prefer  *Server
	Exclude map[string]bool // providers already tried
	// WarmFirst puts candidates with a warm member ahead of every cold one
	WarmFirst bool
	// Hold is provider time before the paid clock starts, billed into the cost and the price
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
	var rs []ranked
	for _, p := range e.reg.Ordered() {
		name := p.Name()
		if in.Exclude[name] || !e.health.Healthy(name) {
			continue
		}
		c, open, ok := e.fit(p, in)
		if !ok || !open {
			continue
		}
		// A pool member carries no egress restriction, so a restricted package creates cold
		c.Warm = e.warm != nil && len(pk.EgressAllow) == 0 &&
			e.warm.WarmAvailable(name, c.Size.Name)
		rs = append(rs, ranked{Candidate: c, slow: e.slowRank(name), prefer: in.Prefer.is(c)})
	}
	if len(rs) == 0 {
		return nil, e.noCandidate(req)
	}
	/* Ordering */
	// A stable sort leaves equal costs in the order the providers setting lists them
	// With WarmFirst a warm member is quick to claim even where creates are slow, so it comes first
	// The preferred server comes ahead of every other
	slices.SortStableFunc(rs, func(a, b ranked) int {
		warm := 0
		if in.WarmFirst {
			warm = cmp.Compare(b2i(b.Warm), b2i(a.Warm))
		}
		return cmp.Or(cmp.Compare(b2i(b.prefer), b2i(a.prefer)), warm,
			cmp.Compare(a.slow, b.slow), cmp.Compare(a.CostMicros, b.CostMicros))
	})
	out := make([]Candidate, len(rs))
	for i, r := range rs {
		out[i] = r.Candidate
		// Cost first looks at the candidate a cold order would take, and no other
		out[i].Warm = r.Warm && (in.WarmFirst || i == 0)
	}
	return out, nil
}

// Quote is the first candidate and true, else the cheapest landing or a zero Candidate and false
func (e *Engine) Quote(in Inputs) (Candidate, bool) {
	if cs, err := e.Candidates(in); err == nil {
		return cs[0], true
	}
	var best Candidate
	for i, l := range e.Lands(in) {
		// A strict less keeps the providers order among equal prices
		if i == 0 || l.PriceMicros < best.PriceMicros {
			best = l.Candidate
		}
	}
	return best, false
}

// ExtensionPrice is ext's fixed price, else its minutes on c's server marked up without a fee
func (c Candidate) ExtensionPrice(ext catalog.Package, r pricing.Rule) (int64, error) {
	// A zero Candidate is the quote of a start no server lands
	if c.Provider == nil {
		return 0, fmt.Errorf("%w: the candidate has no provider", ErrNoServer)
	}
	if ext.FixedPrice {
		return ext.PriceMicros, nil
	}
	run := time.Duration(ext.Minutes) * time.Minute
	return r.Price(costMicros(c.HourlyMicros, run, c.Provider.BillingIncrement())), nil
}

// ExtensionPrice is pkg's price on the named server as a Candidate there prices it
func (e *Engine) ExtensionPrice(
	pkg catalog.Package, name, size string, r pricing.Rule,
) (int64, error) {
	p, ok := e.reg.Get(name)
	if !ok {
		return 0, fmt.Errorf("%w: provider %q is not configured", ErrNoServer, name)
	}
	sz, ok := sizeNamed(p, size)
	if !ok {
		return 0, fmt.Errorf("%w: provider %q has no size %q", ErrNoServer, name, size)
	}
	c := Candidate{Provider: p, Size: sz, HourlyMicros: p.HourlyCostMicros(sz)}
	return c.ExtensionPrice(pkg, r)
}

// sizeNamed finds a size of a provider by name
func sizeNamed(p provider.Provider, name string) (provider.Size, bool) {
	sz, ok := provider.As[provider.Sizer](p)
	if !ok {
		return provider.Size{}, false
	}
	for _, s := range sz.Sizes() {
		if s.Name == name {
			return s, true
		}
	}
	return provider.Size{}, false
}

// Landing is the size a package lands on at one provider, whatever the provider's state now
type Landing struct {
	Candidate
	// Open is false while the provider is unhealthy or would refuse the start now
	Open bool
}

// Lands returns each provider that can host in.Package, health aside, in the providers order
func (e *Engine) Lands(in Inputs) []Landing {
	var out []Landing
	for _, p := range e.reg.Ordered() {
		name := p.Name()
		if in.Exclude[name] {
			continue
		}
		if c, open, ok := e.fit(p, in); ok {
			out = append(out, Landing{Candidate: c, Open: open && e.health.Healthy(name)})
		}
	}
	return out
}

// fit is p's candidate, health aside, ok false when p cannot host, open false when it refuses now
func (e *Engine) fit(p provider.Provider, in Inputs) (Candidate, bool, bool) {
	pk := in.Package
	req := in.Requirements
	req.GPU = req.GPU || pk.Resources.GPUCount > 0
	need := time.Duration(pk.Minutes)*time.Minute + max(in.HoldMax, 0) + LifetimeSlack
	billed := in.Rule.Billed(p.Name(), time.Duration(pk.Minutes)*time.Minute+max(in.Hold, 0))
	if !e.reg.ImageAccepted(p, in.ImageAccept) {
		return Candidate{}, false, false
	}
	caps := e.reg.Capabilities(p)
	if !capable(caps, req) {
		return Candidate{}, false, false
	}
	// A restricted package needs rules outside the VM, which Koyeb, Boat and local lack
	if len(pk.EgressAllow) > 0 && !caps.EgressControl {
		return Candidate{}, false, false
	}
	if caps.MaxLifetime != 0 && caps.MaxLifetime < need {
		return Candidate{}, false, false
	}
	size, ok := p.SizeFor(pk.Resources)
	if !ok {
		return Candidate{}, false, false
	}
	hourly := p.HourlyCostMicros(size)
	cost := costMicros(hourly, billed, p.BillingIncrement())
	open := true
	var fee int64
	if sc, ok := provider.As[provider.StartCoster](p); ok {
		// A refused start counts no fee, so the package still lands where its hours fit
		if fee, open = sc.StartCostMicros(size); !open {
			fee = 0
		}
		cost += fee
	}
	c := Candidate{
		Provider: p, Size: size, CostMicros: cost, PriceMicros: in.Rule.Price(cost),
		HourlyMicros: hourly, StartFeeMicros: fee,
	}
	if pk.FixedPrice {
		// A fixed price of 0 is free, which only a cost of 0 fits
		if c.PriceMicros > pk.PriceMicros {
			return Candidate{}, false, false
		}
		c.PriceMicros = pk.PriceMicros
	}
	if in.BudgetMicros != 0 && c.PriceMicros > in.BudgetMicros && !in.Prefer.is(c) {
		return Candidate{}, false, false
	}
	return c, open, true
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
	slow   int
	prefer bool
}

// is reports whether c runs on s, false for a nil s
func (s *Server) is(c Candidate) bool {
	return s != nil && s.Provider == c.Provider.Name() && s.Size == c.Size.Name
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

// HedgeFits reports whether the price of b beside a fits budget, a having run pre before the order
//
// Whichever attempt wins runs the whole of run and the loser runs until it is destroyed, so the
// worst case is the dearer one winning, both start fees paid, and the loser living loserLife.
// pre counts on both sides, which keeps the check an upper bound. A budget of 0 fits only when
// both candidates cost nothing.
func HedgeFits(
	a, b Candidate, run, pre, loserLife time.Duration, budget int64, r pricing.Rule,
) bool {
	incA, incB := a.Provider.BillingIncrement(), b.Provider.BillingIncrement()
	winner := max(costMicros(a.HourlyMicros, pre+run, incA), costMicros(b.HourlyMicros, run, incB))
	loser := max(costMicros(a.HourlyMicros, pre+loserLife, incA),
		costMicros(b.HourlyMicros, pre+loserLife, incB))
	worst := winner + a.StartFeeMicros + b.StartFeeMicros + loser
	return r.Price(worst) <= budget
}
