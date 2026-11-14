package config

import (
	"cmp"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/pricing"
)

// Pricing holds the markup on official cost, how long a quote holds and how far it may rise
type Pricing struct {
	// Rule is the markup in basis points and each provider's billed overhead
	Rule pricing.Rule
	// QuoteHold is how long a quoted price stays payable
	QuoteHold time.Duration
	// QuoteRiseMicros is how far a held start's cost may be over what the order paid
	QuoteRiseMicros int64
}

type pricingFile struct {
	Markup          float64           `toml:"markup"`
	QuoteHold       string            `toml:"quote_hold"`
	QuoteRiseMicros int64             `toml:"quote_rise_micros"`
	Overhead        map[string]string `toml:"overhead"`
}

const (
	maxOverhead = 5 * time.Minute
	// maxQuoteHold keeps a held quote inside the 24 h that retention keeps quotes for
	maxQuoteHold = 24 * time.Hour
	maxQuoteRise = 1_000_000
)

func (f pricingFile) read(d pricingFile) (Pricing, error) {
	if !(f.Markup >= 0 && f.Markup <= 10) {
		return Pricing{}, errors.New("pricing.markup must be a finite number from 0 to 10")
	}
	hold, err := time.ParseDuration(cmp.Or(f.QuoteHold, d.QuoteHold))
	if err != nil || hold < time.Minute || hold > maxQuoteHold {
		return Pricing{}, errors.New(
			"pricing.quote_hold must be a Go duration from 1m to 24h such as 45m")
	}
	if f.QuoteRiseMicros < 0 || f.QuoteRiseMicros > maxQuoteRise {
		return Pricing{}, fmt.Errorf("pricing.quote_rise_micros must be 0 to %d", maxQuoteRise)
	}
	p := Pricing{
		Rule:      pricing.Rule{MarkupBP: int64(math.Round(f.Markup * 10_000))},
		QuoteHold: hold, QuoteRiseMicros: f.QuoteRiseMicros,
	}
	for name, raw := range f.Overhead {
		if !knownProviders[name] {
			return Pricing{}, fmt.Errorf("pricing.overhead.%s is not a provider, use one of %s",
				name, knownList())
		}
		over, err := time.ParseDuration(raw)
		if err != nil || over < 0 || over > maxOverhead {
			return Pricing{}, fmt.Errorf(
				"pricing.overhead.%s must be a Go duration from 0s to %s such as 10s",
				name, maxOverhead)
		}
		if p.Rule.Overhead == nil {
			p.Rule.Overhead = map[string]time.Duration{}
		}
		p.Rule.Overhead[name] = over
	}
	return p, nil
}
