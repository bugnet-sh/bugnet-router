package config

import (
	"cmp"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/pricing"
)

// Pricing holds the markup on official cost and how long a quote holds
type Pricing struct {
	// Rule is the markup in basis points and each provider's billed overhead
	Rule pricing.Rule
	// QuoteHold is how long a quoted price stays payable
	QuoteHold time.Duration
}

type pricingFile struct {
	Markup    float64           `toml:"markup"`
	QuoteHold string            `toml:"quote_hold"`
	Overhead  map[string]string `toml:"overhead"`
}

const maxOverhead = 5 * time.Minute

func (f pricingFile) read(d pricingFile) (Pricing, error) {
	if !(f.Markup >= 0 && f.Markup <= 10) {
		return Pricing{}, errors.New("pricing.markup must be a finite number from 0 to 10")
	}
	hold, err := time.ParseDuration(cmp.Or(f.QuoteHold, d.QuoteHold))
	if err != nil || hold < time.Minute {
		return Pricing{}, errors.New(
			"pricing.quote_hold must be a Go duration of at least 1m such as 45m")
	}
	p := Pricing{
		Rule:      pricing.Rule{MarkupBP: int64(math.Round(f.Markup * 10_000))},
		QuoteHold: hold,
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
