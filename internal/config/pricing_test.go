package config

import (
	"strings"
	"testing"
	"time"
)

func TestPricingDefaults(t *testing.T) {
	c, err := LoadFile(writeFile(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	if c.Pricing.Rule.MarkupBP != 1000 || len(c.Pricing.Rule.Overhead) != 0 ||
		c.Pricing.QuoteHold != 45*time.Minute {
		t.Fatalf("pricing defaults %+v", c.Pricing)
	}
	if c.Boxd.USDPerEUR != 1.12 {
		t.Fatalf("boxd usd_per_eur %v, want 1.12", c.Boxd.USDPerEUR)
	}
}

func TestPricingSettings(t *testing.T) {
	c, err := LoadFile(writeFile(t, `[pricing]
markup = 0.1234
quote_hold = "1h"

[pricing.overhead]
boat = "10s"
e2b = "0s"

[boxd]
usd_per_eur = 1.2
`))
	if err != nil {
		t.Fatal(err)
	}
	p := c.Pricing
	if p.Rule.MarkupBP != 1234 || p.QuoteHold != time.Hour ||
		p.Rule.Overhead["boat"] != 10*time.Second || p.Rule.Overhead["e2b"] != 0 {
		t.Fatalf("pricing %+v", p)
	}
	if c.Boxd.USDPerEUR != 1.2 {
		t.Fatalf("boxd usd_per_eur %v", c.Boxd.USDPerEUR)
	}
	c, err = LoadFile(writeFile(t, "[pricing]\nmarkup = 0.10\n"))
	if err != nil || c.Pricing.Rule.MarkupBP != 1000 {
		t.Fatalf("markup 0.10 gave %+v, %v", c.Pricing, err)
	}
}

func TestPricingRefusals(t *testing.T) {
	for _, c := range []struct{ name, body, want string }{
		{"negative markup", "[pricing]\nmarkup = -1\n", "pricing.markup"},
		{"nan markup", "[pricing]\nmarkup = nan\n", "pricing.markup"},
		{"large markup", "[pricing]\nmarkup = 11\n", "pricing.markup"},
		{"unknown overhead", "[pricing.overhead]\nnosuch = \"1s\"\n", "nosuch"},
		{"long overhead", "[pricing.overhead]\nboat = \"6m\"\n", "pricing.overhead.boat"},
		{"negative overhead", "[pricing.overhead]\nboat = \"-1s\"\n", "pricing.overhead.boat"},
		{"bad overhead", "[pricing.overhead]\nboat = \"soon\"\n", "pricing.overhead.boat"},
		{"short hold", "[pricing]\nquote_hold = \"30s\"\n", "pricing.quote_hold"},
		{"bad hold", "[pricing]\nquote_hold = \"x\"\n", "pricing.quote_hold"},
		{"low rate", "[boxd]\nusd_per_eur = 0.4\n", "boxd.usd_per_eur"},
		{"high rate", "[boxd]\nusd_per_eur = 3.5\n", "boxd.usd_per_eur"},
		{"nan rate", "[boxd]\nusd_per_eur = nan\n", "boxd.usd_per_eur"},
	} {
		_, err := LoadFile(writeFile(t, c.body))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err %v, want it to name %s", c.name, err, c.want)
		}
	}
}
