package hostrules

import (
	"slices"

	"golang.org/x/net/publicsuffix"

	"github.com/bugnet-sh/bugnet-router/internal/config"
)

// defaultE2BDomain is E2B's domain when e2b.domain is empty
const defaultE2BDomain = "e2b.app"

// providerDomains are DP 10.3's provider domains
var providerDomains = []string{
	"e2b.app", "e2b.dev", "e2b-juliett.dev", "boxd.sh", "koyeb.app", "boat.dev", "ascii.dev",
}

// New builds the one rule set every caller uses, the x402 form, from the router's settings
func New(x config.X402, n config.Names, e2bDomain string) Rules {
	r := Rules{
		ProviderDomains: slices.Clone(providerDomains),
		SharedSuffixes:  slices.Clone(x.SharedHostingSuffixes),
		AllowedShared:   slices.Clone(x.AllowedSharedHosts),
		Blocked:         slices.Clone(x.BlockedHosts),
	}
	if e2bDomain == "" {
		e2bDomain = defaultE2BDomain
	}
	if !slices.Contains(r.ProviderDomains, e2bDomain) {
		r.ProviderDomains = append(r.ProviderDomains, e2bDomain)
	}
	for _, name := range []string{
		n.PreviewDomain, n.TransferHost, n.SecretsHost, n.SSHHost, n.EgressHost,
	} {
		if name == "" {
			continue
		}
		// The registrable domain, so every name under bugnet.sh is refused
		domain, err := publicsuffix.EffectiveTLDPlusOne(name)
		if err != nil {
			domain = name
		}
		if !slices.Contains(r.BugnetNames, domain) {
			r.BugnetNames = append(r.BugnetNames, domain)
		}
	}
	return r
}

// ForSecrets is r as secret hosts take it, with AllowedShared and Blocked empty
func (r Rules) ForSecrets() Rules {
	r.AllowedShared, r.Blocked = nil, nil
	return r
}
