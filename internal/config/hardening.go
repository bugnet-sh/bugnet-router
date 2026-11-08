package config

import (
	"bufio"
	"cmp"
	"errors"
	"fmt"
	"io/fs"
	"net/netip"
	"os"
	"slices"
	"strings"
)

// MaxPools is the most mining pool entries the pool list may hold
const MaxPools = 10_000

// maxMailPorts and maxRanges bound the hardening lists, as bugnet-run harden does
const (
	maxMailPorts = 64
	maxRanges    = 1000
)

// Hardening holds the rules bugnet-run harden loads in every sandbox
type Hardening struct {
	// MailPorts are blocked over TCP to any address
	MailPorts []int
	// DenyRanges are the private, link-local and metadata ranges, as canonical prefixes
	DenyRanges []string
	// PoolList is the file of mining pool addresses, one per line
	PoolList string
	// Rules holds each provider's hardening rule by name, a provider without one never runs harden
	Rules map[string]HardenRule
}

// HardenRule is one provider's hardening settings
type HardenRule struct {
	// Required fails a boot whose harden fails, so the saga fails over
	Required bool
	// Allow holds the provider's own control addresses, reachable before any block
	Allow []string
}

type hardeningFile struct {
	MailPorts  []int64  `toml:"mail_ports"`
	DenyRanges []string `toml:"deny_ranges"`
	PoolList   string   `toml:"pool_list"`
}

// defaultDenyRanges are CP 6.9's private, link-local and metadata ranges
var defaultDenyRanges = []string{
	"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "100.64.0.0/10", "fc00::/7",
	"169.254.0.0/16", "fe80::/10", "fd00:ec2::254/128",
}

func defaultHardening() hardeningFile {
	return hardeningFile{
		// Clones, since go-toml decodes a list into the slice it finds
		MailPorts:  []int64{25, 465, 587},
		DenyRanges: slices.Clone(defaultDenyRanges),
		PoolList:   "/etc/bugnet/mining-pools.txt",
	}
}

// hardening checks the hardening table and each provider's rule and sets them on c
func (f file) hardening(c *Config) error {
	h := f.Hardening
	if len(h.MailPorts) > maxMailPorts {
		return fmt.Errorf("hardening.mail_ports holds more than %d ports", maxMailPorts)
	}
	c.Hardening.MailPorts = []int{}
	for i, p := range h.MailPorts {
		if p < 1 || p > 65535 {
			return fmt.Errorf("hardening.mail_ports entry %d must be a port from 1 to 65535", i+1)
		}
		c.Hardening.MailPorts = append(c.Hardening.MailPorts, int(p))
	}
	ranges, err := prefixes("hardening.deny_ranges", h.DenyRanges)
	if err != nil {
		return err
	}
	c.Hardening.DenyRanges = ranges
	c.Hardening.PoolList = cmp.Or(h.PoolList, defaultHardening().PoolList)
	c.Hardening.Rules = map[string]HardenRule{}
	for _, r := range []struct {
		name     string
		required bool
		allow    []string
	}{
		{"e2b", f.E2B.HardenRequired, f.E2B.HardenAllow},
		{"boxd", f.Boxd.HardenRequired, f.Boxd.HardenAllow},
		{"koyeb", f.Koyeb.HardenRequired, f.Koyeb.HardenAllow},
		{"boat", f.Boat.HardenRequired, f.Boat.HardenAllow},
	} {
		allow, err := prefixes(r.name+".harden_allow", r.allow)
		if err != nil {
			return err
		}
		c.Hardening.Rules[r.name] = HardenRule{Required: r.required, Allow: allow}
	}
	return nil
}

// prefixes reads addresses or CIDRs as canonical prefixes, naming an entry by its index alone
func prefixes(key string, entries []string) ([]string, error) {
	if len(entries) > maxRanges {
		return nil, fmt.Errorf("%s holds more than %d entries", key, maxRanges)
	}
	out := []string{}
	for i, e := range entries {
		p, ok := parsePrefix(e)
		if !ok {
			return nil, fmt.Errorf("%s entry %d must be an IP address or a CIDR such as "+
				"10.0.0.0/8", key, i+1)
		}
		if !slices.Contains(out, p.String()) {
			out = append(out, p.String())
		}
	}
	return out, nil
}

// parsePrefix reads a CIDR, or an address as its own one-address prefix
func parsePrefix(s string) (netip.Prefix, bool) {
	if p, err := netip.ParsePrefix(s); err == nil && p.Addr().Zone() == "" {
		return p.Masked(), true
	}
	if a, err := netip.ParseAddr(s); err == nil && a.Zone() == "" {
		return netip.PrefixFrom(a, a.BitLen()), true
	}
	return netip.Prefix{}, false
}

// LoadPoolList reads one address or CIDR per line, skipping blanks and # lines, empty when missing
func LoadPoolList(path string) ([]string, error) {
	if path == "" {
		return nil, nil
	}
	f, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("config: the mining pool list: %w", err)
	}
	defer func() { _ = f.Close() }()
	var out []string
	sc := bufio.NewScanner(f)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		p, ok := parsePrefix(line)
		if !ok {
			return nil, fmt.Errorf("config: %s line %d is not an IP address or a CIDR", path, n)
		}
		if len(out) == MaxPools {
			return nil, fmt.Errorf("config: %s holds more than %d entries", path, MaxPools)
		}
		out = append(out, p.String())
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("config: the mining pool list: %w", err)
	}
	return out, nil
}
