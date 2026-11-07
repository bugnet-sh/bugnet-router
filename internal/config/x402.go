package config

import (
	"cmp"
	"fmt"
	"math"
	"slices"

	"github.com/oynozan/bugnet-router/internal/hostname"
)

// X402 holds the x402 host kind's settings
type X402 struct {
	// Enabled false refuses EnableX402 and CheckX402 and tells egress never to pay
	Enabled               bool
	MaxServices           int
	MaxPriceMicros        int64
	UnpaidRequestsPerHost uint32
	UnpaidBytesPerHost    uint64
	// SharedHostingSuffixes is the gateway's list, refused unless in AllowedSharedHosts
	SharedHostingSuffixes []string
	// AllowedSharedHosts is written by the deploy from the reviewed index, never by hand
	AllowedSharedHosts []string
	// BlockedHosts is the operator's list, refused at enable and dropped from ResolveTunnel
	BlockedHosts []string
}

type x402File struct {
	Enabled               bool     `toml:"enabled"`
	MaxServices           int64    `toml:"max_services"`
	MaxPriceMicros        int64    `toml:"max_price_micros"`
	UnpaidRequestsPerHost int64    `toml:"unpaid_requests_per_host"`
	UnpaidBytesPerHost    string   `toml:"unpaid_bytes_per_host"`
	SharedHostingSuffixes []string `toml:"shared_hosting_suffixes"`
	AllowedSharedHosts    []string `toml:"allowed_shared_hosts"`
	BlockedHosts          []string `toml:"blocked_hosts"`
}

const (
	// maxX402Services matches the 32 hosts one placement may hold
	maxX402Services = 32
	// maxX402PriceMicros is the founder's ceiling on one paid call, 0.50 USD
	maxX402PriceMicros = 500_000
)

// sharedHostingSuffixes is the gateway's list, the same as bugnet-proto's shared vector file
var sharedHostingSuffixes = []string{
	"workers.dev", "onrender.com", "vercel.app", "netlify.app", "pages.dev", "herokuapp.com",
	"fly.dev", "up.railway.app", "replit.app", "ngrok-free.app", "ngrok.io", "trycloudflare.com",
	"github.io", "glitch.me", "deno.dev", "web.app",
}

// read checks the x402 table against its bounds, d holding the defaults
func (f x402File) read(d x402File) (X402, error) {
	if f.MaxServices < 1 || f.MaxServices > maxX402Services {
		return X402{}, fmt.Errorf("x402.max_services must be 1 to %d", maxX402Services)
	}
	if f.MaxPriceMicros < 1 || f.MaxPriceMicros > maxX402PriceMicros {
		return X402{}, fmt.Errorf("x402.max_price_micros must be 1 to %d", maxX402PriceMicros)
	}
	if f.UnpaidRequestsPerHost < 1 || f.UnpaidRequestsPerHost > math.MaxUint32 {
		return X402{}, fmt.Errorf("x402.unpaid_requests_per_host must be 1 to %d",
			uint64(math.MaxUint32))
	}
	size, err := byteSize("x402.unpaid_bytes_per_host",
		cmp.Or(f.UnpaidBytesPerHost, d.UnpaidBytesPerHost))
	if err != nil {
		return X402{}, err
	}
	x := X402{
		Enabled: f.Enabled, MaxServices: int(f.MaxServices), MaxPriceMicros: f.MaxPriceMicros,
		UnpaidRequestsPerHost: uint32(f.UnpaidRequestsPerHost), UnpaidBytesPerHost: uint64(size),
	}
	for _, l := range []struct {
		key  string
		raw  []string
		into *[]string
	}{
		{"x402.shared_hosting_suffixes", f.SharedHostingSuffixes, &x.SharedHostingSuffixes},
		{"x402.allowed_shared_hosts", f.AllowedSharedHosts, &x.AllowedSharedHosts},
		{"x402.blocked_hosts", f.BlockedHosts, &x.BlockedHosts},
	} {
		names, err := hostList(l.key, l.raw)
		if err != nil {
			return X402{}, err
		}
		*l.into = names
	}
	return x, nil
}

// hostList checks a list of exact host names, each once, nil when it is empty
func hostList(key string, names []string) ([]string, error) {
	for i, name := range names {
		if !hostname.Valid(name) {
			// An entry is never quoted, since a secret could have been pasted there
			return nil, fmt.Errorf("%s entry %d must be a lowercase host name such as "+
				"host.example.com, with no scheme, port, wildcard or final dot", key, i+1)
		}
		if first := slices.Index(names[:i], name); first >= 0 {
			return nil, fmt.Errorf("%s entry %d repeats entry %d", key, i+1, first+1)
		}
	}
	if len(names) == 0 {
		return nil, nil
	}
	return slices.Clone(names), nil
}
