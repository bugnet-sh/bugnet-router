// Package lium sells GPU sandboxes on Lium pods, each one GPU type on a whole Secure node.
//
// Hosts set their own prices, so a size carries a cap instead of a price, and the adapter quotes
// the cap. A background loop reads Lium's keyless node listing every 30 seconds. A size is
// offered only while the newest listing is under 2 minutes old and holds a node the size may
// rent, so a Lium outage moves GPU orders to the next GPU provider. Nodes in the first countries
// always win, and other countries that are not blocked serve only when the first hold none.
package lium

import (
	"cmp"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/gpu"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const (
	providerName       = "lium"
	defaultBaseURL     = "https://lium.io/api"
	defaultMaxLifetime = 2 * time.Hour
	// arch is the only architecture our image is built for, Grace nodes are never rented
	arch = "amd64"
)

// SizeSpec is one size sold, one GPU type on a whole node, as a [[lium.sizes]] table states it
type SizeSpec struct {
	Name string
	// Model is the canonical name of the size's GPU model
	Model string
	// Match holds the gpu_class and machine_name values of the nodes the size rents
	Match []string
	GPUs  int
	// GPUMemoryGiB is the memory of all the size's GPUs together
	GPUMemoryGiB             int
	VCPU, MemoryMiB, DiskGiB int
	// MaxHourlyMicros is the cap, the most a node of the size may cost an hour
	MaxHourlyMicros int64
}

// Options configures the adapter, BaseURL replaces Lium's API in tests
type Options struct {
	APIKey  string
	BaseURL string
	Log     *slog.Logger // slog.Default when nil
	// Sizes are the sizes sold, at least one
	Sizes []SizeSpec
	// Countries are the node countries tried first, at least one
	Countries []string
	// OtherCountries takes a node outside Countries and BlockedCountries when Countries has none
	OtherCountries bool
	// BlockedCountries are the countries whose nodes are never used
	BlockedCountries []string
	// MinDownloadMbps is the node download floor the listing asks for, 0 for none
	MinDownloadMbps int
	// MaxLifetime is the longest a placement may run with its extensions, 2 hours when 0
	MaxLifetime time.Duration
	// StartBilled is the time Lium bills before the paid clock, priced at the size's cap
	StartBilled time.Duration
	// OpsPerSecond paces every API call, none when 0
	OpsPerSecond int
	// CreatesPerSecond paces every rent beside OpsPerSecond, none when 0
	CreatesPerSecond int
}

// size is one size sold and the resources it guarantees
type size struct {
	SizeSpec
	resources provider.Resources
}

// Provider sells Lium pods, answering sizes and prices from memory
type Provider struct {
	api         *api
	log         *slog.Logger
	sizes       []size // lowest cap first
	maxLifetime time.Duration
	startBilled time.Duration
}

// New checks the options
func New(opts Options) (*Provider, error) {
	return newProvider(opts, time.Now)
}

// newProvider checks the options and reads time from now
func newProvider(opts Options, now func() time.Time) (*Provider, error) {
	switch {
	case opts.APIKey == "":
		return nil, errors.New("lium: an API key is required")
	case len(opts.Countries) == 0:
		return nil, errors.New("lium: at least one country is required, the node countries " +
			"tried first")
	case opts.StartBilled < 0:
		return nil, errors.New("lium: start billed must not be negative")
	}
	sizes, err := sizesFor(opts.Sizes)
	if err != nil {
		return nil, err
	}
	log := cmp.Or(opts.Log, slog.Default())
	return &Provider{
		api: &api{
			base:    strings.TrimRight(cmp.Or(opts.BaseURL, defaultBaseURL), "/"),
			key:     opts.APIKey,
			client:  &http.Client{CheckRedirect: provider.NoRedirect},
			log:     log,
			ops:     limiter(opts.OpsPerSecond),
			creates: limiter(opts.CreatesPerSecond),
		},
		log:         log,
		sizes:       sizes,
		maxLifetime: cmp.Or(opts.MaxLifetime, defaultMaxLifetime),
		startBilled: opts.StartBilled,
	}, nil
}

// sizesFor checks the sizes and orders them lowest cap first
func sizesFor(specs []SizeSpec) ([]size, error) {
	if len(specs) == 0 {
		return nil, errors.New("lium: at least one size is required")
	}
	out := make([]size, 0, len(specs))
	for _, s := range specs {
		if err := checkSize(s, out); err != nil {
			return nil, err
		}
		s.Match = slices.Clone(s.Match)
		out = append(out, size{SizeSpec: s, resources: provider.Resources{
			VCPU: s.VCPU, MemoryMiB: s.MemoryMiB, DiskGiB: s.DiskGiB, GPUCount: s.GPUs,
			GPUMemoryGiB: s.GPUMemoryGiB, GPUModel: s.Model,
		}})
	}
	slices.SortStableFunc(out, func(a, b size) int {
		return cmp.Compare(a.MaxHourlyMicros, b.MaxHourlyMicros)
	})
	return out, nil
}

// checkSize refuses a size that could never be sold right, before the sizes already read
func checkSize(s SizeSpec, before []size) error {
	switch {
	case s.Name == "":
		return errors.New("lium: every size needs a name")
	case slices.ContainsFunc(before, func(b size) bool { return b.Name == s.Name }):
		return fmt.Errorf("lium: size %q repeats the size of another", s.Name)
	case !gpu.Known(s.Model):
		return fmt.Errorf("lium: size %q model must be a canonical GPU model, one of %s", s.Name,
			strings.Join(gpu.Models(), ", "))
	case s.GPUs < 1:
		return fmt.Errorf("lium: size %q gpus must be 1 or more", s.Name)
	case s.MaxHourlyMicros <= 0:
		return fmt.Errorf("lium: size %q max hourly micros must be above 0", s.Name)
	case len(s.Match) == 0 || slices.Contains(s.Match, ""):
		return fmt.Errorf("lium: size %q match must name gpu_class values, none empty", s.Name)
	}
	for _, name := range s.Match {
		// A name of another model would sell that model as this one
		if model, ok := gpu.FromLium(name); ok && model != s.Model {
			return fmt.Errorf("lium: size %q match %q is a %s, not %s", s.Name, name, model,
				s.Model)
		}
	}
	return nil
}

// Name returns "lium"
func (p *Provider) Name() string { return providerName }

// Capabilities reports root exec on amd64 pods, with no egress control and no key dedup
func (p *Provider) Capabilities() provider.Capabilities {
	return provider.Capabilities{
		GPU: true, Arch: arch, NeedsRunnerUpload: true, MaxLifetime: p.maxLifetime,
	}
}

// Sizes lists the sizes sold, lowest cap first
func (p *Provider) Sizes() []provider.Size {
	out := make([]provider.Size, 0, len(p.sizes))
	for _, s := range p.sizes {
		out = append(out, provider.Size{Name: s.Name, Resources: s.resources})
	}
	return out
}

// HourlyCostMicros is the size's cap, since no node above it is rented, 0 for a size not sold
func (p *Provider) HourlyCostMicros(sz provider.Size) int64 {
	if s, ok := p.size(sz.Name); ok {
		return s.MaxHourlyMicros
	}
	return 0
}

// BillingIncrement is one second, Lium bills per second with no minimum
func (p *Provider) BillingIncrement() time.Duration { return time.Second }

// StartCostMicros prices the pull, start and boot Lium bills before the paid clock at the cap
func (p *Provider) StartCostMicros(sz provider.Size) (int64, bool) {
	s, ok := p.size(sz.Name)
	if !ok {
		return 0, false
	}
	seconds := int64((p.startBilled + time.Second - 1) / time.Second)
	return ceilDiv(s.MaxHourlyMicros*seconds, 3600), true
}

func (p *Provider) size(name string) (size, bool) {
	i := slices.IndexFunc(p.sizes, func(s size) bool { return s.Name == name })
	if i < 0 {
		return size{}, false
	}
	return p.sizes[i], true
}

// ceilDiv divides a non-negative a by a positive b, rounding up
func ceilDiv(a, b int64) int64 { return (a + b - 1) / b }
