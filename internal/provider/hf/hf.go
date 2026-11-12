// Package hf sells GPU sandboxes as Hugging Face Jobs, one Job per sandbox on one GPU flavor
package hf

import (
	"cmp"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/bugnet-sh/bugnet-router/internal/gpu"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const (
	providerName       = "hf"
	defaultMaxLifetime = 2 * time.Hour
	// arch is the only architecture sbx-server and our image are built for
	arch = "amd64"
)

// namespaceName is an organization name as the Hub writes one, so it is one plain path segment
var namespaceName = regexp.MustCompile(`^[A-Za-z0-9_](?:[A-Za-z0-9_.-]{0,94}[A-Za-z0-9_])?$`)

// Options configures the adapter
type Options struct {
	// Token is HF_JOBS_TOKEN, sent to huggingface.co alone
	Token string
	// Namespace is hf.namespace, the organization the Jobs run and bill under
	Namespace string
	// Flavors are the flavors sold, every GPU flavor when empty
	Flavors []string
	// MaxLifetime is the longest a placement may run with its extensions, 2 hours when 0
	MaxLifetime time.Duration
	// StartBilled is the time HF bills before the paid clock, priced in whole minutes
	StartBilled time.Duration
	// OpsPerSecond paces every Jobs call, none when 0
	OpsPerSecond int
	// HardwareEvery spaces the hardware list reads, an hour when 0
	HardwareEvery time.Duration
	Log           *slog.Logger // slog.Default when nil
}

// Provider sells HF Jobs, answering sizes and prices from memory
type Provider struct {
	jobs        *jobsClient
	log         *slog.Logger
	hw          *hardware
	maxLifetime time.Duration
	startBilled time.Duration
}

// newProvider checks the options and builds the adapter
func newProvider(opts Options) (*Provider, error) {
	switch {
	case opts.Token == "" || strings.ContainsFunc(opts.Token, func(r rune) bool {
		return r > unicode.MaxASCII || !unicode.IsGraphic(r) || unicode.IsSpace(r)
	}):
		return nil, errors.New("hf: a Jobs token is required, printable with no spaces")
	case opts.Namespace == "":
		return nil, errors.New("hf: a namespace is required, the organization the Jobs bill to")
	case !namespaceName.MatchString(opts.Namespace) || strings.Contains(opts.Namespace, "..") ||
		strings.Contains(opts.Namespace, "--"):
		return nil, errors.New("hf: the namespace must be an organization name, letters, " +
			"digits, '-', '_' and '.'")
	case opts.StartBilled < 0:
		return nil, errors.New("hf: start billed must not be negative")
	case opts.OpsPerSecond < 0:
		return nil, errors.New("hf: ops per second must not be negative")
	case opts.HardwareEvery < 0:
		return nil, errors.New("hf: the hardware interval must not be negative")
	}
	for _, f := range opts.Flavors {
		if !gpu.HFFlavor(f) {
			return nil, fmt.Errorf("hf: flavor %q is not an HF GPU flavor, use one of %s", f,
				strings.Join(gpu.HFFlavors(), ", "))
		}
	}
	log := cmp.Or(opts.Log, slog.Default())
	return &Provider{
		jobs:        newJobsClient(opts.Token, opts.Namespace, opts.OpsPerSecond, log),
		log:         log,
		hw:          newHardware(opts.Flavors),
		maxLifetime: cmp.Or(opts.MaxLifetime, defaultMaxLifetime),
		startBilled: opts.StartBilled,
	}, nil
}

// Name returns "hf"
func (p *Provider) Name() string { return providerName }

// Capabilities reports root exec on amd64 VMs whose exec text is lossy, with no key dedup
func (p *Provider) Capabilities() provider.Capabilities {
	return provider.Capabilities{
		GPU: true, Arch: arch, NeedsRunnerUpload: true, MaxLifetime: p.maxLifetime,
		LossyExecText: true,
	}
}

// SizeFor picks the cheapest flavor sold whose model matches a GPU floor and that covers it
func (p *Provider) SizeFor(floor provider.Resources) (provider.Size, bool) {
	if floor.GPUCount == 0 && floor.GPUMemoryGiB == 0 && floor.GPUModel == "" {
		return provider.Size{}, false
	}
	for _, f := range p.hw.sold() {
		r := f.resources
		if (floor.GPUModel == "" || floor.GPUModel == r.GPUModel) && r.VCPU >= floor.VCPU &&
			r.MemoryMiB >= floor.MemoryMiB && r.DiskGiB >= floor.DiskGiB &&
			r.GPUCount >= floor.GPUCount && r.GPUMemoryGiB >= floor.GPUMemoryGiB {
			return provider.Size{Name: f.name, Resources: r}, true
		}
	}
	return provider.Size{}, false
}

// Sizes lists the flavors sold, cheapest first
func (p *Provider) Sizes() []provider.Size {
	sold := p.hw.sold()
	out := make([]provider.Size, 0, len(sold))
	for _, f := range sold {
		out = append(out, provider.Size{Name: f.name, Resources: f.resources})
	}
	return out
}

// HourlyCostMicros is the flavor's hour with the exposed ports' fee, 0 for a size not sold
func (p *Provider) HourlyCostMicros(sz provider.Size) int64 {
	if f, ok := p.hw.flavor(sz.Name); ok {
		return f.hourlyMicros()
	}
	return 0
}

// BillingIncrement is one minute, HF bills Jobs by the minute
func (p *Provider) BillingIncrement() time.Duration { return time.Minute }

// StartCostMicros prices the Starting time, boot steps and rounding HF bills before the paid clock
func (p *Provider) StartCostMicros(sz provider.Size) (int64, bool) {
	f, ok := p.hw.flavor(sz.Name)
	if !ok {
		return 0, false
	}
	minutes := int64((p.startBilled + time.Minute - 1) / time.Minute)
	return ceilDiv(f.hourlyMicros()*minutes, 60), true
}

// ceilDiv divides a non-negative a by a positive b, rounding up
func ceilDiv(a, b int64) int64 { return (a + b - 1) / b }
