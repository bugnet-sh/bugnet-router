// Package hf sells GPU sandboxes as Hugging Face Jobs, one Job per sandbox on one GPU flavor
package hf

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/bugnet-sh/bugnet-router/internal/gpu"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const (
	providerName       = "hf"
	defaultMaxLifetime = 2 * time.Hour
	// defaultPollEvery spaces the Job reads of a wait, so ten waits stay under the Hub's limit
	defaultPollEvery = 3 * time.Second
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
	// OnChange runs after a hardware read changed a price or the flavors sold, for a margin check
	OnChange func()
	Log      *slog.Logger // slog.Default when nil
	// SandboxSeed is HF_SANDBOX_SEED decoded, the 32 bytes every sandbox's token derives from
	SandboxSeed []byte
	// Image is images.hf.ref, the image by digest Jobs start from unless a create names one
	Image string
	// ExtendRoom is the room for extensions in each Job's timeout
	ExtendRoom time.Duration
	// TimeoutMargin is each Job's timeout beyond its lifetime and room
	TimeoutMargin time.Duration
	// PollEvery spaces the Job reads of a wait for RUNNING, 3 seconds when 0
	PollEvery time.Duration
	// SBXServerSHA256 is the digest of the sbx-server build each Job fetches at boot
	SBXServerSHA256 string
}

// Provider sells HF Jobs, answering sizes and prices from memory
type Provider struct {
	jobs        *jobsClient
	log         *slog.Logger
	hw          *hardware
	maxLifetime time.Duration
	startBilled time.Duration
	// hwEvery spaces the hardware reads, hwRetry the read after one that failed
	hwEvery, hwRetry time.Duration
	onChange         func()
	// stop ends the hardware reads New starts, which close done
	stop context.CancelFunc
	done chan struct{}

	seed              []byte
	image, sbxSHA256  string
	extendRoom        time.Duration
	margin, pollEvery time.Duration
	sbx               *sbxClient
	now               func() time.Time
	// kmu guards kept, the Job each key got in this process, and creating, each key's create
	kmu      sync.Mutex
	kept     map[string]keptJob
	creating map[string]chan struct{}
}

// New checks the options and starts the hardware reads, which Close stops
func New(opts Options) (*Provider, error) {
	p, err := newProvider(opts)
	if err != nil {
		return nil, err
	}
	p.start()
	return p, nil
}

// start begins the hardware reads
func (p *Provider) start() {
	ctx, cancel := context.WithCancel(context.Background())
	p.stop, p.done = cancel, make(chan struct{})
	go func() {
		defer close(p.done)
		p.readHardware(ctx)
	}()
}

// Close stops the hardware reads and waits for them to end
func (p *Provider) Close() error {
	if p.stop != nil {
		p.stop()
		<-p.done
	}
	return nil
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
	case len(opts.SandboxSeed) != seedBytes:
		return nil, errors.New("hf: a sandbox seed of 32 bytes is required")
	case opts.Image != "" && !imageByDigest.MatchString(opts.Image):
		return nil, errors.New("hf: the image must be named by digest, name@sha256:<64 hex>")
	case opts.ExtendRoom < 0:
		return nil, errors.New("hf: the extend room must not be negative")
	case opts.TimeoutMargin < 0:
		return nil, errors.New("hf: the timeout margin must not be negative")
	case opts.PollEvery < 0:
		return nil, errors.New("hf: the poll interval must not be negative")
	case !sha256Hex.MatchString(opts.SBXServerSHA256):
		return nil, errors.New("hf: the sbx-server sha256 must be 64 lowercase hex digits")
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
		hwEvery:     cmp.Or(opts.HardwareEvery, hardwareEvery),
		hwRetry:     hardwareRetry,
		onChange:    opts.OnChange,
		seed:        slices.Clone(opts.SandboxSeed),
		image:       opts.Image,
		sbxSHA256:   opts.SBXServerSHA256,
		extendRoom:  opts.ExtendRoom,
		margin:      opts.TimeoutMargin,
		pollEvery:   cmp.Or(opts.PollEvery, defaultPollEvery),
		sbx:         newSBXClient(),
		now:         time.Now,
		kept:        map[string]keptJob{},
		creating:    map[string]chan struct{}{},
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
