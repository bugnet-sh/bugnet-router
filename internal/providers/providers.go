// Package providers builds the provider registry the router binaries run with
package providers

import (
	"cmp"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/netip"
	"os"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/boat"
	"github.com/bugnet-sh/bugnet-router/internal/provider/boxd"
	"github.com/bugnet-sh/bugnet-router/internal/provider/e2b"
	"github.com/bugnet-sh/bugnet-router/internal/provider/hf"
	"github.com/bugnet-sh/bugnet-router/internal/provider/koyeb"
	"github.com/bugnet-sh/bugnet-router/internal/provider/lium"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/provider/traced"
	"github.com/bugnet-sh/bugnet-router/internal/telemetry"
)

// localSize covers every package, since a host sandbox has no fixed shape
var localSize = provider.Size{Name: "local", Resources: provider.Resources{
	VCPU: math.MaxInt32, MemoryMiB: math.MaxInt32, DiskGiB: math.MaxInt32,
	GPUCount: math.MaxInt32, GPUMemoryGiB: math.MaxInt32,
}}

// Option adds to what Build reads beside the settings
type Option func(*options)

type options struct {
	// packages are the packages sold, held to margin_floor at each new HF price
	packages []catalog.Package
}

// WithPackages names the packages sold, so a new HF price that breaks one's margin logs an error
func WithPackages(pkgs []catalog.Package) Option {
	return func(o *options) { o.packages = pkgs }
}

// Build returns the configured providers in their listed order with the features they passed,
// each wrapped with traced.Wrap while tel is enabled
func Build(cfg config.Config, vk valkey.Client, tel *telemetry.Telemetry, opts ...Option) (
	_ *provider.Registry, err error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	var ps []provider.Provider
	// A refusal closes the providers built so far, Lium's and HF's background reads among them
	defer func() {
		if err != nil {
			closeAll(ps)
		}
	}()
	for _, name := range cfg.Providers {
		switch name {
		case "local":
			p, err := local.Open(cfg.LocalEnabled, local.Options{
				Name:             "local",
				Root:             cfg.LocalRoot,
				Sizes:            []provider.Size{localSize},
				HourlyCostMicros: cfg.LocalHourlyMicros,
				Increment:        time.Second,
				RunnerPath:       cfg.LocalRunnerPath,
			})
			if err != nil {
				return nil, fmt.Errorf("providers: %w", err)
			}
			ps = append(ps, p)
		case "boxd":
			key := cfg.ProviderKeys["boxd"]
			if key == "" {
				return nil, fmt.Errorf("providers: boxd needs BOXD_API_KEY")
			}
			p, err := boxd.New(boxdOptions(cfg, key))
			if err != nil {
				return nil, fmt.Errorf("providers: %w", err)
			}
			ps = append(ps, p)
		case "e2b":
			p, err := buildE2B(cfg, cfg.ProviderKeys["e2b"])
			if err != nil {
				return nil, fmt.Errorf("providers: %w", err)
			}
			ps = append(ps, p)
		case "koyeb":
			p, err := buildKoyeb(cfg, cfg.ProviderKeys["koyeb"])
			if err != nil {
				return nil, fmt.Errorf("providers: %w", err)
			}
			ps = append(ps, p)
		case "boat":
			p, err := buildBoat(
				cfg.Boat, cfg.ProviderKeys["boat"], vk, cfg.ValkeyPrefix, cfg.Hooks.BoatWindow,
				cfg.Images.Boat.Ref)
			if err != nil {
				return nil, fmt.Errorf("providers: %w", err)
			}
			ps = append(ps, p)
		case "lium":
			p, err := buildLium(cfg)
			if err != nil {
				return nil, fmt.Errorf("providers: %w", err)
			}
			ps = append(ps, p)
		case "hf":
			p, err := buildHF(cfg, vk, o.packages)
			if err != nil {
				return nil, fmt.Errorf("providers: %w", err)
			}
			ps = append(ps, p)
		default:
			return nil, fmt.Errorf("providers: %s has no adapter", name)
		}
	}
	if tel.Enabled() {
		tr := tel.Tracer("bugnet-router/provider")
		for i, p := range ps {
			ps[i] = traced.Wrap(p, tr, tel.SandboxHash)
		}
	}
	reg, err := provider.NewRegistry(ps, features(cfg))
	if err != nil {
		return nil, fmt.Errorf("providers: %w", err)
	}
	return reg, nil
}

// closeAll closes every provider in ps that has a Close
func closeAll(ps []provider.Provider) {
	for _, p := range ps {
		if c, ok := provider.As[io.Closer](p); ok {
			_ = c.Close()
		}
	}
}

// features are the data-plane features and images the settings give each provider
func features(cfg config.Config) provider.Features {
	return provider.Features{
		Preview: cfg.Previews.Providers, SSH: cfg.SSH.Providers, Secrets: cfg.Secrets.Providers,
		NativeSSH: cfg.NativeSSH(),
		Images:    cfg.Images.Versions(cfg.Providers), ImageExempt: cfg.Images.Exempt,
	}
}

// runnerKey names the setting that points at each architecture's bugnet-run build
var runnerKey = map[string]string{"amd64": "runner.linux_amd64", "arm64": "runner.linux_arm64"}

// Runners reads once the bugnet-run build of every architecture a provider in reg uploads to
func Runners(cfg config.Config, reg *provider.Registry) (map[string][]byte, error) {
	paths := map[string]string{"amd64": cfg.RunnerLinuxAMD64, "arm64": cfg.RunnerLinuxARM64}
	out := map[string][]byte{}
	for _, p := range reg.Ordered() {
		caps := p.Capabilities()
		arch := caps.RunnerArch()
		if _, read := out[arch]; read || !caps.NeedsRunnerUpload {
			continue
		}
		key := runnerKey[arch]
		if key == "" {
			return nil, fmt.Errorf("providers: %s runs %s, which bugnet-run is not built for",
				p.Name(), arch)
		}
		if paths[arch] == "" {
			return nil, fmt.Errorf("providers: %s uploads bugnet-run, so %s must name its "+
				"linux/%s build, make build in bugnet-run writes it to dist", p.Name(), key, arch)
		}
		data, err := os.ReadFile(paths[arch])
		if err == nil && len(data) == 0 {
			err = errors.New("the file is empty")
		}
		if err != nil {
			return nil, fmt.Errorf("providers: read the bugnet-run build in %s: %w", key, err)
		}
		out[arch] = data
	}
	return out, nil
}

// buildE2B builds E2B from its settings, an adapter without templates is never a candidate
func buildE2B(c config.Config, key string) (*e2b.Provider, error) {
	if key == "" {
		return nil, errors.New("e2b needs E2B_API_KEY")
	}
	opts := e2bOptions(c, key)
	if len(opts.Templates) == 0 {
		slog.Warn("e2b has no templates and is never a candidate, set e2b.templates")
	}
	p, err := e2b.New(opts)
	if err != nil {
		return nil, fmt.Errorf("e2b.templates: %w", err)
	}
	return p, nil
}

// e2bOptions maps E2B's settings and the hardening deny ranges, blocked outside the VM
func e2bOptions(c config.Config, key string) e2b.Options {
	var templates []e2b.Template
	for _, t := range c.E2B.Templates {
		templates = append(templates, e2b.Template{
			Name: t.Size, TemplateID: t.Template, VCPU: t.VCPU, MemoryMiB: t.MemoryMiB,
			DiskGiB: t.DiskGiB,
		})
	}
	var deny []netip.Prefix
	for _, r := range c.Hardening.DenyRanges {
		if p, err := netip.ParsePrefix(r); err == nil {
			deny = append(deny, p)
		}
	}
	return e2b.Options{
		APIKey: key, Domain: c.E2B.Domain, Templates: templates, MaxLifetime: c.E2B.MaxLifetime,
		DenyOut: deny, TeamID: c.E2B.TeamID,
	}
}

// boxdOptions maps the settings boxd reads, the gate's port and route among them, to its options
func boxdOptions(cfg config.Config, key string) boxd.Options {
	return boxd.Options{
		APIKey: key, Endpoint: cfg.Boxd.Endpoint, Region: cfg.Boxd.Region,
		GatePort: cfg.Previews.GatePort, GateRoute: cfg.Boxd.GateRoute,
		ParkedGiB: cfg.Boxd.ParkedGiB, Snapshots: cfg.Images.Boxd.Refs,
	}
}

// buildKoyeb builds Koyeb from its settings, every fra instance type is a size unless named
func buildKoyeb(cfg config.Config, token string) (*koyeb.Provider, error) {
	if token == "" {
		return nil, errors.New("koyeb needs KOYEB_API_TOKEN")
	}
	p, err := koyeb.New(koyebOptions(cfg, token))
	switch {
	case errors.Is(err, koyeb.ErrTransferSizes):
		return nil, fmt.Errorf("transfers.koyeb_part or transfers.koyeb_buffer_total: %w", err)
	case err != nil:
		return nil, fmt.Errorf("koyeb.instance_types: %w", err)
	}
	return p, nil
}

// koyebOptions maps the settings Koyeb reads, its upload parts among them, to its options
func koyebOptions(cfg config.Config, token string) koyeb.Options {
	return koyeb.Options{
		APIToken: token, Region: cfg.Koyeb.Region, InstanceTypeBySize: cfg.Koyeb.InstanceTypes,
		PartBytes: cfg.Transfers.KoyebPart, BufferTotal: cfg.Transfers.KoyebBufferTotal,
		OrganizationID:       cfg.Koyeb.OrganizationID,
		PoolIdleHourlyMicros: cfg.Koyeb.PoolIdleHourlyMicros,
		Image:                cfg.Images.Koyeb.Ref,
	}
}

// buildBoat builds Boat from its settings, every machine type is a size unless some are named
func buildBoat(
	cfg config.Boat, key string, vk valkey.Client, prefix string, window time.Duration,
	from string,
) (*boat.Provider, error) {
	if key == "" {
		return nil, errors.New("boat needs BOAT_API_KEY")
	}
	p, err := boat.New(boat.Options{
		APIKey: key, TypeBySize: cfg.Types, Valkey: vk, Prefix: prefix, HookWindow: window,
		EgressFamilies: cfg.EgressFamilies, EgressIPv6Bits: cfg.EgressIPv6Bits,
		From: from,
	})
	if err != nil {
		return nil, fmt.Errorf("boat.types: %w", err)
	}
	return p, nil
}

// buildLium builds Lium from its settings, its key and the seed of every pod's SSH key
func buildLium(cfg config.Config) (*lium.Provider, error) {
	key, seed := cfg.ProviderKeys["lium"], cfg.ProviderSeeds["lium"]
	switch {
	case key == "":
		return nil, errors.New("lium needs LIUM_API_KEY")
	case len(seed) == 0:
		return nil, errors.New("lium needs LIUM_SSH_SEED")
	}
	return lium.New(liumOptions(cfg, key, seed))
}

// liumOptions maps Lium's settings, max_concurrent and account_every sizing its balance headroom
func liumOptions(cfg config.Config, key string, seed []byte) lium.Options {
	var sizes []lium.SizeSpec
	for _, s := range cfg.Lium.Sizes {
		sizes = append(sizes, lium.SizeSpec{
			Name: s.Size, Model: s.Model, Match: s.Match, GPUs: s.GPUs,
			GPUMemoryGiB: s.GPUMemoryGiB, VCPU: s.VCPU, MemoryMiB: s.MemoryMiB,
			DiskGiB: s.DiskGiB, MaxHourlyMicros: s.MaxHourlyMicros,
		})
	}
	l := cfg.Lium
	return lium.Options{
		APIKey: key, SSHSeed: seed, Sizes: sizes, Countries: l.Countries,
		OtherCountries: l.OtherCountries, BlockedCountries: l.BlockedCountries,
		MinDownloadMbps: l.MinDownloadMbps, MaxLifetime: l.MaxLifetime,
		StartBilled: l.StartBilled, OpsPerSecond: l.OpsPerSecond,
		CreatesPerSecond: cfg.CreatesPerSecond["lium"], Image: cfg.Images.Lium.Ref,
		ImageVersion: cfg.Images.Lium.Version, TemplateVolume: l.TemplateVolume,
		Termination:   l.Termination,
		MaxConcurrent: cmp.Or(cfg.MaxConcurrent["lium"], config.DefaultMaxConcurrent),
		AccountEvery:  cfg.LimitsPoll.AccountEvery,
	}
}

// buildHF builds HF, logging an error when an hourly price read puts one of pkgs under its margin
func buildHF(cfg config.Config, vk valkey.Client, pkgs []catalog.Package) (*hf.Provider, error) {
	token, seed := cfg.ProviderKeys["hf"], cfg.ProviderSeeds["hf"]
	switch {
	case token == "":
		return nil, errors.New("hf needs HF_JOBS_TOKEN")
	case len(seed) == 0:
		return nil, errors.New("hf needs HF_SANDBOX_SEED")
	case cfg.HF.SBXServer == "baked":
		return nil, errors.New(`hf.sbx_server must be "fetch" until an image holds sbx-server`)
	}
	opts := hfOptions(cfg, token, seed)
	// router-api's creates and waits see what router-worker's account check reads
	opts.Valkey, opts.Prefix = vk, cfg.ValkeyPrefix
	var p *hf.Provider
	// The first read may end before New returns, so the check waits for p
	built := make(chan struct{})
	if len(pkgs) > 0 {
		opts.OnChange = func() {
			<-built
			if err := cfg.CheckGPUMargin(pkgs, []provider.Provider{p}); err != nil {
				slog.Error("hf: a new price puts a package under margin_floor", "err", err)
			}
		}
	}
	var err error
	if p, err = hf.New(opts); err != nil {
		return nil, err
	}
	close(built)
	return p, nil
}

// hfOptions maps HF's settings to its options
func hfOptions(cfg config.Config, token string, seed []byte) hf.Options {
	h := cfg.HF
	return hf.Options{
		Token: token, Namespace: h.Namespace, Flavors: h.Flavors, MaxLifetime: h.MaxLifetime,
		StartBilled: h.StartBilled, OpsPerSecond: h.OpsPerSecond, SandboxSeed: seed,
		Image: cfg.Images.HF.Ref, ExtendRoom: h.ExtendRoom, TimeoutMargin: h.TimeoutMargin,
		PollEvery: h.PollEvery, SBXServerSHA256: h.SBXServerSHA256,
	}
}
