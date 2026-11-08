// Package providers builds the provider registry the router binaries run with
package providers

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/oynozan/bugnet-router/internal/config"
	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/provider/boat"
	"github.com/oynozan/bugnet-router/internal/provider/boxd"
	"github.com/oynozan/bugnet-router/internal/provider/e2b"
	"github.com/oynozan/bugnet-router/internal/provider/koyeb"
	"github.com/oynozan/bugnet-router/internal/provider/local"
)

// localSize covers every package, since a host sandbox has no fixed shape
var localSize = provider.Size{Name: "local", Resources: provider.Resources{
	VCPU: math.MaxInt32, MemoryMiB: math.MaxInt32, DiskGiB: math.MaxInt32,
	GPUCount: math.MaxInt32, GPUMemoryGiB: math.MaxInt32,
}}

// Build returns the configured providers in their listed order with the features they passed
func Build(cfg config.Config, vk valkey.Client) (*provider.Registry, error) {
	var ps []provider.Provider
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
			p, err := buildE2B(cfg.E2B, cfg.ProviderKeys["e2b"])
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
			p, err := buildBoat(cfg.Boat, cfg.ProviderKeys["boat"], vk, cfg.ValkeyPrefix)
			if err != nil {
				return nil, fmt.Errorf("providers: %w", err)
			}
			ps = append(ps, p)
		default:
			return nil, fmt.Errorf("providers: %s has no adapter", name)
		}
	}
	reg, err := provider.NewRegistry(ps, provider.Features{
		Preview: cfg.Previews.Providers, SSH: cfg.SSH.Providers, Secrets: cfg.Secrets.Providers,
		BoatNativeSSH: cfg.Boat.SSHPath == "native",
	})
	if err != nil {
		return nil, fmt.Errorf("providers: %w", err)
	}
	return reg, nil
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
func buildE2B(cfg config.E2B, key string) (*e2b.Provider, error) {
	if key == "" {
		return nil, errors.New("e2b needs E2B_API_KEY")
	}
	var templates []e2b.Template
	for _, t := range cfg.Templates {
		templates = append(templates, e2b.Template{
			Name: t.Size, TemplateID: t.Template, VCPU: t.VCPU, MemoryMiB: t.MemoryMiB,
			DiskGiB: t.DiskGiB,
		})
	}
	if len(templates) == 0 {
		slog.Warn("e2b has no templates and is never a candidate, set e2b.templates")
	}
	p, err := e2b.New(e2b.Options{
		APIKey: key, Domain: cfg.Domain, Templates: templates, MaxLifetime: cfg.MaxLifetime,
	})
	if err != nil {
		return nil, fmt.Errorf("e2b.templates: %w", err)
	}
	return p, nil
}

// boxdOptions maps the settings boxd reads, the gate's port and route among them, to its options
func boxdOptions(cfg config.Config, key string) boxd.Options {
	return boxd.Options{
		APIKey: key, Endpoint: cfg.Boxd.Endpoint, Region: cfg.Boxd.Region,
		GatePort: cfg.Previews.GatePort, GateRoute: cfg.Boxd.GateRoute,
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
	}
}

// buildBoat builds Boat from its settings, every machine type is a size unless some are named
func buildBoat(
	cfg config.Boat, key string, vk valkey.Client, prefix string,
) (*boat.Provider, error) {
	if key == "" {
		return nil, errors.New("boat needs BOAT_API_KEY")
	}
	p, err := boat.New(boat.Options{APIKey: key, TypeBySize: cfg.Types, Valkey: vk, Prefix: prefix})
	if err != nil {
		return nil, fmt.Errorf("boat.types: %w", err)
	}
	return p, nil
}
