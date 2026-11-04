// Package providers builds the provider registry the router binaries run with
package providers

import (
	"cmp"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"time"

	"github.com/oynozan/bugnet-router/internal/config"
	"github.com/oynozan/bugnet-router/internal/provider"
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

// Build returns the configured providers in preference order
func Build(cfg config.Config) (*provider.Registry, error) {
	var ps []provider.Provider
	for _, name := range cfg.Providers {
		switch name {
		case "local":
			p, err := local.FromEnv(local.Options{
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
			p, err := boxd.New(boxd.Options{
				APIKey:   key,
				Endpoint: os.Getenv("BUGNET_BOXD_ENDPOINT"),
				Region:   cmp.Or(os.Getenv("BUGNET_BOXD_REGION"), "fra"),
			})
			if err != nil {
				return nil, fmt.Errorf("providers: %w", err)
			}
			ps = append(ps, p)
		case "e2b":
			p, err := buildE2B(cfg.ProviderKeys["e2b"])
			if err != nil {
				return nil, fmt.Errorf("providers: %w", err)
			}
			ps = append(ps, p)
		case "koyeb":
			p, err := buildKoyeb(cfg.ProviderKeys["koyeb"])
			if err != nil {
				return nil, fmt.Errorf("providers: %w", err)
			}
			ps = append(ps, p)
		default:
			return nil, fmt.Errorf("providers: %s has no adapter yet", name)
		}
	}
	return provider.NewRegistry(ps), nil
}

// buildE2B reads the E2B settings, an adapter without templates is built but never a candidate
func buildE2B(key string) (*e2b.Provider, error) {
	if key == "" {
		return nil, errors.New("e2b needs E2B_API_KEY")
	}
	templates, err := e2b.ParseTemplates(os.Getenv("BUGNET_E2B_TEMPLATES"))
	if err != nil {
		return nil, fmt.Errorf("BUGNET_E2B_TEMPLATES: %w", err)
	}
	var lifetime time.Duration
	if v := os.Getenv("BUGNET_E2B_MAX_LIFETIME"); v != "" {
		lifetime, err = time.ParseDuration(v)
		if err != nil || lifetime <= 0 {
			return nil, fmt.Errorf("BUGNET_E2B_MAX_LIFETIME %q is not a positive duration", v)
		}
	}
	if len(templates) == 0 {
		slog.Warn("e2b has no templates and is never a candidate, set BUGNET_E2B_TEMPLATES")
	}
	return e2b.New(e2b.Options{
		APIKey:      key,
		Domain:      os.Getenv("BUGNET_E2B_DOMAIN"),
		Templates:   templates,
		MaxLifetime: lifetime,
	})
}

// buildKoyeb reads the Koyeb settings, every fra instance type is a size unless some are named
func buildKoyeb(token string) (*koyeb.Provider, error) {
	if token == "" {
		return nil, errors.New("koyeb needs KOYEB_API_TOKEN")
	}
	types, err := koyeb.ParseInstanceTypes(os.Getenv("BUGNET_KOYEB_INSTANCE_TYPES"))
	if err != nil {
		return nil, fmt.Errorf("BUGNET_KOYEB_INSTANCE_TYPES: %w", err)
	}
	return koyeb.New(koyeb.Options{
		APIToken:           token,
		Region:             cmp.Or(os.Getenv("BUGNET_KOYEB_REGION"), "fra"),
		InstanceTypeBySize: types,
	})
}
