// Package providers builds the provider registry the router binaries run with
package providers

import (
	"cmp"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/oynozan/bugnet-router/internal/config"
	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/provider/boxd"
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
		default:
			return nil, fmt.Errorf("providers: %s has no adapter yet", name)
		}
	}
	return provider.NewRegistry(ps), nil
}
