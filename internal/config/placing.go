package config

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"time"
)

// Boot step timeouts the saga runs each sandbox's boot under, which gpu_placing_budget must outlast
const (
	// DefaultReadyTimeout is a provider's ready_timeout when its table sets none
	DefaultReadyTimeout = 60 * time.Second
	// UploadTimeout bounds the baked runner's ping, and the upload with its ping after it
	UploadTimeout = 60 * time.Second
	// SetupTimeout covers update-ca-certificates and the gate's own 10 s health wait
	SetupTimeout = 90 * time.Second
	// HardenTimeout covers an nftables install of up to 60 s and the load
	HardenTimeout = 2 * time.Minute
	// GPUCreateBudget is the longest a GPU provider's Create runs
	GPUCreateBudget = 20 * time.Second
)

const (
	minReadyTimeout     = 60 * time.Second
	maxReadyTimeout     = 15 * time.Minute
	maxGPUPlacingBudget = time.Hour
)

// MinGPUPlacingBudget is Lium's slowest failing boot plus the next GPU provider's create
func MinGPUPlacingBudget(liumReady time.Duration) time.Duration {
	return GPUCreateBudget + liumReady + 2*UploadTimeout + SetupTimeout + HardenTimeout +
		GPUCreateBudget
}

// placing reads each provider's ready timeout, the GPU order and the GPU placing budget
func (f file) placing(c *Config) error {
	d := defaults()
	c.ReadyTimeouts = map[string]time.Duration{}
	for _, r := range []struct{ name, raw, def string }{
		{"local", f.Local.ReadyTimeout, d.Local.ReadyTimeout},
		{"boxd", f.Boxd.ReadyTimeout, d.Boxd.ReadyTimeout},
		{"e2b", f.E2B.ReadyTimeout, d.E2B.ReadyTimeout},
		{"koyeb", f.Koyeb.ReadyTimeout, d.Koyeb.ReadyTimeout},
		{"boat", f.Boat.ReadyTimeout, d.Boat.ReadyTimeout},
		{"lium", f.Lium.ReadyTimeout, d.Lium.ReadyTimeout},
		{"hf", f.HF.ReadyTimeout, d.HF.ReadyTimeout},
	} {
		got, err := time.ParseDuration(cmp.Or(r.raw, r.def))
		if err != nil || got < minReadyTimeout || got > maxReadyTimeout {
			return fmt.Errorf("%s.ready_timeout must be a Go duration from %s to %s such as %s",
				r.name, minReadyTimeout, maxReadyTimeout, r.def)
		}
		c.ReadyTimeouts[r.name] = got
	}
	order, err := gpuOrder(f.GPUOrder)
	if err != nil {
		return err
	}
	c.GPUOrder = order
	budget, err := time.ParseDuration(cmp.Or(f.GPUPlacingBudget, d.GPUPlacingBudget))
	if err != nil || budget > maxGPUPlacingBudget {
		return fmt.Errorf("gpu_placing_budget must be a Go duration up to %s such as %s",
			maxGPUPlacingBudget, d.GPUPlacingBudget)
	}
	if least := MinGPUPlacingBudget(c.ReadyTimeouts["lium"]); budget < least {
		return fmt.Errorf("gpu_placing_budget must be at least %s, lium's slowest boot that "+
			"fails and the next GPU provider's create, or that provider never gets its turn", least)
	}
	c.GPUPlacingBudget = budget
	return nil
}

// gpuOrder checks the GPU order, which may name a provider that providers does not list
func gpuOrder(names []string) ([]string, error) {
	seen := map[string]bool{}
	for i, name := range names {
		switch {
		case name == "":
			return nil, errors.New("gpu_order has an empty entry")
		case !knownProviders[name]:
			// An unknown entry is never quoted, since a secret could have been pasted there
			return nil, fmt.Errorf("gpu_order entry %d is not one of %s", i+1, knownList())
		case seen[name]:
			return nil, fmt.Errorf("gpu_order lists %q more than once", name)
		}
		seen[name] = true
	}
	return slices.Clone(names), nil
}
