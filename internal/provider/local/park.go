package local

import (
	"context"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// Park does nothing, a local sandbox has no cheaper idle state
func (p *Provider) Park(context.Context, provider.Ref) error { return nil }

// Unpark does nothing, a local sandbox never slept
func (p *Provider) Unpark(context.Context, provider.Ref) error { return nil }

// ParkedHourlyCostMicros is 0, a local sandbox costs nothing
func (p *Provider) ParkedHourlyCostMicros(provider.Size) int64 { return 0 }
