package hf

import (
	"context"
	"fmt"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

var _ provider.Gater = (*Provider)(nil)

// PrepareGate calls nothing, each Job exposes the gate's port publicly from its create
func (p *Provider) PrepareGate(
	_ context.Context, ref provider.Ref, port int,
) (map[string]string, error) {
	if _, err := p.GateRoute(ref, port); err != nil {
		return nil, err
	}
	return map[string]string{}, nil
}

// GateRoute is the gate port's public URL on HF's proxy, the gate key being its only credential
func (p *Provider) GateRoute(ref provider.Ref, port int) (provider.GateRoute, error) {
	id, ok := jobOf(ref)
	if !ok {
		return provider.GateRoute{}, notHF(ref)
	}
	if err := provider.CheckGatePort(port); err != nil {
		return provider.GateRoute{}, err
	}
	if port != provider.DefaultGatePort {
		// Any other port is sbx-server's, holding the sandbox token, or one HF never exposed
		return provider.GateRoute{}, invalid(fmt.Errorf("hf Jobs expose the gate on port %d, "+
			"not %d", provider.DefaultGatePort, port), false)
	}
	base, err := sbxURL(id, port, "")
	if err != nil {
		return provider.GateRoute{}, err
	}
	return provider.GateRoute{BaseURL: base}, nil
}
