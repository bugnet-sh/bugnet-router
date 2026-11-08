package boxd

import (
	"context"
	"fmt"
	"strconv"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/boxd/boxdpb"
)

const (
	routeSubdomain = "subdomain"
	routeDefault   = "default"
	// gateProxy names the subdomain proxy to the gate, served at bn.<machine>.<access domain>
	gateProxy    = "bn"
	metaGateHost = "gate_host"
	botOff       = "off"
)

var _ provider.Gater = (*Provider)(nil)

// PrepareGate opens a bn proxy to port, or turns bot protection off on the pinned default proxy
func (p *Provider) PrepareGate(
	ctx context.Context, ref provider.Ref, port int,
) (map[string]string, error) {
	id, err := machineID(ref)
	if err != nil {
		return nil, err
	}
	if err := provider.CheckGatePort(port); err != nil {
		return nil, err
	}
	vm, err := p.api.GetVm(ctx, &boxdpb.GetVmRequest{VmId: id})
	if err != nil {
		return nil, classify(ctx, err)
	}
	if vm.GetName() == "" || vm.GetAccessDomain() == "" {
		return nil, &provider.Error{
			Code: provider.CodeInternal,
			Err:  fmt.Errorf("boxd machine %s reports no name or access domain", id),
		}
	}
	host := vm.GetName() + "." + vm.GetAccessDomain()
	if p.gateRoute == routeSubdomain {
		if err := p.gateProxy(ctx, id, port); err != nil {
			return nil, err
		}
		return map[string]string{metaGateHost: gateProxy + "." + host}, nil
	}
	_, err = p.api.SetProxyPort(ctx, &boxdpb.SetProxyPortRequest{
		Vm: id, Name: defaultProxy, Port: strconv.Itoa(port),
	})
	if err != nil {
		return nil, classify(ctx, err)
	}
	// Bot protection checks only the main address and sends curl a 503 challenge
	if vm.GetBotProtectionMode() != botOff || vm.GetBotProtection() {
		_, err := p.api.SetBotProtection(ctx, &boxdpb.SetBotProtectionRequest{
			VmId: id, Mode: botOff,
		})
		if err != nil {
			return nil, classify(ctx, err)
		}
	}
	return map[string]string{metaGateHost: host}, nil
}

// gateProxy creates the bn proxy, or points the one an earlier call made at port
func (p *Provider) gateProxy(ctx context.Context, id string, port int) error {
	_, err := p.api.CreateProxy(ctx, &boxdpb.CreateProxyRequest{
		Vm: id, Name: gateProxy, Port: uint32(port),
	})
	if nameTaken(err) {
		_, err = p.api.SetProxyPort(ctx, &boxdpb.SetProxyPortRequest{
			Vm: id, Name: gateProxy, Port: strconv.Itoa(port),
		})
	}
	return classify(ctx, err)
}

// GateRoute is the host PrepareGate stored, which needs no credential beside the gate key
func (p *Provider) GateRoute(ref provider.Ref, port int) (provider.GateRoute, error) {
	id, err := machineID(ref)
	if err != nil {
		return provider.GateRoute{}, err
	}
	if err := provider.CheckGatePort(port); err != nil {
		return provider.GateRoute{}, err
	}
	host := ref.Meta[metaGateHost]
	if host == "" {
		return provider.GateRoute{}, provider.GateUnprepared(providerName, id, metaGateHost)
	}
	return provider.GateRoute{BaseURL: "https://" + host}, nil
}
