package boat

import (
	"context"
	"net/netip"
	"slices"

	"github.com/bugnet-sh/bugnet-router/internal/netguard"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/boat/api"
)

// Egress families Options.EgressFamilies names
const (
	FamilyIPv6 = "ipv6"
	FamilyIPv4 = "ipv4"
)

const defaultIPv6Bits = 128

var _ provider.EgressSourcer = (*Provider)(nil)

// EgressSource turns the sandbox's ip into a prefix, reading Boat and never the sandbox
func (p *Provider) EgressSource(
	ctx context.Context, ref provider.Ref,
) (provider.EgressSource, bool, error) {
	id, err := sandboxID(ref)
	if err != nil {
		return provider.EgressSource{}, false, err
	}
	sb, err := p.get(ctx, id)
	if err != nil {
		return provider.EgressSource{}, false, err
	}
	switch sb.State {
	case api.SandboxStateReady, api.SandboxStateIdle, api.SandboxStateRunning:
	default:
		return provider.EgressSource{}, false, nil
	}
	src, ok := p.sourceOf(sb)
	return src, ok, nil
}

// sourceOf applies the adapter's rules to one sandbox reply
func (p *Provider) sourceOf(sb sandbox) (provider.EgressSource, bool) {
	none := provider.EgressSource{}
	if sb.IP == nil {
		return none, false
	}
	ip, err := netip.ParseAddr(*sb.IP)
	if err != nil || ip.Zone() != "" {
		return none, false
	}
	ip = ip.Unmap()
	if !ip.IsGlobalUnicast() || netguard.Refused(ip) {
		return none, false
	}
	if ip.Is4() {
		hasOwn := sb.SSHEndpoint == nil || *sb.SSHEndpoint == ""
		if !hasOwn || !slices.Contains(p.egressFamilies, FamilyIPv4) {
			return none, false
		}
		return provider.EgressSource{Prefix: netip.PrefixFrom(ip, 32), Bind: ip}, true
	}
	if !slices.Contains(p.egressFamilies, FamilyIPv6) {
		return none, false
	}
	prefix, err := ip.Prefix(p.egressBits)
	if err != nil {
		return none, false
	}
	return provider.EgressSource{Prefix: prefix, Bind: ip}, true
}
