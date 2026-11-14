package boxd

import (
	"context"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/boxd/boxdpb"
)

// ReadUsage asks for the organization's usage in one call, every machine that used anything in
// the window. boxd names a machine by its id alone, so no deployment is attributed. A window
// that runs to now is never complete, and a machine deleted lately is complete up to about 25
// minutes later.
func (p *Provider) ReadUsage(
	ctx context.Context, from, to time.Time, _ []provider.Ref, _ string,
) (provider.Usage, error) {
	resp, err := p.api.GetOrgUsage(ctx, &boxdpb.GetOrgUsageRequest{
		Since: from.Unix(), Until: to.Unix(),
	})
	if err != nil {
		return provider.Usage{}, classify(ctx, err)
	}
	out := provider.Usage{
		TotalMicros: p.usdMicros(resp.GetCurrency(), resp.GetTotalCostMicro()),
		Complete:    resp.GetComplete(),
	}
	for _, vm := range resp.GetVms() {
		s := vm.GetSeconds()
		out.Sandboxes = append(out.Sandboxes, provider.SandboxUsage{
			ID:         vm.GetVmId(),
			Seconds:    int64(s.GetRunning() + s.GetStandby()),
			CostMicros: p.usdMicros(resp.GetCurrency(), vm.GetCostMicro()),
		})
	}
	return out, nil
}

// usd converts EUR micros to micro dollars at the configured rate, rounded up
func (p *Provider) usd(eurMicros int64) int64 {
	return (eurMicros*p.rateBP + rateBase - 1) / rateBase
}

// usdMicros converts a cost in boxd's currency to micro dollars at the configured rate
func (p *Provider) usdMicros(currency string, micros int64) int64 {
	if currency == "eur" {
		return p.usd(micros)
	}
	return micros
}
