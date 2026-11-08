package boxd

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/boxd/boxdpb"
)

// ReadLimits reads the home organization's machine limit and the machines counting against it
func (p *Provider) ReadLimits(ctx context.Context) (provider.Limits, error) {
	lim, err := p.api.GetOrgMachineLimit(ctx, &boxdpb.GetOrgMachineLimitRequest{})
	if err != nil {
		return provider.Limits{}, classify(ctx, err)
	}
	return provider.Limits{MaxConcurrent: int(lim.GetMaxVms()), Active: int(lim.GetVmCount())}, nil
}

// CheckAccount reads the subscription from Whoami, then the credit balance of the organization
func (p *Provider) CheckAccount(ctx context.Context) (provider.Account, error) {
	who, err := p.api.Whoami(ctx, &boxdpb.WhoamiRequest{})
	if err != nil {
		return p.accountError(ctx, err)
	}
	if b := who.GetBilling(); b != nil {
		switch {
		case b.GetSubscriptionStatus() == "past_due" || b.GetSubscriptionStatus() == "canceled":
			return provider.Account{
				Block: provider.BlockUnfunded, Detail: "subscription " + b.GetSubscriptionStatus(),
			}, nil
		case b.GetPastDueSince() != 0:
			return provider.Account{Block: provider.BlockUnfunded, Detail: "payment past due"}, nil
		}
	}
	lim, err := p.api.GetOrgMachineLimit(ctx, &boxdpb.GetOrgMachineLimitRequest{})
	if err != nil {
		return p.accountError(ctx, err)
	}
	org, err := p.api.GetOrgBilling(ctx, &boxdpb.GetOrgBillingRequest{Org: lim.GetOrgName()})
	if err != nil {
		return p.accountError(ctx, err)
	}
	if org.GetBillingMode() == "credits" && org.GetBalanceMicro() <= 0 {
		return provider.Account{Block: provider.BlockUnfunded, Detail: "no credit balance"}, nil
	}
	return provider.Account{}, nil
}

// accountError reads a refused key as the account, any other error stays an error
func (p *Provider) accountError(ctx context.Context, err error) (provider.Account, error) {
	switch status.Code(err) {
	case codes.PermissionDenied, codes.Unauthenticated:
		return provider.Account{Block: provider.BlockAuth, Detail: status.Code(err).String()}, nil
	}
	return provider.Account{}, classify(ctx, err)
}
