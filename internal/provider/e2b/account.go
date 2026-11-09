package e2b

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/e2b/api"
)

// ReadLimits counts the sandboxes running on the team, its cap not being readable with a key
func (p *Provider) ReadLimits(ctx context.Context) (provider.Limits, error) {
	params := &api.GetV2SandboxesParams{
		State: &[]api.SandboxState{api.Running}, Limit: new(api.PaginationLimit(1)),
	}
	h, _, err := p.send(ctx, false, func(ctx context.Context) (*http.Response, error) {
		return p.api.GetV2Sandboxes(ctx, params)
	})
	if err != nil {
		return provider.Limits{}, err
	}
	n, err := strconv.Atoi(h.Get("X-Total-Running"))
	if err != nil || n < 0 {
		return provider.Limits{}, &provider.Error{
			Code: provider.CodeInternal, Err: fmt.Errorf("e2b list answered no X-Total-Running"),
		}
	}
	return provider.Limits{Active: n}, nil
}

// CheckAccount reads the team's metrics, or lists one sandbox when no team is set
func (p *Provider) CheckAccount(ctx context.Context) (provider.Account, error) {
	_, _, err := p.send(ctx, false, func(ctx context.Context) (*http.Response, error) {
		if p.teamID == "" {
			return p.api.GetV2Sandboxes(ctx, &api.GetV2SandboxesParams{
				Limit: new(api.PaginationLimit(1)),
			})
		}
		return p.api.GetTeamsTeamIDMetricsMax(ctx, p.teamID, &api.GetTeamsTeamIDMetricsMaxParams{
			Metric: api.GetTeamsTeamIDMetricsMaxParamsMetric("concurrent_sandboxes"),
		})
	})
	if err == nil {
		return provider.Account{}, nil
	}
	return provider.AccountReading(err)
}
