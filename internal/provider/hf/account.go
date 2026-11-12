package hf

import (
	"context"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

var _ provider.LimitsReader = (*Provider)(nil)

// ReadLimits counts the namespace's live Jobs, the router's or not, as HF documents no cap
func (p *Provider) ReadLimits(ctx context.Context) (provider.Limits, error) {
	jobs, err := p.jobs.listJobs(ctx, nil, liveStages)
	if isCode(err, provider.CodeUnfunded) {
		// HF runs no Job for a namespace it refuses for credit
		return provider.Limits{}, nil
	}
	if err != nil {
		return provider.Limits{}, err
	}
	live := map[string]bool{}
	for _, j := range jobs {
		// HF keeps ended Jobs listed, and a list that shifts between pages repeats one
		if liveStage(j.Status.Stage) {
			live[j.ID] = true
		}
	}
	return provider.Limits{Active: len(live)}, nil
}
