package hf

import (
	"context"
	"fmt"
	"strings"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/gpustatus"
)

// Job stages, every value of status.stage HF documents
const (
	stageScheduling = "SCHEDULING"
	stageRunning    = "RUNNING"
	stageCompleted  = "COMPLETED"
	stageCanceled   = "CANCELED"
	stageDeleted    = "DELETED"
	stageError      = "ERROR"
)

// jobTimeout is the message HF writes on a Job its timeout stopped
const jobTimeout = "Job timeout"

// liveStages are a Job that can still serve, the stages every list asks for
var liveStages = []string{stageScheduling, stageRunning}

var (
	// stageKills are the kill reasons of the ended stages, when HF gives no reason of its own
	stageKills = map[string]string{
		stageCompleted: gpustatus.HFCompleted, stageCanceled: gpustatus.HFCanceled,
		stageDeleted: gpustatus.HFDeleted, stageError: gpustatus.HFError,
	}
	// accountKills are the cancel reasons that name the account, whose Job reads as gone
	accountKills = map[string]bool{
		gpustatus.HFNoCredits: true, gpustatus.HFNoSubscription: true,
		gpustatus.HFSpendLimit: true, gpustatus.HFResourceGroupLimit: true,
	}
)

// liveStage reports a stage of a Job that can still serve
func liveStage(stage string) bool { return stage == stageScheduling || stage == stageRunning }

// jobOf returns the Job id of a ref this provider made
func jobOf(ref provider.Ref) (string, bool) {
	return ref.ID, ref.Provider == providerName && ref.ID != ""
}

// notHF is the not_found error of a ref this provider never made
func notHF(ref provider.Ref) error {
	return &provider.Error{Code: provider.CodeNotFound,
		Err: fmt.Errorf("sandbox %s/%s is not an hf Job", ref.Provider, ref.ID)}
}

// Inspect reads the Job and maps its stage, design 4.2
func (p *Provider) Inspect(ctx context.Context, ref provider.Ref) (provider.Status, error) {
	id, ok := jobOf(ref)
	if !ok {
		return provider.Status{State: provider.StateGone}, nil
	}
	j, err := p.jobs.getJob(ctx, id)
	if e, ok := provider.AsError(err); ok && e.Code == provider.CodeNotFound {
		return provider.Status{State: provider.StateGone, KillReason: gpustatus.HFNotFound}, nil
	}
	if err != nil {
		return provider.Status{}, err
	}
	return p.statusOf(j)
}

// statusOf maps a Job's stage, an ERROR the account caused reading as gone as 5.12 expects
func (p *Provider) statusOf(j job) (provider.Status, error) {
	st := provider.Status{
		Reason: scrub(j.Status.Message, p.jobs.token), KillReason: p.killReason(j),
	}
	switch s := j.Status.Stage; {
	case s == stageScheduling:
		st.State = provider.StateStarting
	case s == stageRunning:
		st.State = provider.StateRunning
	case s == stageError && !accountKills[cancelReason(j)]:
		st.State = provider.StateFailed
	case stageKills[s] != "":
		st.State = provider.StateGone
	default:
		return provider.Status{}, &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("hf Job %s has stage %.40q", j.ID, s)}
	}
	return st, nil
}

// killReason is timeout for HF's timeout in any stage, else the cancel reason, else the stage's
func (p *Provider) killReason(j job) string {
	if strings.TrimSpace(j.Status.Message) == jobTimeout {
		return gpustatus.HFTimeout
	}
	if r := cancelReason(j); r != "" {
		return scrub(r, strings.ToLower(p.jobs.token))
	}
	return stageKills[j.Status.Stage]
}

// cancelReason is the Job's cancelReason in lowercase, as gpustatus names them
func cancelReason(j job) string {
	return strings.ToLower(strings.TrimSpace(j.Status.CancelReason))
}
