package hf

import (
	"context"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"time"

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

// ListOwned lists the live Jobs carrying every label, checking each one HF lists
func (p *Provider) ListOwned(
	ctx context.Context, labels map[string]string,
) ([]provider.Summary, error) {
	if err := checkLabels(labels); err != nil {
		return nil, err
	}
	jobs, err := p.jobs.listJobs(ctx, labels, liveStages)
	if err != nil {
		return nil, err
	}
	var out []provider.Summary
	seen := map[string]bool{}
	for _, j := range jobs {
		// HF keeps ended Jobs, and a sweep must never reach a Job the filters let through wrongly
		if !liveStage(j.Status.Stage) || !hasLabels(j.Labels, labels) ||
			!jobID.MatchString(j.ID) || seen[j.ID] {
			continue
		}
		seen[j.ID] = true
		state := provider.StateRunning
		if j.Status.Stage == stageScheduling {
			state = provider.StateStarting
		}
		out = append(out, provider.Summary{
			Ref: refOf(j, "", time.Time{}, 0), Labels: maps.Clone(j.Labels),
			CreatedAt: j.CreatedAt, State: state,
		})
	}
	return out, nil
}

// Destroy cancels the Job, nil once HF shows it ended or no longer has it
func (p *Provider) Destroy(ctx context.Context, ref provider.Ref) error {
	id, ok := jobOf(ref)
	if !ok {
		return nil
	}
	err := p.jobs.cancelJob(ctx, id)
	if err == nil || isCode(err, provider.CodeNotFound) {
		return nil
	}
	// What HF answers to a cancel of an ended Job is UNVERIFIED, so the Job's read decides
	j, rerr := p.jobs.getJob(ctx, id)
	if isCode(rerr, provider.CodeNotFound) || rerr == nil && !liveStage(j.Status.Stage) {
		return nil
	}
	return err
}

// SetLifetime changes nothing at HF, it refuses an until past the Job's own end as invalid
func (p *Provider) SetLifetime(ctx context.Context, ref provider.Ref, until time.Time) error {
	id, ok := jobOf(ref)
	if !ok {
		return notHF(ref)
	}
	end, known := p.MaxUntil(ref)
	if known && until.After(end) {
		return pastEnd(until, end)
	}
	if known && p.jobs.held() > 0 {
		// An extension holds the payer's wallet lock, which a 429's hold must not keep
		return nil
	}
	j, err := p.jobs.getJob(ctx, id)
	switch {
	case isCode(err, provider.CodeNotFound):
		return err
	case err != nil && known:
		// The end is checked already, so the read only tells a Job HF ended apart
		return nil
	case err != nil:
		return err
	case !liveStage(j.Status.Stage):
		return &provider.Error{Code: provider.CodeNotFound,
			Err: fmt.Errorf("hf Job %s ended as %.40s", id, j.Status.Stage)}
	case known:
		return nil
	}
	if end, ok := p.endOf(refOf(j, "", time.Time{}, 0)); ok && until.After(end) {
		return pastEnd(until, end)
	}
	return nil
}

// MaxUntil is the Job's own end, its timeout less the margin from its creation, design 5.13
func (p *Provider) MaxUntil(ref provider.Ref) (time.Time, bool) {
	if ref.Provider != providerName {
		return time.Time{}, false
	}
	return p.endOf(ref)
}

// endOf reads the Job's end from the creation time and timeout its ref keeps
func (p *Provider) endOf(ref provider.Ref) (time.Time, bool) {
	created, err := time.Parse(time.RFC3339, ref.Meta[metaCreatedAt])
	if err != nil {
		return time.Time{}, false
	}
	secs, err := strconv.ParseInt(ref.Meta[metaTimeout], 10, 64)
	if err != nil || secs <= 0 || secs >= maxTimeoutSeconds {
		return time.Time{}, false
	}
	return created.Add(time.Duration(secs)*time.Second - p.margin), true
}

// pastEnd is the refusal of an until past the Job's end, which no call can move
func pastEnd(until, end time.Time) error {
	return invalid(fmt.Errorf("until %s is past the hf Job's own end at %s",
		until.UTC().Format(time.RFC3339), end.UTC().Format(time.RFC3339)), false)
}

// isCode reports whether err is a provider error of code
func isCode(err error, code provider.Code) bool {
	e, ok := provider.AsError(err)
	return ok && e.Code == code
}
