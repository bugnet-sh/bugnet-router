package hf

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/gpustatus"
)

const (
	// minProtocol is the oldest sbx-server protocol the adapter speaks, design 4.10
	minProtocol = 3
	// healthTimeout bounds one health read, so a stalled proxy never holds the whole wait
	healthTimeout = 10 * time.Second
)

// WaitReady reads the Job every pollEvery until it runs, then until its sbx-server takes the token
func (p *Provider) WaitReady(ctx context.Context, ref provider.Ref) error {
	id, ok := jobOf(ref)
	if !ok {
		return notHF(ref)
	}
	last, stage := errors.New("not read yet"), ""
	for {
		j, err := p.jobs.getJob(ctx, id)
		switch {
		case err == nil:
			stage = j.Status.Stage
			wait, end := p.readyStep(ctx, ref, j)
			if end != nil || wait == nil {
				return end
			}
			// A health read the deadline cut short says less than the one before it
			if ctx.Err() == nil {
				last = wait
			}
		case stopsTheWait(err):
			return err
		case ctx.Err() == nil:
			last = err
		}
		if err := sleep(ctx, p.pollEvery); err != nil {
			return notReady(p.jobs.jobName(id), stage, err, last)
		}
	}
}

// readyStep reads one poll, wait being why the wait goes on and end an error that ends it
func (p *Provider) readyStep(ctx context.Context, ref provider.Ref, j job) (wait, end error) {
	switch s := j.Status.Stage; {
	case j.ID != ref.ID:
		// A read of another Job would send it this token, or derive a token from its labels
		return nil, &provider.Error{Code: provider.CodeInternal,
			Err: fmt.Errorf("hf answered Job %.40q for Job %s", p.jobs.jobName(j.ID),
				p.jobs.jobName(ref.ID))}
	case s == stageScheduling:
		return errors.New("stage SCHEDULING"), nil
	case s == stageRunning:
		return p.checkHealth(ctx, ref, j)
	case stageKills[s] != "":
		return nil, p.startEnded(j)
	}
	return nil, &provider.Error{Code: provider.CodeInternal,
		Err: fmt.Errorf("hf Job %s has stage %.40q", p.jobs.jobName(j.ID),
			p.jobs.words(j.Status.Stage))}
}

// checkHealth asks the Job's sbx-server for its health with the sandbox's token
func (p *Provider) checkHealth(ctx context.Context, ref provider.Ref, j job) (wait, end error) {
	token := ref.Meta[metaSecret]
	if token == "" {
		var err error
		if token, err = p.tokenOf(j); err != nil {
			return nil, err
		}
	}
	sb := sandbox{id: ref.ID, name: p.jobs.jobName(ref.ID), token: token}
	hctx, cancel := context.WithTimeout(ctx, p.healthWait)
	defer cancel()
	h, err := p.health(hctx, sb)
	switch {
	case isCode(err, provider.CodeInvalid):
		return nil, err
	case err != nil:
		return err, nil
	case h.Protocol == nil:
		// Only the server's own answer names a protocol, a proxy page never does
		return fmt.Errorf("hf Job %s answered a health with no protocol", sb.name), nil
	case *h.Protocol < minProtocol:
		return nil, refusedServer(sb.name, fmt.Errorf("sbx-server speaks protocol %d, %d or more "+
			"is needed", *h.Protocol, minProtocol))
	case h.Mode == "" && h.Auth == "":
		// sbx-server shows its mode and auth only to its own token
		return nil, refusedServer(sb.name, errors.New(
			"hf_token_refused, sbx-server does not take the sandbox's token"))
	case h.Mode != "dedicated" || h.Auth != "required":
		return nil, refusedServer(sb.name, fmt.Errorf("sbx-server runs with mode %.20q and auth "+
			"%.20q, dedicated and required are needed", p.sbxWords(h.Mode, token),
			p.sbxWords(h.Auth, token)))
	}
	return nil, nil
}

// refusedServer is the error of the Job named name whose sbx-server can never serve this sandbox
func refusedServer(name string, err error) error {
	return &provider.Error{Code: provider.CodeInternal, CanFailOver: true,
		Err: fmt.Errorf("hf Job %s: %w", name, err)}
}

// startEnded is the error of a Job that ended before it was ready, design 4.13
func (p *Provider) startEnded(j job) error {
	why := fmt.Errorf("hf Job %s ended its start as %s: %s", p.jobs.jobName(j.ID),
		j.Status.Stage, p.jobs.words(j.Status.Message))
	switch r := cancelReason(j); {
	case r == gpustatus.HFNoCredits || r == gpustatus.HFNoSubscription:
		return provider.Named(&provider.Error{
			Code: provider.CodeUnfunded, CanFailOver: true, Err: why,
		}, providerName)
	case r == gpustatus.HFSpendLimit || r == gpustatus.HFResourceGroupLimit:
		return &provider.Error{Code: provider.CodeQuota, CanFailOver: true, Err: why}
	case j.Status.Stage == stageError:
		// PID 1 exited before any user had the sandbox, so the boot itself failed
		return &provider.Error{Code: provider.CodeCapacity, CanFailOver: true, Err: why}
	}
	return &provider.Error{Code: provider.CodeNotFound, Err: why}
}

// stopsTheWait reports a read error no later read can change
func stopsTheWait(err error) bool {
	e, ok := provider.AsError(err)
	return ok && e.Code != provider.CodeInternal && e.Code != provider.CodeRateLimited
}

// notReady is a wait on the Job named name its context ended, capacity while HF still scheduled it
func notReady(name, stage string, err, last error) error {
	cause := fmt.Errorf("hf Job %s not ready: %w, last %w", name, err, last)
	if stage == stageScheduling {
		return &provider.Error{Code: provider.CodeCapacity, CanFailOver: true, Err: cause}
	}
	return &provider.Error{Code: provider.CodeInternal, CanFailOver: true, Err: cause}
}
