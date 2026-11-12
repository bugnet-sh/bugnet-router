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
			return notReady(id, stage, err, last)
		}
	}
}

// readyStep reads one poll, wait being why the wait goes on and end an error that ends it
func (p *Provider) readyStep(ctx context.Context, ref provider.Ref, j job) (wait, end error) {
	switch s := j.Status.Stage; {
	case s == stageScheduling:
		return errors.New("stage SCHEDULING"), nil
	case s == stageRunning:
		return p.checkHealth(ctx, ref, j)
	case stageKills[s] != "":
		return nil, p.startEnded(j)
	}
	return nil, &provider.Error{Code: provider.CodeInternal,
		Err: fmt.Errorf("hf Job %s has stage %.40q", j.ID, j.Status.Stage)}
}

// checkHealth asks the Job's sbx-server for its health with the sandbox's token
func (p *Provider) checkHealth(ctx context.Context, ref provider.Ref, j job) (wait, end error) {
	token := ref.Meta[metaSecret]
	if token == "" {
		key := j.Labels[labelKey]
		if !keyHex.MatchString(key) {
			return nil, &provider.Error{Code: provider.CodeInternal, Err: fmt.Errorf(
				"hf Job %s has no token in its ref and no key to derive one from", j.ID)}
		}
		token = p.sandboxToken(key)
	}
	hctx, cancel := context.WithTimeout(ctx, healthTimeout)
	defer cancel()
	h, err := p.sbx.health(hctx, j.ID, token)
	switch {
	case isCode(err, provider.CodeInvalid):
		return nil, err
	case err != nil:
		return err, nil
	case h.Protocol == nil:
		// Only the server's own answer names a protocol, a proxy page never does
		return fmt.Errorf("hf Job %s answered a health with no protocol", j.ID), nil
	case *h.Protocol < minProtocol:
		return nil, refusedServer(j.ID, fmt.Errorf("sbx-server speaks protocol %d, %d or more "+
			"is needed", *h.Protocol, minProtocol))
	case h.Mode == "" && h.Auth == "":
		// sbx-server shows its mode and auth only to its own token
		return nil, refusedServer(j.ID, errors.New(
			"hf_token_refused, sbx-server does not take the sandbox's token"))
	case h.Mode != "dedicated" || h.Auth != "required":
		return nil, refusedServer(j.ID, fmt.Errorf("sbx-server runs with mode %.20q and auth "+
			"%.20q, dedicated and required are needed", h.Mode, h.Auth))
	}
	return nil, nil
}

// refusedServer is the error of a Job whose sbx-server can never serve this sandbox
func refusedServer(id string, err error) error {
	return &provider.Error{Code: provider.CodeInternal, CanFailOver: true,
		Err: fmt.Errorf("hf Job %s: %w", id, err)}
}

// startEnded is the error of a Job that ended before it was ready, design 4.13
func (p *Provider) startEnded(j job) error {
	why := fmt.Errorf("hf Job %s ended its start as %s: %s", j.ID, j.Status.Stage,
		scrub(j.Status.Message, p.jobs.token))
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

// notReady is a wait its context ended, capacity while HF still had the Job scheduling
func notReady(id, stage string, err, last error) error {
	cause := fmt.Errorf("hf Job %s not ready: %w, last %w", id, err, last)
	if stage == stageScheduling {
		return &provider.Error{Code: provider.CodeCapacity, CanFailOver: true, Err: cause}
	}
	return &provider.Error{Code: provider.CodeInternal, CanFailOver: true, Err: cause}
}
