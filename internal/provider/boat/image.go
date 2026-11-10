package boat

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/boat/api"
)

const (
	savedPoll = 5 * time.Second
	savedWait = 30 * time.Minute
)

// createBody builds a create's body, for a sold sandbox or for a builder an image is saved from
//
// A builder carries no labels and has snapshots on, since Boat saves only such a sandbox. Every
// sandbox keeps noEnv, which a saved snapshot and every deploy from it inherit.
func (p *Provider) createBody(req provider.CreateRequest, s size) (api.CreateSandboxRequest, error) {
	body := api.CreateSandboxRequest{
		Type:       new(api.CreateSandboxRequestType(s.typ.name)),
		TtlSeconds: new(wholeSeconds(req.Lifetime)),
		NoEnv:      new(true),
		Snapshots:  new(req.Builder),
		Monitoring: new(false),
		FailFast:   new(true),
	}
	if req.Builder {
		if len(req.Labels) > 0 || req.Image != "" {
			return body, invalid(errors.New("a boat builder carries no labels and no image"))
		}
		return body, nil
	}
	if err := checkLabels(req.Labels); err != nil {
		return body, err
	}
	if len(req.Labels) > 0 {
		body.Labels = new(maps.Clone(req.Labels))
	}
	if from := cmp.Or(req.Image, p.from); from != "" {
		body.From = &from
	}
	return body, nil
}

// missingFrom lets a create refused for a missing saved snapshot fail over to another provider
func (p *Provider) missingFrom(err error, body api.CreateSandboxRequest) error {
	e, ok := provider.AsError(err)
	if !ok || body.From == nil || (e.Code != provider.CodeNotFound && e.Code != provider.CodeAuth) {
		return err
	}
	slog.Error("boat could not deploy from a saved snapshot", "snapshot", *body.From,
		"code", string(e.Code))
	e.CanFailOver = true
	return e
}

// savedStatus reads one saved snapshot, found false when Boat has none by that name
func (p *Provider) savedStatus(ctx context.Context, name string) (string, bool, error) {
	raw, err := p.send(ctx, noEffect, func(ctx context.Context) (*http.Response, error) {
		return p.api.GetSavedSnapshot(ctx, name)
	})
	if isCode(err, provider.CodeNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	var reply struct {
		Snapshot struct {
			Status string `json:"status"`
			Error  string `json:"error"`
		} `json:"snapshot"`
	}
	if err := decode(raw, &reply); err != nil {
		return "", false, internal(fmt.Errorf("boat saved snapshot %s reply: %w", name, err))
	}
	if reply.Snapshot.Status == "failed" {
		return "failed", true, internal(fmt.Errorf("boat could not save snapshot %s: %s", name,
			reply.Snapshot.Error))
	}
	return reply.Snapshot.Status, true, nil
}

// SaveImage saves the builder as a snapshot called name and returns once Boat says it is ready
func (p *Provider) SaveImage(ctx context.Context, sandbox provider.Ref, name string) (string, error) {
	id, err := sandboxID(sandbox)
	if err != nil {
		return "", err
	}
	// A second save under one name replaces the artifact that live deploys may start from
	if _, found, err := p.savedStatus(ctx, name); err != nil {
		return "", err
	} else if found {
		return "", invalid(fmt.Errorf("boat saved snapshot %s exists already", name))
	}
	body := api.SaveSavedSnapshotJSONRequestBody{SandboxId: id, Name: name}
	if _, err := p.send(ctx, noEffect, func(ctx context.Context) (*http.Response, error) {
		return p.api.SaveSavedSnapshot(ctx, body)
	}); err != nil {
		return "", err
	}
	deadline := time.Now().Add(p.imageWait)
	for {
		st, _, err := p.savedStatus(ctx, name)
		if err == nil && st == "ready" {
			return name, nil
		}
		if e, ok := provider.AsError(err); st == "failed" || ok && e.Code != provider.CodeInternal &&
			e.Code != provider.CodeRateLimited && e.Code != provider.CodeOutcomeUnknown {
			return "", err
		}
		if time.Now().After(deadline) {
			return "", internal(fmt.Errorf("boat saved snapshot %s was not ready in %v", name,
				p.imageWait))
		}
		if !sleep(ctx, p.imagePoll) {
			return "", fmt.Errorf("boat saved snapshot %s not ready: %w", name, ctx.Err())
		}
	}
}

// DeleteImage deletes a saved snapshot and treats one already gone as deleted
func (p *Provider) DeleteImage(ctx context.Context, ref string) error {
	_, err := p.send(ctx, noEffect, func(ctx context.Context) (*http.Response, error) {
		return p.api.DeleteSavedSnapshot(ctx, ref)
	})
	if isCode(err, provider.CodeNotFound) {
		return nil
	}
	return err
}
