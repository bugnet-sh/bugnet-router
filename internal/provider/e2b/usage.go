package e2b

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/e2b/api"
)

const (
	// eventsKept is how long E2B keeps a sandbox's events
	eventsKept = 7 * 24 * time.Hour
	// eventsPage is how many events one page of ReadUsage reads
	eventsPage = 100
	// deploymentLabel is the metadata key naming the deployment that made a sandbox
	deploymentLabel = "bugnet_deployment"
)

// ReadUsage reads the team's killed events newest first, 100 a page, and prices each one's
// execution time at the rate of its template. The cursor is the offset of the next event. A
// window E2B no longer keeps events for is skipped with a log line.
func (p *Provider) ReadUsage(
	ctx context.Context, from, to time.Time, _ []provider.Ref, cursor string,
) (provider.Usage, error) {
	if to.Before(time.Now().Add(-eventsKept)) {
		slog.Warn("e2b keeps no events for the day, its cost check is skipped",
			"from", from.UTC())
		return provider.Usage{TotalMicros: -1, Complete: true, Attributed: true}, nil
	}
	offset, _ := strconv.Atoi(cursor)
	params := &api.GetEventsSandboxesParams{
		Types: &[]string{killedType}, Limit: new(int32(eventsPage)),
		Offset: new(int32(max(offset, 0))), OrderAsc: new(false),
	}
	_, raw, err := p.send(ctx, false, func(ctx context.Context) (*http.Response, error) {
		return p.api.GetEventsSandboxes(ctx, params)
	})
	if err != nil {
		return provider.Usage{}, err
	}
	var events []api.SandboxEvent
	if err := json.Unmarshal(raw, &events); err != nil {
		return provider.Usage{}, &provider.Error{
			Code: provider.CodeInternal, Err: fmt.Errorf("e2b events reply: %w", err),
		}
	}
	out := provider.Usage{TotalMicros: -1, Complete: true, Attributed: true}
	reachedOlder := false
	for _, ev := range events {
		switch {
		case ev.Timestamp.Before(from):
			reachedOlder = true
		case ev.Timestamp.Before(to):
			u, named := p.eventUsage(ev)
			out.Sandboxes = append(out.Sandboxes, u)
			out.Attributed = out.Attributed && named
		}
	}
	if len(events) == eventsPage && !reachedOlder {
		out.Next = strconv.Itoa(offset + len(events))
	}
	return out, nil
}

// eventUsage prices one killed event, its cost -1 when the template is not one of ours, and
// reports whether the event carries the sandbox's metadata
func (p *Provider) eventUsage(ev api.SandboxEvent) (provider.SandboxUsage, bool) {
	u := provider.SandboxUsage{ID: ev.SandboxId, CostMicros: -1}
	if ev.EventData == nil {
		return u, false
	}
	data := *ev.EventData
	// The kill event gives execution_time in milliseconds
	if ms, ok := data["execution_time"].(float64); ok && ms >= 0 {
		u.Seconds = int64(math.Round(ms / 1000))
		for _, t := range p.templates {
			if t.TemplateID == ev.SandboxTemplateId {
				u.CostMicros = (hourlyMicros(t.size().Resources)*u.Seconds + 3599) / 3600
			}
		}
	}
	md, named := data["sandbox_metadata"].(map[string]any)
	if named {
		u.Deployment, _ = md[deploymentLabel].(string)
	}
	return u, named
}

// ListLabelled lists up to limit running or paused sandboxes carrying every label from the
// token on
func (p *Provider) ListLabelled(
	ctx context.Context, labels map[string]string, cursor string, limit int,
) (provider.ForeignPage, error) {
	params := &api.GetV2SandboxesParams{
		State: &[]api.SandboxState{api.Running, api.Paused},
		Limit: new(api.PaginationLimit(listPageSize)),
	}
	if len(labels) > 0 {
		params.Metadata = new(metadataQuery(labels))
	}
	if cursor != "" {
		params.NextToken = &cursor
	}
	var out provider.ForeignPage
	for len(out.Sandboxes) < limit {
		h, raw, err := p.send(ctx, false, func(ctx context.Context) (*http.Response, error) {
			return p.api.GetV2Sandboxes(ctx, params)
		})
		if err != nil {
			return provider.ForeignPage{}, err
		}
		var page []api.ListedSandbox
		if err := json.Unmarshal(raw, &page); err != nil {
			return provider.ForeignPage{}, &provider.Error{
				Code: provider.CodeInternal, Err: fmt.Errorf("e2b sandbox list: %w", err),
			}
		}
		for _, s := range page {
			var md map[string]string
			if s.Metadata != nil {
				md = *s.Metadata
			}
			if hasLabels(md, labels) {
				out.Sandboxes = append(out.Sandboxes, provider.Summary{
					Ref: newRef(s.SandboxID), Labels: md, CreatedAt: s.StartedAt.UTC(),
					State: stateOf(s.State), ExpiresAt: s.EndAt.UTC(),
				})
			}
		}
		next := h.Get("X-Next-Token")
		if next == "" {
			out.Next = ""
			return out, nil
		}
		params.NextToken, out.Next = &next, next
	}
	return out, nil
}
