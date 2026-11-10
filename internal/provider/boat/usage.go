package boat

import (
	"context"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/boat/api"
)

const (
	// usagePage is how many sandboxes' usage one page of ReadUsage asks for
	usagePage = 25
	// deploymentLabel is the label naming the deployment that made a sandbox
	deploymentLabel = "bugnet_deployment"
	// refsCursor and listCursor start the cursors of the two phases of ReadUsage
	refsCursor = "r"
	listCursor = "l:"
)

// usageReply holds the fields of GET /sandboxes/{id}/usage the cost check reads
type usageReply struct {
	Seconds float64 `json:"seconds"`
	Dollars float64 `json:"dollars"`
	Running bool    `json:"running"`
}

// ReadUsage asks for each of refs in turn, 25 a page, then lists the account's sandboxes to name
// the ones that are not in refs. The first phase's cursor is r<offset> and the second's l:<cursor>.
func (p *Provider) ReadUsage(
	ctx context.Context, from, to time.Time, refs []provider.Ref, cursor string,
) (provider.Usage, error) {
	if after, ok := strings.CutPrefix(cursor, listCursor); ok {
		return p.usageListing(ctx, from, to, refs, after)
	}
	ids := make([]string, 0, len(refs))
	for _, r := range refs {
		if r.Provider == providerName && r.ID != "" {
			ids = append(ids, r.ID)
		}
	}
	slices.Sort(ids)
	ids = slices.Compact(ids)
	offset := 0
	if rest, ok := strings.CutPrefix(cursor, refsCursor); ok {
		offset, _ = strconv.Atoi(rest)
	}
	offset = min(max(offset, 0), len(ids))
	end := min(offset+usagePage, len(ids))
	out := provider.Usage{TotalMicros: -1, Complete: true, Attributed: true}
	for _, id := range ids[offset:end] {
		u, found, err := p.sandboxUsage(ctx, id, from, to)
		if err != nil {
			return provider.Usage{}, err
		}
		if !found {
			continue
		}
		out.Complete = out.Complete && !u.Running
		out.Sandboxes = append(out.Sandboxes, provider.SandboxUsage{
			ID: id, Seconds: int64(math.Round(u.Seconds)),
			CostMicros: int64(math.Round(u.Dollars * 1e6)),
		})
	}
	if end < len(ids) {
		out.Next = refsCursor + strconv.Itoa(end)
	} else {
		out.Next = listCursor
	}
	return out, nil
}

// sandboxUsage reads one sandbox's usage in the window, found false when Boat no longer has it
func (p *Provider) sandboxUsage(
	ctx context.Context, id string, from, to time.Time,
) (usageReply, bool, error) {
	params := &api.UsageParams{
		Since: new(from.UTC().Format(time.RFC3339)), Until: new(to.UTC().Format(time.RFC3339)),
	}
	raw, err := p.send(ctx, noEffect, func(ctx context.Context) (*http.Response, error) {
		return p.api.Usage(ctx, id, params)
	})
	if isCode(err, provider.CodeNotFound) {
		return usageReply{}, false, nil
	}
	if err != nil {
		return usageReply{}, false, err
	}
	var u usageReply
	if err := decode(raw, &u); err != nil {
		return usageReply{}, false, internal(err)
	}
	return u, true, nil
}

// usageListing reads one page of the account's sandboxes in every state and names the ones with
// the bugnet label made in the window that are not in refs
func (p *Provider) usageListing(
	ctx context.Context, from, to time.Time, refs []provider.Ref, cursor string,
) (provider.Usage, error) {
	ours := make(map[string]bool, len(refs))
	for _, r := range refs {
		ours[r.ID] = true
	}
	params := &api.SandboxesParams{Limit: new(pageLimit)}
	if cursor != "" {
		params.Cursor = &cursor
	}
	raw, err := p.send(ctx, noEffect, func(ctx context.Context) (*http.Response, error) {
		return p.api.Sandboxes(ctx, params)
	})
	if err != nil {
		return provider.Usage{}, err
	}
	var page sandboxPage
	if err := decode(raw, &page); err != nil {
		return provider.Usage{}, internal(err)
	}
	out := provider.Usage{TotalMicros: -1, Complete: true, Attributed: true}
	for _, sb := range page.Sandboxes {
		made, ok := parseTime(sb.CreatedAt)
		if sb.ID == "" || ours[sb.ID] || sb.Labels["bugnet"] != "1" || !ok ||
			made.Before(from) || !made.Before(to) {
			continue
		}
		out.Sandboxes = append(out.Sandboxes, provider.SandboxUsage{
			ID: sb.ID, CostMicros: -1, Deployment: sb.Labels[deploymentLabel],
		})
	}
	if next := page.PageInfo.NextCursor; page.PageInfo.HasMore && next != "" {
		out.Next = listCursor + next
	}
	return out, nil
}
