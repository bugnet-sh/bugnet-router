package koyeb

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// usageDetail is one instance's billed time in GET /v1/usages/details
type usageDetail struct {
	AppID           string  `json:"app_id"`
	InstanceType    string  `json:"instance_type"`
	DurationSeconds flexInt `json:"duration_seconds"`
}

// usageKeys are the names the detail list may come under in the reply
var usageKeys = []string{"usage_details", "usages", "details"}

// ReadUsage reads the organization's usage details in the window a page of 100 at a time. The
// cursor is the offset of the next detail. Koyeb names an instance by its app and service, so no
// deployment is attributed.
func (p *Provider) ReadUsage(
	ctx context.Context, from, to time.Time, _ []provider.Ref, cursor string,
) (provider.Usage, error) {
	offset, _ := strconv.Atoi(cursor)
	q := url.Values{
		"starting_time": {from.UTC().Format(time.RFC3339)},
		"ending_time":   {to.UTC().Format(time.RFC3339)},
		"limit":         {pageLimit}, "offset": {strconv.Itoa(max(offset, 0))},
	}
	var raw json.RawMessage
	err := p.api.send(ctx, false, http.MethodGet, "/v1/usages/details", q, nil, &raw)
	if err != nil {
		return provider.Usage{}, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return provider.Usage{}, internal(fmt.Errorf("koyeb usage reply: %w", err))
	}
	var details []usageDetail
	for _, key := range usageKeys {
		if items := fields[key]; len(items) > 0 && string(items) != "null" {
			if err := json.Unmarshal(items, &details); err != nil {
				return provider.Usage{}, internal(fmt.Errorf("koyeb usage reply: %w", err))
			}
			break
		}
	}
	// Koyeb bills by the second as the day goes on, so a past day is complete
	out := provider.Usage{TotalMicros: -1, Complete: !to.After(time.Now())}
	for _, d := range details {
		u := provider.SandboxUsage{ID: d.AppID, Seconds: int64(d.DurationSeconds), CostMicros: -1}
		for _, t := range instanceTypes {
			if t.id == d.InstanceType {
				u.CostMicros = t.secondMicros * u.Seconds
			}
		}
		out.Sandboxes = append(out.Sandboxes, u)
	}
	if n, _ := strconv.Atoi(pageLimit); len(details) >= n {
		out.Next = strconv.Itoa(offset + len(details))
	}
	return out, nil
}
