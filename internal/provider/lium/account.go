package lium

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

var (
	_ provider.AccountChecker = (*Provider)(nil)
	_ provider.LimitsReader   = (*Provider)(nil)
	_ provider.UsageReader    = (*Provider)(nil)
)

const (
	// defaultAccountEvery is limits_poll.account_every's default
	defaultAccountEvery = 5 * time.Minute
	// rentRoom is the time of each pod Lium wants the balance to cover before it rents another
	rentRoom = 15 * time.Minute
	// warningPodsStopped is the balance warning of an account whose pods Lium stopped at zero
	warningPodsStopped = "pods_stopped"
	// warningUnknown names a balance warning whose kind cannot be read
	warningUnknown = "unknown"
	// maxUSD bounds an amount the adapter reads, far above any balance
	maxUSD = 1e9
	// maxBilledSeconds bounds a pod's billed seconds, far above any pod's life
	maxBilledSeconds = 1e9
	// usagePage is how many records one page of ReadUsage reads
	usagePage = 25
	// usageRefs and usageListing start the cursors of the two phases of ReadUsage
	usageRefs    = "r"
	usageListing = "l:"
)

// userMe holds the fields of GET /users/me the account check reads, their types unverified
type userMe struct {
	Balance json.RawMessage `json:"balance"`
	Warning json.RawMessage `json:"balance_warning"`
}

// CheckAccount reads the balance and its warning and holds the balance to design 3.9's headroom
func (p *Provider) CheckAccount(ctx context.Context) (provider.Account, error) {
	var me userMe
	if _, err := p.api.do(ctx, call{method: http.MethodGet, route: "/users/me"}, &me); err != nil {
		return provider.AccountReading(err)
	}
	kind := warningKind(me.Warning)
	if kind == warningPodsStopped {
		return provider.Account{Block: provider.BlockUnfunded,
			Detail: "lium warns " + kind + ", it stopped the account's pods at zero balance"}, nil
	}
	balance, ok := microsOf(me.Balance)
	if !ok {
		return provider.Account{}, &provider.Error{Code: provider.CodeInternal,
			Err: errors.New("lium GET /users/me names no balance the adapter can read")}
	}
	var pods []podItem
	// GET /pods answers BALANCE_REQUIRED on an account with no balance and no pods
	if _, err := p.api.do(ctx, call{method: http.MethodGet, route: "/pods"}, &pods); err != nil {
		return provider.AccountReading(err)
	}
	var acc provider.Account
	if kind != "" {
		p.log.Error("lium: the account has a balance warning", "kind", kind,
			"balance", usd(balance))
		acc.Detail = "lium warns " + kind
	}
	if need := p.headroom(pods); balance < need {
		detail := fmt.Sprintf("balance %s is under the headroom %s", usd(balance), usd(need))
		if acc.Detail != "" {
			detail += ", " + acc.Detail
		}
		return provider.Account{Block: provider.BlockUnfunded, Detail: detail}, nil
	}
	return acc, nil
}

// headroom is design 3.9's floor, 15 minutes of each billed pod and of every slot past a check
func (p *Provider) headroom(pods []podItem) int64 {
	dearest := p.sizes[len(p.sizes)-1].MaxHourlyMicros
	var need int64
	for _, pod := range pods {
		// A stopped pod is not charged, and a deleting one is gone
		if pod.Status == statusDeleting || stoppedStatuses[pod.Status] {
			continue
		}
		price := dearest
		if pod.Price != nil && *pod.Price >= 0 && *pod.Price < maxUSD {
			price = micros(*pod.Price)
		}
		need += ceilDiv(price*int64(rentRoom/time.Second), 3600)
	}
	window := int64((p.accountEvery + rentRoom) / time.Second)
	return need + ceilDiv(dearest*int64(p.slots)*window, 3600)
}

// warningKind reads balance_warning's kind from an object or a bare word, empty for none
func warningKind(raw json.RawMessage) string {
	if !present(raw) {
		return ""
	}
	var w struct {
		Kind string `json:"kind"`
	}
	if json.Unmarshal(raw, &w) == nil && w.Kind != "" {
		return w.Kind
	}
	var s string
	if json.Unmarshal(raw, &s) == nil && s != "" {
		return s
	}
	return warningUnknown
}

// microsOf reads USD written as a JSON number or a number in a string, in micro-USD
func microsOf(raw json.RawMessage) (int64, bool) {
	f, ok := numberOf(raw)
	if !ok || math.Abs(f) >= maxUSD {
		return 0, false
	}
	return micros(f), true
}

// numberOf reads a JSON number or a number in a string, never NaN or infinite
func numberOf(raw json.RawMessage) (float64, bool) {
	if !present(raw) {
		return 0, false
	}
	var f float64
	if json.Unmarshal(raw, &f) != nil {
		var s string
		if json.Unmarshal(raw, &s) != nil {
			return 0, false
		}
		var err error
		if f, err = strconv.ParseFloat(strings.TrimSpace(s), 64); err != nil {
			return 0, false
		}
	}
	return f, !math.IsNaN(f) && !math.IsInf(f, 0)
}

// usd writes micro-USD as dollars and cents
func usd(m int64) string {
	return fmt.Sprintf("$%.2f", float64(m)/1e6)
}

// ReadLimits counts the pods the key sees that are not being deleted, as Lium names no cap
func (p *Provider) ReadLimits(ctx context.Context) (provider.Limits, error) {
	pods, err := p.listPods(ctx)
	if err != nil {
		return provider.Limits{}, err
	}
	n := 0
	for _, pod := range pods {
		if pod.Status != statusDeleting {
			n++
		}
	}
	return provider.Limits{Active: n}, nil
}

// podRecord holds the fields of GET /pods/{id}/record the cost check reads, their types unverified
type podRecord struct {
	Name          json.RawMessage `json:"pod_name"`
	Removed       json.RawMessage `json:"removed"`
	RemovedAt     json.RawMessage `json:"removed_at"`
	BilledSeconds json.RawMessage `json:"billed_seconds"`
	Total         json.RawMessage `json:"total"`
}

// ReadUsage reads each ref's record 25 a page, then lists the key's pods to name ours not in refs
func (p *Provider) ReadUsage(
	ctx context.Context, from, to time.Time, refs []provider.Ref, cursor string,
) (provider.Usage, error) {
	if cursor == usageListing {
		return p.usageListing(ctx, from, to, refs)
	}
	var ids []string
	for _, r := range refs {
		if id, ok := podID(r); ok {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	ids = slices.Compact(ids)
	offset := 0
	if rest, ok := strings.CutPrefix(cursor, usageRefs); ok {
		offset, _ = strconv.Atoi(rest)
	}
	offset = min(max(offset, 0), len(ids))
	end := min(offset+usagePage, len(ids))
	out := provider.Usage{TotalMicros: -1, Complete: true, Attributed: true, Next: usageListing}
	for _, id := range ids[offset:end] {
		u, final, found, err := p.podUsage(ctx, id)
		if err != nil {
			return provider.Usage{}, err
		}
		if found {
			out.Sandboxes = append(out.Sandboxes, u)
			out.Complete = out.Complete && final
		}
	}
	if end < len(ids) {
		out.Next = usageRefs + strconv.Itoa(end)
	}
	return out, nil
}

// podUsage reads one pod's record, final once Lium removed the pod, found false with no record
func (p *Provider) podUsage(
	ctx context.Context, id string,
) (u provider.SandboxUsage, final, found bool, err error) {
	var r podRecord
	_, err = p.api.do(ctx, call{method: http.MethodGet, route: podRoute(id, "/record")}, &r)
	if e, ok := provider.AsError(err); ok && e.Code == provider.CodeNotFound {
		return u, false, false, nil
	}
	if err != nil {
		return u, false, false, err
	}
	u = provider.SandboxUsage{ID: id, CostMicros: -1}
	if s, ok := numberOf(r.BilledSeconds); ok && s >= 0 && s < maxBilledSeconds {
		u.Seconds = int64(math.Round(s))
	}
	// The unit of total is unverified, read as USD as every other amount Lium writes
	if m, ok := microsOf(r.Total); ok && m >= 0 {
		u.CostMicros = m
	}
	var name string
	if json.Unmarshal(r.Name, &name) == nil {
		if _, labels, ok := parsePodName(name); ok {
			u.Deployment = labels[labelDeployment]
		}
	}
	var removed bool
	_ = json.Unmarshal(r.Removed, &removed)
	return u, removed || present(r.RemovedAt), true, nil
}

// usageListing names the bugnet pods the key sees, made in the window, that are not in refs
func (p *Provider) usageListing(
	ctx context.Context, from, to time.Time, refs []provider.Ref,
) (provider.Usage, error) {
	pods, err := p.listPods(ctx)
	if err != nil {
		return provider.Usage{}, err
	}
	ours := make(map[string]bool, len(refs))
	for _, r := range refs {
		ours[r.ID] = true
	}
	out := provider.Usage{TotalMicros: -1, Complete: true, Attributed: true}
	for _, pod := range pods {
		_, labels, named := parsePodName(pod.Name)
		made, ok := parseTime(pod.CreatedAt)
		if pod.ID == "" || ours[pod.ID] || !named || labels[labelBugnet] != "1" || !ok ||
			made.Before(from) || !made.Before(to) {
			continue
		}
		out.Sandboxes = append(out.Sandboxes, provider.SandboxUsage{
			ID: pod.ID, CostMicros: -1, Deployment: labels[labelDeployment],
		})
	}
	return out, nil
}
