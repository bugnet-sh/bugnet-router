package hf

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/gpustatus"
)

var (
	_ provider.AccountChecker = (*Provider)(nil)
	_ provider.LimitsReader   = (*Provider)(nil)
)

const (
	// accountWindow is how long a Job HF ended for the account's sake speaks for it, design 4.9
	accountWindow = 10 * time.Minute
	// maxSightings bounds the sightings kept, the first seen going first
	maxSightings = 256
	// detailSightings is how many sightings an Account's Detail names
	detailSightings = 3
)

// unfundedKills are the cancel reasons that say the account cannot pay, design 4.9
var unfundedKills = map[string]bool{gpustatus.HFNoCredits: true, gpustatus.HFNoSubscription: true}

// sighting is one answer of HF that spoke of the account
type sighting struct {
	// what says what HF answered, in words free of the token
	what     string
	unfunded bool
	// at is when HF ended the Job or answered, seen when the adapter first saw it
	at, seen time.Time
}

// accountLog keeps the sightings by Job or by route, so a Job read again later is not new
type accountLog struct {
	mu        sync.Mutex
	sightings map[string]sighting
}

// add keeps s under key, a key seen before keeping its first sighting unless renew is set
func (l *accountLog) add(key string, s sighting, renew bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if old, ok := l.sightings[key]; ok && !renew {
		if old.at.Before(s.at) {
			s.at = old.at
		}
		s.seen = old.seen
	}
	if l.sightings == nil {
		l.sightings = map[string]sighting{}
	}
	l.sightings[key] = s
	for len(l.sightings) > maxSightings {
		oldest := ""
		for k, v := range l.sightings {
			if oldest == "" || v.seen.Before(l.sightings[oldest].seen) {
				oldest = k
			}
		}
		delete(l.sightings, oldest)
	}
}

// recent returns the sightings of the last accountWindow, newest first
func (l *accountLog) recent(now time.Time) []sighting {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []sighting
	for _, s := range l.sightings {
		if now.Sub(s.at) <= accountWindow {
			out = append(out, s)
		}
	}
	slices.SortFunc(out, func(a, b sighting) int {
		return cmp.Or(b.at.Compare(a.at), strings.Compare(a.what, b.what))
	})
	return out
}

// noteJob keeps a Job HF ended for the account's sake, from when HF says it ended
func (c *jobsClient) noteJob(j job) {
	r := cancelReason(j)
	if !accountKills[r] {
		return
	}
	now := c.now()
	at := now
	if end, ok := endTime(j.FinishedAt); ok && end.Before(now) {
		at = end
	}
	c.account.add("job "+j.ID, sighting{
		what: fmt.Sprintf("hf ended Job %.40q for %s", j.ID, r), unfunded: unfundedKills[r],
		at: at, seen: now,
	}, false)
}

// noteRefusal keeps a call HF refused for credit, the newest refusal of each route counting
func (c *jobsClient) noteRefusal(route string, ae *apiError) {
	now := c.now()
	c.account.add("call "+route, sighting{
		what: route + " " + ae.Error(), unfunded: true, at: now, seen: now,
	}, true)
}

// endTime reads an RFC 3339 time, false for null or any other shape
func endTime(raw json.RawMessage) (time.Time, bool) {
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	return t, err == nil
}

// CheckAccount asks HF whether the token may write Jobs, then reads what Jobs answers said of
// the account in the last 10 minutes, as HF documents no balance, design 4.9
func (p *Provider) CheckAccount(ctx context.Context) (provider.Account, error) {
	if err := p.jobs.authCheck(ctx); err != nil {
		return provider.AccountReading(err)
	}
	var unfunded, limits []sighting
	for _, s := range p.jobs.account.recent(p.jobs.now()) {
		if s.unfunded {
			unfunded = append(unfunded, s)
		} else {
			limits = append(limits, s)
		}
	}
	if len(limits) > 0 {
		p.log.Error("hf: Jobs ended at a limit the account sets", "jobs", describe(limits))
	}
	if len(unfunded) > 0 {
		return provider.Account{
			Block: provider.BlockUnfunded, Detail: describe(append(unfunded, limits...)),
		}, nil
	}
	return provider.Account{Detail: describe(limits)}, nil
}

// describe names the first sightings and counts the rest
func describe(ss []sighting) string {
	words := make([]string, 0, detailSightings+1)
	for i, s := range ss {
		if i == detailSightings {
			words = append(words, fmt.Sprintf("and %d more", len(ss)-i))
			break
		}
		words = append(words, s.what)
	}
	return strings.Join(words, ", ")
}

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
