package hf

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/valkey-io/valkey-go"

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
	// sharedTimeout bounds a call to the shared sightings, past it this process's own serve alone
	sharedTimeout = time.Second
	// sharedRetryAfter keeps a hung Valkey from slowing every Jobs answer by sharedTimeout
	sharedRetryAfter = 30 * time.Second
)

// unfundedKills are the cancel reasons that say the account cannot pay, design 4.9
var unfundedKills = map[string]bool{gpustatus.HFNoCredits: true, gpustatus.HFNoSubscription: true}

// sighting is one answer of HF that spoke of the account
type sighting struct {
	// what says what HF answered, cut and free of the token
	what     string
	unfunded bool
	// at is when HF ended the Job or answered, seen when the adapter first saw it
	at, seen time.Time
}

// accountLog keeps the sightings by Job or by route, so a Job read again later is not new
type accountLog struct {
	mu        sync.Mutex
	sightings map[string]sighting
	// shared keeps the sightings of router-api and router-worker alike, nil for this process's
	shared *sharedSightings
	// sharedDown is set while Valkey fails, which is then skipped until sharedRetry
	sharedDown  bool
	sharedRetry time.Time
}

// add keeps s under key and returns what it kept, a key seen before keeping its first time
func (l *accountLog) add(key string, s sighting, renew bool) sighting {
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
	return s
}

// window returns this process's sightings of the accountWindow before now, by key
func (l *accountLog) window(now time.Time) map[string]sighting {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := map[string]sighting{}
	for k, s := range l.sightings {
		if now.Sub(s.at) <= accountWindow {
			out[k] = s
		}
	}
	return out
}

// useShared reports whether to ask Valkey, which is skipped for a while after it fails
func (l *accountLog) useShared(now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.shared != nil && (!l.sharedDown || !now.Before(l.sharedRetry))
}

// noteShared logs when the shared sightings stop or start answering
func (c *jobsClient) noteShared(err error) {
	l := &c.account
	l.mu.Lock()
	defer l.mu.Unlock()
	if err != nil {
		l.sharedRetry = time.Now().Add(sharedRetryAfter)
	}
	if down := err != nil; down != l.sharedDown {
		l.sharedDown = down
		if down {
			c.log.Warn("hf: account sightings in valkey unavailable, the account check reads "+
				"this process's own", "err", err)
		} else {
			c.log.Info("hf: account sightings in valkey answer again")
		}
	}
}

// recent returns the last accountWindow's sightings newest first, from Valkey unless it fails
func (c *jobsClient) recent(ctx context.Context) []sighting {
	now := c.now()
	var byKey map[string]sighting
	if c.account.useShared(time.Now()) {
		// A check given up on says nothing of Valkey, so the read has its own deadline
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sharedTimeout)
		shared, err := c.account.shared.window(sctx, now)
		cancel()
		c.noteShared(err)
		byKey = shared
	}
	if byKey == nil {
		byKey = c.account.window(now)
	}
	out := slices.Collect(maps.Values(byKey))
	slices.SortFunc(out, func(a, b sighting) int {
		return cmp.Or(b.at.Compare(a.at), strings.Compare(a.what, b.what))
	})
	return out
}

// note keeps s under key in this process and in Valkey, for the account check of every process
func (c *jobsClient) note(ctx context.Context, key string, s sighting, renew bool) {
	s = c.account.add(key, s, renew)
	if !c.account.useShared(time.Now()) {
		return
	}
	// A wait or create that ended with this answer still leaves its sighting
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sharedTimeout)
	defer cancel()
	c.noteShared(c.account.shared.add(sctx, key, s, renew, c.now()))
}

// noteJob keeps a Job HF ended for the account's sake, from when HF says it ended
func (c *jobsClient) noteJob(ctx context.Context, j job) {
	r := cancelReason(j)
	if !accountKills[r] {
		return
	}
	now := c.now()
	at := now
	if end, ok := endTime(j.FinishedAt); ok && end.Before(now) {
		at = end
	}
	id := c.jobName(j.ID)
	c.note(ctx, "job "+id, sighting{
		what: fmt.Sprintf("hf ended Job %.40q for %s", id, r), unfunded: unfundedKills[r],
		at: at, seen: now,
	}, false)
}

// noteRefusal keeps a call HF refused for credit, the newest refusal of each route counting
func (c *jobsClient) noteRefusal(ctx context.Context, route string, ae *apiError) {
	now := c.now()
	c.note(ctx, "call "+route, sighting{
		what: route + " " + ae.Error(), unfunded: true, at: now, seen: now,
	}, true)
}

/* Shared sightings */

// sharedSightings keeps every process's sightings in Valkey, a sorted set beside a hash
type sharedSightings struct {
	c valkey.Client
	// times scores each key by its sighting's time in Unix ms, words holds what each one says
	times, words string
}

// sharedWords is what one sighting says, as it lies in Valkey
type sharedWords struct {
	What     string `json:"what"`
	Unfunded bool   `json:"unfunded"`
}

// newSharedSightings keeps the sightings in vk under prefix
func newSharedSightings(vk valkey.Client, prefix string) *sharedSightings {
	return &sharedSightings{
		c: vk, times: prefix + "hf:sightings", words: prefix + "hf:sightings:words",
	}
}

// The set keeps a key's earlier time unless renewed, then drops what left the window or the bound
var addSightingScript = valkey.NewLuaScript(`
if ARGV[7] == 'renew' then
	redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
else
	redis.call('ZADD', KEYS[1], 'LT', ARGV[1], ARGV[2])
end
redis.call('HSET', KEYS[2], ARGV[2], ARGV[3])
local drop = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', '(' .. ARGV[4])
local over = redis.call('ZCARD', KEYS[1]) - #drop - tonumber(ARGV[5])
if over > 0 then
	for _, k in ipairs(redis.call('ZRANGE', KEYS[1], #drop, #drop + over - 1)) do
		drop[#drop + 1] = k
	end
end
for _, k in ipairs(drop) do
	redis.call('ZREM', KEYS[1], k)
	redis.call('HDEL', KEYS[2], k)
end
redis.call('PEXPIRE', KEYS[1], ARGV[6])
redis.call('PEXPIRE', KEYS[2], ARGV[6])
return 1
`)

// add keeps s under key, trimmed to the accountWindow before now and the newest maxSightings
func (sh *sharedSightings) add(
	ctx context.Context, key string, s sighting, renew bool, now time.Time,
) error {
	words, err := json.Marshal(sharedWords{What: s.what, Unfunded: s.unfunded})
	if err != nil {
		return err
	}
	ms := func(t time.Time) string { return strconv.FormatInt(t.UnixMilli(), 10) }
	mode := "keep"
	if renew {
		mode = "renew"
	}
	args := []string{
		ms(s.at), key, string(words), ms(now.Add(-accountWindow)), strconv.Itoa(maxSightings),
		strconv.FormatInt(accountWindow.Milliseconds(), 10), mode,
	}
	if err := addSightingScript.Exec(ctx, sh.c, []string{sh.times, sh.words}, args).
		Error(); err != nil {
		return fmt.Errorf("keep hf account sighting in valkey: %w", err)
	}
	return nil
}

// windowScript answers each key of the window with its time and its words, empty when gone
var windowScript = valkey.NewLuaScript(`
local keys = redis.call('ZRANGEBYSCORE', KEYS[1], ARGV[1], '+inf', 'WITHSCORES')
local out = {}
for i = 1, #keys, 2 do
	out[#out + 1] = keys[i]
	out[#out + 1] = keys[i + 1]
	out[#out + 1] = redis.call('HGET', KEYS[2], keys[i]) or ''
end
return out
`)

// window returns every process's sightings of the accountWindow before now, by key
func (sh *sharedSightings) window(ctx context.Context, now time.Time) (map[string]sighting, error) {
	from := strconv.FormatInt(now.Add(-accountWindow).UnixMilli(), 10)
	flat, err := windowScript.Exec(ctx, sh.c, []string{sh.times, sh.words}, []string{from}).
		AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("read hf account sightings from valkey: %w", err)
	}
	out := map[string]sighting{}
	for i := 0; i+2 < len(flat); i += 3 {
		// Valkey writes a score as a double, which may take an exponent
		ms, err := strconv.ParseFloat(flat[i+1], 64)
		var w sharedWords
		// A sighting whose words are gone or unreadable cannot say whether it is unfunded
		if err != nil || json.Unmarshal([]byte(flat[i+2]), &w) != nil {
			continue
		}
		at := time.UnixMilli(int64(ms))
		out[flat[i]] = sighting{what: w.What, unfunded: w.Unfunded, at: at, seen: at}
	}
	return out, nil
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
	for _, s := range p.jobs.recent(ctx) {
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
