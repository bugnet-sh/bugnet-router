package fetch

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/valkey-io/valkey-go"
)

// Resource names of the upstream limit buckets
const (
	ResourceGitHubCore    = "gh:core"
	ResourceGitHubGraphQL = "gh:graphql"
	ResourceHuggingFace   = "hf"
)

// vkTimeout is how long Valkey may take before the estimator goes without it
const vkTimeout = 100 * time.Millisecond

// limitState is one upstream bucket as last seen
type limitState struct {
	remaining, limit int
	reset            time.Time
}

// Guard keeps each replica inside Bugnet's share of the upstream rate limits
//
// Every answer writes the bucket's remaining count and reset to the resource's Valkey hash. Below
// the reserve share of the limit, no replica starts a new fetch of that kind until the reset. When
// Valkey does not answer, the replica goes by the last value it saw.
type Guard struct {
	c       valkey.Client
	prefix  string
	reserve float64
	log     *slog.Logger
	now     func() time.Time

	mu    sync.Mutex
	local map[string]limitState
}

// NewGuard returns a Guard whose reserve is a share of the hourly limit
func NewGuard(c valkey.Client, prefix string, reserve float64, log *slog.Logger) *Guard {
	if log == nil {
		log = slog.Default()
	}
	return &Guard{c: c, prefix: prefix, reserve: reserve, log: log, now: time.Now,
		local: map[string]limitState{}}
}

func (g *Guard) key(resource string) string { return g.prefix + "est:rl:" + resource }

// Record stores the bucket an answer's headers describe, named by x-ratelimit-resource when set
func (g *Guard) Record(ctx context.Context, resource string, h http.Header) {
	if name := h.Get("X-Ratelimit-Resource"); name == "core" || name == "graphql" {
		resource = "gh:" + name
	}
	remaining, err1 := strconv.Atoi(h.Get("X-Ratelimit-Remaining"))
	limit, err2 := strconv.Atoi(h.Get("X-Ratelimit-Limit"))
	reset, err3 := strconv.ParseInt(h.Get("X-Ratelimit-Reset"), 10, 64)
	if err1 != nil || err3 != nil || reset <= 0 {
		return
	}
	if err2 != nil {
		limit = 0
	}
	g.put(ctx, resource, limitState{remaining, limit, time.Unix(reset, 0)})
}

// Exhaust marks the bucket spent until the time given
func (g *Guard) Exhaust(ctx context.Context, resource string, until time.Time) {
	g.put(ctx, resource, limitState{reset: until})
}

func (g *Guard) put(ctx context.Context, resource string, s limitState) {
	g.mu.Lock()
	g.local[resource] = s
	g.mu.Unlock()
	if !s.reset.After(g.now()) {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, vkTimeout)
	defer cancel()
	key := g.key(resource)
	cmds := valkey.Commands{
		g.c.B().Hset().Key(key).FieldValue().
			FieldValue("remaining", strconv.Itoa(s.remaining)).
			FieldValue("limit", strconv.Itoa(s.limit)).
			FieldValue("reset", strconv.FormatInt(s.reset.Unix(), 10)).Build(),
		g.c.B().Pexpireat().Key(key).MillisecondsTimestamp(s.reset.UnixMilli()).Build(),
	}
	for _, r := range g.c.DoMulti(ctx, cmds...) {
		if err := r.Error(); err != nil {
			g.log.Warn("estimator: write rate limit state", "resource", resource, "err", err)
			return
		}
	}
}

// Allow says whether a new fetch of this resource may start, and when it may if it may not
func (g *Guard) Allow(ctx context.Context, resource string) (bool, time.Time) {
	s := g.read(ctx, resource)
	if !s.reset.After(g.now()) {
		return true, time.Time{}
	}
	if float64(s.remaining) < g.reserve*float64(s.limit) || (s.limit == 0 && s.remaining == 0) {
		return false, s.reset
	}
	return true, time.Time{}
}

// read prefers Valkey's state and falls back to the last one this replica saw
func (g *Guard) read(ctx context.Context, resource string) limitState {
	ctx, cancel := context.WithTimeout(ctx, vkTimeout)
	defer cancel()
	m, err := g.c.Do(ctx, g.c.B().Hgetall().Key(g.key(resource)).Build()).AsStrMap()
	if err == nil && len(m) > 0 {
		rem, _ := strconv.Atoi(m["remaining"])
		lim, _ := strconv.Atoi(m["limit"])
		reset, _ := strconv.ParseInt(m["reset"], 10, 64)
		return limitState{rem, lim, time.Unix(reset, 0)}
	}
	if err == nil {
		// Valkey answered and holds nothing, so the bucket expired or no replica spent it
		return limitState{}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.local[resource]
}
