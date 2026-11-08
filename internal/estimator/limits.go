package estimator

import (
	"context"
	"sync"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/ratelimit"
)

// vkWait is how long Valkey may take before a replica uses its in-process bucket
const vkWait = 100 * time.Millisecond

// buckets takes tokens from the three estimate buckets in Valkey, and from an in-process bucket
// at the same rate while Valkey does not answer within 100 ms
type buckets struct {
	rl    *ratelimit.Limiter
	now   func() time.Time
	mu    sync.Mutex
	local map[string]*localBucket
}

type localBucket struct {
	tokens float64
	at     time.Time
}

func newBuckets(c valkey.Client, prefix string, now func() time.Time) *buckets {
	return &buckets{rl: ratelimit.New(c, prefix, 1, 1), now: now, local: map[string]*localBucket{}}
}

// take spends one token of b, or says how long until there is one
func (s *buckets) take(ctx context.Context, b ratelimit.Bucket) (bool, time.Duration) {
	vctx, cancel := context.WithTimeout(ctx, vkWait)
	defer cancel()
	ok, wait, err := s.rl.TakeKey(vctx, b)
	if err == nil {
		return ok, wait
	}
	return s.takeLocal(b)
}

func (s *buckets) takeLocal(b ratelimit.Bucket) (bool, time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	lb := s.local[b.Key]
	if lb == nil {
		lb = &localBucket{tokens: float64(b.Burst), at: now}
		s.local[b.Key] = lb
	}
	lb.tokens = min(float64(b.Burst), lb.tokens+now.Sub(lb.at).Seconds()*b.PerSecond)
	lb.at = now
	if lb.tokens < 1 {
		return false, time.Duration((1 - lb.tokens) / b.PerSecond * float64(time.Second))
	}
	lb.tokens--
	return true, 0
}
