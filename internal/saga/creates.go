package saga

import (
	"context"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/health"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/ratelimit"
)

// createWait is how long an attempt waits for a create token before it fails over
var createWait = time.Second

const (
	// maxCreatePause is the longest a provider's creates pause on its Retry-After
	maxCreatePause = 5 * time.Minute
	// reasonRateLimited is the failure reason of an attempt that found no create token
	reasonRateLimited = "rate_limited"
)

// createBucket is the provider's create bucket, false when its creates_per_second is 0
func (s *Saga) createBucket(name string) (ratelimit.Bucket, bool) {
	rate := s.d.CreatesPerSecond[name]
	if rate <= 0 {
		return ratelimit.Bucket{}, false
	}
	return ratelimit.Bucket{Key: "rl:create:" + name, PerSecond: float64(rate), Burst: rate}, true
}

// takeCreate spends a create token of name, waiting up to createWait for one, false if none came
func (s *Saga) takeCreate(ctx context.Context, name string) bool {
	b, ok := s.createBucket(name)
	if !ok {
		return true
	}
	limit := time.Now().Add(createWait)
	for {
		taken, wait, err := s.limits.TakeKey(ctx, b)
		if err != nil {
			// A bucket that cannot be read must not stop every start
			s.d.Log.Warn("create bucket unreadable, creating without it", "provider", name,
				"err", err)
			return true
		}
		if taken {
			return true
		}
		if time.Now().Add(wait).After(limit) {
			return false
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
	}
}

func (s *Saga) pauseKey(name string) string { return s.d.Prefix + "pause:" + name + ":create" }

// createsPaused reports whether name told us to wait before creating, a failed read meaning no
func (s *Saga) createsPaused(ctx context.Context, name string) bool {
	if s.d.Valkey == nil {
		return false
	}
	n, err := s.d.Valkey.Do(ctx, s.d.Valkey.B().Exists().Key(s.pauseKey(name)).Build()).AsInt64()
	return err == nil && n > 0
}

// pauseCreates holds the creates on name back for wait, at most maxCreatePause
func (s *Saga) pauseCreates(ctx context.Context, name string, wait time.Duration) {
	if s.d.Valkey == nil || wait <= 0 {
		return
	}
	wait = min(wait, maxCreatePause)
	cmd := s.d.Valkey.B().Set().Key(s.pauseKey(name)).Value("1").
		PxMilliseconds(wait.Milliseconds()).Build()
	if err := s.d.Valkey.Do(ctx, cmd).Error(); err != nil {
		s.d.Log.Warn("cannot pause creates", "provider", name, "err", err)
	}
}

// providerFailed acts on an error from name and reports whether it counts against the provider
func (s *Saga) providerFailed(ctx context.Context, name string, err error, creating bool) bool {
	e, ok := provider.AsError(err)
	if !ok {
		return true
	}
	// A rate limit or a quota refusal is the account's, never the provider's health
	switch {
	case provider.AccountBlock(e.Code) != provider.BlockNone:
		health.NoteAccount(ctx, s.d.River, name, e.Code)
	case e.Code == provider.CodeQuota:
		health.NoteQuota(ctx, s.d.River, name)
		return false
	case e.Code == provider.CodeRateLimited:
		if creating {
			s.pauseCreates(ctx, name, e.RetryAfter)
		}
		return false
	}
	return true
}
