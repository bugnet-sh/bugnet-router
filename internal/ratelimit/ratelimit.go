// Package ratelimit bounds operations with token buckets every replica shares in Valkey.
//
// Take spends from a placement's operation bucket, ops:<placement>. TakeKey, TakeAll, Peek and
// PeekAll work on any bucket a caller names, whose refill may be a fraction of a token a second, so
// limits such as 10 an hour need no other form.
//
// One script serves every form. A bucket holds micro-tokens timed by the Valkey server's clock in
// microseconds, so a rate of tokens a second is also micro-tokens a microsecond. Peek writes
// nothing.
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/valkey-io/valkey-go"
)

// Defaults of the operation bucket, its refill a second and its capacity
const (
	DefaultPerSecond = 10
	DefaultBurst     = 20
)

// Script modes
const (
	modeTake = "take"
	modePeek = "peek"
)

// takeScript takes one token from every bucket in KEYS when each has one, and none otherwise
var takeScript = valkey.NewLuaScript(`
local mode = ARGV[1]
local now = redis.call('TIME')
now = tonumber(now[1]) * 1000000 + tonumber(now[2])
local wait = 0
local held = {}
for i, key in ipairs(KEYS) do
	local rate, cap = tonumber(ARGV[i * 2]), tonumber(ARGV[i * 2 + 1]) * 1000000
	local got = redis.call('HMGET', key, 'tokens', 'at')
	local tokens, at = tonumber(got[1]) or cap, tonumber(got[2]) or now
	-- A clock behind the bucket's, as after a failover, refills from now and not from the old clock
	if now < at then
		at = now
		if mode == 'take' then
			redis.call('HSET', key, 'at', at)
			redis.call('PEXPIRE', key, math.ceil((cap - tokens) / rate / 1000))
		end
	end
	tokens = math.min(cap, tokens + math.max(0, now - at) * rate)
	if tokens < 1000000 then
		wait = math.max(wait, math.ceil((1000000 - tokens) / rate / 1000))
	end
	held[i] = {tokens, cap, rate}
end
if wait > 0 then
	return {0, wait}
end
if mode == 'take' then
	for i, key in ipairs(KEYS) do
		local tokens, cap, rate = held[i][1] - 1000000, held[i][2], held[i][3]
		redis.call('HSET', key, 'tokens', tokens, 'at', now)
		redis.call('PEXPIRE', key, math.ceil((cap - tokens) / rate / 1000))
	end
end
return {1, 0}
`)

// Bucket names one keyed bucket, its refill a second (fractions allowed) and its capacity
type Bucket struct {
	Key       string // under the limiter's prefix, such as "rl:links:<placement>"
	PerSecond float64
	Burst     int
}

// PerHour is the refill a second of n tokens an hour
func PerHour(n int) float64 { return float64(n) / 3600 }

// PerMinute is the refill a second of n tokens a minute
func PerMinute(n int) float64 { return float64(n) / 60 }

// Limiter gives out tokens from buckets in Valkey under one key prefix
type Limiter struct {
	c         valkey.Client
	prefix    string
	perSecond int
	burst     int
}

// New returns a Limiter whose operation buckets refill perSecond up to burst, the defaults below 1
func New(c valkey.Client, prefix string, perSecond, burst int) *Limiter {
	if perSecond < 1 {
		perSecond = DefaultPerSecond
	}
	if burst < 1 {
		burst = DefaultBurst
	}
	return &Limiter{c: c, prefix: prefix, perSecond: perSecond, burst: burst}
}

// Ops is the placement's operation bucket, ops:<placement>
func (l *Limiter) Ops(placement uuid.UUID) Bucket {
	return Bucket{
		Key: "ops:" + placement.String(), PerSecond: float64(l.perSecond), Burst: l.burst,
	}
}

// Take spends a token of the placement's bucket, or says how long until one is there
func (l *Limiter) Take(ctx context.Context, placement uuid.UUID) (bool, time.Duration, error) {
	ok, wait, err := l.run(ctx, modeTake, []Bucket{l.Ops(placement)})
	if err != nil {
		return false, 0, fmt.Errorf("take an operation of placement %s: %w", placement, err)
	}
	return ok, wait, nil
}

// TakeKey spends one token of the bucket, or says how long until one is there
func (l *Limiter) TakeKey(ctx context.Context, b Bucket) (bool, time.Duration, error) {
	return l.run(ctx, modeTake, []Bucket{b})
}

// TakeAll spends one token from every bucket or from none, in one script
func (l *Limiter) TakeAll(ctx context.Context, bs []Bucket) (bool, time.Duration, error) {
	if len(bs) == 0 {
		return true, 0, nil
	}
	return l.run(ctx, modeTake, bs)
}

// Peek answers what TakeKey would, and spends nothing
func (l *Limiter) Peek(ctx context.Context, b Bucket) (bool, time.Duration, error) {
	return l.run(ctx, modePeek, []Bucket{b})
}

// PeekAll answers what TakeAll would, in one script, and spends nothing
func (l *Limiter) PeekAll(ctx context.Context, bs []Bucket) (bool, time.Duration, error) {
	if len(bs) == 0 {
		return true, 0, nil
	}
	return l.run(ctx, modePeek, bs)
}

var errBadBucket = errors.New("ratelimit: a bucket needs a key, a positive finite rate and a burst")

// run sends the buckets to the script, refusing a bad one before it reaches Valkey
func (l *Limiter) run(ctx context.Context, mode string, bs []Bucket) (bool, time.Duration, error) {
	keys := make([]string, 0, len(bs))
	args := make([]string, 0, 1+2*len(bs))
	args = append(args, mode)
	for _, b := range bs {
		if b.Key == "" || !(b.PerSecond > 0) || math.IsInf(b.PerSecond, 0) || b.Burst < 1 {
			return false, 0, errBadBucket
		}
		keys = append(keys, l.prefix+b.Key)
		args = append(args, strconv.FormatFloat(b.PerSecond, 'g', -1, 64),
			strconv.Itoa(b.Burst))
	}
	res, err := takeScript.Exec(ctx, l.c, keys, args).AsIntSlice()
	if err == nil && len(res) != 2 {
		err = errors.New("the script gave no answer and wait")
	}
	if err != nil {
		return false, 0, fmt.Errorf("ratelimit: %w", err)
	}
	return res[0] == 1, time.Duration(res[1]) * time.Millisecond, nil
}
