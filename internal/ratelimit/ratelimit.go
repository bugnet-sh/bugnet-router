// Package ratelimit bounds each sandbox's operations with a token bucket every replica shares
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/valkey-io/valkey-go"
)

// Defaults of the bucket, its refill a second and its capacity
const (
	DefaultPerSecond = 10
	DefaultBurst     = 20
)

// The bucket holds micro-tokens timed in the server's microseconds, so its sums stay whole numbers
var takeScript = valkey.NewLuaScript(`
local rate, cap = tonumber(ARGV[1]), tonumber(ARGV[2]) * 1000000
local now = redis.call('TIME')
now = tonumber(now[1]) * 1000000 + tonumber(now[2])
local held = redis.call('HMGET', KEYS[1], 'tokens', 'at')
local tokens, at = tonumber(held[1]) or cap, tonumber(held[2]) or now
-- A clock behind the bucket's, as after a failover, refills from now and not from the old clock
if now < at then
	at = now
	redis.call('HSET', KEYS[1], 'at', at)
	redis.call('PEXPIRE', KEYS[1], math.ceil((cap - tokens) / rate / 1000))
end
tokens = math.min(cap, tokens + math.max(0, now - at) * rate)
if tokens < 1000000 then
	return {0, math.ceil((1000000 - tokens) / rate / 1000)}
end
tokens = tokens - 1000000
redis.call('HSET', KEYS[1], 'tokens', tokens, 'at', now)
redis.call('PEXPIRE', KEYS[1], math.ceil((cap - tokens) / rate / 1000))
return {1, 0}
`)

// Limiter gives out one token per operation from a bucket per placement in Valkey
type Limiter struct {
	c      valkey.Client
	prefix string
	args   []string
}

// New returns a Limiter refilling perSecond tokens a second up to burst, the defaults below 1
func New(c valkey.Client, prefix string, perSecond, burst int) *Limiter {
	if perSecond < 1 {
		perSecond = DefaultPerSecond
	}
	if burst < 1 {
		burst = DefaultBurst
	}
	return &Limiter{
		c: c, prefix: prefix, args: []string{strconv.Itoa(perSecond), strconv.Itoa(burst)},
	}
}

// Take spends a token of the placement's bucket, or says how long until one is there
func (l *Limiter) Take(ctx context.Context, placement uuid.UUID) (bool, time.Duration, error) {
	key := l.prefix + "ops:" + placement.String()
	res, err := takeScript.Exec(ctx, l.c, []string{key}, l.args).AsIntSlice()
	if err == nil && len(res) != 2 {
		err = errors.New("the script gave no answer and wait")
	}
	if err != nil {
		return false, 0, fmt.Errorf("take an operation of placement %s: %w", placement, err)
	}
	return res[0] == 1, time.Duration(res[1]) * time.Millisecond, nil
}
