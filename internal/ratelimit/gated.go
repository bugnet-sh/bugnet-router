package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/valkey-io/valkey-go"
)

// modeGate only reads the blockers and takes no token
const modeGate = "gate"

// gatedScript refuses at once while any blocker key exists, else answers as takeScript does for
// the first key's bucket, or answers ok without a token in gate mode
var gatedScript = valkey.NewLuaScript(`
for i = 2, #KEYS do
	if redis.call('EXISTS', KEYS[i]) == 1 then
		return {2, i - 1}
	end
end
if ARGV[1] == 'gate' then
	return {1, 0}
end
local BUCKETS = {KEYS[1]}
` + takeBody)

// Gated is what a gated take found
type Gated struct {
	// Blocked is the blocker key, without the prefix, that refused the take, empty for none
	Blocked string
	// Taken says the token was spent, always false in a gate-only read
	Taken bool
	// Wait is how long until the bucket holds a token, when it was empty
	Wait time.Duration
}

// TakeUnless spends one token of b unless a blocker key exists, in one script
//
// With take false it only reads the blockers, so a call that must be refused while a blocker is
// set but never costs a token can share the call. A blocker names a key under the prefix.
func (l *Limiter) TakeUnless(
	ctx context.Context, b Bucket, take bool, blockers ...string,
) (Gated, error) {
	if b.Key == "" || !(b.PerSecond > 0) || math.IsInf(b.PerSecond, 0) || b.Burst < 1 {
		return Gated{}, errBadBucket
	}
	keys := []string{l.prefix + b.Key}
	for _, k := range blockers {
		keys = append(keys, l.prefix+k)
	}
	mode := modeGate
	if take {
		mode = modeTake
	}
	args := []string{mode, strconv.FormatFloat(b.PerSecond, 'g', -1, 64), strconv.Itoa(b.Burst)}
	res, err := gatedScript.Exec(ctx, l.c, keys, args).AsIntSlice()
	if err == nil && len(res) != 2 {
		err = errors.New("the script gave no answer and wait")
	}
	if err != nil {
		return Gated{}, fmt.Errorf("ratelimit: %w", err)
	}
	switch res[0] {
	case 2:
		return Gated{Blocked: blockers[res[1]-1]}, nil
	case 1:
		return Gated{Taken: take}, nil
	}
	return Gated{Wait: time.Duration(res[1]) * time.Millisecond}, nil
}
