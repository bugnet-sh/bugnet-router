// Package streams keeps each job's frames in a Valkey stream that only the job's lease owner
// may append to.
//
// Keys, all under the prefix, are job:<id>:s (the stream, one JSON frame per entry in field f),
// job:<id>:owner (<replica>:<epoch>, expiring with the lease), job:<id>:epoch (a counter that
// gives every new owner a fresh value), job:<id>:cancel (set when a cancel was asked for) and
// jobs:leases (a sorted set of job ids scored by lease expiry in Unix ms). Every write that
// needs the lease runs as a Lua script that first checks the owner value, so an owner whose
// lease expired can never append after a peer took over.
package streams

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/oynozan/bugnet-run/frame"
	"github.com/valkey-io/valkey-go"
)

const (
	leaseTTL = 5 * time.Second
	// epochTTL bounds an epoch key no append ever gave the stream's expiry
	epochTTL  = 24 * time.Hour
	readCount = 256
	scanPage  = 64
)

var (
	// ErrLeaseLost means another owner holds the job's lease or it expired
	ErrLeaseLost = errors.New("streams: lease lost")
	// ErrBadCursor means a cursor is not a stream entry ID
	ErrBadCursor = errors.New("streams: malformed cursor")
)

var cursorPattern = regexp.MustCompile(`^[0-9]+(-[0-9]+)?$`)

// Entry is one frame of a job's stream
type Entry struct {
	ID    string // Valkey stream entry ID, used as the cursor
	Frame frame.Frame
}

// Terminal reports whether f ends a job's stream
func Terminal(f frame.Frame) bool {
	return f.Kind == frame.KindExit ||
		(f.Kind == frame.KindError && f.Reason != frame.ReasonJobExists)
}

// Bus reads and writes job streams and their leases
type Bus struct {
	c      valkey.Client
	prefix string
}

// NewBus returns a Bus whose keys all start with prefix
func NewBus(c valkey.Client, prefix string) *Bus {
	return &Bus{c: c, prefix: prefix}
}

func (b *Bus) key(jobID, kind string) string { return b.prefix + "job:" + jobID + ":" + kind }

func (b *Bus) leasesKey() string { return b.prefix + "jobs:leases" }

func unixMs(t time.Time) string { return strconv.FormatInt(t.UnixMilli(), 10) }

/* Leases */

var acquireScript = valkey.NewLuaScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then return false end
local epoch = redis.call('INCR', KEYS[2])
if redis.call('PTTL', KEYS[2]) == -1 then redis.call('PEXPIRE', KEYS[2], ARGV[5]) end
local owner = ARGV[1] .. ':' .. epoch
redis.call('SET', KEYS[1], owner, 'PX', ARGV[2])
redis.call('ZADD', KEYS[3], ARGV[3], ARGV[4])
return owner
`)

var renewScript = valkey.NewLuaScript(`
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return false end
redis.call('PEXPIRE', KEYS[1], ARGV[2])
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4])
return 1
`)

var releaseScript = valkey.NewLuaScript(`
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return false end
redis.call('DEL', KEYS[1])
if ARGV[3] == '1' then
	redis.call('ZREM', KEYS[2], ARGV[2])
else
	redis.call('ZADD', KEYS[2], 0, ARGV[2])
end
return 1
`)

var appendScript = valkey.NewLuaScript(`
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return false end
local id = redis.call('XADD', KEYS[2], '*', 'f', ARGV[2])
if ARGV[3] ~= '0' then
	redis.call('PEXPIREAT', KEYS[2], ARGV[3])
	redis.call('PEXPIREAT', KEYS[3], ARGV[3])
end
return id
`)

// Lease is one owner's claim on a job's stream, valid while its owner value is the stored one
type Lease struct {
	bus   *Bus
	job   string
	owner string
	// StreamExpiry is when Append makes the stream and epoch expire, never when zero
	StreamExpiry time.Time
}

// AcquireLease claims the job for replica unless someone holds it, reporting false when held
func (b *Bus) AcquireLease(ctx context.Context, jobID, replica string) (Lease, bool, error) {
	keys := []string{b.key(jobID, "owner"), b.key(jobID, "epoch"), b.leasesKey()}
	args := []string{
		replica, strconv.FormatInt(leaseTTL.Milliseconds(), 10),
		unixMs(time.Now().Add(leaseTTL)), jobID, strconv.FormatInt(epochTTL.Milliseconds(), 10),
	}
	owner, err := acquireScript.Exec(ctx, b.c, keys, args).ToString()
	if valkey.IsValkeyNil(err) {
		return Lease{}, false, nil
	}
	if err != nil {
		return Lease{}, false, fmt.Errorf("acquire lease of job %s: %w", jobID, err)
	}
	return Lease{bus: b, job: jobID, owner: owner}, true, nil
}

// JobID returns the job the lease is for
func (l Lease) JobID() string { return l.job }

// Owner returns the owner value, the replica and epoch joined by a colon
func (l Lease) Owner() string { return l.owner }

func (l Lease) run(ctx context.Context, what string, s *valkey.Lua, keys, args []string) error {
	err := s.Exec(ctx, l.bus.c, keys, args).Error()
	if valkey.IsValkeyNil(err) {
		return ErrLeaseLost
	}
	if err != nil {
		return fmt.Errorf("%s lease of job %s: %w", what, l.job, err)
	}
	return nil
}

// Renew extends the lease by its full time to live, failing with ErrLeaseLost once it is gone
func (l Lease) Renew(ctx context.Context) error {
	b := l.bus
	keys := []string{b.key(l.job, "owner"), b.leasesKey()}
	args := []string{
		l.owner, strconv.FormatInt(leaseTTL.Milliseconds(), 10),
		unixMs(time.Now().Add(leaseTTL)), l.job,
	}
	return l.run(ctx, "renew", renewScript, keys, args)
}

// Release gives the lease up and marks it expired so a peer takes the job over at once
func (l Lease) Release(ctx context.Context) error {
	keys := []string{l.bus.key(l.job, "owner"), l.bus.leasesKey()}
	return l.run(ctx, "release", releaseScript, keys, []string{l.owner, l.job, "0"})
}

// End gives the lease of a finished job up so no peer takes the job over
func (l Lease) End(ctx context.Context) error {
	keys := []string{l.bus.key(l.job, "owner"), l.bus.leasesKey()}
	return l.run(ctx, "end", releaseScript, keys, []string{l.owner, l.job, "1"})
}

// Append adds f to the job's stream and returns its entry ID, or ErrLeaseLost from a stale owner
func (l Lease) Append(ctx context.Context, f frame.Frame) (string, error) {
	raw, err := json.Marshal(f)
	if err != nil {
		return "", fmt.Errorf("encode frame: %w", err)
	}
	expiry := "0"
	if !l.StreamExpiry.IsZero() {
		expiry = unixMs(l.StreamExpiry)
	}
	b := l.bus
	keys := []string{b.key(l.job, "owner"), b.key(l.job, "s"), b.key(l.job, "epoch")}
	id, err := appendScript.Exec(ctx, b.c, keys, []string{l.owner, string(raw), expiry}).ToString()
	if valkey.IsValkeyNil(err) {
		return "", ErrLeaseLost
	}
	if err != nil {
		return "", fmt.Errorf("append to job %s: %w", l.job, err)
	}
	return id, nil
}

// ExpiredLeases returns the jobs whose lease expired by now or was released
func (b *Bus) ExpiredLeases(ctx context.Context, now time.Time) ([]string, error) {
	cmd := b.c.B().Zrangebyscore().Key(b.leasesKey()).Min("-inf").Max(unixMs(now)).Build()
	ids, err := b.c.Do(ctx, cmd).AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("expired leases: %w", err)
	}
	return ids, nil
}

/* Reading */

func decode(raw valkey.XRangeEntry) (Entry, error) {
	var f frame.Frame
	if err := json.Unmarshal([]byte(raw.FieldValues["f"]), &f); err != nil {
		return Entry{}, fmt.Errorf("decode stream entry %s: %w", raw.ID, err)
	}
	return Entry{ID: raw.ID, Frame: f}, nil
}

// Read returns entries after cursor, from the start when it is empty, waiting up to block for one
func (b *Bus) Read(
	ctx context.Context, jobID, cursor string, block time.Duration,
) ([]Entry, error) {
	if cursor == "" {
		cursor = "0"
	}
	if !cursorPattern.MatchString(cursor) {
		return nil, fmt.Errorf("%w %q", ErrBadCursor, cursor)
	}
	key := b.key(jobID, "s")
	read := b.c.B().Xread().Count(readCount)
	var cmd valkey.Completed
	if block > 0 {
		cmd = read.Block(max(block.Milliseconds(), 1)).Streams().Key(key).Id(cursor).Build()
	} else {
		cmd = read.Streams().Key(key).Id(cursor).Build()
	}
	streams, err := b.c.Do(ctx, cmd).AsXRead()
	if valkey.IsValkeyNil(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read job %s: %w", jobID, err)
	}
	raw := streams[key]
	entries := make([]Entry, 0, len(raw))
	for _, r := range raw {
		e, err := decode(r)
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}

func (b *Bus) revRange(ctx context.Context, jobID, end string, n int64) ([]Entry, error) {
	cmd := b.c.B().Xrevrange().Key(b.key(jobID, "s")).End(end).Start("-").Count(n).Build()
	raw, err := b.c.Do(ctx, cmd).AsXRange()
	if err != nil {
		return nil, fmt.Errorf("read job %s backwards: %w", jobID, err)
	}
	entries := make([]Entry, 0, len(raw))
	for _, r := range raw {
		e, err := decode(r)
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// Last returns the job's newest entry, reporting false for an empty stream
func (b *Bus) Last(ctx context.Context, jobID string) (Entry, bool, error) {
	entries, err := b.revRange(ctx, jobID, "+", 1)
	if err != nil || len(entries) == 0 {
		return Entry{}, false, err
	}
	return entries[0], true, nil
}

// LastOffsets returns the byte offsets the stream holds output up to and whether it has ended
func (b *Bus) LastOffsets(ctx context.Context, jobID string) (int64, int64, bool, error) {
	var out, errOff int64
	haveOut, haveErr, done := false, false, false
	end := "+"
	for first := true; ; first = false {
		page, err := b.revRange(ctx, jobID, end, scanPage)
		if err != nil {
			return 0, 0, false, err
		}
		for i, e := range page {
			f := e.Frame
			if first && i == 0 {
				done = Terminal(f)
			}
			switch f.Kind {
			case frame.KindStdout:
				if !haveOut {
					out, haveOut = f.Offset+int64(len(f.Data)), true
				}
			case frame.KindStderr:
				if !haveErr {
					errOff, haveErr = f.Offset+int64(len(f.Data)), true
				}
			case frame.KindHeartbeat, frame.KindExit:
				// The runner sends these only after every byte before its offsets
				if !haveOut {
					out = f.Out
				}
				if !haveErr {
					errOff = f.Err
				}
				return out, errOff, done, nil
			}
			if haveOut && haveErr {
				return out, errOff, done, nil
			}
		}
		if len(page) < scanPage {
			return out, errOff, done, nil
		}
		end = "(" + page[len(page)-1].ID
	}
}

/* Cancel requests */

// RequestCancel records that the job should end as cancelled, forgetting it at expireAt
func (b *Bus) RequestCancel(ctx context.Context, jobID string, expireAt time.Time) error {
	cmd := b.c.B().Set().Key(b.key(jobID, "cancel")).Value("1").
		PxatMillisecondsTimestamp(expireAt.UnixMilli()).Build()
	if err := b.c.Do(ctx, cmd).Error(); err != nil {
		return fmt.Errorf("request cancel of job %s: %w", jobID, err)
	}
	return nil
}

// CancelRequested reports whether a cancel was asked for the job
func (b *Bus) CancelRequested(ctx context.Context, jobID string) (bool, error) {
	n, err := b.c.Do(ctx, b.c.B().Exists().Key(b.key(jobID, "cancel")).Build()).AsInt64()
	if err != nil {
		return false, fmt.Errorf("check cancel of job %s: %w", jobID, err)
	}
	return n == 1, nil
}
