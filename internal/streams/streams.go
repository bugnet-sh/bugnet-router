// Package streams keeps each job's frames in a Valkey stream that only the job's lease owner
// may append to.
//
// Keys, all under the prefix, are job:<id>:s (the stream, one JSON frame per entry in field f),
// job:<id>:owner (<replica>:<epoch>, expiring with the lease), job:<id>:epoch (a counter that
// gives every new owner a fresh value), job:<id>:cancel (set when a cancel was asked for),
// placement:<id>:out (what the placement's job streams cost so far), placement:<id>:tail (what
// final tails stored past that budget cost) and jobs:leases (a sorted set of job ids scored by
// lease expiry in Unix ms). Every write that needs the lease runs as a Lua script that first
// checks the owner value, so an owner whose lease expired can never append after a peer took
// over. An append refuses output and heartbeats once the placement's streams would cost more than
// its budget, except that a job's final tail may still use the placement's tail reserve, and
// trims a stream past a length the budgets keep it far below.
package streams

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/bugnet-sh/bugnet-run/frame"
	"github.com/valkey-io/valkey-go"
)

const (
	leaseTTL = 5 * time.Second
	// epochTTL bounds an epoch key no append ever gave the stream's expiry
	epochTTL  = 24 * time.Hour
	readCount = 256
	scanPage  = 256
	// entryCost is what one entry costs Valkey beside its data, keys and stream bookkeeping
	entryCost = 96
)

// maxEntries trims a stream as a backstop, the output budgets keep every stream far shorter
var maxEntries int64 = 200_000

var (
	// ErrLeaseLost means another owner holds the job's lease or it expired
	ErrLeaseLost = errors.New("streams: lease lost")
	// ErrBadCursor means a cursor is not a stream entry ID
	ErrBadCursor = errors.New("streams: malformed cursor")
	// ErrOverBudget means the frame would take the placement's streams past their budget
	ErrOverBudget = errors.New("streams: the placement's output budget is spent")
)

// Cost estimates the Valkey memory a frame's entry takes, its data stored as base64
func Cost(f frame.Frame) int64 {
	return int64(base64.StdEncoding.EncodedLen(len(f.Data))) + entryCost
}

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

// The scripts assume single-node Valkey, since a job's keys and the lease set share no hash tag

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
if ARGV[3] == '' then
	redis.call('ZREM', KEYS[2], ARGV[2])
else
	redis.call('ZADD', KEYS[2], ARGV[3], ARGV[2])
end
return 1
`)

var appendScript = valkey.NewLuaScript(`
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return false end
local cost, budget, reserve = tonumber(ARGV[4]), tonumber(ARGV[5]), tonumber(ARGV[8])
local counter = KEYS[4]
if budget > 0 and ARGV[6] == '1' and
	tonumber(redis.call('GET', KEYS[4]) or '0') + cost > budget then
	if reserve <= 0 or tonumber(redis.call('GET', KEYS[5]) or '0') + cost > reserve then
		return 'over'
	end
	counter = KEYS[5]
end
local id = redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[7], '*', 'f', ARGV[2])
if budget > 0 then redis.call('INCRBY', counter, cost) end
if ARGV[3] ~= '0' then
	redis.call('PEXPIREAT', KEYS[2], ARGV[3])
	redis.call('PEXPIREAT', KEYS[3], ARGV[3])
	if budget > 0 then
		for _, key in ipairs({KEYS[4], counter}) do
			if redis.call('PEXPIRETIME', key) < tonumber(ARGV[3]) then
				redis.call('PEXPIREAT', key, ARGV[3])
			end
		end
	end
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
	// Placement names the job's placement, whose streams share PlacementBudget unless it is zero
	Placement       string
	PlacementBudget int64
	// PlacementReserve is what final tails may cost the placement past its budget, none when zero
	PlacementReserve int64
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
	return l.giveUp(ctx, "release", "0")
}

// Postpone gives the lease up and keeps scanners from listing the job before at
func (l Lease) Postpone(ctx context.Context, at time.Time) error {
	return l.giveUp(ctx, "postpone", unixMs(at))
}

// End gives the lease of a finished job up so no peer takes the job over
func (l Lease) End(ctx context.Context) error {
	return l.giveUp(ctx, "end", "")
}

// giveUp drops the owner and scores the job at expiry in Unix ms, unlisting it for an empty one
func (l Lease) giveUp(ctx context.Context, what, expiry string) error {
	keys := []string{l.bus.key(l.job, "owner"), l.bus.leasesKey()}
	return l.run(ctx, what, releaseScript, keys, []string{l.owner, l.job, expiry})
}

// Append adds f and returns its entry ID, ErrLeaseLost from a stale owner or ErrOverBudget
func (l Lease) Append(ctx context.Context, f frame.Frame) (string, error) {
	return l.append(ctx, f, false)
}

// AppendTail adds output of a job's final tail, which may use the placement's tail reserve
func (l Lease) AppendTail(ctx context.Context, f frame.Frame) (string, error) {
	return l.append(ctx, f, true)
}

func (l Lease) append(ctx context.Context, f frame.Frame, tail bool) (string, error) {
	raw, err := json.Marshal(f)
	if err != nil {
		return "", fmt.Errorf("encode frame: %w", err)
	}
	expiry := "0"
	if !l.StreamExpiry.IsZero() {
		expiry = unixMs(l.StreamExpiry)
	}
	b := l.bus
	budgetKey, reserveKey := b.key(l.job, "s"), b.key(l.job, "s")
	budget, reserve := int64(0), int64(0)
	if l.Placement != "" && l.PlacementBudget > 0 {
		budgetKey, budget = b.prefix+"placement:"+l.Placement+":out", l.PlacementBudget
		reserveKey = b.prefix + "placement:" + l.Placement + ":tail"
	}
	output := f.Kind == frame.KindStdout || f.Kind == frame.KindStderr
	if tail && output {
		reserve = l.PlacementReserve
	}
	// Skip and final frames are always kept, since readers learn from them what output is missing
	refusable := "0"
	if output || f.Kind == frame.KindHeartbeat {
		refusable = "1"
	}
	keys := []string{
		b.key(l.job, "owner"), b.key(l.job, "s"), b.key(l.job, "epoch"), budgetKey, reserveKey,
	}
	args := []string{
		l.owner, string(raw), expiry, strconv.FormatInt(Cost(f), 10),
		strconv.FormatInt(budget, 10), refusable, strconv.FormatInt(maxEntries, 10),
		strconv.FormatInt(reserve, 10),
	}
	id, err := appendScript.Exec(ctx, b.c, keys, args).ToString()
	switch {
	case valkey.IsValkeyNil(err):
		return "", ErrLeaseLost
	case err != nil:
		return "", fmt.Errorf("append to job %s: %w", l.job, err)
	case id == "over":
		return "", ErrOverBudget
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

func decodeAll(raw []valkey.XRangeEntry) ([]Entry, error) {
	entries := make([]Entry, 0, len(raw))
	for _, r := range raw {
		var f frame.Frame
		if err := json.Unmarshal([]byte(r.FieldValues["f"]), &f); err != nil {
			return nil, fmt.Errorf("decode stream entry %s: %w", r.ID, err)
		}
		entries = append(entries, Entry{ID: r.ID, Frame: f})
	}
	return entries, nil
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
	return decodeAll(streams[key])
}

func (b *Bus) revRange(ctx context.Context, jobID, end string, n int64) ([]Entry, error) {
	cmd := b.c.B().Xrevrange().Key(b.key(jobID, "s")).End(end).Start("-").Count(n).Build()
	raw, err := b.c.Do(ctx, cmd).AsXRange()
	if err != nil {
		return nil, fmt.Errorf("read job %s backwards: %w", jobID, err)
	}
	return decodeAll(raw)
}

// Before returns at most n entries before end, newest first, end being + for the newest
func (b *Bus) Before(ctx context.Context, jobID, end string, n int64) ([]Entry, error) {
	return b.revRange(ctx, jobID, end, n)
}

// After returns at most n entries after cursor in order, from the start when it is empty
func (b *Bus) After(ctx context.Context, jobID, cursor string, n int64) ([]Entry, error) {
	start := "-"
	if cursor != "" {
		if !cursorPattern.MatchString(cursor) {
			return nil, fmt.Errorf("%w %q", ErrBadCursor, cursor)
		}
		start = "(" + cursor
	}
	cmd := b.c.B().Xrange().Key(b.key(jobID, "s")).Start(start).End("+").Count(n).Build()
	raw, err := b.c.Do(ctx, cmd).AsXRange()
	if err != nil {
		return nil, fmt.Errorf("read job %s: %w", jobID, err)
	}
	return decodeAll(raw)
}

// Exists reports whether the job's stream is still kept
func (b *Bus) Exists(ctx context.Context, jobID string) (bool, error) {
	n, err := b.c.Do(ctx, b.c.B().Exists().Key(b.key(jobID, "s")).Build()).AsInt64()
	if err != nil {
		return false, fmt.Errorf("check job %s: %w", jobID, err)
	}
	return n == 1, nil
}

// Unlink removes the jobs' streams without blocking Valkey
func (b *Bus) Unlink(ctx context.Context, jobIDs ...string) error {
	if len(jobIDs) == 0 {
		return nil
	}
	keys := make([]string, len(jobIDs))
	for i, id := range jobIDs {
		keys[i] = b.key(id, "s")
	}
	if err := b.c.Do(ctx, b.c.B().Unlink().Key(keys...).Build()).Error(); err != nil {
		return fmt.Errorf("unlink job streams: %w", err)
	}
	return nil
}

// Last returns the job's newest entry, reporting false for an empty stream
func (b *Bus) Last(ctx context.Context, jobID string) (Entry, bool, error) {
	entries, err := b.revRange(ctx, jobID, "+", 1)
	if err != nil || len(entries) == 0 {
		return Entry{}, false, err
	}
	return entries[0], true, nil
}

// Offsets is what LastOffsets found in a job's stream
type Offsets struct {
	Out, Err int64 // the furthest output offsets anywhere in the stream
	Done     bool  // the newest entry ends the stream
	Stored   int64 // what the output entries cost by Cost
	Skipped  bool  // a skip frame says the stream left output out
}

// LastOffsets reads the furthest output offsets anywhere in the stream and what its output costs
func (b *Bus) LastOffsets(ctx context.Context, jobID string) (Offsets, error) {
	var o Offsets
	end := "+"
	// The whole stream is read, so a stale entry near its end can never rewind a resume
	for first := true; ; first = false {
		page, err := b.revRange(ctx, jobID, end, scanPage)
		if err != nil {
			return Offsets{}, err
		}
		for i, e := range page {
			f := e.Frame
			if first && i == 0 {
				o.Done = Terminal(f)
			}
			switch f.Kind {
			case frame.KindStdout:
				o.Out = max(o.Out, f.Offset+int64(len(f.Data)))
				o.Stored += Cost(f)
			case frame.KindStderr:
				o.Err = max(o.Err, f.Offset+int64(len(f.Data)))
				o.Stored += Cost(f)
			case frame.KindSkip:
				o.Out, o.Err, o.Skipped = max(o.Out, f.Out), max(o.Err, f.Err), true
			case frame.KindHeartbeat, frame.KindExit:
				o.Out, o.Err = max(o.Out, f.Out), max(o.Err, f.Err)
			}
		}
		if len(page) < scanPage {
			return o, nil
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

// ClearCancel forgets a cancel request that never reached the job
func (b *Bus) ClearCancel(ctx context.Context, jobID string) error {
	if err := b.c.Do(ctx, b.c.B().Del().Key(b.key(jobID, "cancel")).Build()).Error(); err != nil {
		return fmt.Errorf("clear cancel of job %s: %w", jobID, err)
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
