// Package transfer resolves, stages and records the file transfers the edges serve.
//
// A transfer is a Valkey hash, xfer:<sha256 hex of its bnx_ token>, with the fields DP 8.3.1
// names, that lives for the start window plus the longest a transfer may run, and at least until
// its last lease ends, so a resume claimed late keeps its record. The window bounds when a
// transfer may start, and xfer:open:<placement> holds each unexpired token by its window's end,
// so a placement holds at most a fixed number. Claim turns an issued record active under a fresh
// lease in one script, which also counts the placement's active transfers in
// xfer:active:<placement>, the claims of one URL and the bytes it moved. A lease lapses unless
// renewed, and a lapsed record is issued again. Release hands a failed attempt back, and Done
// makes the URL used for good.
//
// Resolve turns the path a caller gave into the sandbox path a transfer uses, and says whether
// it must be staged through the exec user's home because the provider's file API, which runs as
// that user, cannot reach it. The staging helpers run bugnet-run's stat, stage-out and place
// under sudo for those paths.
package transfer

import (
	"cmp"
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"time"

	"github.com/google/uuid"
	routerv1 "github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/valkey-io/valkey-go"
)

// Errors a store answers with
var (
	ErrUnknown     = errors.New("transfer: unknown token")
	ErrUsed        = errors.New("transfer: the URL was used")
	ErrExpired     = errors.New("transfer: the URL's start window passed")
	ErrInUse       = errors.New("transfer: another request holds the URL")
	ErrClaimsSpent = errors.New("transfer: the URL used up its claims or bytes")
	ErrTooMany     = errors.New("transfer: the placement holds too many transfers")
)

// State is where a record stands
type State string

// The states a record moves through
const (
	Issued State = "issued"
	Active State = "active"
	Done   State = "done"
)

const (
	// LeaseLength is how long a claim holds a record without a renewal
	LeaseLength = 30 * time.Second
	// ByteSlack is what one URL may move beyond twice its size
	ByteSlack  = 64 << 20
	leaseBytes = 16
)

// Record is one transfer, as CreateTransfer wrote it and its claims changed it
type Record struct {
	// ID is the hex SHA-256 of the token
	ID         string
	Placement  uuid.UUID
	Direction  routerv1.TransferDirection
	Path       string
	Size       int64
	Mode       fs.FileMode
	SHA256     []byte
	ETag       string
	ModifiedAt time.Time
	// ExpiresAt ends the window in which the transfer must start
	ExpiresAt time.Time
	State     State
	Claims    int
	BytesSent int64
	// StagedPath is where the exec user reads or writes the file, empty when not staged
	StagedPath string
}

// Limits bound what one placement and one URL may hold
type Limits struct {
	MaxTokens, MaxActive, MaxClaims int
	// TTL is how long a record lives, the start window plus the longest transfer
	TTL time.Duration
	// Lease is how long a claim holds a record without a renewal, LeaseLength when zero
	Lease time.Duration
}

// Store keeps transfer records in Valkey under one key prefix
type Store struct {
	c      valkey.Client
	prefix string
	l      Limits
}

// NewStore returns a store holding placements and URLs to l
func NewStore(c valkey.Client, prefix string, l Limits) *Store {
	l.Lease = cmp.Or(l.Lease, LeaseLength)
	return &Store{c: c, prefix: prefix, l: l}
}

// ID is the record id of a token, the hex of its SHA-256
func ID(tokenSHA []byte) string { return hex.EncodeToString(tokenSHA) }

// Short is the first 8 hex characters of an id, the only form logs carry
func Short(id string) string { return id[:min(len(id), 8)] }

func (s *Store) recordKey(id string) string { return s.prefix + "xfer:" + id }

func (s *Store) setKey(kind string, p uuid.UUID) string {
	return s.prefix + "xfer:" + kind + ":" + p.String()
}

//go:embed claim.lua
var claimLua string

var claimScript = valkey.NewLuaScript(claimLua)

// putScript writes a record unless it exists, refusing past the placement's open tokens
var putScript = valkey.NewLuaScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then
	return 0
end
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', ARGV[1])
if redis.call('ZCARD', KEYS[2]) >= tonumber(ARGV[4]) then
	return -1
end
redis.call('HSET', KEYS[1], unpack(ARGV, 6))
redis.call('PEXPIRE', KEYS[1], ARGV[3])
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[5])
if redis.call('PEXPIRETIME', KEYS[2]) < tonumber(ARGV[2]) then
	redis.call('PEXPIREAT', KEYS[2], ARGV[2])
end
return 1
`)

// renewScript extends the lease ARGV[1] holds and counts its bytes, answering the record
var renewScript = valkey.NewLuaScript(`
local st = redis.call('HMGET', KEYS[1], 'state', 'lease')
if st[1] ~= 'active' or st[2] ~= ARGV[1] then
	return {}
end
local leaseUntil = tonumber(ARGV[2]) + tonumber(ARGV[3])
redis.call('HSET', KEYS[1], 'lease_until', leaseUntil)
redis.call('HINCRBY', KEYS[1], 'bytes_sent', ARGV[4])
if redis.call('PEXPIRETIME', KEYS[1]) < leaseUntil then
	redis.call('PEXPIREAT', KEYS[1], leaseUntil)
end
redis.call('ZADD', KEYS[2], leaseUntil, ARGV[5])
if redis.call('PEXPIRETIME', KEYS[2]) < leaseUntil then
	redis.call('PEXPIREAT', KEYS[2], leaseUntil)
end
return redis.call('HGETALL', KEYS[1])
`)

// endScript moves the record ARGV[1] holds to state ARGV[2] and counts its last bytes
var endScript = valkey.NewLuaScript(`
local st = redis.call('HMGET', KEYS[1], 'state', 'lease')
if st[1] ~= 'active' or st[2] ~= ARGV[1] then
	return 0
end
redis.call('HSET', KEYS[1], 'state', ARGV[2], 'lease', '', 'lease_until', 0)
redis.call('HINCRBY', KEYS[1], 'bytes_sent', ARGV[3])
redis.call('ZREM', KEYS[2], ARGV[4])
return 1
`)

// Put writes r as issued, answering the record already there and false when the id exists
func (s *Store) Put(ctx context.Context, r Record, now time.Time) (Record, bool, error) {
	r.State, r.Claims, r.BytesSent = Issued, 0, 0
	args := []string{
		ms(now), ms(r.ExpiresAt), strconv.FormatInt(s.l.TTL.Milliseconds(), 10),
		strconv.Itoa(s.l.MaxTokens), r.ID,
	}
	args = append(args, fields(r)...)
	keys := []string{s.recordKey(r.ID), s.setKey("open", r.Placement)}
	n, err := putScript.Exec(ctx, s.c, keys, args).AsInt64()
	switch {
	case err != nil:
		return Record{}, false, fmt.Errorf("transfer: put %s: %w", Short(r.ID), err)
	case n == -1:
		return Record{}, false, ErrTooMany
	case n == 0:
		have, err := s.Get(ctx, r.ID)
		return have, false, err
	}
	return r, true, nil
}

// Get reads the record of id, ErrUnknown when there is none
func (s *Store) Get(ctx context.Context, id string) (Record, error) {
	m, err := s.c.Do(ctx, s.c.B().Hgetall().Key(s.recordKey(id)).Build()).AsStrMap()
	if err != nil {
		return Record{}, fmt.Errorf("transfer: read %s: %w", Short(id), err)
	}
	if len(m) == 0 {
		return Record{}, ErrUnknown
	}
	return parse(id, m)
}

// claimErrors maps the claim script's refusals to errors
var claimErrors = map[string]error{
	"unknown": ErrUnknown, "used": ErrUsed, "in_use": ErrInUse, "expired": ErrExpired,
	"spent": ErrClaimsSpent, "too_many": ErrTooMany,
}

// Claim makes the record of tokenSHA active for edgeID and answers it with its new lease
func (s *Store) Claim(
	ctx context.Context, tokenSHA []byte, edgeID string, now time.Time,
) (Record, string, error) {
	id := ID(tokenSHA)
	placement, err := s.c.Do(ctx, s.c.B().Hget().Key(s.recordKey(id)).Field("placement_id").
		Build()).ToString()
	if valkey.IsValkeyNil(err) {
		return Record{}, "", ErrUnknown
	}
	if err != nil {
		return Record{}, "", fmt.Errorf("transfer: claim %s: %w", Short(id), err)
	}
	p, err := uuid.Parse(placement)
	if err != nil {
		return Record{}, "", fmt.Errorf("transfer: claim %s: %w", Short(id), err)
	}
	lease := newLease()
	keys := []string{s.recordKey(id), s.setKey("active", p)}
	args := []string{
		ms(now), lease, strconv.FormatInt(s.l.Lease.Milliseconds(), 10), edgeID,
		strconv.Itoa(s.l.MaxClaims), strconv.Itoa(s.l.MaxActive), strconv.Itoa(ByteSlack), id,
	}
	out, err := claimScript.Exec(ctx, s.c, keys, args).AsStrSlice()
	if err == nil && len(out) == 0 {
		err = errors.New("the claim answered nothing")
	}
	if err != nil {
		return Record{}, "", fmt.Errorf("transfer: claim %s: %w", Short(id), err)
	}
	if refusal, ok := claimErrors[out[0]]; ok {
		return Record{}, "", refusal
	}
	r, err := parse(id, pairs(out[1:]))
	return r, lease, err
}

// Renew extends the lease and counts sent more bytes, ErrInUse once the lease is lost
func (s *Store) Renew(
	ctx context.Context, r Record, lease string, sent int64, now time.Time,
) (Record, error) {
	keys := []string{s.recordKey(r.ID), s.setKey("active", r.Placement)}
	args := []string{
		lease, ms(now), strconv.FormatInt(s.l.Lease.Milliseconds(), 10),
		strconv.FormatInt(sent, 10), r.ID,
	}
	out, err := renewScript.Exec(ctx, s.c, keys, args).AsStrSlice()
	if err != nil {
		return Record{}, fmt.Errorf("transfer: renew %s: %w", Short(r.ID), err)
	}
	if len(out) == 0 {
		return Record{}, ErrInUse
	}
	return parse(r.ID, pairs(out))
}

// Release hands a failed attempt back as issued, counting its last bytes
func (s *Store) Release(ctx context.Context, r Record, lease string, sent int64) error {
	return s.end(ctx, r, lease, Issued, sent)
}

// Done marks the URL used for good, counting its last bytes
func (s *Store) Done(ctx context.Context, r Record, lease string, sent int64) error {
	return s.end(ctx, r, lease, Done, sent)
}

func (s *Store) end(ctx context.Context, r Record, lease string, to State, sent int64) error {
	keys := []string{s.recordKey(r.ID), s.setKey("active", r.Placement)}
	args := []string{lease, string(to), strconv.FormatInt(sent, 10), r.ID}
	n, err := endScript.Exec(ctx, s.c, keys, args).AsInt64()
	switch {
	case err != nil:
		return fmt.Errorf("transfer: end %s as %s: %w", Short(r.ID), to, err)
	case n == 0:
		return ErrInUse
	}
	return nil
}

/* Encoding */

var directions = map[routerv1.TransferDirection]string{
	routerv1.TransferDirection_TRANSFER_DIRECTION_UPLOAD:   "upload",
	routerv1.TransferDirection_TRANSFER_DIRECTION_DOWNLOAD: "download",
}

func fields(r Record) []string {
	return []string{
		"placement_id", r.Placement.String(), "direction", directions[r.Direction],
		"path", r.Path, "size", strconv.FormatInt(r.Size, 10),
		"mode", strconv.FormatUint(uint64(r.Mode), 8), "sha256", hex.EncodeToString(r.SHA256),
		"etag", r.ETag, "modified_at", strconv.FormatInt(nanos(r.ModifiedAt), 10),
		"expires_at", ms(r.ExpiresAt), "state", string(r.State), "lease", "", "lease_until", "0",
		"claims", strconv.Itoa(r.Claims), "bytes_sent", strconv.FormatInt(r.BytesSent, 10),
		"staged_path", r.StagedPath, "edge", "",
	}
}

func parse(id string, m map[string]string) (Record, error) {
	r := Record{ID: id, Path: m["path"], ETag: m["etag"], State: State(m["state"]),
		StagedPath: m["staged_path"]}
	var errs []error
	num := func(key string) int64 {
		n, err := strconv.ParseInt(m[key], 10, 64)
		errs = append(errs, err)
		return n
	}
	var err error
	r.Placement, err = uuid.Parse(m["placement_id"])
	errs = append(errs, err)
	for d, name := range directions {
		if name == m["direction"] {
			r.Direction = d
		}
	}
	r.Size = num("size")
	mode, err := strconv.ParseUint(m["mode"], 8, 32)
	errs = append(errs, err)
	r.Mode = fs.FileMode(mode)
	r.SHA256, err = hex.DecodeString(m["sha256"])
	errs = append(errs, err)
	if len(r.SHA256) == 0 {
		r.SHA256 = nil
	}
	if n := num("modified_at"); n != 0 {
		r.ModifiedAt = time.Unix(0, n).UTC()
	}
	r.ExpiresAt = time.UnixMilli(num("expires_at")).UTC()
	r.Claims = int(num("claims"))
	r.BytesSent = num("bytes_sent")
	if err := errors.Join(errs...); err != nil || r.Direction == 0 {
		return Record{}, fmt.Errorf("transfer: record %s does not parse: %w", Short(id), err)
	}
	return r, nil
}

func pairs(flat []string) map[string]string {
	m := make(map[string]string, len(flat)/2)
	for i := 0; i+1 < len(flat); i += 2 {
		m[flat[i]] = flat[i+1]
	}
	return m
}

func ms(t time.Time) string { return strconv.FormatInt(t.UnixMilli(), 10) }

// nanos is t in Unix nanoseconds, 0 for the zero time
func nanos(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func newLease() string {
	b := make([]byte, leaseBytes)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
