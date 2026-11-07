// Package budget meters a placement's edge and egress bytes against its budgets in Valkey.
//
// The edges report deltas every few seconds. Report adds them to edge:used:<placement> or
// egress:used:<placement> and answers what is left of each placement's budget, and for the edge
// kind the pace each way. The first report that reaches a budget wins a SET NX on the
// placement's spent key, so exactly one router replica writes BudgetSpent. An extension grows
// the budget and clears the spent keys, so a later crossing writes it again.
//
// The pace halves while both carriers carried the placement's bytes in the last 10 s, which
// edge:seen:<placement> records by Valkey's clock and keeps for 30 s after the last report that
// carried bytes. The used counters and spent keys expire an hour after the placement's deadline.
// MoveDeadline carries the used counters to a new deadline, and an expiry only ever moves later,
// so a report still holding the old deadline cannot shorten it. A report that crossed a budget an
// extension had already grown takes its spent key back through Uncross.
package budget

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/valkey-io/valkey-go"

	"github.com/oynozan/bugnet-router/internal/catalog"
	"github.com/oynozan/bugnet-router/internal/config"
)

// Kind names one of a placement's two budgets
type Kind string

// The budgets, the edge one shared by previews, transfers and SSH
const (
	Edge   Kind = "edge"
	Egress Kind = "egress"
)

// Carrier names the process that carried the bytes, for the pace split
type Carrier string

// The processes that carry edge bytes
const (
	ByEdge Carrier = "edge"
	BySSH  Carrier = "ssh"
)

// Usage is what one process carried for a placement since its last report
type Usage struct {
	Placement uuid.UUID
	In, Out   uint64
}

// Limit is what a placement may use and until when its keys must live
type Limit struct {
	Placement uuid.UUID
	Budget    int64
	Deadline  time.Time
	Payer     string // empty until Task 29 fills it
	// BytesPerSecond is the placement's pace each way, the setting when zero
	BytesPerSecond int64
}

// Answer is what is left of a placement's budget and the pace each way to keep
type Answer struct{ Remaining, Pace uint64 }

const (
	// keyLife is how long a placement's keys outlive its deadline
	keyLife = time.Hour
	// maxDelta caps one report of one placement, so a bad report cannot overflow a counter
	maxDelta = 1 << 40
)

// reportScript adds one placement's delta and answers its sum, its spent claim and both carriers
var reportScript = valkey.NewLuaScript(`
local used = redis.call('INCRBY', KEYS[1], ARGV[1])
if redis.call('PEXPIRETIME', KEYS[1]) < tonumber(ARGV[3]) then
	redis.call('PEXPIREAT', KEYS[1], ARGV[3])
end
local crossed = 0
if used >= tonumber(ARGV[2]) and redis.call('SET', KEYS[2], '1', 'NX', 'PXAT', ARGV[3]) then
	crossed = 1
end
local both = 0
if KEYS[3] then
	local t = redis.call('TIME')
	local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
	if tonumber(ARGV[1]) > 0 then
		redis.call('HSET', KEYS[3], ARGV[4], now)
		redis.call('PEXPIRE', KEYS[3], 30000)
	end
	local seen = redis.call('HMGET', KEYS[3], ARGV[4], ARGV[5])
	local mine, other = tonumber(seen[1]), tonumber(seen[2])
	if mine and other and now - mine < 10000 and now - other < 10000 then
		both = 1
	end
end
return {used, crossed, both}
`)

// keepScript moves the expiry of each key in KEYS that exists to ARGV[1] unless it lies later
var keepScript = valkey.NewLuaScript(`
for _, key in ipairs(KEYS) do
	if redis.call('PEXPIRETIME', key) < tonumber(ARGV[1]) then
		redis.call('PEXPIREAT', key, ARGV[1])
	end
end
return 0
`)

// uncrossScript deletes the spent key KEYS[2] while the sum KEYS[1] is under the budget ARGV[1]
var uncrossScript = valkey.NewLuaScript(`
local used = tonumber(redis.call('GET', KEYS[1]) or '0')
if used < tonumber(ARGV[1]) then
	return redis.call('DEL', KEYS[2])
end
return 0
`)

// Meter counts placements' bytes in Valkey under one key prefix
type Meter struct {
	c      valkey.Client
	prefix string
	s      config.EdgeBudget
}

// New returns a Meter whose paces default to s
func New(c valkey.Client, prefix string, s config.EdgeBudget) *Meter {
	return &Meter{c: c, prefix: prefix, s: s}
}

func (m *Meter) key(kind Kind, what string, p uuid.UUID) string {
	return m.prefix + string(kind) + ":" + what + ":" + p.String()
}

// Report adds usage and answers what is left, the second result names placements just spent
func (m *Meter) Report(
	ctx context.Context, kind Kind, by Carrier, usage []Usage, limits map[uuid.UUID]Limit,
) (map[uuid.UUID]Answer, []uuid.UUID, error) {
	var order []uuid.UUID
	deltas := map[uuid.UUID]uint64{}
	for _, u := range usage {
		if _, ok := limits[u.Placement]; !ok {
			continue
		}
		if _, ok := deltas[u.Placement]; !ok {
			order = append(order, u.Placement)
		}
		deltas[u.Placement] = min(add(add(deltas[u.Placement], u.In), u.Out), maxDelta)
	}
	if len(order) == 0 {
		return map[uuid.UUID]Answer{}, nil, nil
	}
	other := BySSH
	if by == BySSH {
		other = ByEdge
	}
	execs := make([]valkey.LuaExec, len(order))
	for i, p := range order {
		l := limits[p]
		keys := []string{m.key(kind, "used", p), m.key(kind, "spent", p)}
		if kind == Edge {
			keys = append(keys, m.key(kind, "seen", p))
		}
		execs[i] = valkey.LuaExec{Keys: keys, Args: []string{
			strconv.FormatUint(deltas[p], 10), strconv.FormatInt(l.Budget, 10),
			strconv.FormatInt(l.Deadline.Add(keyLife).UnixMilli(), 10), string(by), string(other),
		}}
	}
	answers := make(map[uuid.UUID]Answer, len(order))
	var spent []uuid.UUID
	for i, res := range reportScript.ExecMulti(ctx, m.c, execs...) {
		p := order[i]
		out, err := res.AsIntSlice()
		if err == nil && len(out) != 3 {
			err = fmt.Errorf("the script answered %d values", len(out))
		}
		if err != nil {
			return nil, nil, fmt.Errorf("budget: report %s usage of %s: %w", kind, p, err)
		}
		pace := uint64(m.pace(kind, limits[p]))
		if out[2] == 1 {
			pace /= 2
		}
		answers[p] = Answer{Remaining: left(limits[p].Budget, out[0]), Pace: pace}
		if out[1] == 1 {
			spent = append(spent, p)
		}
	}
	return answers, spent, nil
}

// pace is the placement's own pace each way, or the setting for its kind
func (m *Meter) pace(kind Kind, l Limit) int64 {
	if l.BytesPerSecond > 0 {
		return l.BytesPerSecond
	}
	if kind == Egress {
		return m.s.EgressBytesPerSecond
	}
	return m.s.BytesPerSecond
}

// Remaining answers what is left of the placement's budget of kind
func (m *Meter) Remaining(ctx context.Context, kind Kind, l Limit) (uint64, error) {
	left, err := m.RemainingAll(ctx, kind, []Limit{l})
	if err != nil {
		return 0, err
	}
	return left[l.Placement], nil
}

// RemainingAll answers what is left of each placement's budget of kind, in one read
func (m *Meter) RemainingAll(
	ctx context.Context, kind Kind, ls []Limit,
) (map[uuid.UUID]uint64, error) {
	out := make(map[uuid.UUID]uint64, len(ls))
	if len(ls) == 0 {
		return out, nil
	}
	keys := make([]string, len(ls))
	for i, l := range ls {
		keys[i] = m.key(kind, "used", l.Placement)
	}
	vals, err := m.c.Do(ctx, m.c.B().Mget().Key(keys...).Build()).ToArray()
	if err == nil && len(vals) != len(ls) {
		err = fmt.Errorf("read %d counters, got %d", len(ls), len(vals))
	}
	if err != nil {
		return nil, fmt.Errorf("budget: read %s usage: %w", kind, err)
	}
	for i, l := range ls {
		used, err := vals[i].AsInt64()
		switch {
		case valkey.IsValkeyNil(err):
			used = 0
		case err != nil:
			return nil, fmt.Errorf("budget: read %s usage of %s: %w", kind, l.Placement, err)
		}
		out[l.Placement] = left(l.Budget, used)
	}
	return out, nil
}

// Spent reports whether the placement's budget of kind was crossed and not grown since
func (m *Meter) Spent(ctx context.Context, kind Kind, placement uuid.UUID) (bool, error) {
	n, err := m.c.Do(ctx, m.c.B().Exists().Key(m.key(kind, "spent", placement)).Build()).AsInt64()
	if err != nil {
		return false, fmt.Errorf("budget: read the %s spent key of %s: %w", kind, placement, err)
	}
	return n == 1, nil
}

// Uncross deletes the spent key of kind while the sum is under budget, reporting whether it did
func (m *Meter) Uncross(
	ctx context.Context, kind Kind, placement uuid.UUID, budget int64,
) (bool, error) {
	keys := []string{m.key(kind, "used", placement), m.key(kind, "spent", placement)}
	n, err := uncrossScript.Exec(ctx, m.c, keys, []string{strconv.FormatInt(budget, 10)}).
		AsInt64()
	if err != nil {
		return false, fmt.Errorf("budget: take back the %s spent key of %s: %w", kind,
			placement, err)
	}
	return n == 1, nil
}

// ClearSpent deletes both spent keys, so a later crossing writes BudgetSpent again
func (m *Meter) ClearSpent(ctx context.Context, placement uuid.UUID) error {
	cmd := m.c.B().Del().Key(m.key(Edge, "spent", placement), m.key(Egress, "spent", placement))
	if err := m.c.Do(ctx, cmd.Build()).Error(); err != nil {
		return fmt.Errorf("budget: clear spent of %s: %w", placement, err)
	}
	return nil
}

// MoveDeadline keeps both of the placement's used counters until an hour after deadline
func (m *Meter) MoveDeadline(ctx context.Context, placement uuid.UUID, deadline time.Time) error {
	keys := []string{m.key(Edge, "used", placement), m.key(Egress, "used", placement)}
	at := strconv.FormatInt(deadline.Add(keyLife).UnixMilli(), 10)
	if err := keepScript.Exec(ctx, m.c, keys, []string{at}).Error(); err != nil {
		return fmt.Errorf("budget: move the counters of %s: %w", placement, err)
	}
	return nil
}

// BytesFor is a package's budget of kind, its own figure or the setting scaled by its minutes
func BytesFor(pkg catalog.Package, s config.EdgeBudget, kind Kind) int64 {
	own, per10 := pkg.Edge.Bytes, s.BytesPer10Minutes
	if kind == Egress {
		own, per10 = pkg.Edge.EgressBytes, s.EgressBytesPer10Minutes
	}
	if own > 0 {
		return own
	}
	return per10 * int64(pkg.Minutes) / 10
}

// PaceFor is a package's pace of kind each way, its own figure or the setting
func PaceFor(pkg catalog.Package, s config.EdgeBudget, kind Kind) int64 {
	if kind == Egress {
		return cmp.Or(pkg.Edge.EgressBytesPerSecond, s.EgressBytesPerSecond)
	}
	return cmp.Or(pkg.Edge.BytesPerSecond, s.BytesPerSecond)
}

// add sums without wrapping past the largest uint64
func add(a, b uint64) uint64 {
	if a > math.MaxUint64-b {
		return math.MaxUint64
	}
	return a + b
}

// left is what remains of budget once used is spent, never below zero
func left(budget, used int64) uint64 {
	return uint64(max(budget-max(used, 0), 0))
}
