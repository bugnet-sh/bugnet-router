// Package budget meters a placement's edge and egress bytes against its budgets in Valkey.
//
// The edges report deltas every few seconds. Report adds them to edge:used:<placement> or
// egress:used:<placement> and answers what is left of each placement's budget, and for the edge
// kind the pace each way. The first report that reaches a budget wins a SET NX on the
// placement's spent key, so exactly one router replica writes BudgetSpent. An extension writes
// its grown budgets to <kind>:budget:<placement> and clears the spent keys in one step, so a
// later crossing writes it again. A report compares against the larger of that figure and the
// budget it read, so a report that read the budget before an extension never spends the grown one.
//
// A placement with a payer also adds each delta to wallet:bytes:<payer>:<yyyymmdd> in the same
// script and answers the smaller of its own remainder and the wallet's. The first report that
// reaches the wallet's daily cap wins a SET NX on wallet:spent:<payer>:<yyyymmdd> and says so in
// its Answer, so one replica announces it. The wallet's keys expire 48 hours after they are made
// and a new UTC day starts empty, which restores every placement of the payer.
//
// The pace halves while both carriers carried the placement's bytes in the last 10 s, which
// edge:seen:<placement> records by Valkey's clock and keeps for 30 s after the last report that
// carried bytes. The used counters, budget figures and spent keys expire an hour after the
// placement's deadline. MoveDeadline carries the used counters to a new deadline, and an expiry
// only ever moves later, so a report still holding the old deadline cannot shorten it.
//
// Egress also reports each placement's unpaid use of its x402 hosts. ReportX402Unpaid adds those
// deltas to the hash egress:x402:<placement>, whose fields <host>:req and <host>:bytes count the
// requests Bugnet did not pay and their bytes. The hash holds at most 64 fields, two for each of
// 32 hosts, and a report drops a new field past that. It expires with the used counters and
// MoveDeadline moves it with them.
package budget

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/hostname"
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
	// Payer is the placement's payer as the router decided it, empty for none
	Payer string
	// BytesPerSecond is the placement's pace each way, the setting when zero
	BytesPerSecond int64
}

// Answer is what is left of a placement's budget and the pace each way to keep
type Answer struct {
	Remaining, Pace uint64
	// WalletCrossed is set on the one report that first reached the payer's daily cap
	WalletCrossed bool
}

// X402Unpaid is one host's unpaid use by one placement
type X402Unpaid struct{ Requests, Bytes uint64 }

const (
	// keyLife is how long a placement's keys outlive its deadline
	keyLife = time.Hour
	// maxDelta caps one report of one placement, so a bad report cannot overflow a counter
	maxDelta = 1 << 40
	// x402Fields caps the fields of one placement's unpaid hash, two for each of 32 hosts
	x402Fields = 64
	// walletLife is how long a wallet's day keys live
	walletLife = 48 * time.Hour
)

// reportScript adds one placement's delta and answers its sum, spent claim, carriers and budget
var reportScript = valkey.NewLuaScript(`
local used = redis.call('INCRBY', KEYS[1], ARGV[1])
if redis.call('PEXPIRETIME', KEYS[1]) < tonumber(ARGV[3]) then
	redis.call('PEXPIREAT', KEYS[1], ARGV[3])
end
local budget = math.max(tonumber(ARGV[2]), tonumber(redis.call('GET', KEYS[3]) or '0'))
local crossed = 0
if used >= budget and redis.call('SET', KEYS[2], '1', 'NX', 'PXAT', ARGV[3]) then
	crossed = 1
end
local wused, wcrossed = -1, 0
local w = tonumber(ARGV[6])
if w > 0 then
	wused = redis.call('INCRBY', KEYS[w], ARGV[1])
	if redis.call('PTTL', KEYS[w]) < 0 then
		redis.call('PEXPIRE', KEYS[w], ARGV[8])
	end
	if wused >= tonumber(ARGV[7]) then
		if redis.call('SET', KEYS[w + 1], '1', 'NX', 'PX', ARGV[8]) then
			wcrossed = 1
		end
	end
end
local both = 0
local seen = tonumber(ARGV[9])
if seen > 0 then
	local t = redis.call('TIME')
	local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
	if tonumber(ARGV[1]) > 0 then
		redis.call('HSET', KEYS[seen], ARGV[4], now)
		redis.call('PEXPIRE', KEYS[seen], 30000)
	end
	local got = redis.call('HMGET', KEYS[seen], ARGV[4], ARGV[5])
	local mine, other = tonumber(got[1]), tonumber(got[2])
	if mine and other and now - mine < 10000 and now - other < 10000 then
		both = 1
	end
end
return {used, crossed, both, budget, wused, wcrossed}
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

// unpaidScript adds each field and delta after ARGV[2] while KEYS[1] has room and answers the drops
var unpaidScript = valkey.NewLuaScript(`
local dropped = 0
for i = 3, #ARGV, 2 do
	if redis.call('HEXISTS', KEYS[1], ARGV[i]) == 1 or
		redis.call('HLEN', KEYS[1]) < tonumber(ARGV[2]) then
		redis.call('HINCRBY', KEYS[1], ARGV[i], ARGV[i + 1])
	else
		dropped = dropped + 1
	end
end
if redis.call('PEXPIRETIME', KEYS[1]) < tonumber(ARGV[1]) then
	redis.call('PEXPIREAT', KEYS[1], ARGV[1])
end
return dropped
`)

// growScript raises the budget figures KEYS[1] and KEYS[2] and deletes the spent keys after them
var growScript = valkey.NewLuaScript(`
for i = 1, 2 do
	if tonumber(ARGV[i]) > tonumber(redis.call('GET', KEYS[i]) or '-1') then
		redis.call('SET', KEYS[i], ARGV[i], 'KEEPTTL')
	end
	if redis.call('PEXPIRETIME', KEYS[i]) < tonumber(ARGV[3]) then
		redis.call('PEXPIREAT', KEYS[i], ARGV[3])
	end
end
redis.call('DEL', KEYS[3], KEYS[4])
return 0
`)

// Meter counts placements' bytes in Valkey under one key prefix
type Meter struct {
	c         valkey.Client
	prefix    string
	s         config.EdgeBudget
	walletCap int64
	now       func() time.Time
}

// New returns a Meter whose paces default to s and that holds no wallet cap
func New(c valkey.Client, prefix string, s config.EdgeBudget) *Meter {
	return &Meter{c: c, prefix: prefix, s: s, now: time.Now}
}

// WithWalletCap sets the bytes a payer's placements may carry together each UTC day, none for 0
func (m *Meter) WithWalletCap(n int64) *Meter {
	m.walletCap = max(n, 0)
	return m
}

// WithClock makes the meter read the UTC day from now
func (m *Meter) WithClock(now func() time.Time) *Meter {
	m.now = now
	return m
}

// walletKey names a payer's key of what for the current UTC day
func (m *Meter) walletKey(what, payer string) string {
	return m.prefix + "wallet:" + what + ":" + payer + ":" + m.now().UTC().Format("20060102")
}

// walletOn reports whether the limit's placement counts against a wallet cap
func (m *Meter) walletOn(l Limit) bool { return m.walletCap > 0 && l.Payer != "" }

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
		keys := []string{m.key(kind, "used", p), m.key(kind, "spent", p), m.key(kind, "budget", p)}
		seen, wallet := 0, 0
		if kind == Edge {
			keys = append(keys, m.key(kind, "seen", p))
			seen = len(keys)
		}
		if m.walletOn(l) {
			keys = append(keys, m.walletKey("bytes", l.Payer), m.walletKey("spent", l.Payer))
			wallet = len(keys) - 1
		}
		execs[i] = valkey.LuaExec{Keys: keys, Args: []string{
			strconv.FormatUint(deltas[p], 10), strconv.FormatInt(l.Budget, 10),
			strconv.FormatInt(l.Deadline.Add(keyLife).UnixMilli(), 10), string(by), string(other),
			strconv.Itoa(wallet), strconv.FormatInt(m.walletCap, 10),
			strconv.FormatInt(walletLife.Milliseconds(), 10), strconv.Itoa(seen),
		}}
	}
	answers := make(map[uuid.UUID]Answer, len(order))
	var spent []uuid.UUID
	for i, res := range reportScript.ExecMulti(ctx, m.c, execs...) {
		p := order[i]
		out, err := res.AsIntSlice()
		if err == nil && len(out) != 6 {
			err = fmt.Errorf("the script answered %d values", len(out))
		}
		if err != nil {
			return nil, nil, fmt.Errorf("budget: report %s usage of %s: %w", kind, p, err)
		}
		pace := uint64(m.pace(kind, limits[p]))
		if out[2] == 1 {
			pace /= 2
		}
		remaining := left(out[3], out[0])
		if out[4] >= 0 {
			remaining = min(remaining, left(m.walletCap, out[4]))
		}
		answers[p] = Answer{Remaining: remaining, Pace: pace, WalletCrossed: out[5] == 1}
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
	wallets := map[string]int{}
	for _, l := range ls {
		if _, ok := wallets[l.Payer]; m.walletOn(l) && !ok {
			wallets[l.Payer] = len(keys)
			keys = append(keys, m.walletKey("bytes", l.Payer))
		}
	}
	vals, err := m.c.Do(ctx, m.c.B().Mget().Key(keys...).Build()).ToArray()
	if err == nil && len(vals) != len(keys) {
		err = fmt.Errorf("read %d counters, got %d", len(keys), len(vals))
	}
	if err != nil {
		return nil, fmt.Errorf("budget: read %s usage: %w", kind, err)
	}
	count := func(i int) (int64, error) {
		n, err := vals[i].AsInt64()
		if valkey.IsValkeyNil(err) {
			return 0, nil
		}
		return n, err
	}
	for i, l := range ls {
		used, err := count(i)
		if err != nil {
			return nil, fmt.Errorf("budget: read %s usage of %s: %w", kind, l.Placement, err)
		}
		out[l.Placement] = left(l.Budget, used)
		if at, ok := wallets[l.Payer]; ok && m.walletOn(l) {
			wused, err := count(at)
			if err != nil {
				return nil, fmt.Errorf("budget: read the wallet usage for %s: %w", l.Placement, err)
			}
			out[l.Placement] = min(out[l.Placement], left(m.walletCap, wused))
		}
	}
	return out, nil
}

// Used reads the bytes counted for the placement's budget of kind, zero when none were
func (m *Meter) Used(ctx context.Context, kind Kind, placement uuid.UUID) (int64, error) {
	n, err := m.c.Do(ctx, m.c.B().Get().Key(m.key(kind, "used", placement)).Build()).AsInt64()
	if valkey.IsValkeyNil(err) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("budget: read the %s usage of %s: %w", kind, placement, err)
	}
	return max(n, 0), nil
}

// Spent reports whether the placement's budget of kind was crossed and not grown since
func (m *Meter) Spent(ctx context.Context, kind Kind, placement uuid.UUID) (bool, error) {
	n, err := m.c.Do(ctx, m.c.B().Exists().Key(m.key(kind, "spent", placement)).Build()).AsInt64()
	if err != nil {
		return false, fmt.Errorf("budget: read the %s spent key of %s: %w", kind, placement, err)
	}
	return n == 1, nil
}

// SpentFor reports whether the placement's budget of kind was crossed and not grown since, or
// its payer's wallet reached its cap today
func (m *Meter) SpentFor(ctx context.Context, kind Kind, l Limit) (bool, error) {
	keys := []string{m.key(kind, "spent", l.Placement)}
	if m.walletOn(l) {
		keys = append(keys, m.walletKey("spent", l.Payer))
	}
	n, err := m.c.Do(ctx, m.c.B().Exists().Key(keys...).Build()).AsInt64()
	if err != nil {
		return false, fmt.Errorf("budget: read the %s spent keys of %s: %w", kind, l.Placement, err)
	}
	return n > 0, nil
}

// Grow records an extension's budgets for the reports and clears both spent keys in one step
func (m *Meter) Grow(
	ctx context.Context, placement uuid.UUID, edge, egress int64, deadline time.Time,
) error {
	keys := []string{
		m.key(Edge, "budget", placement), m.key(Egress, "budget", placement),
		m.key(Edge, "spent", placement), m.key(Egress, "spent", placement),
	}
	args := []string{
		strconv.FormatInt(edge, 10), strconv.FormatInt(egress, 10),
		strconv.FormatInt(deadline.Add(keyLife).UnixMilli(), 10),
	}
	if err := growScript.Exec(ctx, m.c, keys, args).Error(); err != nil {
		return fmt.Errorf("budget: grow the budgets of %s: %w", placement, err)
	}
	return nil
}

// MoveDeadline keeps the placement's used counters and unpaid x402 use an hour past deadline
func (m *Meter) MoveDeadline(ctx context.Context, placement uuid.UUID, deadline time.Time) error {
	keys := []string{
		m.key(Edge, "used", placement), m.key(Egress, "used", placement),
		m.key(Egress, "x402", placement),
	}
	at := strconv.FormatInt(deadline.Add(keyLife).UnixMilli(), 10)
	if err := keepScript.Exec(ctx, m.c, keys, []string{at}).Error(); err != nil {
		return fmt.Errorf("budget: move the counters of %s: %w", placement, err)
	}
	return nil
}

// ReportX402Unpaid adds per-host deltas to egress:x402:<placement>, answering the fields dropped
func (m *Meter) ReportX402Unpaid(
	ctx context.Context, deltas map[uuid.UUID]map[string]X402Unpaid,
	limits map[uuid.UUID]Limit,
) (map[uuid.UUID]int, error) {
	var order []uuid.UUID
	var execs []valkey.LuaExec
	for p, hosts := range deltas {
		l, ok := limits[p]
		if !ok {
			continue
		}
		args := []string{
			strconv.FormatInt(l.Deadline.Add(keyLife).UnixMilli(), 10),
			strconv.Itoa(x402Fields),
		}
		// Sorted, so a full hash drops the same hosts every time
		for _, host := range slices.Sorted(maps.Keys(hosts)) {
			u := hosts[host]
			if !hostname.Valid(host) || u == (X402Unpaid{}) {
				continue
			}
			args = append(args,
				host+":req", strconv.FormatUint(min(u.Requests, maxDelta), 10),
				host+":bytes", strconv.FormatUint(min(u.Bytes, maxDelta), 10))
		}
		if len(args) == 2 {
			continue
		}
		order = append(order, p)
		execs = append(execs, valkey.LuaExec{Keys: []string{m.key(Egress, "x402", p)}, Args: args})
	}
	dropped := map[uuid.UUID]int{}
	if len(execs) == 0 {
		return dropped, nil
	}
	for i, res := range unpaidScript.ExecMulti(ctx, m.c, execs...) {
		n, err := res.AsInt64()
		if err != nil {
			return nil, fmt.Errorf("budget: report unpaid x402 use of %s: %w", order[i], err)
		}
		if n > 0 {
			dropped[order[i]] = int(n)
		}
	}
	return dropped, nil
}

// X402Unpaid answers the placement's unpaid totals by host, empty when none
func (m *Meter) X402Unpaid(
	ctx context.Context, placement uuid.UUID,
) (map[string]X402Unpaid, error) {
	fields, err := m.c.Do(ctx, m.c.B().Hgetall().Key(m.key(Egress, "x402", placement)).Build()).
		AsIntMap()
	if err != nil {
		return nil, fmt.Errorf("budget: read unpaid x402 use of %s: %w", placement, err)
	}
	out := make(map[string]X402Unpaid, len(fields)/2)
	for field, n := range fields {
		host, what, ok := strings.Cut(field, ":")
		if !ok {
			continue
		}
		u := out[host]
		switch what {
		case "req":
			u.Requests = uint64(max(n, 0))
		case "bytes":
			u.Bytes = uint64(max(n, 0))
		default:
			continue
		}
		out[host] = u
	}
	return out, nil
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
