package boat

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/oynozan/bugnet-router/internal/provider"
)

const (
	// defaultLimitsTTL keeps GET /limits within the 5 to 15 s the notes advise
	defaultLimitsTTL = 10 * time.Second
	// limitsStale is how old a reply may be before the adapter counts starts itself
	limitsStale   = time.Minute
	limitsTimeout = 5 * time.Second
	// limitsRetry spaces out calls to a /limits the key cannot read
	limitsRetry = 30 * time.Second
	// topStartMicros is the dearest extra start, past twice the included starts
	topStartMicros = 70_000
	// startsTimeout bounds a call to the shared start count, past it this process's count is used
	startsTimeout = time.Second
)

// startWindows are the $20 plan's included starts per window and the multiple Boat refuses past
var startWindows = [...]struct {
	span     time.Duration
	included float64
	ceiling  float64
}{
	{time.Minute, 12, 3}, {time.Hour, 60, 6}, {24 * time.Hour, 200, 6},
}

// startBrackets price an extra start by how far past the included starts it lies
var startBrackets = []struct {
	upTo   float64
	micros int64
}{
	{1, 0}, {1.5, 30_000}, {2, 50_000},
}

// fees knows what the next start costs, from GET /limits or from the starts counted so far
type fees struct {
	mu         sync.Mutex
	fetched    time.Time // when /limits last answered
	attempted  time.Time
	refreshing bool
	failing    bool            // the last call to /limits got no usable reply
	refused    bool            // the last reply said a start would be refused
	fee        int64           // the last reply's next start fee
	available  map[string]bool // machine types the last reply named
	starts     []time.Time     // this process's starts, counted when shared is nil or failing
	shared     *sharedStarts
	sharedDown bool // the last call to the shared count failed
}

// StartCostMicros is the fee the next start of a size costs, false when Boat would refuse it
func (p *Provider) StartCostMicros(sz provider.Size) (int64, bool) {
	s, ok := p.size(sz.Name)
	if !ok {
		return 0, false
	}
	now := time.Now()
	if fee, ok, known := p.limitsFee(s, now); known {
		return fee, ok
	}
	return p.countedFee(now)
}

// limitsFee prices a start from a recent /limits reply, known false when there is none
func (p *Provider) limitsFee(s size, now time.Time) (fee int64, ok, known bool) {
	f := &p.fees
	f.mu.Lock()
	defer f.mu.Unlock()
	wait := p.limitsTTL
	if f.failing {
		wait = max(wait, limitsRetry)
	}
	if !f.refreshing && now.Sub(f.attempted) >= wait {
		f.refreshing, f.attempted = true, now
		go p.refreshLimits()
	}
	if f.fetched.IsZero() || now.Sub(f.fetched) >= limitsStale {
		return 0, false, false
	}
	if avail, known := f.available[s.typ.name]; f.refused || known && !avail {
		return 0, false, true
	}
	return f.fee, true, true
}

// countedFee prices the next start from every replica's starts, or this process's alone
func (p *Provider) countedFee(now time.Time) (int64, bool) {
	f := &p.fees
	if f.shared != nil {
		ctx, cancel := context.WithTimeout(context.Background(), startsTimeout)
		defer cancel()
		counts, err := f.shared.counts(ctx, now)
		f.noteShared(err)
		if err == nil {
			return feeFor(counts)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var counts [len(startWindows)]int
	for i, w := range startWindows {
		for _, at := range f.starts {
			if now.Sub(at) < w.span {
				counts[i]++
			}
		}
	}
	return feeFor(counts)
}

// feeFor prices the next start from the starts counted in each window
func feeFor(counts [len(startWindows)]int) (int64, bool) {
	fee := int64(0)
	for i, w := range startWindows {
		past := float64(counts[i]+1) / w.included
		if past > w.ceiling {
			return 0, false
		}
		bracket := int64(topStartMicros)
		for _, b := range startBrackets {
			if past <= b.upTo {
				bracket = b.micros
				break
			}
		}
		fee = max(fee, bracket)
	}
	return fee, true
}

// started counts one create here and in the shared count, forgetting starts past the last window
func (p *Provider) started(at time.Time) {
	f := &p.fees
	f.mu.Lock()
	keep := f.starts[:0]
	for _, s := range f.starts {
		if at.Sub(s) < startWindows[len(startWindows)-1].span {
			keep = append(keep, s)
		}
	}
	f.starts = append(keep, at)
	f.mu.Unlock()
	if f.shared != nil {
		ctx, cancel := context.WithTimeout(context.Background(), startsTimeout)
		defer cancel()
		f.noteShared(f.shared.add(ctx, at))
	}
}

// noteShared logs when the shared start count stops or starts answering
func (f *fees) noteShared(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if down := err != nil; down != f.sharedDown {
		f.sharedDown = down
		if down {
			slog.Warn("boat start count in valkey unavailable, counting this process's starts",
				"err", err)
		} else {
			slog.Info("boat start count in valkey answers again")
		}
	}
}

/* Shared start count */

// sharedStarts counts every replica's starts in a Valkey sorted set scored by Unix ms
type sharedStarts struct {
	c   valkey.Client
	key string
}

// The set forgets starts past the longest window and expires a whole window after the last one
var addStartScript = valkey.NewLuaScript(`
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[3])
redis.call('PEXPIRE', KEYS[1], ARGV[4])
return 1
`)

func (s *sharedStarts) add(ctx context.Context, at time.Time) error {
	span := startWindows[len(startWindows)-1].span
	itoa := func(n int64) string { return strconv.FormatInt(n, 10) }
	member := itoa(at.UnixNano()) + rand.Text()
	args := []string{
		itoa(at.UnixMilli()), member, itoa(at.Add(-span).UnixMilli()), itoa(span.Milliseconds()),
	}
	if err := addStartScript.Exec(ctx, s.c, []string{s.key}, args).Error(); err != nil {
		return fmt.Errorf("count boat start in valkey: %w", err)
	}
	return nil
}

// counts returns how many starts every replica made in each window before now
func (s *sharedStarts) counts(ctx context.Context, now time.Time) ([len(startWindows)]int, error) {
	cmds := make(valkey.Commands, len(startWindows))
	for i, w := range startWindows {
		after := "(" + strconv.FormatInt(now.Add(-w.span).UnixMilli(), 10)
		cmds[i] = s.c.B().Zcount().Key(s.key).Min(after).Max("+inf").Build()
	}
	var counts [len(startWindows)]int
	for i, res := range s.c.DoMulti(ctx, cmds...) {
		n, err := res.AsInt64()
		if err != nil {
			return counts, fmt.Errorf("read boat starts from valkey: %w", err)
		}
		counts[i] = int(n)
	}
	return counts, nil
}

// limitsReply holds the fields of GET /limits that price a start
type limitsReply struct {
	CanStart *bool `json:"canStart"`
	// NextStartDollars is 0 inside the included starts, more past them and null when refused
	NextStartDollars json.RawMessage `json:"nextStartDollars"`
	MachineTypes     []struct {
		Type      string `json:"type"`
		Available *bool  `json:"available"`
	} `json:"machineTypes"`
}

func (p *Provider) refreshLimits() {
	ctx, cancel := context.WithTimeout(context.Background(), limitsTimeout)
	defer cancel()
	reply, err := p.readLimits(ctx)
	f := &p.fees
	f.mu.Lock()
	defer f.mu.Unlock()
	f.refreshing = false
	if err == nil && len(reply.NextStartDollars) == 0 {
		err = fmt.Errorf("boat limits reply holds no nextStartDollars")
	}
	var dollars *float64
	if err == nil {
		err = json.Unmarshal(reply.NextStartDollars, &dollars)
	}
	if err != nil {
		if !f.failing {
			slog.Warn("boat limits unreadable, counting starts instead", "err", err)
		}
		f.failing = true
		return
	}
	f.fetched, f.failing = time.Now(), false
	f.refused = dollars == nil || (reply.CanStart != nil && !*reply.CanStart)
	f.fee = 0
	if dollars != nil {
		f.fee = int64(math.Round(*dollars * 1e6))
	}
	f.available = map[string]bool{}
	for _, t := range reply.MachineTypes {
		if t.Type != "" && t.Available != nil {
			f.available[t.Type] = *t.Available
		}
	}
}

// readLimits calls GET /limits, which the generated client leaves out
func (p *Provider) readLimits(ctx context.Context) (limitsReply, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL+"/limits", nil)
	if err != nil {
		return limitsReply{}, err
	}
	req.Header.Set("Authorization", "Bearer "+p.key)
	req.Header.Set("User-Agent", "bugnet-router")
	resp, err := p.control.Do(req)
	if err != nil {
		return limitsReply{}, redact(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxReplyBytes))
	if err != nil {
		return limitsReply{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return limitsReply{}, apiError(resp.StatusCode, resp.Header, raw, noEffect)
	}
	var reply limitsReply
	return reply, decode(raw, &reply)
}
