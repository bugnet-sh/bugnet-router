package boat

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"sync"
	"time"

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
)

// startWindows are the $20 plan's included starts per window and the multiple Boat refuses past
var startWindows = []struct {
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

// fees knows what the next start costs, from GET /limits or from the starts this process made
type fees struct {
	mu         sync.Mutex
	fetched    time.Time // when /limits last answered
	attempted  time.Time
	refreshing bool
	failing    bool            // the last call to /limits got no usable reply
	refused    bool            // the last reply said a start would be refused
	fee        int64           // the last reply's next start fee
	available  map[string]bool // machine types the last reply named
	starts     []time.Time
}

// StartCostMicros is the fee the next start of a size costs, false when Boat would refuse it
func (p *Provider) StartCostMicros(sz provider.Size) (int64, bool) {
	s, ok := p.size(sz.Name)
	if !ok {
		return 0, false
	}
	f := &p.fees
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()
	wait := p.limitsTTL
	if f.failing {
		wait = max(wait, limitsRetry)
	}
	if !f.refreshing && now.Sub(f.attempted) >= wait {
		f.refreshing, f.attempted = true, now
		go p.refreshLimits()
	}
	if f.fetched.IsZero() || now.Sub(f.fetched) >= limitsStale {
		return f.counted(now)
	}
	if avail, known := f.available[s.typ.name]; f.refused || known && !avail {
		return 0, false
	}
	return f.fee, true
}

// counted prices the next start from this process's own starts in each window
func (f *fees) counted(now time.Time) (int64, bool) {
	fee := int64(0)
	for _, w := range startWindows {
		n := 0
		for _, at := range f.starts {
			if now.Sub(at) < w.span {
				n++
			}
		}
		past := float64(n+1) / w.included
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

// started counts one create, forgetting starts older than the longest window
func (f *fees) started(at time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	keep := f.starts[:0]
	for _, s := range f.starts {
		if at.Sub(s) < startWindows[len(startWindows)-1].span {
			keep = append(keep, s)
		}
	}
	f.starts = append(keep, at)
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
