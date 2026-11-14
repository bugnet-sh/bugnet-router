package saga

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/spend"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/telemetry/attr"
)

// maxPre is the longest a reservation's sandbox ran before the order, which a hedge's cost counts,
// the reservation's preparation budget and the longest an order waits for it
const maxPre = 55 * time.Second

// attemptResult is what the goroutine of one attempt, or of the wait on a reservation, reports
type attemptResult struct {
	r    *run
	out  outcome
	prov provider.Provider
	// ran says a provider was tried, so the sweep covers it
	ran bool
	// full says the provider had no free slot and nothing ran
	full bool
	// wait marks the wait on a reservation, bound saying the placement is bound or left placing
	wait, bound bool
}

// placing is one placement's search for a sandbox, its attempts running under one budget
type placing struct {
	s       *Saga
	ctx     context.Context // outlives the attempts, so cleanup runs after a loser is cut short
	budget  context.Context
	p       store.Placement
	pkg     catalog.Package
	req     placement.Requirements
	arrived time.Time
	race    *race
	results chan attemptResult
	// paid is what the order paid for its package, and prefer the server its quote holds
	paid   int64
	prefer *placement.Server
	// ran are the attempts that ended having tried a provider, which spend the payment
	ran []*run

	tried     map[string]bool
	attempted []provider.Provider
	started   int
	live      int
	// stopped says no attempt starts after the ones running
	stopped bool
	// hedged says a hedge started or was refused, so the placement hedges at most once
	hedged bool
	// done says the placement left placing, kept being the sandbox it became ready on
	done     bool
	kept     *provider.Ref
	keptProv provider.Provider
	full     bool
	reason   string
	detail   string
	// waiting is the reservation the order awaits and its pseudo attempt, nil without one
	waiting *store.Reservation
	wait    *run
	timer   *time.Timer
	armed   *run
}

// place binds the reservation the order awaits, else tries candidates until one is ready or the
// attempt, time or payment budget runs out, starting a second attempt beside one that runs long
func (s *Saga) place(
	ctx context.Context, p store.Placement, pkg catalog.Package, req placement.Requirements,
	t terms, awaiting *store.Reservation, arrived time.Time,
) {
	stopHeartbeat := s.heartbeat(ctx, p.ID)
	defer stopHeartbeat()
	budget, cancel := context.WithTimeout(ctx, placingBudget)
	defer cancel()
	pl := &placing{
		s: s, ctx: ctx, budget: budget, p: p, pkg: pkg, req: req, arrived: arrived,
		race: &race{}, results: make(chan attemptResult, maxAttempts+2), paid: t.paid,
		prefer: t.prefer, tried: map[string]bool{}, reason: reasonExhausted, waiting: awaiting,
	}
	pl.run()
}

func (pl *placing) run() {
	defer pl.disarm()
	if pl.waiting != nil {
		pl.startWait()
	}
	for {
		if !pl.stopped && pl.live == 0 {
			if c := pl.next(); c != nil {
				pl.launch(*c, false)
			} else {
				pl.stopped = true
			}
		}
		if pl.live == 0 {
			break
		}
		var fire <-chan time.Time
		if pl.timer != nil {
			fire = pl.timer.C
		}
		select {
		case res := <-pl.results:
			pl.live--
			pl.handle(res)
		case <-fire:
			pl.timer = nil
			pl.tryHedge()
		}
	}
	pl.finish()
}

// finish sweeps what the attempts left and fails a placement that never became ready
func (pl *placing) finish() {
	s := pl.s
	if pl.done {
		if pl.wait != nil && len(pl.attempted) == 0 {
			return
		}
		// One attempt on a provider that keys its creates left no sandbox beside the kept one
		if pl.kept == nil || len(pl.attempted) > 1 ||
			!pl.keptProv.Capabilities().OneSandboxPerKey {
			s.sweep(pl.ctx, pl.p.ID, pl.attempted, pl.kept)
		}
		return
	}
	s.fail(pl.ctx, pl.p, pl.reason, pl.detail, pl.attempted)
}

// next picks the cheapest untried candidate that fits what is left and can start now, nil for none
func (pl *placing) next() *placement.Candidate {
	s := pl.s
	for pl.budget.Err() == nil && pl.started < maxAttempts {
		in, ok := s.within(pl.pkg, pl.req, pl.tried, pl.paid-pl.spentMicros(s.now()))
		if !ok {
			return nil
		}
		// The quote holds its server for the first attempt alone
		if pl.started == 0 {
			in.Prefer = pl.prefer
		}
		cands, err := s.d.Engine.Candidates(in)
		if err != nil {
			if !errors.Is(err, placement.ErrNoCandidate) {
				pl.reason, pl.detail = reasonInternal, fmt.Sprintf("find candidates: %v", err)
			} else if pl.started == 0 && pl.full {
				pl.reason = reasonFull
			}
			return nil
		}
		c := cands[0]
		name := c.Provider.Name()
		pl.tried[name] = true
		if reason, detail := s.noRunner(c.Provider); reason != "" {
			pl.reason, pl.detail = reason, detail
			continue
		}
		if s.createsPaused(pl.ctx, name) {
			pl.reason, pl.detail = reasonRateLimited, name+" asked for its creates to wait"
			continue
		}
		return &c
	}
	return nil
}

// spentMicros is what the attempts that ended cost by now, each billed from its start
func (pl *placing) spentMicros(now time.Time) int64 {
	var total int64
	for _, r := range pl.ran {
		c := r.cand
		total += spend.AttemptCost(r.started, now, c.Provider.BillingIncrement(), c.HourlyMicros,
			c.StartFeeMicros)
	}
	return total
}

// noRunner says why a provider that needs a bugnet-run upload cannot take a sandbox, empty when it
// can, since a sandbox without the build could never run a job
func (s *Saga) noRunner(prov provider.Provider) (reason, detail string) {
	caps := prov.Capabilities()
	if caps.NeedsRunnerUpload && len(s.d.Runners[caps.RunnerArch()]) == 0 {
		s.d.Log.Error("no bugnet-run build for a provider that needs one",
			"provider", prov.Name(), "arch", caps.RunnerArch())
		return reasonNoRunner, "no " + caps.RunnerArch() + " build for " + prov.Name()
	}
	return "", ""
}

// launch starts an attempt on c as a goroutine that reports on the results channel
func (pl *placing) launch(c placement.Candidate, hedge bool) {
	s := pl.s
	pl.started++
	pl.live++
	n := pl.started
	r := pl.race.newRun(n, c, hedge, s.now())
	if hedge {
		pl.tried[c.Provider.Name()] = true
	}
	ctx, budget := withRun(pl.ctx, r), withRun(pl.budget, r)
	go func() {
		res := s.runAttempt(ctx, budget, pl, r)
		res.r, res.prov = r, c.Provider
		pl.results <- res
	}()
	if !hedge {
		pl.arm(r, pathOf(c), r.started)
	}
}

// pathOf is the start path a candidate's attempt takes first
func pathOf(c placement.Candidate) Path {
	if c.Warm {
		return PathPooled
	}
	return PathCold
}

// runAttempt claims a warm member for the attempt when it can and else holds a slot and creates
func (s *Saga) runAttempt(
	ctx, budget context.Context, pl *placing, r *run,
) attemptResult {
	c, n := r.cand, r.n
	ctx, span := s.span(ctx, "saga.attempt", attr.Attempt(n), attr.Provider(c.Provider.Name()),
		attr.Size(c.Size.Name))
	budget = trace.ContextWithSpan(budget, span)
	res := s.runAttemptIn(ctx, budget, pl, r)
	if res.out.reason != "" && !res.out.done {
		span.SetAttributes(attr.Reason(res.out.reason))
	}
	span.End()
	return res
}

func (s *Saga) runAttemptIn(
	ctx, budget context.Context, pl *placing, r *run,
) attemptResult {
	c, n := r.cand, r.n
	out, ran := s.attemptWarm(ctx, budget, pl.p, pl.pkg, pl.req, c, n, &pl.race.missed)
	if ran {
		return attemptResult{out: out, ran: true}
	}
	reserved, err := s.reserve(ctx, pl.p.ID, c.Provider.Name(), n)
	switch {
	case errors.Is(err, store.ErrStateConflict) && r.hedge:
		// The placement left placing while the hedge looked for a slot
		return attemptResult{out: outcome{done: true}}
	case err != nil:
		return attemptResult{out: outcome{
			reason: reasonInternal,
			detail: fmt.Sprintf("reserve on %s: %v", c.Provider.Name(), err),
		}}
	case !reserved:
		return attemptResult{full: true}
	}
	return attemptResult{out: s.attemptCold(ctx, budget, pl.p, pl.pkg, pl.req, c, n,
		&pl.race.missed), ran: true}
}

// handle takes in what one goroutine reported
func (pl *placing) handle(res attemptResult) {
	if res.r == pl.armed {
		pl.disarm()
	}
	if res.wait {
		if res.bound {
			pl.done, pl.stopped = true, true
			pl.race.stop()
		}
		return
	}
	if res.ran {
		pl.attempted = append(pl.attempted, res.prov)
		pl.ran = append(pl.ran, res.r)
	}
	if res.full {
		// A provider with no slot takes no attempt number, as it never took an attempt
		pl.full = true
		if res.r.n == pl.started {
			pl.started--
		}
		return
	}
	out := res.out
	if out.done {
		pl.done, pl.stopped = true, true
		if out.kept != nil {
			pl.kept, pl.keptProv = out.kept, res.prov
		}
		pl.race.stop()
		return
	}
	pl.reason, pl.detail = out.reason, out.detail
	if !out.failOver {
		pl.stopped = true
	}
}

/* Waiting on a reservation */

// startWait waits for the reservation the order awaits as a pseudo attempt, which counts as the
// first one for hedging and is measured from the reservation's creation
func (pl *placing) startWait() {
	s := pl.s
	res := *pl.waiting
	c, path := pl.reservedCandidate(res)
	r := pl.race.newRun(0, c, false, res.CreatedAt)
	pl.wait = r
	pl.live++
	budget := withRun(pl.budget, r)
	go func() {
		wctx, cancel := work(budget)
		bound := s.awaitReservation(wctx, pl.p, pl.pkg, res, pl.arrived)
		cancel()
		pl.results <- attemptResult{r: r, wait: true, bound: bound}
	}()
	if c.Provider != nil {
		pl.arm(r, path, res.CreatedAt)
	}
}

// reservedCandidate is the provider and size the reservation's sandbox is on, with the start path
// its time is judged by, and the zero candidate when the member cannot be read
func (pl *placing) reservedCandidate(res store.Reservation) (placement.Candidate, Path) {
	s := pl.s
	ctx, cancel := step(pl.ctx)
	defer cancel()
	mem, err := s.d.Store.MemberByID(ctx, res.MemberID)
	if err != nil {
		return placement.Candidate{}, PathCold
	}
	prov, ok := s.d.Registry.Get(mem.Provider)
	if !ok {
		return placement.Candidate{}, PathCold
	}
	size, ok := sizeNamed(prov, mem.SizeName)
	if !ok {
		return placement.Candidate{}, PathCold
	}
	hourly, fee := quote(prov, size)
	path := PathCold
	if mem.Origin != store.OriginReserve {
		path = PathPooled
	}
	return placement.Candidate{
		Provider: prov, Size: size, HourlyMicros: hourly, StartFeeMicros: fee,
	}, path
}

/* Hedges */

// arm starts the timer of the attempt that runs, unless the placement hedged already
func (pl *placing) arm(r *run, path Path, since time.Time) {
	s := pl.s
	pl.disarm()
	if pl.hedged || s.d.Hedger == nil || r.cand.Provider == nil {
		return
	}
	after, ok := s.d.Hedger.After(r.cand.Provider.Name(), path)
	if !ok {
		return
	}
	pl.timer = time.NewTimer(max(after-s.now().Sub(since), 0))
	pl.armed = r
}

func (pl *placing) disarm() {
	if pl.timer != nil {
		pl.timer.Stop()
	}
	pl.timer, pl.armed = nil, nil
}

// tryHedge starts the next candidate beside the running attempt when every condition of CP 18.7.1
// holds, and otherwise changes nothing, the timer not arming again
func (pl *placing) tryHedge() {
	s := pl.s
	r := pl.armed
	pl.armed = nil
	switch {
	case r == nil, pl.hedged, pl.stopped, r.unknown.Load():
		return
	case pl.live != 1, pl.started >= maxAttempts:
		return
	}
	ctx, cancel := step(pl.ctx)
	defer cancel()
	excluded := maps.Clone(pl.tried)
	excluded[r.cand.Provider.Name()] = true
	rest := pl.paid - pl.spentMicros(s.now())
	in, ok := s.within(pl.pkg, pl.req, excluded, rest)
	if !ok {
		return
	}
	cands, err := s.d.Engine.Candidates(in)
	if err != nil {
		return
	}
	next := cands[0]
	if reason, _ := s.noRunner(next.Provider); reason != "" {
		return
	}
	name := next.Provider.Name()
	if free, err := s.d.Store.HasCapacity(ctx, name); err != nil || !free {
		return
	}
	var left time.Duration
	if dl, ok := pl.budget.Deadline(); ok {
		left = time.Until(dl)
	}
	var pre time.Duration
	if r.n == 0 {
		pre = min(max(s.now().Sub(r.started), 0), maxPre)
	}
	if !s.d.Hedger.Allow(ctx, pl.pkg, r.cand, next, pre, left, rest, s.d.Rule) {
		return
	}
	pl.hedged = true
	pl.launch(next, true)
}
