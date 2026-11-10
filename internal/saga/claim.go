package saga

import (
	"context"
	"fmt"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/pool"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// attemptClaim takes a member of a provider-kept pool for c and runs the pooled attempt on it,
// reporting false when no attempt ran and the candidate goes on as a cold create
//
// A claim reserves a slot as a create does. The member's row, bound to the placement, and the
// attempt's row are written before the call, so a replica that dies in the middle leaves a row a
// retry of the same key settles. Koyeb made the member and ran nothing on it, so Boot runs with
// Pool set before Prepare. A claim that fails falls back to a cold create on the same candidate.
func (s *Saga) attemptClaim(
	ctx, budget context.Context, claimer provider.PoolClaimer, p store.Placement,
	pkg catalog.Package, req placement.Requirements, c placement.Candidate, n int,
) (outcome, bool) {
	prov := c.Provider
	name := prov.Name()
	reserved, err := s.reserve(ctx, p.ID, name, n)
	if err != nil || !reserved || !s.takeCreate(ctx, name) {
		if err == nil && reserved {
			s.release(ctx, p.ID, n)
		}
		return outcome{}, false
	}
	hourly, fee := quote(prov, c.Size)
	key := idempotencyKey(p.OrderID, n, name)
	rec := store.Attempt{
		PlacementID: p.ID, Attempt: n, Provider: name, State: attemptCreating,
		SizeName: c.Size.Name, HourlyMicros: hourly, StartFeeMicros: fee, Path: store.PathPooled,
		IdempotencyKey: key, Hedge: hedgeRun(ctx),
	}
	cctx, cancel := step(ctx)
	m, err := s.d.Store.BindProvided(cctx, name, c.Size.Name, key, rec)
	cancel()
	if err != nil {
		s.d.Log.Warn("record a provider-kept member", "provider", name, "err", err)
		s.release(ctx, p.ID, n)
		return outcome{}, false
	}
	rec.MemberID = m.ID
	s.markHedged(ctx, p.ID)
	started := s.now()
	smp := s.sampleOf(p, prov, c.Size, started)
	smp.StartPath, smp.MemberID = store.PathPooled, m.ID
	wctx, wcancel := work(budget)
	claim, err := claimer.Claim(wctx, pool.NameOf(s.d.DeploymentID, c.Size.Name), key)
	wcancel()
	smp.WakeMS = int(s.now().Sub(started).Milliseconds())
	if err != nil {
		s.d.Log.Warn("claim from a provider-kept pool failed, creating cold", "provider", name,
			"err", err)
		if s.countsAgainst(ctx, name, err, true) {
			s.d.Tracker.RecordFailure(name)
		}
		s.recordSample(ctx, prov, c.Size, smp, err)
		s.retireMember(ctx, m.ID, store.EndFailed, "claim failed: "+err.Error())
		s.release(ctx, p.ID, n)
		return outcome{}, false
	}
	smp.SandboxID = claim.Ref.ID
	rec.SandboxID = claim.Ref.ID
	cctx, cancel = step(ctx)
	err = s.d.Store.MarkMemberClaimed(cctx, m.ID, p.ID, claim.Ref, claim.CreatedAt)
	cancel()
	if err != nil {
		s.recordSample(ctx, prov, c.Size, smp, err)
		s.abandon(ctx, prov, rec, nil, err)
		return outcome{
			failOver: true, reason: reasonInternal,
			detail: fmt.Sprintf("record the claim on %s: %v", name, err),
		}, true
	}
	born := claim.CreatedAt
	m.SandboxCreatedAt = &born
	ref := claim.Ref
	bctx, bcancel := work(ctx)
	ref, boot, err := s.Boot(bctx, prov, ref, BootPlan{Placement: p.ID, Pool: true})
	bcancel()
	if boot.ReadyMS >= 0 {
		smp.ReadyMS = boot.ReadyMS
	}
	smp.RunnerMS = boot.RunnerMS
	var prep Prepared
	if err == nil {
		pctx, pcancel := work(ctx)
		prep, err = s.Prepare(pctx, prov, ref, p.ID, req, true)
		pcancel()
		smp.GateMS = prep.GateMS
	}
	if err != nil {
		if s.countsAgainst(ctx, name, err, false) {
			s.d.Tracker.RecordFailure(name)
		}
		s.recordSample(ctx, prov, c.Size, smp, err)
		s.abandon(ctx, prov, rec, nil, err)
		return outcome{
			failOver: true, reason: reasonBootFailed,
			detail: fmt.Sprintf("prepare on %s: %v", name, err),
		}, true
	}
	s.stampReady(&smp, p)
	s.recordSample(ctx, prov, c.Size, smp, nil)
	return s.commitReady(ctx, p, pkg, c, rec, prep, &m, started), true
}
