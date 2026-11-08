package saga

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/estimator/stats"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// statsReadTimeout bounds the read of the sampler's file before a destroy
const statsReadTimeout = 2 * time.Second

// readStats reads the sampler's stats file, nil for a placement with no estimate to learn for
// and for a sandbox that cannot answer within the time, since a stop never waits on it
func (s *Saga) readStats(
	ctx context.Context, p store.Placement, prov provider.Provider, ref provider.Ref,
) []byte {
	if p.EstimateID == nil {
		return nil
	}
	limit := int(cmpMax(s.d.StatsMaxBytes, stats.DefaultMaxBytes))
	ctx, cancel := context.WithTimeout(ctx, statsReadTimeout)
	defer cancel()
	// One byte more than the limit, so a file past it reads as too large
	raw, _, err := prov.ReadFile(ctx, ref, stats.Path, 0, limit+1)
	if err != nil {
		s.d.Log.Debug("read the sandbox stats", "placement", p.ID, "err", err)
		return nil
	}
	return raw
}

func cmpMax(v int64, def int64) int64 {
	if v <= 0 {
		return def
	}
	return v
}

// flagging are the end reasons whose observation history ignores
var flagging = map[string]bool{
	ReasonAbuse: true, ReasonControlLost: true, ReasonWalletBlocked: true,
}

// observe checks the stats read before the destroy and records them, in a transaction of their
// own after the one that marked the placement terminated, so a bad row never fails the end
func (s *Saga) observe(
	ctx context.Context, p store.Placement, prov provider.Provider, raw []byte,
) {
	if raw == nil || p.EstimateID == nil {
		return
	}
	if err := s.recordObservation(ctx, p, prov, raw); err != nil {
		s.d.Log.Warn("record the usage observation", "placement", p.ID, "err", err)
	}
}

type inputsEntry struct {
	Request struct {
		EntrySHA256 string `json:"entry_sha256"`
	} `json:"request"`
}

func (s *Saga) recordObservation(
	ctx context.Context, p store.Placement, prov provider.Provider, raw []byte,
) error {
	pkg, ok := s.d.Catalog.Get(p.PackageID)
	if !ok {
		return fmt.Errorf("package %q is not in the catalog", p.PackageID)
	}
	size, ok := prov.SizeFor(pkg.Resources)
	if !ok {
		size = provider.Size{Resources: pkg.Resources}
	}
	orders, err := s.d.Store.OrdersNewestFirst(ctx, p.ID)
	if err != nil {
		return err
	}
	var paid int64
	extended := false
	for _, o := range orders {
		paid += o.SecondsBought
		extended = extended || o.Kind == store.OrderExtend
	}
	// The read came a little after the deadline, so the time since ready bounds the CPU too
	paid = max(paid, billedSeconds(p))
	if p.ReadyAt != nil {
		paid = max(paid, int64(s.now().Sub(*p.ReadyAt)/time.Second))
	}
	st, err := stats.CheckMax(raw, int(cmpMax(s.d.StatsMaxBytes, stats.DefaultMaxBytes)), size,
		time.Duration(paid)*time.Second)
	if err != nil {
		s.d.Log.Warn("dropped a sandbox's stats file", "placement", p.ID, "err", err)
		return nil
	}
	est, err := s.d.Store.EstimateByID(ctx, *p.EstimateID)
	if err != nil {
		return err
	}
	var in inputsEntry
	if json.Unmarshal(est.Inputs, &in) != nil {
		in = inputsEntry{}
	}
	entry, _ := hex.DecodeString(in.Request.EntrySHA256)
	install, err := s.d.Store.InstallSeconds(ctx, p.ID)
	if err != nil {
		return err
	}
	return s.d.Store.InsertObservation(ctx, store.Observation{
		PlacementID: p.ID, EstimateID: *p.EstimateID, SourceKey: est.SourceKey,
		Revision: est.Revision, LockKey: est.LockKey, Workload: est.Workload,
		EntrySHA256: entry, PackageID: p.PackageID, SizeName: size.Name,
		PeakMemoryMiB: st.MemPeakMiB, PeakSwapMiB: st.SwapPeakMiB, PeakDiskMiB: st.DiskPeakMiB,
		GPUPeakMemoryMiB: st.GPUMemPeakMiB, CPUSeconds: st.CPUSeconds,
		BusyP95Millicores: st.BusyP95Millicores, ActiveSeconds: int(billedSeconds(p)),
		InstallSeconds: install, OOMKills: st.OOMKills, Extended: extended,
		EndedBy: p.TerminationReason, Flagged: flagging[p.TerminationReason],
	})
}
