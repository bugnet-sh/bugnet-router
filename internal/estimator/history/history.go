// Package history turns the usage observations of past runs into the peaks a quote may use
//
// Only counted payers count (CP 4.7). Each payer collapses to one maximum a dimension, and a
// quote takes the 90th percentile of those, so one wallet's runs never move a quote alone.
package history

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/rules"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// maxRows bounds the observations one level reads, newest first
const maxRows = 1000

// oomPercent is what a run that ran out of memory counts as, of its package's memory
const oomPercent = 125

// Key names the source whose past runs a quote reads
type Key struct {
	// Source is the source key, and a Key without one has no history
	Source string
	// Revision and Lock are empty when the quote knows none, which skips their level
	Revision, Lock string
}

// Settings are the estimator's history settings
type Settings struct {
	Window             time.Duration
	MinObservations    int
	MinPayers          int
	LowerObservations  int
	LowerPayers        int
	MayLower           bool
	OOMPayers          int
	PayerMinAge        time.Duration
	PayerMinPlacements int
	HeadroomPercent    int
	// PackageMemoryMiB holds each start package's memory, for the runs that ran out of it
	PackageMemoryMiB map[string]int
	// Now is time.Now when nil
	Now func() time.Time
}

// SettingsOf reads the history settings off the estimator's and the catalog's start packages
func SettingsOf(c config.Estimator, pkgs []catalog.Package) Settings {
	mem := map[string]int{}
	for _, p := range pkgs {
		mem[p.ID] = p.Resources.MemoryMiB
	}
	return Settings{
		Window: c.HistoryWindow, MinObservations: c.HistoryMinObservations,
		MinPayers: c.HistoryMinPayers, LowerObservations: c.HistoryLowerObservations,
		LowerPayers: c.HistoryLowerPayers, MayLower: c.HistoryMayLower,
		OOMPayers: c.HistoryOOMPayers, PayerMinAge: c.HistoryPayerMinAge,
		PayerMinPlacements: c.HistoryPayerMinPlacements,
		HeadroomPercent:    int(c.MemoryHeadroom*100 + 0.5), PackageMemoryMiB: mem,
	}
}

// row is one counted observation
type row struct {
	payer, pkg                          string
	memMiB, diskMiB, gpuMiB, activeSecs int
	oom                                 bool
}

// Load returns the history of key's source for a workload at the first level with enough
// counted observations and payers, and the zero History when none has
func Load(
	ctx context.Context, q store.Querier, key Key, workload string, s Settings,
) (rules.History, error) {
	if key.Source == "" {
		return rules.History{}, nil
	}
	now := time.Now
	if s.Now != nil {
		now = s.Now
	}
	// A level is its extra condition, the same source and workload throughout
	levels := []struct {
		on    bool
		where string
		arg   string
	}{
		{key.Revision != "", "o.revision = $", key.Revision},
		{key.Lock != "", "o.lock_key = $", key.Lock},
		{true, "", ""},
	}
	for _, lv := range levels {
		if !lv.on {
			continue
		}
		rows, err := load(ctx, q, key.Source, workload, lv.where, lv.arg, s, now())
		if err != nil {
			return rules.History{}, err
		}
		if h, ok := summarize(rows, s); ok {
			return h, nil
		}
	}
	return rules.History{}, nil
}

// load reads the counted observations of one level, newest first
func load(
	ctx context.Context, q store.Querier, source, workload, where, arg string, s Settings,
	now time.Time,
) ([]row, error) {
	args := []any{source, workload, now.Add(-s.Window), s.PayerMinAge.Seconds(),
		s.PayerMinPlacements}
	extra := ""
	if where != "" {
		args = append(args, arg)
		extra = "and " + where + strconv.Itoa(len(args))
	}
	rs, err := q.Query(ctx, fmt.Sprintf(`
		select p.payer, o.package_id, coalesce(o.peak_memory_mib, 0), coalesce(o.peak_disk_mib, 0),
			coalesce(o.gpu_peak_memory_mib, 0), coalesce(o.active_seconds, 0), o.oom_kills > 0
		from usage_observations o
		join placements p on p.id = o.placement_id
		join wallets w on w.payer = p.payer
		where o.source_key = $1 and coalesce(o.workload, '') = $2 and not o.flagged
			and o.recorded_at > $3
			and w.first_seen <= o.recorded_at - make_interval(secs => $4)
			and (select count(*) from placements q where q.payer = p.payer
				and q.state in ('ready', 'terminating', 'terminated')) >= $5
			and not exists (select 1 from wallet_events e
				where e.payer = p.payer and e.kind = 'block')
			%s
		order by o.recorded_at desc, o.placement_id
		limit %d`, extra, maxRows), args...)
	if err != nil {
		return nil, fmt.Errorf("history: select observations: %w", err)
	}
	out, err := pgx.CollectRows(rs, func(r pgx.CollectableRow) (row, error) {
		var x row
		err := r.Scan(&x.payer, &x.pkg, &x.memMiB, &x.diskMiB, &x.gpuMiB, &x.activeSecs, &x.oom)
		return x, err
	})
	if err != nil {
		return nil, fmt.Errorf("history: read observations: %w", err)
	}
	return out, nil
}

// summarize reduces rows to a History, false when they are too few or from too few payers
func summarize(rows []row, s Settings) (rules.History, bool) {
	perPayer := map[string]*row{}
	oomPayers := map[string]map[string]bool{}
	for _, r := range rows {
		if r.oom {
			if pm := s.PackageMemoryMiB[r.pkg]; pm > 0 {
				// The real need was above the package it ran out of
				r.memMiB = max(r.memMiB, pm*oomPercent/100)
			}
			if oomPayers[r.pkg] == nil {
				oomPayers[r.pkg] = map[string]bool{}
			}
			oomPayers[r.pkg][r.payer] = true
		}
		m := perPayer[r.payer]
		if m == nil {
			m = &row{}
			perPayer[r.payer] = m
		}
		m.memMiB, m.diskMiB = max(m.memMiB, r.memMiB), max(m.diskMiB, r.diskMiB)
		m.gpuMiB, m.activeSecs = max(m.gpuMiB, r.gpuMiB), max(m.activeSecs, r.activeSecs)
	}
	if len(rows) < s.MinObservations || len(perPayer) < s.MinPayers || len(rows) == 0 {
		return rules.History{}, false
	}
	var mem, disk, gpu, active []int
	for _, m := range perPayer {
		mem, disk = append(mem, m.memMiB), append(disk, m.diskMiB)
		gpu, active = append(gpu, m.gpuMiB), append(active, (m.activeSecs+59)/60)
	}
	h := rules.History{
		Observations: len(rows), Payers: len(perPayer), MemoryMiB: p90(mem), DiskMiB: p90(disk),
		GPUMemoryMiB: p90(gpu), ActiveMinutes: p90(active), HeadroomPercent: s.HeadroomPercent,
		Lower: s.MayLower && len(rows) >= s.LowerObservations && len(perPayer) >= s.LowerPayers,
	}
	for pkg, payers := range oomPayers {
		if len(payers) >= s.OOMPayers {
			h.OOMPackages = append(h.OOMPackages, pkg)
		}
	}
	slices.Sort(h.OOMPackages)
	return h, true
}

// p90 is percentile_disc(0.9), the smallest value at or below which 90% of the values lie
func p90(v []int) int {
	s := slices.Clone(v)
	slices.Sort(s)
	return s[(9*len(s)+9)/10-1]
}
