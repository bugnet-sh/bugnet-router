package store

import (
	"context"
	"fmt"
	"time"
)

// RetentionTable is a table whose old rows the retention job deletes
type RetentionTable string

// The tables retention trims, in the order it trims them. A row that another table refers to
// goes after the rows that refer to it.
const (
	RetainStartSamples     RetentionTable = "start_samples"
	RetainBenches          RetentionTable = "benches"
	RetainTransferIndex    RetentionTable = "transfer_index"
	RetainProbeSlots       RetentionTable = "probe_slots"
	RetainUsageObservation RetentionTable = "usage_observations"
	RetainEstimates        RetentionTable = "estimates"
	RetainSourceFacts      RetentionTable = "source_facts"
)

// RetentionTables lists the tables retention trims, in its order
var RetentionTables = []RetentionTable{
	RetainStartSamples, RetainBenches, RetainTransferIndex, RetainProbeSlots,
	RetainUsageObservation, RetainEstimates, RetainSourceFacts,
}

// retentionSQL deletes up to $2 rows of a table older than $1 and answers how many
var retentionSQL = map[RetentionTable]string{
	RetainStartSamples: `with d as (delete from start_samples where id in (
			select id from start_samples where started_at < $1 limit $2) returning 1)
		select count(*) from d`,
	RetainBenches: `with d as (delete from benches where id in (
			select id from benches where created_at < $1 limit $2) returning 1)
		select count(*) from d`,
	RetainTransferIndex: `with d as (delete from transfer_index where token_sha256 in (
			select token_sha256 from transfer_index where created_at < $1 limit $2) returning 1)
		select count(*) from d`,
	// A probe's slot that outlived its probe is freed with its row, as ReleaseProbeSlot does
	RetainProbeSlots: `with d as (delete from probe_slots where id in (
			select id from probe_slots where reserved_at < $1 limit $2) returning provider),
		freed as (update provider_capacity c set in_use = greatest(c.in_use - n.n, 0)
			from (select provider, count(*)::int as n from d group by provider) n
			where c.provider = n.provider returning 1)
		select count(*) from d`,
	RetainUsageObservation: `with d as (delete from usage_observations where placement_id in (
			select placement_id from usage_observations where recorded_at < $1 limit $2)
			returning 1)
		select count(*) from d`,
	RetainEstimates: `with d as (delete from estimates where id in (
			select e.id from estimates e where e.served_at < $1
				and not exists (select 1 from placements p where p.estimate_id = e.id)
				and not exists (select 1 from usage_observations o where o.estimate_id = e.id)
			limit $2) returning 1)
		select count(*) from d`,
	RetainSourceFacts: `with d as (delete from source_facts f where (f.source_key, f.revision,
			f.extractor_version) in (
			select g.source_key, g.revision, g.extractor_version from source_facts g
			where g.used_at < $1 and not exists (select 1 from estimates e
				where e.source_key = g.source_key and e.revision = g.revision)
			limit $2) returning 1)
		select count(*) from d`,
}

// Purge deletes up to limit rows of t older than cutoff and returns how many it deleted
func (s *Store) Purge(
	ctx context.Context, t RetentionTable, cutoff time.Time, limit int,
) (int, error) {
	q, ok := retentionSQL[t]
	if !ok {
		return 0, fmt.Errorf("purge: no retention rule for %q", t)
	}
	var n int
	if err := s.q.QueryRow(ctx, q, utc(cutoff), limit).Scan(&n); err != nil {
		return 0, fmt.Errorf("purge %s: %w", t, err)
	}
	return n, nil
}
