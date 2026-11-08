package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// FactsRow is one source's distilled facts at one revision
type FactsRow struct {
	SourceKey        string
	Revision         string
	ExtractorVersion string
	// Facts are RFC 8785 bytes
	Facts       []byte
	FactsSHA256 []byte
}

// factsUsedStep is how seldom a use moves used_at
const factsUsedStep = time.Hour

// PutFacts stores facts, keeping the row already there for the same key
func (s *Store) PutFacts(ctx context.Context, f FactsRow) error {
	_, err := s.q.Exec(ctx, `
		insert into source_facts (source_key, revision, extractor_version, facts, facts_sha256)
		values ($1, $2, $3, $4, $5)
		on conflict (source_key, revision, extractor_version) do update set used_at = now()`,
		f.SourceKey, f.Revision, f.ExtractorVersion, f.Facts, f.FactsSHA256)
	if err != nil {
		return fmt.Errorf("insert source facts: %w", err)
	}
	return nil
}

// GetFacts returns the stored facts, moving used_at at most hourly, or ErrNotFound
func (s *Store) GetFacts(ctx context.Context, key, revision, extractor string) (FactsRow, error) {
	f := FactsRow{SourceKey: key, Revision: revision, ExtractorVersion: extractor}
	err := s.q.QueryRow(ctx, `
		update source_facts set used_at = case
			when used_at < now() - make_interval(secs => $4) then now() else used_at end
		where source_key = $1 and revision = $2 and extractor_version = $3
		returning facts, facts_sha256`, key, revision, extractor, factsUsedStep.Seconds()).
		Scan(&f.Facts, &f.FactsSHA256)
	if errors.Is(err, pgx.ErrNoRows) {
		return FactsRow{}, ErrNotFound
	}
	if err != nil {
		return FactsRow{}, fmt.Errorf("select source facts: %w", err)
	}
	return f, nil
}

// PutCatalog stores a catalog's bytes under their hash when the hash is new
func (s *Store) PutCatalog(ctx context.Context, sha, b []byte) error {
	_, err := s.q.Exec(ctx, `
		insert into catalogs (sha256, bytes) values ($1, $2) on conflict (sha256) do nothing`,
		sha, b)
	if err != nil {
		return fmt.Errorf("insert catalog: %w", err)
	}
	return nil
}

// CatalogBytes returns the catalog stored under sha, or ErrNotFound
func (s *Store) CatalogBytes(ctx context.Context, sha []byte) ([]byte, error) {
	var b []byte
	err := s.q.QueryRow(ctx, `select bytes from catalogs where sha256 = $1`, sha).Scan(&b)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("select catalog: %w", err)
	}
	return b, nil
}

// EstimateRow is one stored estimate
type EstimateRow struct {
	ID               uuid.UUID
	CreatedAt        time.Time
	ServedAt         time.Time
	EstimatorVersion string
	InputsSHA256     []byte
	Inputs           []byte
	Output           []byte
	Availability     []byte
	CatalogSHA256    []byte
	SourceKind       string
	SourceKey        string
	Revision         string
	LockKey          string
	Workload         string
	SourceRead       bool
}

const estimateColumns = `id, created_at, served_at, estimator_version, inputs_sha256, inputs,
	output, availability, catalog_sha256, coalesce(source_kind, ''), coalesce(source_key, ''),
	coalesce(revision, ''), coalesce(lock_key, ''), coalesce(workload, ''), source_read`

func scanEstimate(row pgx.Row) (EstimateRow, error) {
	var e EstimateRow
	err := row.Scan(&e.ID, &e.CreatedAt, &e.ServedAt, &e.EstimatorVersion, &e.InputsSHA256,
		&e.Inputs, &e.Output, &e.Availability, &e.CatalogSHA256, &e.SourceKind, &e.SourceKey,
		&e.Revision, &e.LockKey, &e.Workload, &e.SourceRead)
	if errors.Is(err, pgx.ErrNoRows) {
		return EstimateRow{}, ErrNotFound
	}
	if err != nil {
		return EstimateRow{}, fmt.Errorf("select estimate: %w", err)
	}
	e.CreatedAt, e.ServedAt = utc(e.CreatedAt), utc(e.ServedAt)
	return e, nil
}

// ReuseEstimate moves served_at of the newest row served within maxAge that has these inputs
// and availability, and returns its id, or false when there is none
func (s *Store) ReuseEstimate(
	ctx context.Context, inputsSHA, availability []byte, maxAge time.Duration,
) (uuid.UUID, bool, error) {
	var id uuid.UUID
	err := s.q.QueryRow(ctx, `
		update estimates set served_at = now() where id = (
			select id from estimates
			where inputs_sha256 = $1 and availability = $2
				and served_at > now() - make_interval(secs => $3)
			order by served_at desc limit 1)
		returning id`, inputsSHA, availability, maxAge.Seconds()).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, false, nil
	}
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("reuse estimate: %w", err)
	}
	return id, true, nil
}

// InsertEstimate stores a new estimate
func (s *Store) InsertEstimate(ctx context.Context, e EstimateRow) error {
	_, err := s.q.Exec(ctx, `
		insert into estimates (id, estimator_version, inputs_sha256, inputs, output,
			availability, catalog_sha256, source_kind, source_key, revision, lock_key, workload,
			source_read)
		values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
		e.ID, e.EstimatorVersion, e.InputsSHA256, e.Inputs, e.Output, e.Availability,
		e.CatalogSHA256, nullIfEmpty(e.SourceKind), nullIfEmpty(e.SourceKey),
		nullIfEmpty(e.Revision), nullIfEmpty(e.LockKey), nullIfEmpty(e.Workload), e.SourceRead)
	if err != nil {
		return fmt.Errorf("insert estimate: %w", err)
	}
	return nil
}

// EstimateByID returns the estimate or ErrNotFound
func (s *Store) EstimateByID(ctx context.Context, id uuid.UUID) (EstimateRow, error) {
	return scanEstimate(s.q.QueryRow(ctx,
		`select `+estimateColumns+` from estimates where id = $1`, id))
}

// LiveEstimate returns the estimate when it was served within maxAge, else ErrNotFound
func (s *Store) LiveEstimate(
	ctx context.Context, id uuid.UUID, maxAge time.Duration,
) (EstimateRow, error) {
	return scanEstimate(s.q.QueryRow(ctx, `select `+estimateColumns+` from estimates
		where id = $1 and served_at > now() - make_interval(secs => $2)`, id, maxAge.Seconds()))
}

// Observation is one placement's checked peaks, kept for the estimator's history
type Observation struct {
	PlacementID uuid.UUID
	EstimateID  uuid.UUID
	// SourceKey, Revision, LockKey and Workload come from the estimate, empty for none
	SourceKey, Revision, LockKey, Workload string
	EntrySHA256                            []byte
	PackageID, SizeName                    string
	PeakMemoryMiB, PeakSwapMiB             int
	PeakDiskMiB, GPUPeakMemoryMiB          int
	CPUSeconds                             int64
	BusyP95Millicores, ActiveSeconds       int
	InstallSeconds, OOMKills               int
	Extended                               bool
	EndedBy                                string
	// Flagged is true for an end by abuse or control loss, a flag raised after the end is added
	// by the insert itself
	Flagged bool
}

// InsertObservation records the observation of a placement once, and flags it when the
// placement was flagged for abuse after its end
func (s *Store) InsertObservation(ctx context.Context, o Observation) error {
	_, err := s.q.Exec(ctx, `
		insert into usage_observations (placement_id, estimate_id, source_key, revision, lock_key,
			workload, entry_sha256, package_id, size_name, peak_memory_mib, peak_swap_mib,
			peak_disk_mib, gpu_peak_memory_mib, cpu_seconds, busy_p95_millicores, active_seconds,
			install_seconds, oom_kills, extended, ended_by, flagged)
		values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18,
			$19, $20, $21 or exists (select 1 from provider_signals
				where placement_id = $1 and action = 'flagged'))
		on conflict (placement_id) do nothing`,
		o.PlacementID, nullUUID(o.EstimateID), nullIfEmpty(o.SourceKey), nullIfEmpty(o.Revision),
		nullIfEmpty(o.LockKey), nullIfEmpty(o.Workload), o.EntrySHA256, o.PackageID,
		nullIfEmpty(o.SizeName), o.PeakMemoryMiB, o.PeakSwapMiB, o.PeakDiskMiB,
		o.GPUPeakMemoryMiB, o.CPUSeconds, o.BusyP95Millicores, o.ActiveSeconds,
		o.InstallSeconds, o.OOMKills, o.Extended, nullIfEmpty(o.EndedBy), o.Flagged)
	if err != nil {
		return fmt.Errorf("insert observation: %w", err)
	}
	return nil
}

// FlagObservation marks the placement's observation, if it has one, so history ignores it
func (s *Store) FlagObservation(ctx context.Context, placement uuid.UUID) error {
	_, err := s.q.Exec(ctx,
		`update usage_observations set flagged = true where placement_id = $1`, placement)
	if err != nil {
		return fmt.Errorf("flag observation: %w", err)
	}
	return nil
}

// ObservationOf returns the observation of a placement, or ErrNotFound
func (s *Store) ObservationOf(ctx context.Context, placement uuid.UUID) (Observation, error) {
	var o Observation
	err := s.q.QueryRow(ctx, `
		select placement_id, coalesce(estimate_id, '00000000-0000-0000-0000-000000000000'),
			coalesce(source_key, ''), coalesce(revision, ''), coalesce(lock_key, ''),
			coalesce(workload, ''), coalesce(entry_sha256, ''::bytea), package_id,
			coalesce(size_name, ''), coalesce(peak_memory_mib, 0), coalesce(peak_swap_mib, 0),
			coalesce(peak_disk_mib, 0), coalesce(gpu_peak_memory_mib, 0), coalesce(cpu_seconds, 0),
			coalesce(busy_p95_millicores, 0), coalesce(active_seconds, 0),
			coalesce(install_seconds, 0), oom_kills, extended, coalesce(ended_by, ''), flagged
		from usage_observations where placement_id = $1`, placement).Scan(&o.PlacementID,
		&o.EstimateID, &o.SourceKey, &o.Revision, &o.LockKey, &o.Workload, &o.EntrySHA256,
		&o.PackageID, &o.SizeName, &o.PeakMemoryMiB, &o.PeakSwapMiB, &o.PeakDiskMiB,
		&o.GPUPeakMemoryMiB, &o.CPUSeconds, &o.BusyP95Millicores, &o.ActiveSeconds,
		&o.InstallSeconds, &o.OOMKills, &o.Extended, &o.EndedBy, &o.Flagged)
	if errors.Is(err, pgx.ErrNoRows) {
		return Observation{}, ErrNotFound
	}
	if err != nil {
		return Observation{}, fmt.Errorf("select observation: %w", err)
	}
	return o, nil
}

func nullUUID(id uuid.UUID) any {
	if id == uuid.Nil {
		return nil
	}
	return id
}
