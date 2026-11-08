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
