package estimator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/canon"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/facts"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/fetch"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/rules"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/source"
	"github.com/bugnet-sh/bugnet-router/internal/ratelimit"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// catalogBytes is the canonical catalog the estimates of one router start hash
type catalogBytes struct {
	bytes []byte
	sha   [sha256.Size]byte
}

// catalogDoc is the catalog as stored, packages_file and the image manifests
type catalogDoc struct {
	Packages []catalog.Package `json:"packages"`
	Images   []rules.Image     `json:"images"`
}

func newCatalogBytes(c *catalog.Catalog, images []rules.Image) (catalogBytes, error) {
	imgs := slices.Clone(images)
	slices.SortFunc(imgs, func(a, b rules.Image) int {
		return bytes.Compare([]byte(a.Provider), []byte(b.Provider))
	})
	if imgs == nil {
		imgs = []rules.Image{}
	}
	b, err := canon.Marshal(catalogDoc{Packages: c.All(), Images: imgs})
	if err != nil {
		return catalogBytes{}, fmt.Errorf("estimator: catalog bytes: %w", err)
	}
	return catalogBytes{bytes: b, sha: sha256.Sum256(b)}, nil
}

// factsRef names the source_facts row an estimate read, so facts are stored once
type factsRef struct {
	SourceKey        string `json:"source_key"`
	Revision         string `json:"revision"`
	ExtractorVersion string `json:"extractor_version"`
	FactsSHA256      string `json:"facts_sha256"`
}

// storedInputs is CP 4.6's canonical inputs
type storedInputs struct {
	V                  string        `json:"v"`
	Request            rules.Request `json:"request"`
	FactsRef           *factsRef     `json:"facts_ref"`
	History            rules.History `json:"history"`
	CatalogSHA256      string        `json:"catalog_sha256"`
	MaxLifetimeMinutes int           `json:"max_lifetime_minutes"`
	// Prices is absent in a row stored before prices came with the request
	Prices map[string]int64 `json:"prices,omitempty"`
}

// filledInputs is storedInputs with the facts it names filled in, as GetEstimate returns it
type filledInputs struct {
	storedInputs
	Facts *facts.Facts `json:"facts,omitempty"`
}

// storedOutput is the answer as stored, the availability it was recommended under included
type storedOutput struct {
	rules.Output
	Recommendation rules.Recommendation `json:"recommendation"`
	Available      []string             `json:"available"`
}

func decodeJSON(b []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	return dec.Decode(v)
}

type stored struct{ id, inputsSHA string }

// persist stores the answer, reusing the row of an identical quote, and keeps the facts it names
func (s *Service) persist(
	ctx context.Context, in rules.Input, out rules.Output, rec rules.Recommendation,
	available []string, src source.Source, fv facts.Facts,
) (stored, error) {
	si := storedInputs{
		V: rules.Version, Request: in.Request, History: in.History,
		CatalogSHA256: s.CatalogSHA256(), MaxLifetimeMinutes: in.MaxLifetimeMinutes,
		Prices: in.Prices,
	}
	var factsRow *store.FactsRow
	if fv.Repo != nil || fv.Model != nil {
		fb, err := canon.Marshal(fv)
		if err != nil {
			return stored{}, err
		}
		sum := sha256.Sum256(fb)
		key := fetch.FactsKey(src, fetch.Resolved{Subdir: in.Request.Source.Subdir})
		factsRow = &store.FactsRow{SourceKey: key, Revision: in.Request.Source.Revision,
			ExtractorVersion: facts.ExtractorVersion, Facts: fb, FactsSHA256: sum[:]}
		si.FactsRef = &factsRef{SourceKey: key, Revision: factsRow.Revision,
			ExtractorVersion: factsRow.ExtractorVersion, FactsSHA256: hex.EncodeToString(sum[:])}
	}
	ib, err := canon.Marshal(si)
	if err != nil {
		return stored{}, err
	}
	isum := sha256.Sum256(ib)
	got := stored{inputsSHA: hex.EncodeToString(isum[:])}
	ab, err := canon.Marshal(available)
	if err != nil {
		return got, err
	}
	st := s.d.Store
	id, reused, err := st.ReuseEstimate(ctx, isum[:], ab, s.d.Config.MaxEstimateAge)
	if err != nil {
		return got, err
	}
	if reused {
		got.id = id.String()
		return got, nil
	}
	if !s.storedToken(ctx) {
		return got, nil
	}
	ob, err := canon.Marshal(storedOutput{Output: out, Recommendation: rec, Available: available})
	if err != nil {
		return got, err
	}
	if factsRow != nil {
		if err := st.PutFacts(ctx, *factsRow); err != nil {
			return got, err
		}
	}
	row := store.EstimateRow{
		ID: uuid.New(), EstimatorVersion: rules.Version, InputsSHA256: isum[:], Inputs: ib,
		Output: ob, Availability: ab, CatalogSHA256: s.catalog.sha[:],
		SourceKind: in.Request.Source.Kind, Revision: in.Request.Source.Revision,
		Workload: string(in.Request.Workload), SourceRead: in.Request.Source.Read,
	}
	if factsRow != nil {
		row.SourceKey = factsRow.SourceKey
		if in.Facts.Repo != nil {
			row.LockKey = in.Facts.Repo.LockKey
		}
	}
	if err := st.InsertEstimate(ctx, row); err != nil {
		return got, err
	}
	got.id = row.ID.String()
	return got, nil
}

// storedToken takes a token of the stored-rows bucket
func (s *Service) storedToken(ctx context.Context) bool {
	cfg := s.d.Config
	ok, _ := s.buckets.take(ctx, ratelimit.Bucket{Key: "rl:est:stored",
		PerSecond: ratelimit.PerMinute(cfg.StoredPerMinute), Burst: cfg.StoredPerMinute})
	return ok
}

// Stored is an estimate as GetEstimate returns it
type Stored struct {
	ID               uuid.UUID
	EstimatorVersion string
	// Inputs are RFC 8785 bytes with the facts filled in
	Inputs       []byte
	Output       []byte
	InputsSHA256 string
	CreatedAt    time.Time
	Catalog      []byte
}

// Get returns a stored estimate with its facts and catalog, ErrEstimateUnknown when none
func (s *Service) Get(ctx context.Context, id uuid.UUID) (Stored, error) {
	row, err := s.d.Store.EstimateByID(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return Stored{}, ErrEstimateUnknown
	}
	if err != nil {
		return Stored{}, err
	}
	var si storedInputs
	if err := decodeJSON(row.Inputs, &si); err != nil {
		return Stored{}, fmt.Errorf("estimator: decode inputs: %w", err)
	}
	fi := filledInputs{storedInputs: si}
	if r := si.FactsRef; r != nil {
		fr, err := s.d.Store.GetFacts(ctx, r.SourceKey, r.Revision, r.ExtractorVersion)
		if err != nil {
			return Stored{}, fmt.Errorf("estimator: read the facts of an estimate: %w", err)
		}
		var f facts.Facts
		if err := decodeJSON(fr.Facts, &f); err != nil {
			return Stored{}, fmt.Errorf("estimator: decode facts: %w", err)
		}
		fi.Facts = &f
	}
	ib, err := canon.Marshal(fi)
	if err != nil {
		return Stored{}, err
	}
	cat, err := s.d.Store.CatalogBytes(ctx, row.CatalogSHA256)
	if err != nil {
		return Stored{}, fmt.Errorf("estimator: read the catalog of an estimate: %w", err)
	}
	return Stored{
		ID: row.ID, EstimatorVersion: row.EstimatorVersion, Inputs: ib, Output: row.Output,
		InputsSHA256: hex.EncodeToString(row.InputsSHA256), CreatedAt: row.CreatedAt, Catalog: cat,
	}, nil
}

// Replay runs the rules on stored inputs and catalog bytes, and returns the output bytes the
// run gives, which equal the stored output when nothing but the rule code changed
func Replay(inputs, catalogJSON, output []byte) ([]byte, error) {
	var fi filledInputs
	if err := decodeJSON(inputs, &fi); err != nil {
		return nil, fmt.Errorf("estimator: decode inputs: %w", err)
	}
	var cd catalogDoc
	if err := decodeJSON(catalogJSON, &cd); err != nil {
		return nil, fmt.Errorf("estimator: decode catalog: %w", err)
	}
	var so storedOutput
	if err := decodeJSON(output, &so); err != nil {
		return nil, fmt.Errorf("estimator: decode output: %w", err)
	}
	in := rules.Input{
		Request: fi.Request, History: fi.History, Catalog: cd.Packages, Images: cd.Images,
		MaxLifetimeMinutes: fi.MaxLifetimeMinutes, Prices: fi.Prices,
	}
	if fi.Facts != nil {
		in.Facts = *fi.Facts
	}
	out := rules.Run(in)
	rec := rules.Recommend(in, out,
		func(id string) bool { return slices.Contains(so.Available, id) })
	return canon.Marshal(storedOutput{Output: out, Recommendation: rec, Available: so.Available})
}
