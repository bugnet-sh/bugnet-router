// Package estimator turns an agent's intent into a resource floor and a package recommendation
//
// The service reads a source's facts through its fetchers, runs the pure rules on them and stores
// every distinct answer with its inputs, so a quote can be replayed. Estimate is free, so it is
// bounded three ways before and after the fetch, by calls, by new sources and by stored rows.
package estimator

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/canon"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/facts"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/fetch"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/history"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/rules"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/source"
	"github.com/bugnet-sh/bugnet-router/internal/ratelimit"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// Argument bounds of Estimate, CP 4.2
const (
	maxCommandBytes = 1000
	maxMinutes      = 1440
	maxNeeds        = 8
)

// Deps are the service's collaborators
type Deps struct {
	Store   *store.Store
	Valkey  valkey.Client
	Prefix  string
	Config  config.Estimator
	Catalog *catalog.Catalog
	// Images holds the manifest of every configured provider
	Images []rules.Image
	// MaxLifetimeMinutes is the longest run any provider allows, 0 when unlimited
	MaxLifetimeMinutes int
	// GitHub and HuggingFace read sources, a nil one leaves its sources unread
	GitHub, HuggingFace fetch.Fetcher
	Log                 *slog.Logger
	// Now is time.Now unless a test sets it
	Now func() time.Time
}

// Service answers Estimate
type Service struct {
	d       Deps
	log     *slog.Logger
	now     func() time.Time
	buckets *buckets
	catalog catalogBytes
	history history.Settings
}

// New returns a Service, building the catalog bytes its estimates hash
func New(d Deps) (*Service, error) {
	s := &Service{d: d, log: d.Log, now: d.Now}
	if s.log == nil {
		s.log = slog.Default()
	}
	if s.now == nil {
		s.now = time.Now
	}
	s.buckets = newBuckets(d.Valkey, d.Prefix, s.now)
	cb, err := newCatalogBytes(d.Catalog, d.Images)
	if err != nil {
		return nil, err
	}
	s.catalog = cb
	s.history = history.SettingsOf(d.Config, d.Catalog.All())
	s.history.Now = s.now
	return s, nil
}

// Start stores the catalog the service quotes from when its hash is new
func (s *Service) Start(ctx context.Context) error {
	return s.d.Store.PutCatalog(ctx, s.catalog.sha[:], s.catalog.bytes)
}

// CatalogSHA256 is the hex hash of the catalog bytes the service quotes from
func (s *Service) CatalogSHA256() string { return hex.EncodeToString(s.catalog.sha[:]) }

// Request is the arguments of one Estimate, already read off the wire
type Request struct {
	Source          string
	Workload        rules.Workload
	EntryCommand    string
	ExpectedMinutes int // 0 when the agent gave none
	// MinutesGiven says expected_minutes was present, so 0 is out of range
	MinutesGiven bool
	Needs        []rules.Need
	// Available says whether a start package can be hosted now, all of them when nil
	Available func(packageID string) bool
}

// Answer is what Estimate returns
type Answer struct {
	// EstimateID is empty when the answer was not stored
	EstimateID   string
	Version      string
	InputsSHA256 string
	Source       rules.ResolvedSource
	Output       rules.Output
	Rec          rules.Recommendation
}

// AdmitNewSource takes a token of the new-sources bucket, as a fetch of an uncached source must
func (s *Service) AdmitNewSource(ctx context.Context) (time.Duration, bool) {
	ok, wait := s.buckets.take(ctx, s.bucket("rl:est:new", ratelimit.PerMinute(
		s.d.Config.NewSourcesPerMinute), s.d.Config.NewSourcesPerMinute))
	return wait, ok
}

func (s *Service) bucket(key string, perSecond float64, burst int) ratelimit.Bucket {
	return ratelimit.Bucket{Key: key, PerSecond: perSecond, Burst: burst}
}

func (s *Service) validate(r Request) error {
	switch {
	case len(r.EntryCommand) > maxCommandBytes:
		return invalid("entry_command is longer than %d bytes", maxCommandBytes)
	case r.MinutesGiven && (r.ExpectedMinutes < 1 || r.ExpectedMinutes > maxMinutes):
		return invalid("expected_minutes must be 1 to %d", maxMinutes)
	case len(r.Needs) > maxNeeds:
		return invalid("needs holds more than %d entries", maxNeeds)
	}
	return nil
}

// Estimate answers a quote, fetching the source within fetch_budget
func (s *Service) Estimate(ctx context.Context, req Request) (Answer, error) {
	if err := s.validate(req); err != nil {
		return Answer{}, err
	}
	if source.ContainsSecret(req.Source) || source.ContainsSecret(req.EntryCommand) {
		return Answer{}, ErrSecretInArgument
	}
	src, err := source.Parse(req.Source)
	if err != nil {
		return Answer{}, fmt.Errorf("%w: %w", ErrSourceInvalid, err)
	}
	cfg := s.d.Config
	if ok, wait := s.buckets.take(ctx, s.bucket("rl:est:all", float64(cfg.MaxPerSecond),
		cfg.MaxBurst)); !ok {
		return Answer{}, &RateLimitedError{Wait: wait}
	}
	fv, res, err := s.read(ctx, src)
	if err != nil {
		return Answer{}, err
	}
	hints := facts.Hints(req.EntryCommand, fv)
	rr := rules.Request{
		Source: res, Workload: req.Workload, Minutes: req.ExpectedMinutes,
		Needs: req.Needs, Hints: hints, DefaultContextTokens: cfg.DefaultContextTokens,
		Partial: res.Unread == rules.UnreadTimeout,
	}
	if req.EntryCommand != "" {
		rr.EntrySHA256 = canon.Sum([]byte(req.EntryCommand))
	}
	in := rules.Input{
		Request: rr, Facts: fv, Catalog: s.d.Catalog.All(), Images: s.d.Images,
		MaxLifetimeMinutes: s.d.MaxLifetimeMinutes, History: s.loadHistory(ctx, src, res, fv, req),
	}
	out := rules.Run(in)
	available := s.available(in.Catalog, req.Available)
	rec := rules.Recommend(in, out, func(id string) bool { return slices.Contains(available, id) })
	ans := Answer{Version: rules.Version, Source: res, Output: out, Rec: rec}
	st, err := s.persist(ctx, in, out, rec, available, src, fv)
	if err != nil {
		// The quote still answers, without an id, so its placement carries no source
		s.log.Warn("estimator: store the estimate", "err", err)
	}
	ans.EstimateID, ans.InputsSHA256 = st.id, st.inputsSHA
	return ans, nil
}

// available lists the start packages a provider can host now, sorted
func (s *Service) available(pkgs []catalog.Package, f func(string) bool) []string {
	out := []string{}
	for _, p := range pkgs {
		if p.ExtensionOf == "" && (f == nil || f(p.ID)) {
			out = append(out, p.ID)
		}
	}
	slices.Sort(out)
	return out
}

// read fetches the source's facts within the budget and resolves the source
func (s *Service) read(
	ctx context.Context, src source.Source,
) (facts.Facts, rules.ResolvedSource, error) {
	res := rules.ResolvedSource{Kind: string(src.Kind), Canonical: src.Canonical(nil)}
	var f fetch.Fetcher
	switch src.Kind {
	case source.KindNone:
		return facts.Facts{}, res, nil
	case source.KindURL:
		res.Unread = rules.UnreadNotSupported
		return facts.Facts{}, res, nil
	case source.KindGitHub:
		f = s.d.GitHub
	case source.KindHuggingFace:
		f = s.d.HuggingFace
		res.Ref = src.Revision
	}
	if f == nil {
		res.Unread = rules.UnreadUpstreamError
		return facts.Facts{}, res, nil
	}
	fctx, cancel := context.WithTimeout(ctx, s.d.Config.FetchBudget)
	defer cancel()
	fv, r, err := f.Facts(fctx, src)
	var lim *fetch.LimitedError
	switch {
	case errors.As(err, &lim):
		return facts.Facts{}, res, &RateLimitedError{Wait: lim.Wait}
	case err != nil && ctx.Err() != nil:
		return facts.Facts{}, res, ctx.Err()
	case err != nil:
		s.log.Warn("estimator: fetch a source", "err", err)
		res.Unread = rules.UnreadUpstreamError
		return facts.Facts{}, res, nil
	}
	res.Canonical = src.Canonical(r.Subdir)
	res.Ref, res.Subdir, res.Revision = r.Ref, r.Subdir, r.Revision
	if r.Unread != "" {
		res.Unread = r.Unread
		return facts.Facts{}, res, nil
	}
	res.Read = true
	return fv, res, nil
}

// SourceOf returns the source a placement copies from an estimate id, false when the id is not
// a live estimate, in which case it is ignored
func (s *Service) SourceOf(ctx context.Context, id string) (store.PlacementSource, bool) {
	u, err := uuid.Parse(id)
	if err != nil || u == uuid.Nil {
		s.log.Warn("estimator: an estimate id that is not a UUID was ignored")
		return store.PlacementSource{}, false
	}
	row, err := s.d.Store.LiveEstimate(ctx, u, s.d.Config.MaxEstimateAge)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			s.log.Warn("estimator: read an estimate", "err", err)
		} else {
			s.log.Warn("estimator: an unknown or stale estimate id was ignored")
		}
		return store.PlacementSource{}, false
	}
	var in storedInputs
	if err := decodeJSON(row.Inputs, &in); err != nil {
		s.log.Warn("estimator: decode an estimate", "err", err)
		return store.PlacementSource{}, false
	}
	rs := in.Request.Source
	ps := store.PlacementSource{
		EstimateID: u, Kind: rs.Kind, Canonical: rs.Canonical, Ref: rs.Ref,
		Revision: rs.Revision, Read: rs.Read, Unread: string(rs.Unread),
	}
	ps.Label = Label(rs)
	return ps, true
}

// Label is the source_label of a read GitHub or Hugging Face source, empty for any other
func Label(rs rules.ResolvedSource) string {
	if !rs.Read {
		return ""
	}
	switch rs.Kind {
	case string(source.KindGitHub):
		parts := strings.SplitN(rs.Canonical, "/", 4)
		if len(parts) < 3 || len(rs.Revision) < 7 {
			return ""
		}
		return strings.Join(parts[:3], "/") + "@" + rs.Revision[:7]
	case string(source.KindHuggingFace):
		return strings.TrimPrefix(rs.Canonical, "huggingface.co/")
	}
	return ""
}

// loadHistory reads what past runs of the source needed, nothing when the source has no facts to
// key it by or the read fails, since a quote still answers from its rules
func (s *Service) loadHistory(
	ctx context.Context, src source.Source, res rules.ResolvedSource, fv facts.Facts, req Request,
) rules.History {
	if fv.Repo == nil && fv.Model == nil {
		return rules.History{}
	}
	key := history.Key{Source: fetch.FactsKey(src, fetch.Resolved{Subdir: res.Subdir}),
		Revision: res.Revision}
	if fv.Repo != nil {
		key.Lock = fv.Repo.LockKey
	}
	h, err := history.Load(ctx, s.d.Store.Querier(), key, string(req.Workload), s.history)
	if err != nil {
		s.log.Warn("estimator: read the history", "err", err)
		return rules.History{}
	}
	return h
}
