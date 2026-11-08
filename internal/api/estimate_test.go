package api_test

import (
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/google/uuid"
	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/estimator"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/estimatortest"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/fetch"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

const estimateSHA = "1a2b3c4d5e6f1a2b3c4d5e6f1a2b3c4d5e6f1a2b"

func appRepo(owner, name string) *estimatortest.Repo {
	return &estimatortest.Repo{Owner: owner, Name: name, Default: "main",
		Branches: map[string]string{"main": estimateSHA},
		Commits: map[string]map[string]string{estimateSHA: {
			"Dockerfile":                      "FROM node:20\n",
			".devcontainer/devcontainer.json": `{"hostRequirements": {"memory": "1gb"}}`,
		}}}
}

// newEstimator builds an estimator over the fake GitHub and the API server's own Valkey
func newEstimator(
	t *testing.T, st *store.Store, vk valkey.Client, prefix string, cat *catalog.Catalog,
	opts options,
) (*estimator.Service, *estimatortest.GitHub) {
	t.Helper()
	gh := estimatortest.NewGitHub(t, opts.githubRepos...)
	cfg := config.DefaultEstimator()
	cfg.GithubAPI, cfg.GithubRaw, cfg.GithubGit = gh.URL(), gh.URL(), gh.URL()
	if opts.estimatorTune != nil {
		opts.estimatorTune(&cfg)
	}
	cache := fetch.NewCache(vk, prefix, st, cfg, "api-test", nil)
	guard := fetch.NewGuard(vk, prefix, cfg.GithubReserve, nil)
	svc, err := estimator.New(estimator.Deps{
		Store: st, Valkey: vk, Prefix: prefix, Config: cfg, Catalog: cat,
		GitHub: fetch.NewGitHub(cfg, "tok", cache, guard, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	cache.Admit = svc.AdmitNewSource
	if err := svc.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	return svc, gh
}

func acmeApp() []*estimatortest.Repo { return []*estimatortest.Repo{appRepo("acme", "app")} }

func (e *env) quote(t *testing.T, m *routerv1.EstimateRequest) (*routerv1.EstimateResponse, error) {
	t.Helper()
	resp, err := e.client.Estimate(t.Context(), connect.NewRequest(m))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func TestEstimateArguments(t *testing.T) {
	e := newEnv(t, options{estimator: true})
	minutes := func(n uint32) *routerv1.EstimateRequest {
		return &routerv1.EstimateRequest{ExpectedMinutes: &n}
	}
	for _, m := range []*routerv1.EstimateRequest{
		{EntryCommand: strings.Repeat("x", 1001)}, minutes(0), minutes(1441),
		{Needs: make([]routerv1.Need, 9)},
	} {
		_, err := e.quote(t, m)
		mustCode(t, err, connect.CodeInvalidArgument, "")
	}
	for _, s := range []string{"https://u:p@github.com/a/b", "!!"} {
		_, err := e.quote(t, &routerv1.EstimateRequest{Source: s})
		mustCode(t, err, connect.CodeInvalidArgument, "SOURCE_INVALID")
	}
	_, err := e.quote(t, &routerv1.EstimateRequest{
		EntryCommand: "GH=ghp_" + strings.Repeat("a", 36)})
	mustCode(t, err, connect.CodeInvalidArgument, "SECRET_IN_ARGUMENT")
	if strings.Contains(err.Error(), "ghp_") {
		t.Fatalf("the error echoes the secret: %v", err)
	}
	_, err = e.quote(t, &routerv1.EstimateRequest{
		EntryCommand: "pip install hf_transfer && pip install scikit-learn sk-learn-tools"})
	eq(t, err, nil)
}

func TestEstimateBuckets(t *testing.T) {
	e := newEnv(t, options{estimator: true, estimatorTune: func(c *config.Estimator) {
		c.MaxPerSecond = 1
	}})
	for range 20 {
		_, err := e.quote(t, &routerv1.EstimateRequest{})
		eq(t, err, nil)
	}
	_, err := e.quote(t, &routerv1.EstimateRequest{})
	mustCode(t, err, connect.CodeResourceExhausted, "RATE_LIMITED")
	wantRetry(t, err)
}

func TestEstimateAnswersTheFloorAndTheFits(t *testing.T) {
	e := newEnv(t, options{estimator: true, githubRepos: acmeApp()})
	got, err := e.quote(t, &routerv1.EstimateRequest{Source: "github.com/acme/app",
		Workload: routerv1.Workload_WORKLOAD_TESTS, Needs: []routerv1.Need{routerv1.Need_NEED_SSH}})
	eq(t, err, nil)
	eq(t, got.GetSource().GetKind(), routerv1.SourceKind_SOURCE_KIND_GITHUB)
	eq(t, got.GetSource().GetRead(), true)
	eq(t, got.GetSource().GetRevision(), estimateSHA)
	eq(t, got.GetEstimatorVersion() != "", true)
	eq(t, len(got.GetInputsSha256()), 64)
	if _, err := uuid.Parse(got.GetEstimateId()); err != nil {
		t.Fatalf("estimate_id %q: %v", got.GetEstimateId(), err)
	}
	eq(t, got.GetFloor().GetResources().GetMemoryMib() >= 1024, true)
	eq(t, got.GetRecommendedPackageId() != "", true)
	fits := 0
	for _, o := range got.GetOptions() {
		if o.GetFits() {
			fits++
			eq(t, o.GetTotalPriceMicros() >= o.GetPriceMicros(), true)
		}
	}
	eq(t, fits > 0, true)
	again, err := e.quote(t, &routerv1.EstimateRequest{Source: "github.com/acme/app",
		Workload: routerv1.Workload_WORKLOAD_TESTS, Needs: []routerv1.Need{routerv1.Need_NEED_SSH}})
	eq(t, err, nil)
	eq(t, again.GetEstimateId(), got.GetEstimateId())
}

func (e *env) sourceOf(t *testing.T) (kind, canonical, revision, label string, read bool) {
	t.Helper()
	var estimate *uuid.UUID
	err := e.pool.QueryRow(t.Context(), `
		select estimate_id, coalesce(source_kind, ''), coalesce(source_canonical, ''),
			coalesce(source_revision, ''), coalesce(source_label, ''), coalesce(source_read, false)
		from placements`).Scan(&estimate, &kind, &canonical, &revision, &label, &read)
	if err != nil {
		t.Fatal(err)
	}
	return
}

func TestCreatePlacementCopiesTheSource(t *testing.T) {
	e := newEnv(t, options{estimator: true, githubRepos: acmeApp()})
	q, err := e.quote(t, &routerv1.EstimateRequest{Source: "github.com/acme/app"})
	eq(t, err, nil)
	resp, err := e.client.CreatePlacement(t.Context(), connect.NewRequest(
		&routerv1.CreatePlacementRequest{
			OrderToken: e.startToken(t, smallPkg, smallPrice), EstimateId: q.GetEstimateId()}))
	eq(t, err, nil)
	e.waitReady(t, resp.Msg.GetHandle())
	kind, canonical, revision, label, read := e.sourceOf(t)
	eq(t, []any{kind, canonical, revision, label, read},
		[]any{"github", "github.com/acme/app", estimateSHA, "github.com/acme/app@1a2b3c4", true})
}

func TestURLSourceSetsNoLabel(t *testing.T) {
	e := newEnv(t, options{estimator: true})
	q, err := e.quote(t, &routerv1.EstimateRequest{Source: "https://example.com/x?sig=abc"})
	eq(t, err, nil)
	_, err = e.client.CreatePlacement(t.Context(), connect.NewRequest(
		&routerv1.CreatePlacementRequest{
			OrderToken: e.startToken(t, smallPkg, smallPrice), EstimateId: q.GetEstimateId()}))
	eq(t, err, nil)
	kind, canonical, _, label, read := e.sourceOf(t)
	eq(t, []any{kind, canonical, label, read}, []any{"url", "https://example.com", "", false})
}

func TestStaleOrUnknownEstimateIsIgnored(t *testing.T) {
	e := newEnv(t, options{estimator: true, githubRepos: acmeApp()})
	q, err := e.quote(t, &routerv1.EstimateRequest{Source: "github.com/acme/app"})
	eq(t, err, nil)
	_, err = e.pool.Exec(t.Context(), "update estimates set served_at = now() - interval '2 hours'")
	eq(t, err, nil)
	for _, id := range []string{q.GetEstimateId(), uuid.NewString(), "garbage"} {
		resp, err := e.client.CreatePlacement(t.Context(), connect.NewRequest(
			&routerv1.CreatePlacementRequest{
				OrderToken: e.startToken(t, smallPkg, smallPrice), EstimateId: id}))
		eq(t, err, nil)
		_ = resp
	}
	var n int
	err = e.pool.QueryRow(t.Context(),
		"select count(*) from placements where estimate_id is not null or source_kind is not null").
		Scan(&n)
	eq(t, err, nil)
	eq(t, n, 0)
}

func TestGetEstimateReplaysThroughTheOperatorService(t *testing.T) {
	e := newEnv(t, options{estimator: true, githubRepos: acmeApp()})
	q, err := e.quote(t, &routerv1.EstimateRequest{Source: "github.com/acme/app"})
	eq(t, err, nil)
	op := e.operator(t, operatorToken)
	got, err := op.GetEstimate(t.Context(), connect.NewRequest(
		&routerv1.GetEstimateRequest{EstimateId: q.GetEstimateId()}))
	eq(t, err, nil)
	replay, err := estimator.Replay(got.Msg.GetInputs(), got.Msg.GetCatalog(), got.Msg.GetOutput())
	eq(t, err, nil)
	eq(t, string(replay), string(got.Msg.GetOutput()))
	eq(t, got.Msg.GetInputsSha256(), q.GetInputsSha256())
	eq(t, time.Since(got.Msg.GetCreatedAt().AsTime()) < time.Minute, true)
	_, err = op.GetEstimate(t.Context(), connect.NewRequest(
		&routerv1.GetEstimateRequest{EstimateId: uuid.NewString()}))
	mustCode(t, err, connect.CodeNotFound, "ESTIMATE_UNKNOWN")
	_, err = op.GetEstimate(t.Context(), connect.NewRequest(
		&routerv1.GetEstimateRequest{EstimateId: "x"}))
	mustCode(t, err, connect.CodeInvalidArgument, "")
}
