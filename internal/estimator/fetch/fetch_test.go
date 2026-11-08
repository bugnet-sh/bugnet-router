package fetch_test

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/canon"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/estimatortest"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/facts"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/fetch"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/rules"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/source"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

const (
	sha1 = "1a2b3c4d5e6f1a2b3c4d5e6f1a2b3c4d5e6f1a2b"
	sha2 = "2b3c4d5e6f1a2b3c4d5e6f1a2b3c4d5e6f1a2b3c"
)

// stack is Postgres, Valkey and the settings that point at one fake
type stack struct {
	st     *store.Store
	vk     valkey.Client
	prefix string
	cfg    config.Estimator
	log    func(string)
}

func newStack(t *testing.T, fakeURL string) *stack {
	t.Helper()
	url := testenv.DatabaseURL(t)
	if err := store.Migrate(t.Context(), url); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := pgxpool.New(t.Context(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	vk, prefix := testenv.Valkey(t)
	cfg := config.DefaultEstimator()
	cfg.GithubAPI, cfg.GithubRaw, cfg.GithubGit = fakeURL, fakeURL, fakeURL
	cfg.HuggingFace = fakeURL
	return &stack{st: store.New(pool), vk: vk, prefix: prefix, cfg: cfg}
}

func (s *stack) github(t *testing.T, vk valkey.Client, token string) (*fetch.GitHub, *fetch.Cache) {
	t.Helper()
	if vk == nil {
		vk = s.vk
	}
	cache := fetch.NewCache(vk, s.prefix, s.st, s.cfg, "replica-"+token, nil)
	guard := fetch.NewGuard(vk, s.prefix, s.cfg.GithubReserve, nil)
	return fetch.NewGitHub(s.cfg, token, cache, guard, nil), cache
}

func repo(owner, name string, files map[string]string) *estimatortest.Repo {
	return &estimatortest.Repo{
		Owner: owner, Name: name, Default: "main",
		Branches: map[string]string{"main": sha1, "dev": sha2},
		Tags:     map[string]string{"v1": sha1},
		Commits:  map[string]map[string]string{sha1: files, sha2: files},
	}
}

var appFiles = map[string]string{
	"Dockerfile":                      "FROM node:20\n",
	".devcontainer/devcontainer.json": `{"hostRequirements": {"memory": "16gb", "cpus": 4}}`,
	".github/workflows/ci.yml": "on: push\njobs:\n  test:\n    runs-on: ubuntu-latest\n" +
		"    timeout-minutes: 15\n    steps:\n      - run: npm test\n",
	"README.md": "hello",
}

func parse(t *testing.T, s string) source.Source {
	t.Helper()
	src, err := source.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func read(t *testing.T, f fetch.Fetcher, s string) (facts.Facts, fetch.Resolved) {
	t.Helper()
	fv, res, err := f.Facts(t.Context(), parse(t, s))
	if err != nil {
		t.Fatal(err)
	}
	return fv, res
}

func TestGitHubReadsWithTokenInOneQueryAndOneTree(t *testing.T) {
	gh := estimatortest.NewGitHub(t, repo("acme", "app", appFiles),
		repo("other", "thing", appFiles))
	s := newStack(t, gh.URL())
	g, _ := s.github(t, nil, "tok")
	fv, res := read(t, g, "github.com/acme/app")
	eq(t, res.Unread, rules.UnreadReason(""))
	eq(t, res.Revision, sha1)
	if fv.Repo == nil || fv.Repo.Host == nil || fv.Repo.Host.MemoryMiB < 15000 {
		t.Fatalf("devcontainer host not read: %+v", fv.Repo)
	}
	eq(t, fv.Repo.CI != nil && fv.Repo.CI.Jobs == 1, true)
	eq(t, []int32{gh.Refs.Load(), gh.GraphQL.Load(), gh.Tree.Load(), gh.Raw.Load()},
		[]int32{1, 1, 1, 0})
	// A second call, even for a pushed ref path, costs nothing upstream
	read(t, g, "github.com/acme/app")
	eq(t, []int32{gh.Refs.Load(), gh.GraphQL.Load(), gh.Tree.Load()}, []int32{1, 1, 1})
	for _, a := range gh.Auths() {
		eq(t, a, "Bearer tok")
	}
}

func TestGraphQLQueryTextIsTheSameForEverySource(t *testing.T) {
	gh := estimatortest.NewGitHub(t, repo("acme", "app", appFiles),
		repo("other", "thing", appFiles))
	s := newStack(t, gh.URL())
	g, _ := s.github(t, nil, "tok")
	read(t, g, "github.com/acme/app")
	read(t, g, "github.com/other/thing/tree/dev/sub/dir")
	read(t, g, "github.com/other/thing/tree/v1")
	qs := gh.Queries()
	eq(t, len(qs), 3)
	for _, q := range qs {
		eq(t, q, g.Query())
		for _, bad := range []string{"acme", "thing", "dev", "sub", "v1", sha1} {
			if strings.Contains(q, bad) {
				t.Fatalf("the query text holds %q", bad)
			}
		}
	}
}

func TestAnonymousReadMatchesTheTokenRead(t *testing.T) {
	gh := estimatortest.NewGitHub(t, repo("acme", "app", appFiles))
	s := newStack(t, gh.URL())
	g, _ := s.github(t, nil, "")
	fv, res := read(t, g, "github.com/acme/app")
	eq(t, res.Revision, sha1)
	eq(t, gh.GraphQL.Load(), int32(0))
	eq(t, gh.Raw.Load() > 0 && gh.Raw.Load() <= int32(s.cfg.MaxFiles), true)
	eq(t, gh.Tree.Load(), int32(1))

	gh2 := estimatortest.NewGitHub(t, repo("acme", "app", appFiles))
	s2 := newStack(t, gh2.URL())
	g2, _ := s2.github(t, nil, "tok")
	fv2, _ := read(t, g2, "github.com/acme/app")
	a, _ := canon.Marshal(fv)
	b, _ := canon.Marshal(fv2)
	eq(t, string(a), string(b))
}

func TestNegativeCaching(t *testing.T) {
	gh := estimatortest.NewGitHub(t)
	s := newStack(t, gh.URL())
	g, _ := s.github(t, nil, "tok")
	_, res := read(t, g, "github.com/acme/missing")
	eq(t, res.Unread, rules.UnreadNotFound)
	refs := gh.Refs.Load()
	_, res = read(t, g, "github.com/acme/missing")
	eq(t, res.Unread, rules.UnreadNotFound)
	eq(t, gh.Refs.Load(), refs)
}

func TestPrivateRepoOnlyNegative(t *testing.T) {
	r := repo("acme", "secret", appFiles)
	r.Private = true
	gh := estimatortest.NewGitHub(t, r)
	s := newStack(t, gh.URL())
	g, _ := s.github(t, nil, "tok")
	fv, res := read(t, g, "github.com/acme/secret")
	eq(t, res.Unread, rules.UnreadNotFound)
	eq(t, fv, facts.Facts{})
	eq(t, gh.Tree.Load(), int32(0))
	if _, err := s.st.GetFacts(t.Context(), "github.com/acme/secret", sha1,
		facts.ExtractorVersion); err == nil {
		t.Fatal("facts of a private repository were stored")
	}
	key := s.prefix + "est:neg:github:github.com/acme/secret"
	n, err := s.vk.Do(t.Context(), s.vk.B().Get().Key(key).Build()).ToString()
	eq(t, err, nil)
	eq(t, n, "not_found")
}

func TestStartupTurnsOffAPrivateToken(t *testing.T) {
	gh := estimatortest.NewGitHub(t, repo("acme", "app", appFiles))
	gh.PrivateTokenRepos = true
	s := newStack(t, gh.URL())
	g, _ := s.github(t, nil, "tok")
	eq(t, g.Authenticated(), true)
	g.CheckToken(t.Context())
	eq(t, g.Authenticated(), false)
	read(t, g, "github.com/acme/app")
	eq(t, gh.GraphQL.Load(), int32(0))
	for _, a := range gh.Auths() {
		eq(t, a, "")
	}
}

func TestPushBetweenCalls(t *testing.T) {
	gh := estimatortest.NewGitHub(t, repo("acme", "app", appFiles))
	gh.Moved["Dockerfile"] = true
	s := newStack(t, gh.URL())
	g, _ := s.github(t, nil, "tok")
	fv, _ := read(t, g, "github.com/acme/app")
	eq(t, gh.Raw.Load(), int32(1))
	eq(t, fv.Repo.Dockerfile, true)
}

func TestRateHeadersByResource(t *testing.T) {
	gh := estimatortest.NewGitHub(t, repo("acme", "app", appFiles),
		repo("other", "thing", appFiles))
	gh.Limit, gh.Remaining = 100, 5
	s := newStack(t, gh.URL())
	g, _ := s.github(t, nil, "tok")
	_, res := read(t, g, "github.com/acme/app")
	eq(t, res.Unread, rules.UnreadReason(""))
	for _, key := range []string{"est:rl:gh:core", "est:rl:gh:graphql"} {
		m, err := s.vk.Do(t.Context(), s.vk.B().Hgetall().Key(s.prefix+key).Build()).AsStrMap()
		eq(t, err, nil)
		eq(t, m["remaining"], "5")
		eq(t, m["limit"], "100")
	}
	// Below the reserve share, a new source is not fetched until the reset
	before := gh.Tree.Load()
	_, res = read(t, g, "github.com/other/thing")
	eq(t, res.Unread, rules.UnreadRateLimited)
	eq(t, gh.Tree.Load(), before)
}

func TestBudgetAndPartial(t *testing.T) {
	gh := estimatortest.NewGitHub(t, repo("acme", "app", appFiles))
	gh.Delay = 150 * time.Millisecond
	s := newStack(t, gh.URL())
	g, cache := s.github(t, nil, "tok")
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	_, res, err := g.Facts(ctx, parse(t, "github.com/acme/app"))
	eq(t, err, nil)
	eq(t, res.Unread, rules.UnreadTimeout)
	// The fetch goes on in the background and fills the cache for the next quote
	deadline := time.Now().Add(10 * time.Second)
	for cache.Active() > 0 || gh.Tree.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the background fetch never finished")
		}
		time.Sleep(20 * time.Millisecond)
	}
	gh.Delay = 0
	before := gh.Tree.Load()
	_, res = read(t, g, "github.com/acme/app")
	eq(t, res.Unread, rules.UnreadReason(""))
	eq(t, gh.Tree.Load(), before)
}

func TestTwoReplicasOneFetch(t *testing.T) {
	gh := estimatortest.NewGitHub(t, repo("acme", "app", appFiles))
	gh.Delay = 100 * time.Millisecond
	s := newStack(t, gh.URL())
	a, _ := s.github(t, nil, "a")
	b, _ := s.github(t, nil, "b")
	done := make(chan fetch.Resolved, 2)
	for _, g := range []*fetch.GitHub{a, b} {
		go func() {
			_, res, _ := g.Facts(t.Context(), parse(t, "github.com/acme/app"))
			done <- res
		}()
	}
	for range 2 {
		res := <-done
		eq(t, res.Unread, rules.UnreadReason(""))
	}
	eq(t, gh.Tree.Load(), int32(1))
	eq(t, gh.GraphQL.Load(), int32(1))
}

func TestValkeyStoppedMidTest(t *testing.T) {
	gh := estimatortest.NewGitHub(t, repo("acme", "app", appFiles))
	s := newStack(t, gh.URL())
	flaky := &estimatortest.Flaky{Client: s.vk}
	g, _ := s.github(t, flaky, "tok")
	read(t, g, "github.com/acme/app")
	flaky.Down.Store(true)
	start := time.Now()
	// Postgres still holds the facts, and a missing repository is remembered in-process
	_, res := read(t, g, "github.com/acme/app")
	eq(t, res.Unread, rules.UnreadReason(""))
	eq(t, gh.Tree.Load(), int32(1))
	_, res = read(t, g, "github.com/acme/missing")
	eq(t, res.Unread, rules.UnreadNotFound)
	refs := gh.Refs.Load()
	_, res = read(t, g, "github.com/acme/missing")
	eq(t, res.Unread, rules.UnreadNotFound)
	eq(t, gh.Refs.Load(), refs)
	if time.Since(start) > 3*time.Second {
		t.Fatalf("a stopped Valkey slowed the reads to %s", time.Since(start))
	}
}

func TestSubdirectoryAndRefSplit(t *testing.T) {
	files := map[string]string{
		"pkg/api/Dockerfile": "FROM node:20\n",
		"README.md":          "x",
	}
	r := repo("acme", "mono", files)
	r.Branches["feature/x"] = sha2
	gh := estimatortest.NewGitHub(t, r)
	s := newStack(t, gh.URL())
	g, _ := s.github(t, nil, "tok")
	fv, res := read(t, g, "github.com/acme/mono/tree/feature/x/pkg/api")
	eq(t, res.Ref, "feature/x")
	eq(t, res.Subdir, []string{"pkg", "api"})
	eq(t, res.Revision, sha2)
	eq(t, fv.Repo.Dockerfile, true)
	// The same path again answers from the cache with the same split
	_, res2 := read(t, g, "github.com/acme/mono/tree/feature/x/pkg/api")
	eq(t, res2.Ref, "feature/x")
	eq(t, res2.Subdir, []string{"pkg", "api"})
	eq(t, gh.Refs.Load(), int32(1))
}

func TestLimitedAdmitStopsTheFetch(t *testing.T) {
	gh := estimatortest.NewGitHub(t, repo("acme", "app", appFiles))
	s := newStack(t, gh.URL())
	g, cache := s.github(t, nil, "tok")
	cache.Admit = func(context.Context) (time.Duration, bool) { return 7 * time.Second, false }
	_, _, err := g.Facts(t.Context(), parse(t, "github.com/acme/app"))
	var lim *fetch.LimitedError
	if !asLimited(err, &lim) || lim.Wait != 7*time.Second {
		t.Fatalf("got %v, want a LimitedError of 7s", err)
	}
	eq(t, gh.Tree.Load(), int32(0))
}
