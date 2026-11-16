package estimator_test

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/estimator"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/estimatortest"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/fetch"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/rules"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

const catalogJSON = `{"packages": [
	{"id": "small-30m", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 30, "price_micros": 120000},
	{"id": "big-30m", "resources": {"vcpu": 8, "memory_mib": 32768, "disk_gib": 40},
		"minutes": 30, "price_micros": 400000},
	{"id": "small-10m-more", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10, "price_micros": 45000, "extension_of": "small-30m"}
]}`

const sha1 = "1a2b3c4d5e6f1a2b3c4d5e6f1a2b3c4d5e6f1a2b"

func loadCatalog(t *testing.T, packages string) *catalog.Catalog {
	t.Helper()
	p := filepath.Join(t.TempDir(), "packages.json")
	if err := os.WriteFile(p, []byte(packages), 0o644); err != nil {
		t.Fatal(err)
	}
	c, err := catalog.Load(p)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

type rig struct {
	svc    *estimator.Service
	st     *store.Store
	pool   *pgxpool.Pool
	vk     valkey.Client
	prefix string
	cfg    config.Estimator
	gh     *estimatortest.GitHub
	hf     *estimatortest.HuggingFace
}

var appFiles = map[string]string{
	"Dockerfile":                      "FROM node:20\n",
	".devcontainer/devcontainer.json": `{"hostRequirements": {"memory": "6gb", "cpus": 2}}`,
}

func app(owner, name string) *estimatortest.Repo {
	return &estimatortest.Repo{Owner: owner, Name: name, Default: "main",
		Branches: map[string]string{"main": sha1},
		Commits:  map[string]map[string]string{sha1: appFiles}}
}

// newRig builds a service over Postgres, Valkey and the fakes, tune changing the settings
func newRig(t *testing.T, vk valkey.Client, tune func(*config.Estimator),
	repos ...*estimatortest.Repo) *rig {
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
	var prefix string
	if vk == nil {
		vk, prefix = testenv.Valkey(t)
	} else {
		prefix = "bn:test:" + uuid.NewString() + ":"
	}
	gh := estimatortest.NewGitHub(t, repos...)
	hf := estimatortest.NewHuggingFace(t, &estimatortest.Model{
		ID: "Qwen/Qwen2.5-7B-Instruct", SHA: sha1, Library: "transformers", Type: "qwen2",
		Params: map[string]int64{"BF16": 7615616512},
		Files:  map[string]int64{"model.safetensors": 15231233024},
		Config: `{"num_hidden_layers": 28, "num_key_value_heads": 4, "hidden_size": 3584, ` +
			`"num_attention_heads": 28, "max_position_embeddings": 32768}`})
	cfg := config.DefaultEstimator()
	cfg.GithubAPI, cfg.GithubRaw, cfg.GithubGit = gh.URL(), gh.URL(), gh.URL()
	cfg.HuggingFace = hf.URL()
	if tune != nil {
		tune(&cfg)
	}
	st := store.New(pool)
	cache := fetch.NewCache(vk, prefix, st, cfg, "r1", nil)
	guard := fetch.NewGuard(vk, prefix, cfg.GithubReserve, nil)
	svc, err := estimator.New(estimator.Deps{
		Store: st, Valkey: vk, Prefix: prefix, Config: cfg, Catalog: loadCatalog(t, catalogJSON),
		GitHub:      fetch.NewGitHub(cfg, "tok", cache, guard, nil),
		HuggingFace: fetch.NewHuggingFace(cfg, "", cache, guard, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	cache.Admit = svc.AdmitNewSource
	if err := svc.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	return &rig{svc: svc, st: st, pool: pool, vk: vk, prefix: prefix, cfg: cfg, gh: gh, hf: hf}
}

func (r *rig) count(t *testing.T, table string) int {
	t.Helper()
	var n int
	if err := r.pool.QueryRow(t.Context(), "select count(*) from "+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestRepeatedQuoteReusesItsRow(t *testing.T) {
	r := newRig(t, nil, nil, app("acme", "app"))
	req := estimator.Request{Source: "github.com/acme/app", Workload: rules.WorkloadTests,
		EntryCommand: "npm test -j 4"}
	a, err := r.svc.Estimate(t.Context(), req)
	eq(t, err, nil)
	if a.EstimateID == "" {
		t.Fatal("the quote was not stored")
	}
	b, err := r.svc.Estimate(t.Context(), req)
	eq(t, err, nil)
	eq(t, b.EstimateID, a.EstimateID)
	eq(t, b.InputsSHA256, a.InputsSHA256)
	eq(t, r.count(t, "estimates"), 1)
	eq(t, a.Source.Read, true)
	eq(t, a.Source.Revision, sha1)
	eq(t, a.Output.Floor.MemoryMiB >= 5000, true)
	// A different availability is a different answer
	req.Available = func(id string) bool { return id != "small-30m" }
	c, err := r.svc.Estimate(t.Context(), req)
	eq(t, err, nil)
	if c.EstimateID == a.EstimateID || c.EstimateID == "" {
		t.Fatalf("a new availability reused the row: %q", c.EstimateID)
	}
	eq(t, r.count(t, "estimates"), 2)
	eq(t, r.count(t, "catalogs"), 1)
}

func TestPastStoredBucketNoEstimateID(t *testing.T) {
	r := newRig(t, nil, func(c *config.Estimator) { c.StoredPerMinute = 2 })
	var ids []string
	for _, cmd := range []string{"a", "b", "c"} {
		a, err := r.svc.Estimate(t.Context(), estimator.Request{EntryCommand: cmd})
		eq(t, err, nil)
		ids = append(ids, a.EstimateID)
		eq(t, len(a.Output.Fits), 2)
	}
	eq(t, ids[0] != "" && ids[1] != "", true)
	eq(t, ids[2], "")
	eq(t, r.count(t, "estimates"), 2)
}

func TestEstimateBucketsInProcessWhenValkeyIsDown(t *testing.T) {
	base, _ := testenv.Valkey(t)
	flaky := &estimatortest.Flaky{Client: base}
	flaky.Down.Store(true)
	r := newRig(t, flaky, func(c *config.Estimator) { c.MaxPerSecond, c.MaxBurst = 1, 3 })
	for range 3 {
		_, err := r.svc.Estimate(t.Context(), estimator.Request{})
		eq(t, err, nil)
	}
	_, err := r.svc.Estimate(t.Context(), estimator.Request{})
	var lim *estimator.RateLimitedError
	if !errors.As(err, &lim) || lim.Wait <= 0 {
		t.Fatalf("got %v, want a rate limit with a wait", err)
	}
}

func TestNewSourcesBucket(t *testing.T) {
	r := newRig(t, nil, func(c *config.Estimator) { c.NewSourcesPerMinute = 2 },
		app("a", "one"), app("a", "two"), app("a", "three"))
	for _, s := range []string{"a/one", "a/two"} {
		_, err := r.svc.Estimate(t.Context(), estimator.Request{Source: "github.com/" + s})
		eq(t, err, nil)
	}
	// A source that is cached takes no token
	_, err := r.svc.Estimate(t.Context(), estimator.Request{Source: "github.com/a/one"})
	eq(t, err, nil)
	_, err = r.svc.Estimate(t.Context(), estimator.Request{Source: "github.com/a/three"})
	var lim *estimator.RateLimitedError
	if !errors.As(err, &lim) {
		t.Fatalf("got %v, want a rate limit", err)
	}
	eq(t, r.gh.Tree.Load(), int32(2))
}

func TestArgumentsAndSecrets(t *testing.T) {
	r := newRig(t, nil, nil)
	ctx := t.Context()
	long := strings.Repeat("x", 1001)
	for name, req := range map[string]estimator.Request{
		"command":   {EntryCommand: long},
		"minutes 0": {MinutesGiven: true},
		"minutes":   {MinutesGiven: true, ExpectedMinutes: 1441},
		"needs":     {Needs: make([]rules.Need, 9)},
	} {
		if _, err := r.svc.Estimate(ctx, req); !errors.Is(err, estimator.ErrInvalidArgument) {
			t.Errorf("%s: got %v", name, err)
		}
	}
	for _, s := range []string{"https://u:p@github.com/a/b", "not a source!"} {
		if _, err := r.svc.Estimate(ctx, estimator.Request{Source: s}); !errors.Is(err,
			estimator.ErrSourceInvalid) {
			t.Errorf("%q: got %v", s, err)
		}
	}
	key := "ghp_" + strings.Repeat("a", 36)
	for _, req := range []estimator.Request{{EntryCommand: "curl -H " + key},
		{Source: "github.com/a/" + key}} {
		if _, err := r.svc.Estimate(ctx, req); !errors.Is(err, estimator.ErrSecretInArgument) {
			t.Errorf("got %v", err)
		}
	}
	// Ordinary words that start like secret prefixes are quoted
	a, err := r.svc.Estimate(ctx,
		estimator.Request{EntryCommand: "pip install hf_transfer sk-learn"})
	eq(t, err, nil)
	if a.EstimateID == "" {
		t.Fatal("not stored")
	}
	if r.count(t, "estimates") != 1 {
		t.Fatal("rows")
	}
	var inputs string
	if err := r.pool.QueryRow(ctx, "select convert_from(inputs, 'UTF8') from estimates").
		Scan(&inputs); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(inputs, "hf_transfer") || strings.Contains(inputs, "pip install") {
		t.Fatalf("the stored inputs hold command text: %s", inputs)
	}
}

func TestURLAndNoneSources(t *testing.T) {
	r := newRig(t, nil, nil)
	a, err := r.svc.Estimate(t.Context(),
		estimator.Request{Source: "https://example.com/x?token=abc"})
	eq(t, err, nil)
	eq(t, a.Source.Kind, "url")
	eq(t, a.Source.Canonical, "https://example.com")
	eq(t, a.Source.Unread, rules.UnreadNotSupported)
	eq(t, a.Source.Read, false)
	b, err := r.svc.Estimate(t.Context(), estimator.Request{})
	eq(t, err, nil)
	eq(t, b.Source.Kind, "none")
	var inputs string
	if err := r.pool.QueryRow(t.Context(), "select string_agg(convert_from(inputs, 'UTF8'), '')"+
		" from estimates").Scan(&inputs); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(inputs, "token=abc") || strings.Contains(inputs, "/x") {
		t.Fatalf("the stored inputs hold a URL's path or query: %s", inputs)
	}
}

func TestBudgetEndsPartial(t *testing.T) {
	r := newRig(t, nil, func(c *config.Estimator) { c.FetchBudget = 80 * time.Millisecond },
		app("acme", "app"))
	r.gh.Delay = 300 * time.Millisecond
	a, err := r.svc.Estimate(t.Context(), estimator.Request{Source: "github.com/acme/app"})
	eq(t, err, nil)
	eq(t, a.Output.Partial, true)
	eq(t, a.Source.Unread, rules.UnreadTimeout)
	var unread bool
	for _, w := range a.Rec.Warnings {
		if w.Code == rules.WarningSourceUnread && strings.Contains(w.Message, "took too long") {
			unread = true
		}
	}
	eq(t, unread, true)
}

func TestSourceOfCopiesAndIgnoresStale(t *testing.T) {
	r := newRig(t, nil, nil, app("acme", "app"))
	a, err := r.svc.Estimate(t.Context(), estimator.Request{Source: "github.com/acme/app"})
	eq(t, err, nil)
	src, ok := r.svc.SourceOf(t.Context(), a.EstimateID)
	eq(t, ok, true)
	eq(t, src.Label, "github.com/acme/app@1a2b3c4")
	eq(t, src.Kind, "github")
	eq(t, src.Revision, sha1)
	eq(t, src.Read, true)
	if _, ok := r.svc.SourceOf(t.Context(), "not-a-uuid"); ok {
		t.Fatal("a malformed id was accepted")
	}
	if _, ok := r.svc.SourceOf(t.Context(), uuid.NewString()); ok {
		t.Fatal("an unknown id was accepted")
	}
	_, err = r.pool.Exec(t.Context(), "update estimates set served_at = now() - interval '2 hours'")
	eq(t, err, nil)
	if _, ok := r.svc.SourceOf(t.Context(), a.EstimateID); ok {
		t.Fatal("a stale estimate was accepted")
	}
	b, err := r.svc.Estimate(t.Context(), estimator.Request{Source: "https://example.com/x"})
	eq(t, err, nil)
	u, ok := r.svc.SourceOf(t.Context(), b.EstimateID)
	eq(t, ok, true)
	eq(t, u.Label, "")
	eq(t, u.Canonical, "https://example.com")
}

func TestHuggingFaceLabelIsTheModelID(t *testing.T) {
	r := newRig(t, nil, nil)
	a, err := r.svc.Estimate(t.Context(), estimator.Request{Source: "Qwen/Qwen2.5-7B-Instruct"})
	eq(t, err, nil)
	eq(t, a.Source.Unread, rules.UnreadReason(""))
	eq(t, a.Source.Read, true)
	eq(t, a.Output.Floor.DownloadBytes > 0, true)
	src, ok := r.svc.SourceOf(t.Context(), a.EstimateID)
	eq(t, ok, true)
	eq(t, src.Label, "Qwen/Qwen2.5-7B-Instruct")
}

func TestReplayIdenticalAfterCatalogChange(t *testing.T) {
	r := newRig(t, nil, nil, app("acme", "app"))
	a, err := r.svc.Estimate(t.Context(), estimator.Request{Source: "github.com/acme/app",
		Workload: rules.WorkloadTests})
	eq(t, err, nil)
	id := uuid.MustParse(a.EstimateID)
	st, err := r.svc.Get(t.Context(), id)
	eq(t, err, nil)
	got, err := estimator.Replay(st.Inputs, st.Catalog, st.Output)
	eq(t, err, nil)
	eq(t, string(got), string(st.Output))

	// A service started on a different catalog still serves the stored catalog bytes
	other, err := estimator.New(estimator.Deps{
		Store: r.st, Valkey: r.vk, Prefix: r.prefix, Config: r.cfg,
		Catalog: loadCatalog(t, `{"packages": [{"id": "only", "resources": {"vcpu": 1,
			"memory_mib": 512, "disk_gib": 5}, "minutes": 5, "price_micros": 1}]}`),
	})
	eq(t, err, nil)
	st2, err := other.Get(t.Context(), id)
	eq(t, err, nil)
	eq(t, string(st2.Catalog), string(st.Catalog))
	got2, err := estimator.Replay(st2.Inputs, st2.Catalog, st2.Output)
	eq(t, err, nil)
	eq(t, string(got2), string(st.Output))

	_, err = r.svc.Get(t.Context(), uuid.New())
	eq(t, errors.Is(err, estimator.ErrEstimateUnknown), true)
}

func TestShuffledAvailabilityGivesTheSameBytes(t *testing.T) {
	r := newRig(t, nil, nil)
	a, err := r.svc.Estimate(t.Context(),
		estimator.Request{Available: func(string) bool { return true }})
	eq(t, err, nil)
	b, err := r.svc.Estimate(t.Context(), estimator.Request{})
	eq(t, err, nil)
	eq(t, a.EstimateID, b.EstimateID)
}

// the handed prices are stored with the inputs, and a row without them reads the fixed prices
func TestReplayKeepsPrices(t *testing.T) {
	cat, err := json.Marshal(map[string]any{"packages": []catalog.Package{
		{ID: "a", Resources: provider.Resources{VCPU: 1, MemoryMiB: 1024, DiskGiB: 5}, Minutes: 10},
		{ID: "a-more", Resources: provider.Resources{VCPU: 1, MemoryMiB: 1024, DiskGiB: 5},
			Minutes: 10, ExtensionOf: "a", PriceMicros: 7, FixedPrice: true},
	}})
	eq(t, err, nil)
	out := []byte(`{"available": ["a"]}`)
	total := func(inputs string) int64 {
		got, err := estimator.Replay([]byte(inputs), cat, out)
		eq(t, err, nil)
		var o struct{ Fits []rules.Fit }
		eq(t, json.Unmarshal(got, &o), nil)
		return o.Fits[0].TotalPriceMicros
	}
	const req = `"request": {"workload": "serve", "minutes": 30}`
	eq(t, total(`{`+req+`, "prices": {"a": 3355, "a-more": 3300}}`), int64(9955))
	eq(t, total(`{`+req+`}`), int64(0+2*7))
}

// the request's prices price the fits and the stored inputs, so other prices are another row
func TestEstimateTakesHandedPrices(t *testing.T) {
	r := newRig(t, nil, nil)
	req := estimator.Request{Workload: rules.WorkloadScript, ExpectedMinutes: 50,
		MinutesGiven: true, Prices: map[string]int64{"small-30m": 9955, "small-10m-more": 3300}}
	a, err := r.svc.Estimate(t.Context(), req)
	eq(t, err, nil)
	totals := map[string]int64{}
	for _, f := range a.Output.Fits {
		totals[f.PackageID] = f.TotalPriceMicros
	}
	// small-30m takes two extensions, 9955 + 2 x 3300, and big-30m keeps its fixed price
	eq(t, totals, map[string]int64{"small-30m": 16555, "big-30m": 400000})
	st, err := r.svc.Get(t.Context(), uuid.MustParse(a.EstimateID))
	eq(t, err, nil)
	var in struct{ Prices map[string]int64 }
	eq(t, json.Unmarshal(st.Inputs, &in), nil)
	eq(t, in.Prices, req.Prices)
	again, err := r.svc.Estimate(t.Context(), req)
	eq(t, err, nil)
	eq(t, again.EstimateID, a.EstimateID)
	req.Prices = map[string]int64{"small-30m": 25031, "small-10m-more": 24420}
	dearer, err := r.svc.Estimate(t.Context(), req)
	eq(t, err, nil)
	if dearer.EstimateID == a.EstimateID || dearer.InputsSHA256 == a.InputsSHA256 {
		t.Fatalf("other prices reused the row %q", a.EstimateID)
	}
}
