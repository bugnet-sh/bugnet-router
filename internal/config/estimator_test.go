package config

import (
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestEstimatorDefaults(t *testing.T) {
	c, err := LoadFile(writeFile(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	e := c.Estimator
	eq(t, e, DefaultEstimator())
	eq(t, []any{e.GithubAPI, e.GithubRaw, e.GithubGit, e.HuggingFace}, []any{
		"https://api.github.com", "https://raw.githubusercontent.com", "https://github.com",
		"https://huggingface.co"})
	eq(t, []time.Duration{e.FetchBudget, e.BackgroundBudget, e.CacheTTL, e.RefTTL, e.NegativeTTL,
		e.HistoryWindow, e.MaxEstimateAge},
		[]time.Duration{5 * time.Second, 10 * time.Second, time.Hour, time.Minute,
			5 * time.Minute, 2160 * time.Hour, time.Hour})
	eq(t, []int{e.MaxBackground, e.MaxFiles, e.GithubConcurrency, e.MaxPerSecond, e.MaxBurst,
		e.NewSourcesPerMinute, e.StoredPerMinute, e.DefaultContextTokens},
		[]int{8, 16, 4, 5, 20, 30, 60, 8192})
	eq(t, []int64{e.MaxFileBytes, e.MaxFactsBytes, e.StatsMaxBytes},
		[]int64{1 << 20, 8 << 10, 4 << 10})
	eq(t, e.GithubReserve, 0.1)
	eq(t, e.HistoryMayLower, true)
	eq(t, len(c.ImageTools), 4)
	eq(t, c.ImageTools["e2b"], ImageTools{})
}

func TestExampleTomlHoldsEstimatorDefaults(t *testing.T) {
	c := loadExample(t)
	eq(t, c.Estimator, DefaultEstimator())
	doc, err := outline([]byte(committed(t, "example.toml")))
	if err != nil {
		t.Fatal(err)
	}
	var written, images []string
	for _, s := range doc {
		if len(s.path) == 2 && s.path[0] == "estimator" {
			written = append(written, s.path[1])
		}
		if len(s.path) == 3 && s.path[0] == "images" && len(s.path[1]) > 2 && s.path[2] == "tools" {
			images = append(images, s.path[1])
		}
	}
	var keys []string
	schema := reflect.TypeFor[estimatorFile]()
	for i := range schema.NumField() {
		keys = append(keys, schema.Field(i).Tag.Get("toml"))
	}
	slices.Sort(written)
	slices.Sort(keys)
	slices.Sort(images)
	eq(t, written, keys)
	eq(t, images, []string{"boat", "boxd", "e2b", "koyeb"})
}

func TestEstimatorSettings(t *testing.T) {
	c, err := LoadFile(writeFile(t, `providers = ["e2b", "boxd"]

[estimator]
github_api = "http://127.0.0.1:9000"
fetch_budget = "2s"
max_per_second = 7
github_reserve = 0.25
max_facts_bytes = "16KiB"
history_payer_min_age = "0s"
history_may_lower = false

[images.e2b]
tools = { git = "2.43.0", tar = "1.35", python3 = "none", node = "", docker = "" }

[images.boat]
tools = { git = "9" }
`))
	if err != nil {
		t.Fatal(err)
	}
	e := c.Estimator
	eq(t, []any{e.GithubAPI, e.FetchBudget, e.MaxPerSecond, e.GithubReserve, e.MaxFactsBytes,
		e.HistoryPayerMinAge, e.HistoryMayLower},
		[]any{"http://127.0.0.1:9000", 2 * time.Second, 7, 0.25, int64(16 << 10),
			time.Duration(0), false})
	// Only the listed providers keep a manifest
	eq(t, c.ImageTools, map[string]ImageTools{
		"e2b":  {Git: "2.43.0", Tar: "1.35", Python3: "none"},
		"boxd": {},
	})
}

func TestEstimatorBounds(t *testing.T) {
	cases := []struct{ toml, want string }{
		{"[estimator]\nmax_per_second = 0", "estimator.max_per_second must be 1 to 1000000"},
		{"[estimator]\nmax_files = -1", "estimator.max_files must be 1 to 1000000"},
		{"[estimator]\nfetch_budget = \"0s\"",
			"estimator.fetch_budget must be a positive Go duration such as 1h"},
		{"[estimator]\nnegative_ttl = \"5 minutes\"",
			"estimator.negative_ttl must be a positive Go duration such as 1h"},
		{"[estimator]\ngithub_reserve = 1.5", "estimator.github_reserve must be 0 up to 1"},
		{"[estimator]\nmemory_headroom = -0.1", "estimator.memory_headroom must be 0 to 1"},
		{"[estimator]\nmax_file_bytes = \"0\"", "estimator.max_file_bytes must be 1 byte"},
		{"[estimator]\ngithub_api = \"ftp://example.com\"",
			"estimator.github_api must be an http or https URL with a host and no path"},
		{"[estimator]\nhuggingface = \"https://u:p@example.com\"",
			"estimator.huggingface must be an http or https URL with a host and no path"},
		{"[estimator]\ngithub_raw = \"https://example.com/x\"",
			"estimator.github_raw must be an http or https URL with a host and no path"},
		{"[estimator]\ntoken = \"x\"", "line 4: estimator.token is not a setting"},
		{"[images.e2b]\ntools = { rust = \"1\" }", "images.e2b.tools"},
	}
	for _, c := range cases {
		msg := problem(t, c.toml+"\n")
		if !strings.Contains(msg, c.want) {
			t.Errorf("%s gave %q, want %q", c.toml, msg, c.want)
		}
		if strings.Contains(msg, "u:p") {
			t.Errorf("%s: the error quotes the URL's userinfo: %s", c.toml, msg)
		}
	}
}

func TestSourceTokensComeFromTheEnvironment(t *testing.T) {
	path := writeFile(t, serving(t))
	c, err := Load(path, env(map[string]string{
		"BUGNET_GITHUB_TOKEN": "ghp_x", "BUGNET_HF_TOKEN": "hf_x"}))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, []string{c.GithubToken, c.HFToken}, []string{"ghp_x", "hf_x"})
	none, err := Load(path, env(nil))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, none.GithubToken+none.HFToken, "")
}
