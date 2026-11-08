package config

import (
	"cmp"
	"fmt"
	"math"
	"net/url"
	"slices"
	"time"
)

// Estimator holds the resource estimator's settings
type Estimator struct {
	GithubAPI, GithubRaw, GithubGit, HuggingFace string
	// FetchBudget is the time every upstream fetch of one estimate shares
	FetchBudget time.Duration
	// BackgroundBudget is how long a fetch may go on to fill the cache
	BackgroundBudget time.Duration
	// MaxBackground bounds the background fetches per replica
	MaxBackground int
	CacheTTL      time.Duration
	RefTTL        time.Duration
	NegativeTTL   time.Duration
	// MaxFiles bounds the signal files read through raw
	MaxFiles      int
	MaxFileBytes  int64
	MaxFactsBytes int64
	// GithubReserve is the share of the hourly limit below which no new fetch starts
	GithubReserve     float64
	GithubConcurrency int
	// MaxPerSecond and MaxBurst bound Estimate calls for the whole router
	MaxPerSecond int
	MaxBurst     int
	// NewSourcesPerMinute bounds quotes that must fetch a source nobody cached
	NewSourcesPerMinute int
	// StoredPerMinute bounds new estimate rows
	StoredPerMinute      int
	DefaultContextTokens int
	MemoryHeadroom       float64
	StatsMaxBytes        int64
	HistoryWindow        time.Duration
	// The history settings are read by the learning loop
	HistoryMinObservations    int
	HistoryMinPayers          int
	HistoryLowerObservations  int
	HistoryLowerPayers        int
	HistoryMayLower           bool
	HistoryOOMPayers          int
	HistoryPayerMinAge        time.Duration
	HistoryPayerMinPlacements int
	// MaxEstimateAge is how long ago a quote may have been served for CreatePlacement to use it
	MaxEstimateAge   time.Duration
	EstimatesKeep    time.Duration
	SourceFactsKeep  time.Duration
	ObservationsKeep time.Duration
}

// ImageTools is one provider's image manifest, a version, "none", or empty when not measured
type ImageTools struct {
	Git, Tar, Python3, Node, Docker string
}

type estimatorFile struct {
	GithubAPI                 string  `toml:"github_api"`
	GithubRaw                 string  `toml:"github_raw"`
	GithubGit                 string  `toml:"github_git"`
	HuggingFace               string  `toml:"huggingface"`
	FetchBudget               string  `toml:"fetch_budget"`
	BackgroundBudget          string  `toml:"background_budget"`
	MaxBackground             int64   `toml:"max_background"`
	CacheTTL                  string  `toml:"cache_ttl"`
	RefTTL                    string  `toml:"ref_ttl"`
	NegativeTTL               string  `toml:"negative_ttl"`
	MaxFiles                  int64   `toml:"max_files"`
	MaxFileBytes              string  `toml:"max_file_bytes"`
	MaxFactsBytes             string  `toml:"max_facts_bytes"`
	GithubReserve             float64 `toml:"github_reserve"`
	GithubConcurrency         int64   `toml:"github_concurrency"`
	MaxPerSecond              int64   `toml:"max_per_second"`
	MaxBurst                  int64   `toml:"max_burst"`
	NewSourcesPerMinute       int64   `toml:"new_sources_per_minute"`
	StoredPerMinute           int64   `toml:"stored_per_minute"`
	DefaultContextTokens      int64   `toml:"default_context_tokens"`
	MemoryHeadroom            float64 `toml:"memory_headroom"`
	StatsMaxBytes             string  `toml:"stats_max_bytes"`
	HistoryWindow             string  `toml:"history_window"`
	HistoryMinObservations    int64   `toml:"history_min_observations"`
	HistoryMinPayers          int64   `toml:"history_min_payers"`
	HistoryLowerObservations  int64   `toml:"history_lower_observations"`
	HistoryLowerPayers        int64   `toml:"history_lower_payers"`
	HistoryMayLower           bool    `toml:"history_may_lower"`
	HistoryOOMPayers          int64   `toml:"history_oom_payers"`
	HistoryPayerMinAge        string  `toml:"history_payer_min_age"`
	HistoryPayerMinPlacements int64   `toml:"history_payer_min_placements"`
	MaxEstimateAge            string  `toml:"max_estimate_age"`
	EstimatesKeep             string  `toml:"estimates_keep"`
	SourceFactsKeep           string  `toml:"source_facts_keep"`
	ObservationsKeep          string  `toml:"observations_keep"`
}

// imagesFile holds one table per provider
type imagesFile struct {
	E2B   imageFile `toml:"e2b"`
	Boat  imageFile `toml:"boat"`
	Boxd  imageFile `toml:"boxd"`
	Koyeb imageFile `toml:"koyeb"`
}

type imageFile struct {
	Tools imageToolsFile `toml:"tools"`
}

type imageToolsFile struct {
	Git     string `toml:"git"`
	Tar     string `toml:"tar"`
	Python3 string `toml:"python3"`
	Node    string `toml:"node"`
	Docker  string `toml:"docker"`
}

func defaultEstimator() estimatorFile {
	return estimatorFile{
		GithubAPI: "https://api.github.com", GithubRaw: "https://raw.githubusercontent.com",
		GithubGit: "https://github.com", HuggingFace: "https://huggingface.co",
		// three cold round trips to three hosts need more than 1.5s on a loaded machine
		FetchBudget: "5s", BackgroundBudget: "10s", MaxBackground: 8, CacheTTL: "1h",
		RefTTL: "60s", NegativeTTL: "5m", MaxFiles: 16, MaxFileBytes: "1MiB",
		MaxFactsBytes: "8KiB", GithubReserve: 0.1, GithubConcurrency: 4, MaxPerSecond: 5,
		MaxBurst: 20, NewSourcesPerMinute: 30, StoredPerMinute: 60, DefaultContextTokens: 8192,
		MemoryHeadroom: 0.1, StatsMaxBytes: "4KiB", HistoryWindow: "2160h",
		HistoryMinObservations: 5, HistoryMinPayers: 3, HistoryLowerObservations: 10,
		HistoryLowerPayers: 5, HistoryMayLower: true, HistoryOOMPayers: 2,
		HistoryPayerMinAge: "168h", HistoryPayerMinPlacements: 3, MaxEstimateAge: "1h",
		EstimatesKeep: "168h", SourceFactsKeep: "168h", ObservationsKeep: "2160h",
	}
}

// maxEstimatorCount keeps every count and rate far inside what the buckets hold
const maxEstimatorCount = 1_000_000

// read checks the estimator table against its bounds
func (f estimatorFile) read(d estimatorFile) (Estimator, error) {
	var e Estimator
	for _, u := range []struct {
		key, raw string
		into     *string
	}{
		{"github_api", f.GithubAPI, &e.GithubAPI}, {"github_raw", f.GithubRaw, &e.GithubRaw},
		{"github_git", f.GithubGit, &e.GithubGit}, {"huggingface", f.HuggingFace, &e.HuggingFace},
	} {
		v, err := upstreamURL("estimator."+u.key, u.raw)
		if err != nil {
			return Estimator{}, err
		}
		*u.into = v
	}
	for _, n := range []struct {
		key  string
		raw  int64
		into *int
	}{
		{"max_background", f.MaxBackground, &e.MaxBackground},
		{"max_files", f.MaxFiles, &e.MaxFiles},
		{"github_concurrency", f.GithubConcurrency, &e.GithubConcurrency},
		{"max_per_second", f.MaxPerSecond, &e.MaxPerSecond},
		{"max_burst", f.MaxBurst, &e.MaxBurst},
		{"new_sources_per_minute", f.NewSourcesPerMinute, &e.NewSourcesPerMinute},
		{"stored_per_minute", f.StoredPerMinute, &e.StoredPerMinute},
		{"default_context_tokens", f.DefaultContextTokens, &e.DefaultContextTokens},
		{"history_min_observations", f.HistoryMinObservations, &e.HistoryMinObservations},
		{"history_min_payers", f.HistoryMinPayers, &e.HistoryMinPayers},
		{"history_lower_observations", f.HistoryLowerObservations, &e.HistoryLowerObservations},
		{"history_lower_payers", f.HistoryLowerPayers, &e.HistoryLowerPayers},
		{"history_oom_payers", f.HistoryOOMPayers, &e.HistoryOOMPayers},
		{"history_payer_min_placements", f.HistoryPayerMinPlacements,
			&e.HistoryPayerMinPlacements},
	} {
		if n.raw < 1 || n.raw > maxEstimatorCount {
			return Estimator{}, fmt.Errorf("estimator.%s must be 1 to %d", n.key,
				maxEstimatorCount)
		}
		*n.into = int(n.raw)
	}
	for _, p := range []struct {
		key, raw, def string
		into          *time.Duration
		zeroOK        bool
	}{
		{"fetch_budget", f.FetchBudget, d.FetchBudget, &e.FetchBudget, false},
		{"background_budget", f.BackgroundBudget, d.BackgroundBudget, &e.BackgroundBudget, false},
		{"cache_ttl", f.CacheTTL, d.CacheTTL, &e.CacheTTL, false},
		{"ref_ttl", f.RefTTL, d.RefTTL, &e.RefTTL, false},
		{"negative_ttl", f.NegativeTTL, d.NegativeTTL, &e.NegativeTTL, false},
		{"history_window", f.HistoryWindow, d.HistoryWindow, &e.HistoryWindow, false},
		{"history_payer_min_age", f.HistoryPayerMinAge, d.HistoryPayerMinAge,
			&e.HistoryPayerMinAge, true},
		{"max_estimate_age", f.MaxEstimateAge, d.MaxEstimateAge, &e.MaxEstimateAge, false},
		{"estimates_keep", f.EstimatesKeep, d.EstimatesKeep, &e.EstimatesKeep, false},
		{"source_facts_keep", f.SourceFactsKeep, d.SourceFactsKeep, &e.SourceFactsKeep, false},
		{"observations_keep", f.ObservationsKeep, d.ObservationsKeep, &e.ObservationsKeep, false},
	} {
		v, err := time.ParseDuration(cmp.Or(p.raw, p.def))
		if err != nil || v < 0 || (v == 0 && !p.zeroOK) {
			return Estimator{}, fmt.Errorf("estimator.%s must be a positive Go duration such "+
				"as 1h", p.key)
		}
		*p.into = v
	}
	for _, b := range []struct {
		key, raw, def string
		into          *int64
	}{
		{"estimator.max_file_bytes", f.MaxFileBytes, d.MaxFileBytes, &e.MaxFileBytes},
		{"estimator.max_facts_bytes", f.MaxFactsBytes, d.MaxFactsBytes, &e.MaxFactsBytes},
		{"estimator.stats_max_bytes", f.StatsMaxBytes, d.StatsMaxBytes, &e.StatsMaxBytes},
	} {
		n, err := byteSize(b.key, cmp.Or(b.raw, b.def))
		if err != nil {
			return Estimator{}, err
		}
		*b.into = n
	}
	if !(f.GithubReserve >= 0 && f.GithubReserve < 1) {
		return Estimator{}, fmt.Errorf("estimator.github_reserve must be 0 up to 1")
	}
	if !(f.MemoryHeadroom >= 0 && f.MemoryHeadroom <= 1) || math.IsNaN(f.MemoryHeadroom) {
		return Estimator{}, fmt.Errorf("estimator.memory_headroom must be 0 to 1")
	}
	e.GithubReserve, e.MemoryHeadroom = f.GithubReserve, f.MemoryHeadroom
	e.HistoryMayLower = f.HistoryMayLower
	return e, nil
}

// upstreamURL checks a base URL, http or https with a host and nothing after it
func upstreamURL(key, raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" ||
		u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
		(u.Path != "" && u.Path != "/") {
		return "", fmt.Errorf("%s must be an http or https URL with a host and no path", key)
	}
	return u.Scheme + "://" + u.Host, nil
}

// images checks the image manifests and returns one per listed provider that has a table
func (f imagesFile) images(providers []string) map[string]ImageTools {
	out := map[string]ImageTools{}
	for _, p := range []struct {
		name string
		file imageFile
	}{{"boat", f.Boat}, {"boxd", f.Boxd}, {"e2b", f.E2B}, {"koyeb", f.Koyeb}} {
		if !slices.Contains(providers, p.name) {
			continue
		}
		t := p.file.Tools
		out[p.name] = ImageTools(t)
	}
	return out
}

// DefaultEstimator returns the estimator settings at their defaults
func DefaultEstimator() Estimator {
	e, _ := defaultEstimator().read(defaultEstimator())
	return e
}
