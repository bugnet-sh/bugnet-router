// Package config reads the router's settings from environment variables
package config

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// Config holds every router setting
type Config struct {
	DatabaseURL         string
	ValkeyAddr          string
	ValkeyPrefix        string
	ListenAddr          string
	WorkerListenAddr    string
	InternalToken       string
	HandleKey           []byte
	OrderTokenPublicKey ed25519.PublicKey
	EventSigningKey     ed25519.PrivateKey // nil when unset
	EventsURL           string
	PackagesFile        string
	Providers           []string
	MarginFloor         float64
	ReplicaID           string
	DeploymentID        string // labels this deployment's sandboxes so the sweep keeps to its own
	RunnerLinuxAMD64    string
	RunnerLinuxARM64    string
	ProviderKeys        map[string]string
	LocalEnabled        bool
	LocalRoot           string
	LocalRunnerPath     string // host build of bugnet-run the local provider runs
	LocalHourlyMicros   int64
	// JobOutputBudget is what one job's output may cost its Valkey stream, in bytes
	JobOutputBudget int64
	// PlacementOutputBudget is what a placement's job streams may cost together, in bytes
	PlacementOutputBudget int64
	// PlacementTailReserve is what final tails may cost a placement past its budget, in bytes
	PlacementTailReserve int64
	// MaxRunningJobs bounds the jobs one placement runs at once
	MaxRunningJobs int
}

const (
	minHandleKeyBytes = 32
	databaseURLEnv    = "BUGNET_DATABASE_URL"
)

// Defaults of the output limits, the jobs service's own
const (
	defaultJobOutputBudget       = "8MiB"
	defaultPlacementOutputBudget = "32MiB"
	defaultPlacementTailReserve  = "8MiB"
	defaultMaxRunningJobs        = "16"
)

// byteUnits are the suffixes a byte size may end in
var byteUnits = []struct {
	suffix string
	size   int64
}{{"KiB", 1 << 10}, {"MiB", 1 << 20}}

var providerKeyVars = []struct{ name, env string }{
	{"boxd", "BOXD_API_KEY"},
	{"e2b", "E2B_API_KEY"},
	{"koyeb", "KOYEB_API_TOKEN"},
	{"boat", "BOAT_API_KEY"},
}

var deploymentID = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)

var knownProviders = map[string]bool{
	"koyeb": true, "boat": true, "e2b": true, "boxd": true, "local": true,
}

// Load reads the configuration from the process environment
func Load() (Config, error) {
	return LoadFrom(os.LookupEnv)
}

// LoadFrom reads the configuration through a lookup function
func LoadFrom(getenv func(string) (string, bool)) (Config, error) {
	get := func(name, def string) string {
		if v, ok := getenv(name); ok && v != "" {
			return v
		}
		return def
	}
	var c Config
	var err error
	require := func(name string) string {
		v := get(name, "")
		if v == "" && err == nil {
			err = fmt.Errorf("config: %s is required", name)
		}
		return v
	}
	c.DatabaseURL = require(databaseURLEnv)
	c.InternalToken = require("BUGNET_INTERNAL_TOKEN")
	pubRaw := require("BUGNET_ORDER_TOKEN_PUBLIC_KEY")
	c.PackagesFile = require("BUGNET_PACKAGES_FILE")
	handleRaw := require("BUGNET_HANDLE_KEY")
	if err != nil {
		return Config{}, err
	}
	if len(c.InternalToken) < 32 {
		return Config{}, fmt.Errorf("config: BUGNET_INTERNAL_TOKEN must be at least 32 characters")
	}
	pub, err := decodeKey("BUGNET_ORDER_TOKEN_PUBLIC_KEY", pubRaw, ed25519.PublicKeySize)
	if err != nil {
		return Config{}, err
	}
	c.OrderTokenPublicKey = ed25519.PublicKey(pub)
	hk, derr := base64.StdEncoding.DecodeString(handleRaw)
	if derr != nil || len(hk) < minHandleKeyBytes {
		return Config{}, fmt.Errorf(
			"config: BUGNET_HANDLE_KEY must be base64 of at least %d bytes", minHandleKeyBytes)
	}
	c.HandleKey = hk
	if raw := get("BUGNET_EVENT_SIGNING_KEY", ""); raw != "" {
		seed, err := decodeKey("BUGNET_EVENT_SIGNING_KEY", raw, ed25519.SeedSize)
		if err != nil {
			return Config{}, err
		}
		c.EventSigningKey = ed25519.NewKeyFromSeed(seed)
	}
	c.ValkeyAddr = get("BUGNET_VALKEY_ADDR", "127.0.0.1:6379")
	c.ValkeyPrefix = get("BUGNET_VALKEY_PREFIX", "bn:")
	c.ListenAddr = get("BUGNET_LISTEN_ADDR", ":8080")
	c.WorkerListenAddr = get("BUGNET_WORKER_LISTEN_ADDR", ":8081")
	c.EventsURL = get("BUGNET_EVENTS_URL", "")
	c.RunnerLinuxAMD64 = get("BUGNET_RUNNER_LINUX_AMD64", "")
	c.RunnerLinuxARM64 = get("BUGNET_RUNNER_LINUX_ARM64", "")
	c.ReplicaID = get("BUGNET_REPLICA_ID", defaultReplicaID())
	c.DeploymentID = get("BUGNET_DEPLOYMENT_ID", "main")
	if !deploymentID.MatchString(c.DeploymentID) {
		return Config{}, fmt.Errorf(
			"config: BUGNET_DEPLOYMENT_ID must be 1 to 32 lowercase letters, digits or hyphens")
	}
	c.LocalEnabled = get("BUGNET_ENABLE_LOCAL_PROVIDER", "") == "1"
	c.LocalRoot = get("BUGNET_LOCAL_ROOT", filepath.Join(os.TempDir(), "bugnet-local"))
	c.LocalRunnerPath = get("BUGNET_LOCAL_RUNNER", "")
	c.LocalHourlyMicros, err = strconv.ParseInt(get("BUGNET_LOCAL_HOURLY_MICROS", "0"), 10, 64)
	if err != nil || c.LocalHourlyMicros < 0 {
		return Config{}, fmt.Errorf(
			"config: BUGNET_LOCAL_HOURLY_MICROS must be a non-negative whole number")
	}
	for _, b := range []struct {
		name, def string
		into      *int64
	}{
		{"BUGNET_JOB_OUTPUT_BUDGET", defaultJobOutputBudget, &c.JobOutputBudget},
		{"BUGNET_PLACEMENT_OUTPUT_BUDGET", defaultPlacementOutputBudget, &c.PlacementOutputBudget},
		{"BUGNET_PLACEMENT_TAIL_RESERVE", defaultPlacementTailReserve, &c.PlacementTailReserve},
	} {
		if *b.into, err = byteSize(b.name, get(b.name, b.def)); err != nil {
			return Config{}, err
		}
	}
	c.MaxRunningJobs, err = strconv.Atoi(get("BUGNET_MAX_RUNNING_JOBS", defaultMaxRunningJobs))
	if err != nil || c.MaxRunningJobs < 1 {
		return Config{}, fmt.Errorf(
			"config: BUGNET_MAX_RUNNING_JOBS must be a positive whole number")
	}
	c.MarginFloor, err = strconv.ParseFloat(get("BUGNET_MARGIN_FLOOR", "0.5"), 64)
	if err != nil || math.IsNaN(c.MarginFloor) || math.IsInf(c.MarginFloor, 0) ||
		c.MarginFloor < 0 || c.MarginFloor > 10 {
		return Config{}, fmt.Errorf(
			"config: BUGNET_MARGIN_FLOOR must be a finite number from 0 to 10")
	}
	seen := map[string]bool{}
	for _, name := range strings.Split(get("BUGNET_PROVIDERS", "koyeb,boat,e2b,boxd"), ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			return Config{}, fmt.Errorf("config: BUGNET_PROVIDERS has an empty entry")
		}
		if seen[name] {
			return Config{}, fmt.Errorf("config: BUGNET_PROVIDERS lists %q more than once", name)
		}
		seen[name] = true
		if !knownProviders[name] {
			return Config{}, fmt.Errorf("config: BUGNET_PROVIDERS has unknown provider %q", name)
		}
		if name == "local" && !c.LocalEnabled {
			return Config{}, fmt.Errorf(
				"config: BUGNET_PROVIDERS names local but BUGNET_ENABLE_LOCAL_PROVIDER is not 1")
		}
		if name == "local" && c.LocalRunnerPath == "" {
			return Config{}, fmt.Errorf(
				"config: BUGNET_PROVIDERS names local but BUGNET_LOCAL_RUNNER is not set")
		}
		c.Providers = append(c.Providers, name)
	}
	c.ProviderKeys = map[string]string{}
	for _, pk := range providerKeyVars {
		if v := get(pk.env, ""); v != "" {
			c.ProviderKeys[pk.name] = v
		}
	}
	return c, nil
}

// DatabaseURL reads only BUGNET_DATABASE_URL from the process environment
func DatabaseURL() (string, error) {
	return DatabaseURLFrom(os.LookupEnv)
}

// DatabaseURLFrom reads only BUGNET_DATABASE_URL through a lookup function
func DatabaseURLFrom(getenv func(string) (string, bool)) (string, error) {
	if v, ok := getenv(databaseURLEnv); ok && v != "" {
		return v, nil
	}
	return "", fmt.Errorf("config: %s is required", databaseURLEnv)
}

// byteSize reads a positive whole number of bytes, KiB or MiB, such as 65536, 64KiB or 8MiB
func byteSize(name, raw string) (int64, error) {
	digits, unit := raw, int64(1)
	for _, u := range byteUnits {
		if d, ok := strings.CutSuffix(raw, u.suffix); ok {
			digits, unit = d, u.size
			break
		}
	}
	n, err := strconv.ParseInt(digits, 10, 64)
	if err != nil || n < 1 || n > math.MaxInt64/unit {
		return 0, fmt.Errorf(
			"config: %s must be a positive whole number of bytes, optionally ending in KiB or MiB",
			name)
	}
	return n * unit, nil
}

func decodeKey(name, raw string, size int) ([]byte, error) {
	b, err := base64.StdEncoding.DecodeString(raw)
	if err != nil || len(b) != size {
		return nil, fmt.Errorf("config: %s must be base64 of %d bytes", name, size)
	}
	return b, nil
}

func defaultReplicaID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "router"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}
