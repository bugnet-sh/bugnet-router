// Package config reads the router's settings from a TOML file and its secrets from the environment.
//
// The settings file holds everything that is not a secret, and config/example.toml shows every
// key, each at its default but e2b.templates. It is decoded strictly, so an unknown key, a key in
// the wrong case, or a secret pasted into the file, is an error naming the key and its line.
// Relative paths in it resolve against the working directory. Secrets come only from the
// environment, and no error message ever carries one.
package config

import (
	"bytes"
	"cmp"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"
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
	OrderTokenPublicKey ed25519.PublicKey  // nil when the file has none
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
	Boxd           Boxd
	E2B            E2B
	Koyeb          Koyeb
	Boat           Boat
}

// Boxd holds the boxd adapter's settings
type Boxd struct {
	Region   string `toml:"region"`
	Endpoint string `toml:"endpoint"` // overrides the region's endpoint when set
}

// E2B holds the E2B adapter's settings
type E2B struct {
	Domain      string        // e2b.app when empty
	MaxLifetime time.Duration // the adapter's default when zero
	Templates   []E2BTemplate
}

// E2BTemplate is one template built on the E2B account, sold as the size Size
type E2BTemplate struct {
	Size      string `toml:"size"`
	Template  string `toml:"template"`
	VCPU      int    `toml:"vcpu"`
	MemoryMiB int    `toml:"memory_mib"`
	DiskGiB   int    `toml:"disk_gib"`
}

// Koyeb holds the Koyeb adapter's settings
type Koyeb struct {
	Region string `toml:"region"`
	// InstanceTypes maps each size sold to its instance type, every fra type when empty
	InstanceTypes map[string]string `toml:"instance_types"`
}

// Boat holds the Boat adapter's settings
type Boat struct {
	// Types maps each size sold to its machine type, every type when empty
	Types map[string]string `toml:"types"`
}

/* Settings file */

// file is the settings file's schema, each struct type naming one table
type file struct {
	OrderTokenPublicKey string     `toml:"order_token_public_key"`
	PackagesFile        string     `toml:"packages_file"`
	Providers           *[]string  `toml:"providers"`
	MarginFloor         float64    `toml:"margin_floor"`
	DeploymentID        string     `toml:"deployment_id"`
	ReplicaID           string     `toml:"replica_id"`
	EventsURL           string     `toml:"events_url"`
	API                 apiFile    `toml:"api"`
	Worker              workerFile `toml:"worker"`
	Valkey              valkeyFile `toml:"valkey"`
	Runner              runnerFile `toml:"runner"`
	Limits              limitsFile `toml:"limits"`
	Local               localFile  `toml:"local"`
	Boxd                Boxd       `toml:"boxd"`
	E2B                 e2bFile    `toml:"e2b"`
	Koyeb               Koyeb      `toml:"koyeb"`
	Boat                Boat       `toml:"boat"`
}

type apiFile struct {
	ListenAddr string `toml:"listen_addr"`
}

type workerFile struct {
	ListenAddr string `toml:"listen_addr"`
}

type valkeyFile struct {
	Addr   string `toml:"addr"`
	Prefix string `toml:"prefix"`
}

type runnerFile struct {
	LinuxAMD64 string `toml:"linux_amd64"`
	LinuxARM64 string `toml:"linux_arm64"`
}

type limitsFile struct {
	JobOutputBudget       string `toml:"job_output_budget"`
	PlacementOutputBudget string `toml:"placement_output_budget"`
	PlacementTailReserve  string `toml:"placement_tail_reserve"`
	MaxRunningJobs        int64  `toml:"max_running_jobs"`
}

type localFile struct {
	Enabled      bool   `toml:"enabled"`
	Root         string `toml:"root"`
	Runner       string `toml:"runner"`
	HourlyMicros int64  `toml:"hourly_micros"`
}

type e2bFile struct {
	Domain      string        `toml:"domain"`
	MaxLifetime string        `toml:"max_lifetime"`
	Templates   []E2BTemplate `toml:"templates"`
}

// defaults are the settings of an empty file, an empty string in the file also means its default
func defaults() file {
	return file{
		MarginFloor:  0.5,
		DeploymentID: "main",
		API:          apiFile{ListenAddr: ":8080"},
		Worker:       workerFile{ListenAddr: ":8081"},
		Valkey:       valkeyFile{Addr: "127.0.0.1:6379", Prefix: "bn:"},
		Limits: limitsFile{
			JobOutputBudget: "8MiB", PlacementOutputBudget: "32MiB",
			PlacementTailReserve: "8MiB", MaxRunningJobs: 16,
		},
		Local: localFile{Root: filepath.Join(os.TempDir(), "bugnet-local")},
		Boxd:  Boxd{Region: "fra"},
		Koyeb: Koyeb{Region: "fra"},
	}
}

var defaultProviders = []string{"koyeb", "boat", "e2b", "boxd"}

var knownProviders = map[string]bool{
	"koyeb": true, "boat": true, "e2b": true, "boxd": true, "local": true,
}

var deploymentID = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)

// LoadFile reads and checks the settings file at path, leaving every secret unset
func LoadFile(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	f, err := decode(raw)
	if err != nil {
		return Config{}, fmt.Errorf("config: %s: %w", path, err)
	}
	c, err := f.config()
	if err != nil {
		return Config{}, fmt.Errorf("config: %s: %w", path, err)
	}
	return c, nil
}

// decode checks the document's keys against the schema, then decodes it strictly over the defaults
func decode(raw []byte) (file, error) {
	doc, err := outline(raw)
	if err != nil {
		return file{}, err
	}
	if err := check(doc); err != nil {
		return file{}, err
	}
	f := defaults()
	dec := toml.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return file{}, errors.New(describe(err, doc))
	}
	return f, nil
}

func (f file) config() (Config, error) {
	d := defaults()
	c := Config{
		PackagesFile:      f.PackagesFile,
		MarginFloor:       f.MarginFloor,
		DeploymentID:      cmp.Or(f.DeploymentID, d.DeploymentID),
		ReplicaID:         cmp.Or(f.ReplicaID, defaultReplicaID()),
		EventsURL:         f.EventsURL,
		ListenAddr:        cmp.Or(f.API.ListenAddr, d.API.ListenAddr),
		WorkerListenAddr:  cmp.Or(f.Worker.ListenAddr, d.Worker.ListenAddr),
		ValkeyAddr:        cmp.Or(f.Valkey.Addr, d.Valkey.Addr),
		ValkeyPrefix:      cmp.Or(f.Valkey.Prefix, d.Valkey.Prefix),
		RunnerLinuxAMD64:  f.Runner.LinuxAMD64,
		RunnerLinuxARM64:  f.Runner.LinuxARM64,
		LocalEnabled:      f.Local.Enabled,
		LocalRoot:         cmp.Or(f.Local.Root, d.Local.Root),
		LocalRunnerPath:   f.Local.Runner,
		LocalHourlyMicros: f.Local.HourlyMicros,
		Boxd: Boxd{
			Region: cmp.Or(f.Boxd.Region, d.Boxd.Region), Endpoint: f.Boxd.Endpoint,
		},
		E2B: E2B{Domain: f.E2B.Domain, Templates: slices.Clone(f.E2B.Templates)},
		Koyeb: Koyeb{
			Region:        cmp.Or(f.Koyeb.Region, d.Koyeb.Region),
			InstanceTypes: maps.Clone(f.Koyeb.InstanceTypes),
		},
		Boat: Boat{Types: maps.Clone(f.Boat.Types)},
	}
	if f.OrderTokenPublicKey != "" {
		pub, err := decodeKey("order_token_public_key", f.OrderTokenPublicKey,
			ed25519.PublicKeySize)
		if err != nil {
			return Config{}, err
		}
		c.OrderTokenPublicKey = ed25519.PublicKey(pub)
	}
	if !deploymentID.MatchString(c.DeploymentID) {
		return Config{}, errors.New(
			"deployment_id must be 1 to 32 lowercase letters, digits or hyphens")
	}
	if math.IsNaN(c.MarginFloor) || math.IsInf(c.MarginFloor, 0) ||
		c.MarginFloor < 0 || c.MarginFloor > 10 {
		return Config{}, errors.New("margin_floor must be a finite number from 0 to 10")
	}
	if c.LocalHourlyMicros < 0 {
		return Config{}, errors.New("local.hourly_micros must be a non-negative whole number")
	}
	if f.Limits.MaxRunningJobs < 1 {
		return Config{}, errors.New("limits.max_running_jobs must be a positive whole number")
	}
	c.MaxRunningJobs = int(f.Limits.MaxRunningJobs)
	for _, b := range []struct {
		key, raw, def string
		into          *int64
	}{
		{"limits.job_output_budget", f.Limits.JobOutputBudget, d.Limits.JobOutputBudget,
			&c.JobOutputBudget},
		{"limits.placement_output_budget", f.Limits.PlacementOutputBudget,
			d.Limits.PlacementOutputBudget, &c.PlacementOutputBudget},
		{"limits.placement_tail_reserve", f.Limits.PlacementTailReserve,
			d.Limits.PlacementTailReserve, &c.PlacementTailReserve},
	} {
		n, err := byteSize(b.key, cmp.Or(b.raw, b.def))
		if err != nil {
			return Config{}, err
		}
		*b.into = n
	}
	if f.E2B.MaxLifetime != "" {
		lifetime, err := time.ParseDuration(f.E2B.MaxLifetime)
		if err != nil || lifetime <= 0 {
			return Config{}, errors.New(
				"e2b.max_lifetime must be a positive Go duration such as 24h")
		}
		c.E2B.MaxLifetime = lifetime
	}
	providers, err := providerList(f.Providers, c)
	if err != nil {
		return Config{}, err
	}
	c.Providers = providers
	for _, p := range []*string{
		&c.PackagesFile, &c.RunnerLinuxAMD64, &c.RunnerLinuxARM64, &c.LocalRoot,
		&c.LocalRunnerPath,
	} {
		if *p == "" {
			continue
		}
		abs, err := filepath.Abs(*p)
		if err != nil {
			return Config{}, fmt.Errorf("resolve %s: %w", *p, err)
		}
		*p = abs
	}
	return c, nil
}

// providerList checks the preference order, the default when the file names none
func providerList(given *[]string, c Config) ([]string, error) {
	names := defaultProviders
	if given != nil {
		if len(*given) == 0 {
			return nil, errors.New("providers must name at least one provider")
		}
		names = *given
	}
	seen := map[string]bool{}
	for _, name := range names {
		switch {
		case name == "":
			return nil, errors.New("providers has an empty entry")
		case seen[name]:
			return nil, fmt.Errorf("providers lists %q more than once", name)
		case !knownProviders[name]:
			return nil, fmt.Errorf("providers has unknown provider %q", name)
		case name == "local" && !c.LocalEnabled:
			return nil, errors.New("providers names local but local.enabled is not true")
		case name == "local" && c.LocalRunnerPath == "":
			return nil, errors.New("providers names local but local.runner is not set")
		}
		seen[name] = true
	}
	return slices.Clone(names), nil
}

/* Secrets */

const (
	minHandleKeyBytes = 32
	databaseURLEnv    = "BUGNET_DATABASE_URL"
)

var providerKeyVars = []struct{ name, env string }{
	{"boxd", "BOXD_API_KEY"},
	{"e2b", "E2B_API_KEY"},
	{"koyeb", "KOYEB_API_TOKEN"},
	{"boat", "BOAT_API_KEY"},
}

// Load reads the settings file at path and the secrets through getenv, as a server needs them
func Load(path string, getenv func(string) (string, bool)) (Config, error) {
	c, err := LoadFile(path)
	if err != nil {
		return Config{}, err
	}
	if c.OrderTokenPublicKey == nil {
		return Config{}, fmt.Errorf("config: %s: order_token_public_key is required", path)
	}
	if c.PackagesFile == "" {
		return Config{}, fmt.Errorf("config: %s: packages_file is required", path)
	}
	if err := c.readSecrets(getenv); err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	return c, nil
}

func (c *Config) readSecrets(getenv func(string) (string, bool)) error {
	get := func(name string) string {
		v, _ := getenv(name)
		return v
	}
	for _, name := range []string{databaseURLEnv, "BUGNET_INTERNAL_TOKEN", "BUGNET_HANDLE_KEY"} {
		if get(name) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	c.DatabaseURL = get(databaseURLEnv)
	c.InternalToken = get("BUGNET_INTERNAL_TOKEN")
	if len(c.InternalToken) < 32 {
		return errors.New("BUGNET_INTERNAL_TOKEN must be at least 32 characters")
	}
	hk, err := base64.StdEncoding.DecodeString(get("BUGNET_HANDLE_KEY"))
	if err != nil || len(hk) < minHandleKeyBytes {
		return fmt.Errorf("BUGNET_HANDLE_KEY must be base64 of at least %d bytes",
			minHandleKeyBytes)
	}
	c.HandleKey = hk
	if raw := get("BUGNET_EVENT_SIGNING_KEY"); raw != "" {
		seed, err := decodeKey("BUGNET_EVENT_SIGNING_KEY", raw, ed25519.SeedSize)
		if err != nil {
			return err
		}
		c.EventSigningKey = ed25519.NewKeyFromSeed(seed)
	}
	c.ProviderKeys = map[string]string{}
	for _, pk := range providerKeyVars {
		if v := get(pk.env); v != "" {
			c.ProviderKeys[pk.name] = v
		}
	}
	return nil
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

/* Values */

// byteUnits are the suffixes a byte size may end in
var byteUnits = []struct {
	suffix string
	size   int64
}{{"KiB", 1 << 10}, {"MiB", 1 << 20}}

// maxByteSize caps byte settings at 1 TiB so derived allowances never overflow
const maxByteSize = 1 << 40

// byteSize reads a positive whole number of bytes, KiB or MiB, such as 65536, 64KiB or 8MiB
func byteSize(key, raw string) (int64, error) {
	digits, unit := raw, int64(1)
	for _, u := range byteUnits {
		if d, ok := strings.CutSuffix(raw, u.suffix); ok {
			digits, unit = d, u.size
			break
		}
	}
	n, err := strconv.ParseInt(digits, 10, 64)
	if err != nil || n < 1 || n > maxByteSize/unit {
		return 0, fmt.Errorf(
			"%s must be 1 byte to 1 TiB, a whole number optionally ending in KiB or MiB", key)
	}
	return n * unit, nil
}

func decodeKey(name, raw string, size int) ([]byte, error) {
	b, err := base64.StdEncoding.DecodeString(raw)
	if err != nil || len(b) != size {
		return nil, fmt.Errorf("%s must be base64 of %d bytes", name, size)
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
