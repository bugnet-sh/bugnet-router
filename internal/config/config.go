// Package config reads the router's settings from environment variables
package config

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Config holds every router setting
type Config struct {
	DatabaseURL         string
	ValkeyAddr          string
	ValkeyPrefix        string
	ListenAddr          string
	InternalToken       string
	OrderTokenPublicKey ed25519.PublicKey
	EventSigningKey     ed25519.PrivateKey // nil when unset
	EventsURL           string
	PackagesFile        string
	Providers           []string
	MarginFloor         float64
	ReplicaID           string
	RunnerLinuxAMD64    string
	RunnerLinuxARM64    string
	ProviderKeys        map[string]string
	LocalEnabled        bool
	LocalRoot           string
}

var providerKeyVars = []struct{ name, env string }{
	{"boxd", "BOXD_API_KEY"},
	{"e2b", "E2B_API_KEY"},
	{"koyeb", "KOYEB_API_TOKEN"},
	{"boat", "BOAT_API_KEY"},
}

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
	c.DatabaseURL = require("BUGNET_DATABASE_URL")
	c.InternalToken = require("BUGNET_INTERNAL_TOKEN")
	pubRaw := require("BUGNET_ORDER_TOKEN_PUBLIC_KEY")
	c.PackagesFile = require("BUGNET_PACKAGES_FILE")
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
	c.EventsURL = get("BUGNET_EVENTS_URL", "")
	c.RunnerLinuxAMD64 = get("BUGNET_RUNNER_LINUX_AMD64", "")
	c.RunnerLinuxARM64 = get("BUGNET_RUNNER_LINUX_ARM64", "")
	c.ReplicaID = get("BUGNET_REPLICA_ID", defaultReplicaID())
	c.LocalEnabled = get("BUGNET_ENABLE_LOCAL_PROVIDER", "") == "1"
	c.LocalRoot = get("BUGNET_LOCAL_ROOT", filepath.Join(os.TempDir(), "bugnet-local"))
	c.MarginFloor, err = strconv.ParseFloat(get("BUGNET_MARGIN_FLOOR", "0.5"), 64)
	if err != nil || c.MarginFloor < 0 || c.MarginFloor > 10 {
		return Config{}, fmt.Errorf("config: BUGNET_MARGIN_FLOOR must be a number from 0 to 10")
	}
	for _, name := range strings.Split(get("BUGNET_PROVIDERS", "koyeb,boat,e2b,boxd"), ",") {
		name = strings.TrimSpace(name)
		if !knownProviders[name] {
			return Config{}, fmt.Errorf("config: BUGNET_PROVIDERS has unknown provider %q", name)
		}
		if name == "local" && !c.LocalEnabled {
			return Config{}, fmt.Errorf(
				"config: BUGNET_PROVIDERS names local but BUGNET_ENABLE_LOCAL_PROVIDER is not 1")
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
