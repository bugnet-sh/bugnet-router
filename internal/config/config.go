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
	"io"
	"maps"
	"math"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"

	"github.com/bugnet-sh/bugnet-router/internal/bytesize"
	"github.com/bugnet-sh/bugnet-router/internal/dpkeys"
)

// Config holds every router setting
type Config struct {
	DatabaseURL  string
	ValkeyAddr   string
	ValkeyPrefix string
	ListenAddr   string
	// APIReusePort binds router-api with SO_REUSEPORT, so a new version binds beside a draining one
	APIReusePort        bool
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
	// HookSecrets are the webhook signing secrets by provider, from the environment
	HookSecrets       map[string]HookSecret
	LocalEnabled      bool
	LocalRoot         string
	LocalRunnerPath   string // host build of bugnet-run the local provider runs
	LocalHourlyMicros int64
	// JobOutputBudget is what one job's output may cost its Valkey stream, in bytes
	JobOutputBudget int64
	// PlacementOutputBudget is what a placement's job streams may cost together, in bytes
	PlacementOutputBudget int64
	// PlacementTailReserve is what final tails may cost a placement past its budget, in bytes
	PlacementTailReserve int64
	// MaxRunningJobs bounds the jobs one placement runs at once
	MaxRunningJobs int
	// SandboxOpsPerSecond is how fast a placement's operation bucket refills, in tokens a second
	SandboxOpsPerSecond int
	// SandboxOpsBurst is how many operations a placement's full bucket admits at once
	SandboxOpsBurst int
	Boxd            Boxd
	E2B             E2B
	Koyeb           Koyeb
	Boat            Boat
	// DataplaneWorkers is how many River workers run the dataplane queue
	DataplaneWorkers int
	// DefaultWorkers is how many River workers run the default queue
	DefaultWorkers int
	// LocalSSHDPath is the host's sshd, which the local provider's ssh-up never installs
	LocalSSHDPath string
	Names         Names
	EdgeBudget    EdgeBudget
	Previews      Previews
	Transfers     Transfers
	SSH           SSH
	Secrets       Secrets
	Fetch         Fetch
	Bootstrap     Bootstrap
	Hooks         Hooks
	X402          X402
	Hardening     Hardening
	LimitsPoll    LimitsPoll
	Health        Health
	Bench         Bench
	Abuse         Abuse
	Reconcile     Reconcile
	Wallets       Wallets
	Stats         Stats
	Estimator     Estimator
	// Localrun is read only by a localrun build and zero in every other
	Localrun Localrun
	// Images holds each listed provider's image manifest
	Images map[string]ImageTools
	// GithubToken and HFToken read sources for the estimator, empty for anonymous reads
	GithubToken string
	HFToken     string
	// ProviderOps is each provider account's user operations a second, absent for no bucket
	ProviderOps map[string]int
	// MaxConcurrent is each provider's own cap on concurrent sandboxes, absent for the account's
	MaxConcurrent map[string]int
	// CreatesPerSecond is each provider's create rate, absent for no bucket
	CreatesPerSecond map[string]int
	Sessions         Sessions
	// Pools holds the warm pool settings and the pools themselves
	Pools Pools
	// EdgeToken admits bugnet-edge to EdgeService, no caller when empty
	EdgeToken string
	// SecretsToken admits bugnet-secrets to SecretsRouteService, no caller when empty
	SecretsToken string
	// SSHToken admits bugnet-ssh to SshGatewayService, no caller when empty
	SSHToken string
	// EgressToken admits bugnet-egress to EgressService, no caller when empty
	EgressToken string
	// EgressFetchToken is the bnf_ token the router's private fetch opens egress tunnels with
	EgressFetchToken string
	// OperatorToken admits bugnet-dev's operator commands to OperatorService, none when empty
	OperatorToken string
	// PaymentsToken admits Payments to PaymentsService, no caller when empty
	PaymentsToken string
	// SessionsToken admits bugnet-edge --role sessions to SessionPageService, none when empty
	SessionsToken string
	// DataPlaneKeys are the roots every derived data-plane value and sealed provider ref uses
	DataPlaneKeys dpkeys.Keys
}

// Boxd holds the boxd adapter's settings
type Boxd struct {
	Region   string `toml:"region"`
	Endpoint string `toml:"endpoint"` // overrides the region's endpoint when set
	// GateRoute is default for the pinned default proxy, or subdomain for a bn proxy to the gate
	GateRoute string `toml:"gate_route"`
	// ParkedGiB is the disk written per hibernated pool member, which sets its idle cost
	ParkedGiB int `toml:"parked_gib"`
}

// E2B holds the E2B adapter's settings
type E2B struct {
	Domain      string        // e2b.app when empty
	MaxLifetime time.Duration // the adapter's default when zero
	Templates   []E2BTemplate
	// TeamID names the team the account check reads, empty for the list call instead
	TeamID string
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
	// OrganizationID is read from the account when empty
	OrganizationID string `toml:"organization_id"`
}

// Boat holds the Boat adapter's settings
type Boat struct {
	// Types maps each size sold to its machine type, every type when empty
	Types map[string]string `toml:"types"`
	// SSHPath is gate for SSH through the bugnet-run gate, or native for Boat's own sshd
	SSHPath string `toml:"ssh_path"`
	// EgressFamilies are the families of Boat's ip taken as a sandbox's egress source
	EgressFamilies []string `toml:"egress_families"`
	// EgressIPv6Bits is the prefix width of an IPv6 egress source, 64 to 128
	EgressIPv6Bits int `toml:"egress_ipv6_bits"`
}

// Names holds the public names the edges serve, each empty until a deployment sets it
type Names struct {
	PreviewDomain string `toml:"preview_domain"` // previews are <id>.<preview_domain>
	TransferHost  string `toml:"transfer_host"`
	SecretsHost   string `toml:"secrets_host"`
	SSHHost       string `toml:"ssh_host"`
	SSHPort       int    `toml:"ssh_port"` // known_hosts names the port when it is not 22
	EgressHost    string `toml:"egress_host"`
	HooksHost     string `toml:"hooks_host"` // provider webhooks arrive here
}

// EdgeBudget bounds one placement's previews, transfers and SSH together, and its egress apart
type EdgeBudget struct {
	// BytesPer10Minutes is the edge bytes both ways, scaled by the package's paid minutes
	BytesPer10Minutes int64
	// BytesPerSecond is the edge pace each way, shared by the edge and the SSH gateway
	BytesPerSecond int64
	// RequestsPerSecond is the preview request rate, with RequestsBurst as its burst
	RequestsPerSecond int
	RequestsBurst     int
	// QueuedRequests is how many preview requests may be in flight, more wait up to 30 s
	QueuedRequests int
	// WebSockets is how many preview WebSockets may be open
	WebSockets int
	// EgressBytesPer10Minutes is the egress tunnel bytes both ways, scaled the same way
	EgressBytesPer10Minutes int64
	// EgressBytesPerSecond is the egress pace each way
	EgressBytesPerSecond int64
	// EgressRequestsInFlight is how many proxied requests may be in flight
	EgressRequestsInFlight int
}

// Previews holds the preview settings
type Previews struct {
	// Providers lists the providers whose gate route passed the live test
	Providers []string `toml:"providers"`
	// GatePort is the port the bugnet-run gate listens on inside every sandbox
	GatePort        int  `toml:"gate_port"`
	MaxPerPlacement int  `toml:"max_per_placement"`
	Interstitial    bool `toml:"interstitial"`
}

// Transfers holds the transfer settings
type Transfers struct {
	// URLTTL is the window in which a transfer must start
	URLTTL time.Duration
	// MaxBytes is the per-file cap, which a package's edge transfer_max_bytes replaces
	MaxBytes              int64
	MaxDuration           time.Duration
	IdleTimeout           time.Duration
	MaxActivePerPlacement int
	MaxTokensPerPlacement int
	// MaxClaims is how many times one URL may be claimed, with 2 x size + 64 MiB of bytes
	MaxClaims int
	// KoyebPart is the size of each buffered and retried Koyeb upload part
	KoyebPart int64
	// KoyebBufferTotal bounds every Koyeb part buffer of one router-api process together
	KoyebBufferTotal int64
}

// SSH holds the SSH gateway settings
type SSH struct {
	// Providers lists the providers whose SSH live test passed
	Providers []string
	// HostKeys are the gateway's public host key lines, for known_hosts
	HostKeys                   []string
	SSHDPort                   int
	SSHDFallbackPort           int
	MaxConnectionsPerPlacement int
	MaxKeysPerPlacement        int
	ConnectionLease            time.Duration
	// PrepareTimeout is how long OpenSshUpstream waits for sshd to be ready
	PrepareTimeout time.Duration
	// InstallTimeout bounds apt-get update and install in ssh-up
	InstallTimeout time.Duration
}

// Secrets holds the secret and egress proxy settings
type Secrets struct {
	// Providers lists the providers whose secret live test passed
	Providers []string
	// EgressIPs are the proxy's addresses, pinned in every forwarder
	EgressIPs []netip.Addr
	// CACertFile is the Bugnet CA certificate in PEM, no CA yet when empty
	CACertFile string
	CAVersion  int
	// BoxPublicKeys are the proxy's sealing keys, newest first
	BoxPublicKeys []BoxKey
	LinkTTL       time.Duration
	// PutGrantTTL is how long a bnsp_ grant for the bridge stays valid
	PutGrantTTL time.Duration
	// CustomHosts false refuses and stops serving hosts outside the presets
	CustomHosts     bool
	MaxPerPlacement int
	LinksPerHour    int
	PutsPerHour     int
	ApplyWait       time.Duration
	// SourceCheck is [secrets.source_check]
	SourceCheck SourceCheck
}

// SourceCheck holds the source-address check settings of DP 10.12.9
type SourceCheck struct {
	// Modes is off, report or enforce by provider, a provider absent is off
	Modes map[string]string
	// RefuseUnknown is on_unknown = "refuse"
	RefuseUnknown bool
	// RefreshInterval allows at most one provider read per placement in this time
	RefreshInterval time.Duration
	// ReadTimeout bounds one provider read
	ReadTimeout time.Duration
}

// BoxKey is one X25519 public key of the egress proxy, by its id
type BoxKey struct {
	ID  string
	Key []byte // 32 bytes
}

// Fetch holds the settings of the router's private repository fetch
type Fetch struct {
	// MaxBytes caps one private repository tarball, counted as egress
	MaxBytes int64
	// EgressAddr is the proxy's listener, dialled directly with a PROXY v2 header
	EgressAddr string
	// EgressRootsFile is a PEM bundle the proxy's outer certificate is checked against, the
	// system roots when empty
	EgressRootsFile string
}

// Bootstrap holds the settings of FetchSource
type Bootstrap struct {
	// Dir is where repositories land, under the exec user's home
	Dir string
	// HeartbeatStale is how long a private fetch may stay silent before it reads as interrupted
	HeartbeatStale time.Duration
	// MaxPrivateAttempts caps the private fetches of one placement
	MaxPrivateAttempts int
	// MaxConcurrent caps the private fetches one replica runs at once
	MaxConcurrent int
}

// Hooks holds the provider webhook settings
type Hooks struct {
	// Providers lists the providers whose webhooks are verified
	Providers []string
	// BoatWindow is how far Boat's X-Ascii-Timestamp may be from now
	BoatWindow time.Duration
	// HintUnique folds the hints for one sandbox inside this window into one job
	HintUnique time.Duration
}

// HookSecret is one provider's webhook signing secret and the one it replaced
type HookSecret struct {
	Current, Previous []byte
}

type hooksFile struct {
	Providers  []string `toml:"providers"`
	BoatWindow string   `toml:"boat_window"`
	HintUnique string   `toml:"hint_unique"`
}

// Localrun holds the settings only a localrun build reads, for the x402 local run
type Localrun struct {
	// ForwardListen replaces the forwarder's listen address in the egress spec
	ForwardListen string
	// EgressPort replaces the port forwarders dial on the egress proxy
	EgressPort int
}

/* Settings file */

// file is the settings file's schema, each struct type naming one table
type file struct {
	OrderTokenPublicKey string         `toml:"order_token_public_key"`
	PackagesFile        string         `toml:"packages_file"`
	Providers           *[]string      `toml:"providers"`
	MarginFloor         float64        `toml:"margin_floor"`
	DeploymentID        string         `toml:"deployment_id"`
	ReplicaID           string         `toml:"replica_id"`
	EventsURL           string         `toml:"events_url"`
	API                 apiFile        `toml:"api"`
	Worker              workerFile     `toml:"worker"`
	Valkey              valkeyFile     `toml:"valkey"`
	Runner              runnerFile     `toml:"runner"`
	Limits              limitsFile     `toml:"limits"`
	Local               localFile      `toml:"local"`
	Boxd                boxdFile       `toml:"boxd"`
	E2B                 e2bFile        `toml:"e2b"`
	Koyeb               koyebFile      `toml:"koyeb"`
	Boat                boatFile       `toml:"boat"`
	Names               Names          `toml:"names"`
	EdgeBudget          budgetFile     `toml:"edge_budget"`
	Previews            Previews       `toml:"previews"`
	Transfers           xferFile       `toml:"transfers"`
	SSH                 sshFile        `toml:"ssh"`
	Secrets             secretFile     `toml:"secrets"`
	Fetch               fetchFile      `toml:"fetch"`
	Bootstrap           bootstrapFile  `toml:"bootstrap"`
	Hooks               hooksFile      `toml:"hooks"`
	X402                x402File       `toml:"x402"`
	Hardening           hardeningFile  `toml:"hardening"`
	LimitsPoll          limitsPollFile `toml:"limits_poll"`
	Health              healthFile     `toml:"health"`
	Bench               benchFile      `toml:"bench"`
	Abuse               abuseFile      `toml:"abuse"`
	Reconcile           reconcileFile  `toml:"reconcile"`
	Wallets             walletsFile    `toml:"wallets"`
	Stats               statsFile      `toml:"stats"`
	Estimator           estimatorFile  `toml:"estimator"`
	Images              imagesFile     `toml:"images"`
	Sessions            sessionsFile   `toml:"sessions"`
	Pools               poolsFile      `toml:"pools"`
	Localrun            localrunFile   `toml:"localrun"`
}

type apiFile struct {
	ListenAddr string `toml:"listen_addr"`
	ReusePort  bool   `toml:"reuse_port"`
}

type workerFile struct {
	ListenAddr       string `toml:"listen_addr"`
	DataplaneWorkers int64  `toml:"dataplane_workers"`
	DefaultWorkers   int64  `toml:"default_workers"`
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
	SandboxOpsPerSecond   int64  `toml:"sandbox_ops_per_second"`
	SandboxOpsBurst       int64  `toml:"sandbox_ops_burst"`
}

type localFile struct {
	Enabled      bool   `toml:"enabled"`
	Root         string `toml:"root"`
	Runner       string `toml:"runner"`
	HourlyMicros int64  `toml:"hourly_micros"`
	SSHDPath     string `toml:"sshd_path"`
}

type e2bFile struct {
	Domain         string        `toml:"domain"`
	MaxLifetime    string        `toml:"max_lifetime"`
	Templates      []E2BTemplate `toml:"templates"`
	HardenRequired bool          `toml:"harden_required"`
	HardenAllow    []string      `toml:"harden_allow"`
	OpsPerSecond   int64         `toml:"ops_per_second"`
	TeamID         string        `toml:"team_id"`
	MaxConcurrent  int64         `toml:"max_concurrent"`
	CreatesPerSec  int64         `toml:"creates_per_second"`
}

type boxdFile struct {
	Region         string   `toml:"region"`
	Endpoint       string   `toml:"endpoint"`
	GateRoute      string   `toml:"gate_route"`
	ParkedGiB      int64    `toml:"parked_gib"`
	HardenRequired bool     `toml:"harden_required"`
	HardenAllow    []string `toml:"harden_allow"`
	OpsPerSecond   int64    `toml:"ops_per_second"`
	MaxConcurrent  int64    `toml:"max_concurrent"`
	CreatesPerSec  int64    `toml:"creates_per_second"`
}

type koyebFile struct {
	Region         string            `toml:"region"`
	InstanceTypes  map[string]string `toml:"instance_types"`
	HardenRequired bool              `toml:"harden_required"`
	HardenAllow    []string          `toml:"harden_allow"`
	OpsPerSecond   int64             `toml:"ops_per_second"`
	OrganizationID string            `toml:"organization_id"`
	MaxConcurrent  int64             `toml:"max_concurrent"`
	CreatesPerSec  int64             `toml:"creates_per_second"`
}

type boatFile struct {
	Types          map[string]string `toml:"types"`
	SSHPath        string            `toml:"ssh_path"`
	EgressFamilies []string          `toml:"egress_families"`
	EgressIPv6Bits int64             `toml:"egress_ipv6_bits"`
	HardenRequired bool              `toml:"harden_required"`
	HardenAllow    []string          `toml:"harden_allow"`
	OpsPerSecond   int64             `toml:"ops_per_second"`
	MaxConcurrent  int64             `toml:"max_concurrent"`
	CreatesPerSec  int64             `toml:"creates_per_second"`
}

type budgetFile struct {
	BytesPer10Minutes       string `toml:"bytes_per_10_minutes"`
	BytesPerSecond          string `toml:"bytes_per_second"`
	RequestsPerSecond       int64  `toml:"requests_per_second"`
	RequestsBurst           int64  `toml:"requests_burst"`
	QueuedRequests          int64  `toml:"queued_requests"`
	WebSockets              int64  `toml:"websockets"`
	EgressBytesPer10Minutes string `toml:"egress_bytes_per_10_minutes"`
	EgressBytesPerSecond    string `toml:"egress_bytes_per_second"`
	EgressRequestsInFlight  int64  `toml:"egress_requests_in_flight"`
}

type xferFile struct {
	URLTTL                string `toml:"url_ttl"`
	MaxBytes              string `toml:"max_bytes"`
	MaxDuration           string `toml:"max_duration"`
	IdleTimeout           string `toml:"idle_timeout"`
	MaxActivePerPlacement int64  `toml:"max_active_per_placement"`
	MaxTokensPerPlacement int64  `toml:"max_tokens_per_placement"`
	MaxClaims             int64  `toml:"max_claims"`
	KoyebPart             string `toml:"koyeb_part"`
	KoyebBufferTotal      string `toml:"koyeb_buffer_total"`
}

type sshFile struct {
	Providers                  []string `toml:"providers"`
	HostKeys                   []string `toml:"host_keys"`
	SSHDPort                   int64    `toml:"sshd_port"`
	SSHDFallbackPort           int64    `toml:"sshd_fallback_port"`
	MaxConnectionsPerPlacement int64    `toml:"max_connections_per_placement"`
	MaxKeysPerPlacement        int64    `toml:"max_keys_per_placement"`
	ConnectionLease            string   `toml:"connection_lease"`
	PrepareTimeout             string   `toml:"prepare_timeout"`
	InstallTimeout             string   `toml:"install_timeout"`
}

type secretFile struct {
	Providers       []string     `toml:"providers"`
	EgressIPs       []string     `toml:"egress_ips"`
	CACertFile      string       `toml:"ca_cert_file"`
	CAVersion       int64        `toml:"ca_version"`
	BoxPublicKeys   []boxKeyFile `toml:"box_public_keys"`
	LinkTTL         string       `toml:"link_ttl"`
	PutGrantTTL     string       `toml:"put_grant_ttl"`
	CustomHosts     bool         `toml:"custom_hosts"`
	MaxPerPlacement int64        `toml:"max_per_placement"`
	LinksPerHour    int64        `toml:"links_per_hour"`
	PutsPerHour     int64        `toml:"puts_per_hour"`
	ApplyWait       string       `toml:"apply_wait"`
	SourceCheck     sourceFile   `toml:"source_check"`
}

type sourceFile struct {
	Modes           map[string]string `toml:"modes"`
	OnUnknown       string            `toml:"on_unknown"`
	RefreshInterval string            `toml:"refresh_interval"`
	ReadTimeout     string            `toml:"read_timeout"`
}

type boxKeyFile struct {
	ID  string `toml:"id"`
	Key string `toml:"key"`
}

type fetchFile struct {
	MaxBytes        string `toml:"max_bytes"`
	EgressAddr      string `toml:"egress_addr"`
	EgressRootsFile string `toml:"egress_roots_file"`
}

type bootstrapFile struct {
	Dir                string `toml:"dir"`
	HeartbeatStale     string `toml:"heartbeat_stale"`
	MaxPrivateAttempts int64  `toml:"max_private_attempts"`
	MaxConcurrent      int64  `toml:"max_concurrent"`
}

// defaults are the settings of an empty file, an empty string in the file also means its default
func defaults() file {
	return file{
		MarginFloor:  0.5,
		DeploymentID: "main",
		API:          apiFile{ListenAddr: ":8080"},
		Worker:       workerFile{ListenAddr: ":8081", DataplaneWorkers: 8, DefaultWorkers: 20},
		Valkey:       valkeyFile{Addr: "127.0.0.1:6379", Prefix: "bn:"},
		Limits: limitsFile{
			JobOutputBudget: "8MiB", PlacementOutputBudget: "32MiB",
			PlacementTailReserve: "8MiB", MaxRunningJobs: 16,
			SandboxOpsPerSecond: 10, SandboxOpsBurst: 20,
		},
		Local: localFile{
			Root: filepath.Join(os.TempDir(), "bugnet-local"), SSHDPath: "/usr/sbin/sshd",
		},
		Boxd: boxdFile{
			Region: "fra", GateRoute: "default", ParkedGiB: 5, HardenRequired: true,
		},
		E2B:   e2bFile{HardenRequired: true, MaxConcurrent: 20, CreatesPerSec: 1},
		Koyeb: koyebFile{Region: "fra", HardenRequired: true},
		Boat: boatFile{
			SSHPath: "gate", HardenRequired: true, OpsPerSecond: 50,
			EgressFamilies: []string{"ipv6", "ipv4"}, EgressIPv6Bits: 128,
		},
		Names: Names{SSHPort: 22},
		EdgeBudget: budgetFile{
			BytesPer10Minutes: "2GiB", BytesPerSecond: "5MiB", RequestsPerSecond: 100,
			RequestsBurst: 200, QueuedRequests: 64, WebSockets: 32,
			EgressBytesPer10Minutes: "2GiB", EgressBytesPerSecond: "2MiB",
			EgressRequestsInFlight: 32,
		},
		Previews: Previews{GatePort: 29417, MaxPerPlacement: 8, Interstitial: true},
		Transfers: xferFile{
			URLTTL: "10m", MaxBytes: "1GiB", MaxDuration: "30m", IdleTimeout: "60s",
			MaxActivePerPlacement: 2, MaxTokensPerPlacement: 16, MaxClaims: 5,
			KoyebPart: "8MiB", KoyebBufferTotal: "256MiB",
		},
		SSH: sshFile{
			SSHDPort: 22, SSHDFallbackPort: 2022, MaxConnectionsPerPlacement: 8,
			MaxKeysPerPlacement: 4, ConnectionLease: "60s", PrepareTimeout: "40s",
			InstallTimeout: "120s",
		},
		Secrets: secretFile{
			CAVersion: 1, LinkTTL: "10m", PutGrantTTL: "10m", CustomHosts: true,
			MaxPerPlacement: 20, LinksPerHour: 10, PutsPerHour: 30, ApplyWait: "15s",
			SourceCheck: sourceFile{
				OnUnknown: "allow", RefreshInterval: "60s", ReadTimeout: "3s",
			},
		},
		Fetch: fetchFile{MaxBytes: "2GiB", EgressAddr: "127.0.0.1:9443"},
		Bootstrap: bootstrapFile{
			Dir: "src", HeartbeatStale: "1m", MaxPrivateAttempts: 2, MaxConcurrent: 4,
		},
		X402: x402File{
			MaxServices: 5, MaxPriceMicros: maxX402PriceMicros, UnpaidRequestsPerHost: 300,
			UnpaidBytesPerHost: "50MiB",
			// A clone, since go-toml decodes a list into the slice it finds
			SharedHostingSuffixes: slices.Clone(sharedHostingSuffixes),
		},
		Hooks:      hooksFile{BoatWindow: "5m", HintUnique: "10s"},
		Hardening:  defaultHardening(),
		LimitsPoll: defaultLimitsPoll(),
		Health:     defaultHealth(),
		Bench:      defaultBench(),
		Abuse:      defaultAbuse(),
		Reconcile:  defaultReconcile(),
		Wallets:    defaultWallets(),
		Stats:      defaultStats(),
		Estimator:  defaultEstimator(),
		Sessions:   defaultSessions(),
		Pools:      defaultPools(),
	}
}

var defaultProviders = []string{"koyeb", "boat", "e2b", "boxd"}

var knownProviders = map[string]bool{
	"koyeb": true, "boat": true, "e2b": true, "boxd": true, "local": true,
}

var deploymentID = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)

// maxFileBytes keeps a settings file far below the nesting depth that overflows go-toml's stack
const maxFileBytes = 64 << 10

// LoadFile reads and checks the settings file at path, leaving every secret unset
func LoadFile(path string) (Config, error) {
	raw, err := readCapped(path)
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

// readCapped reads the file at path, refusing one larger than maxFileBytes
func readCapped(path string) ([]byte, error) {
	fh, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = fh.Close() }()
	raw, err := io.ReadAll(io.LimitReader(fh, maxFileBytes+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > maxFileBytes {
		return nil, fmt.Errorf("%s: the settings file is larger than 64 KiB", path)
	}
	return raw, nil
}

// decode checks the document against the schema, then decodes it strictly over the defaults
func decode(raw []byte) (file, error) {
	return recovered(func() (file, error) {
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
	})
}

// recovered runs a decode and turns a panic inside go-toml into an error that quotes nothing
func recovered(run func() (file, error)) (f file, err error) {
	defer func() {
		if recover() != nil {
			f, err = file{}, errors.New("the file could not be decoded")
		}
	}()
	return run()
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
		APIReusePort:      f.API.ReusePort,
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
			GateRoute: cmp.Or(f.Boxd.GateRoute, d.Boxd.GateRoute),
			ParkedGiB: int(f.Boxd.ParkedGiB),
		},
		E2B: E2B{
			Domain: f.E2B.Domain, Templates: slices.Clone(f.E2B.Templates), TeamID: f.E2B.TeamID,
		},
		Koyeb: Koyeb{
			Region:         cmp.Or(f.Koyeb.Region, d.Koyeb.Region),
			InstanceTypes:  maps.Clone(f.Koyeb.InstanceTypes),
			OrganizationID: f.Koyeb.OrganizationID,
		},
		Boat: Boat{
			Types: maps.Clone(f.Boat.Types), SSHPath: cmp.Or(f.Boat.SSHPath, d.Boat.SSHPath),
			EgressFamilies: slices.Clone(f.Boat.EgressFamilies),
			EgressIPv6Bits: int(f.Boat.EgressIPv6Bits),
		},
		LocalSSHDPath: cmp.Or(f.Local.SSHDPath, d.Local.SSHDPath),
		Names:         f.Names,
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
	for _, n := range []struct {
		key  string
		raw  int64
		into *int
	}{
		{"limits.max_running_jobs", f.Limits.MaxRunningJobs, &c.MaxRunningJobs},
		{"limits.sandbox_ops_per_second", f.Limits.SandboxOpsPerSecond, &c.SandboxOpsPerSecond},
		{"limits.sandbox_ops_burst", f.Limits.SandboxOpsBurst, &c.SandboxOpsBurst},
	} {
		if n.raw < 1 {
			return Config{}, fmt.Errorf("%s must be a positive whole number", n.key)
		}
		*n.into = int(n.raw)
	}
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
	if err := f.dataPlane(&c); err != nil {
		return Config{}, err
	}
	if err := f.hardening(&c); err != nil {
		return Config{}, err
	}
	if c.X402, err = f.X402.read(d.X402); err != nil {
		return Config{}, err
	}
	if c.LimitsPoll, err = f.LimitsPoll.read(d.LimitsPoll); err != nil {
		return Config{}, err
	}
	if c.Health, err = f.Health.read(d.Health); err != nil {
		return Config{}, err
	}
	if c.Bench, err = f.Bench.read(); err != nil {
		return Config{}, err
	}
	if c.Abuse, err = f.Abuse.read(d.Abuse); err != nil {
		return Config{}, err
	}
	if c.Reconcile, err = f.Reconcile.read(d.Reconcile); err != nil {
		return Config{}, err
	}
	if c.Wallets, err = f.Wallets.read(d.Wallets); err != nil {
		return Config{}, err
	}
	if c.Stats, err = f.Stats.read(d.Stats); err != nil {
		return Config{}, err
	}
	if c.Estimator, err = f.Estimator.read(d.Estimator); err != nil {
		return Config{}, err
	}
	c.Images = f.Images.images(c.Providers)
	c.ProviderOps = map[string]int{}
	for _, o := range []struct {
		name string
		rate int64
	}{{"e2b", f.E2B.OpsPerSecond}, {"boxd", f.Boxd.OpsPerSecond},
		{"koyeb", f.Koyeb.OpsPerSecond}, {"boat", f.Boat.OpsPerSecond}} {
		if o.rate < 0 || o.rate > maxWalletLimit {
			return Config{}, fmt.Errorf("%s.ops_per_second must be 0 to %d", o.name,
				maxWalletLimit)
		}
		if o.rate > 0 {
			c.ProviderOps[o.name] = int(o.rate)
		}
	}
	if err := f.providerCounts(&c); err != nil {
		return Config{}, err
	}
	if f.Boxd.ParkedGiB < 1 || f.Boxd.ParkedGiB > 100_000 {
		return Config{}, errors.New("boxd.parked_gib must be 1 to 100000")
	}
	if c.Sessions, err = f.Sessions.read(d.Sessions); err != nil {
		return Config{}, err
	}
	if c.Pools, err = f.Pools.read(d.Pools); err != nil {
		return Config{}, err
	}
	c.Localrun = f.Localrun.read()
	for _, p := range []*string{
		&c.PackagesFile, &c.RunnerLinuxAMD64, &c.RunnerLinuxARM64, &c.LocalRoot,
		&c.LocalRunnerPath, &c.LocalSSHDPath, &c.Secrets.CACertFile, &c.Hardening.PoolList,
		&c.Fetch.EgressRootsFile,
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

// providerList checks the providers to use, the default when the file names none
func providerList(given *[]string, c Config) ([]string, error) {
	names := defaultProviders
	if given != nil {
		if len(*given) == 0 {
			return nil, errors.New("providers must name at least one provider")
		}
		names = *given
	}
	seen := map[string]bool{}
	for i, name := range names {
		switch {
		case name == "":
			return nil, errors.New("providers has an empty entry")
		case !knownProviders[name]:
			// An unknown entry is never quoted, since a secret could have been pasted there
			return nil, fmt.Errorf("providers entry %d is not one of %s", i+1, knownList())
		case seen[name]:
			return nil, fmt.Errorf("providers lists %q more than once", name)
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
	dataPlaneKeysEnv  = "BUGNET_DATA_PLANE_KEYS"
)

var providerKeyVars = []struct{ name, env string }{
	{"boxd", "BOXD_API_KEY"},
	{"e2b", "E2B_API_KEY"},
	{"koyeb", "KOYEB_API_TOKEN"},
	{"boat", "BOAT_API_KEY"},
}

// ProviderKeyEnv names the environment variable holding a provider's API key, false for none
func ProviderKeyEnv(name string) (string, bool) {
	for _, pk := range providerKeyVars {
		if pk.name == name {
			return pk.env, true
		}
	}
	return "", false
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
	for _, name := range []string{
		databaseURLEnv, "BUGNET_INTERNAL_TOKEN", "BUGNET_HANDLE_KEY", dataPlaneKeysEnv,
	} {
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
	if c.DataPlaneKeys, err = dpkeys.Parse(get(dataPlaneKeysEnv)); err != nil {
		return fmt.Errorf("%s %w", dataPlaneKeysEnv, err)
	}
	if err := c.readCallerTokens(get); err != nil {
		return err
	}
	if raw := get("BUGNET_EVENT_SIGNING_KEY"); raw != "" {
		seed, err := decodeKey("BUGNET_EVENT_SIGNING_KEY", raw, ed25519.SeedSize)
		if err != nil {
			return err
		}
		c.EventSigningKey = ed25519.NewKeyFromSeed(seed)
	}
	c.GithubToken, c.HFToken = get("BUGNET_GITHUB_TOKEN"), get("BUGNET_HF_TOKEN")
	c.ProviderKeys = map[string]string{}
	for _, pk := range providerKeyVars {
		if v := get(pk.env); v != "" {
			c.ProviderKeys[pk.name] = v
		}
	}
	c.HookSecrets = map[string]HookSecret{}
	for _, hk := range hookSecretVars {
		if v := get(hk.env); v != "" {
			c.HookSecrets[hk.name] = HookSecret{
				Current: []byte(v), Previous: []byte(get(hk.env + "_PREVIOUS")),
			}
		}
	}
	return nil
}

// hookSecretVars name the environment variable of each provider's webhook secret
var hookSecretVars = []struct{ name, env string }{
	{"e2b", "E2B_WEBHOOK_SECRET"},
	{"boat", "BOAT_WEBHOOK_SECRET"},
}

// callerTokens are the environment variables of the callers' bearer tokens, in order
var callerTokens = []string{
	"BUGNET_EDGE_TOKEN", "BUGNET_SECRETS_TOKEN", "BUGNET_SSH_TOKEN", "BUGNET_EGRESS_TOKEN",
	"BUGNET_EGRESS_FETCH_TOKEN", "BUGNET_OPERATOR_TOKEN", "BUGNET_PAYMENTS_ROUTER_TOKEN",
	"BUGNET_SESSIONS_TOKEN",
}

// readCallerTokens reads each caller token, empty or 32 characters and unlike every other token
func (c *Config) readCallerTokens(get func(string) string) error {
	into := []*string{&c.EdgeToken, &c.SecretsToken, &c.SSHToken, &c.EgressToken,
		&c.EgressFetchToken, &c.OperatorToken, &c.PaymentsToken, &c.SessionsToken}
	seen := map[string]string{c.InternalToken: "BUGNET_INTERNAL_TOKEN"}
	for i, name := range callerTokens {
		v := get(name)
		if v == "" {
			continue
		}
		if len(v) < 32 {
			return fmt.Errorf("%s must be empty or at least 32 characters", name)
		}
		if other, ok := seen[v]; ok {
			// A shared token would let one caller reach another caller's service
			return fmt.Errorf("%s must differ from %s", name, other)
		}
		seen[v] = name
		*into[i] = v
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

// byteSize reads a byte size setting, naming key when it is not 1 byte to 1 TiB
func byteSize(key, raw string) (int64, error) {
	n, ok := bytesize.Parse(raw)
	if !ok {
		return 0, fmt.Errorf(
			"%s must be 1 byte to 1 TiB, a whole number optionally ending in KiB, MiB or GiB", key)
	}
	return n, nil
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

// knownList names every provider the settings file may list, in a fixed order
func knownList() string {
	names := slices.Sorted(maps.Keys(knownProviders))
	return strings.Join(names, ", ")
}

// providerCounts reads each provider's own cap and create rate, zero meaning the account's
func (f file) providerCounts(c *Config) error {
	c.MaxConcurrent, c.CreatesPerSecond = map[string]int{}, map[string]int{}
	for _, o := range []struct {
		name         string
		max, creates int64
	}{
		{"e2b", f.E2B.MaxConcurrent, f.E2B.CreatesPerSec},
		{"boxd", f.Boxd.MaxConcurrent, f.Boxd.CreatesPerSec},
		{"koyeb", f.Koyeb.MaxConcurrent, f.Koyeb.CreatesPerSec},
		{"boat", f.Boat.MaxConcurrent, f.Boat.CreatesPerSec},
	} {
		for _, n := range []struct {
			key  string
			raw  int64
			into map[string]int
		}{
			{"max_concurrent", o.max, c.MaxConcurrent},
			{"creates_per_second", o.creates, c.CreatesPerSecond},
		} {
			if n.raw < 0 || n.raw > maxProviderCount {
				return fmt.Errorf("%s.%s must be 0 to %d", o.name, n.key, maxProviderCount)
			}
			if n.raw > 0 {
				n.into[o.name] = int(n.raw)
			}
		}
	}
	return nil
}
