// Package provider defines the port every sandbox provider adapter implements
package provider

import (
	"cmp"
	"context"
	"io/fs"
	"net/http"
	"net/netip"
	"time"
)

// DefaultGatePort is the port the bugnet-run gate listens on inside a sandbox
const DefaultGatePort = 29417

// Resources is the hardware a package needs or a size offers
type Resources struct{ VCPU, MemoryMiB, DiskGiB, GPUCount, GPUMemoryGiB int }

// Size is one machine shape a provider sells
type Size struct {
	Name      string
	Resources Resources
}

// Capabilities says what a provider supports, a zero MaxLifetime means unlimited
type Capabilities struct {
	// SSH means users reach sandboxes through the SSH gateway, set by the registry alone
	SSH bool
	// Preview means the edges serve sandbox ports as previews, set by the registry alone
	Preview bool

	GPU, EgressControl, NeedsRunnerUpload bool
	MaxLifetime                           time.Duration
	// Arch is the CPU architecture of the provider's sandboxes, amd64 when empty
	Arch string
	// OneSandboxPerKey means a create repeated under one idempotency key never makes a second
	OneSandboxPerKey bool
	// Secrets means sandboxes can run the egress forwarder and trust the CA, set by the registry
	Secrets bool
	// ExecUser is the user Exec runs as, root when empty
	ExecUser string
}

// RunnerArch names the bugnet-run build the provider's sandboxes run
func (c Capabilities) RunnerArch() string { return cmp.Or(c.Arch, "amd64") }

// Ref names one sandbox at one provider
type Ref struct {
	Provider string            `json:"provider"`
	ID       string            `json:"id"`
	Meta     map[string]string `json:"meta,omitempty"`
}

// CreateRequest asks for a sandbox, a repeated IdempotencyKey returns the first sandbox
type CreateRequest struct {
	IdempotencyKey string
	Size           Size
	Labels         map[string]string
	Lifetime       time.Duration
	// Egress restricts outbound traffic for a restricted package, nil leaving it open
	Egress *EgressAllow
}

// EgressAllow is all a restricted package's sandbox may reach, set at create where EgressControl
type EgressAllow struct {
	// Hosts are the package's hosts and the egress proxy's name
	Hosts []string
	// Proxy holds the egress proxy's addresses
	Proxy []netip.Addr
}

// ExecSpec is a command run directly, without a shell, inside a sandbox
type ExecSpec struct {
	Argv []string
	Cwd  string
	Env  map[string]string
}

// ExecStream is a running command, read its stdout to the end before calling Wait
type ExecStream interface {
	Read(p []byte) (int, error) // raw stdout of the process, io.EOF at its end
	Wait() (exitCode int, err error)
	Close() error
}

// FileEntry describes one file or directory inside a sandbox by its sandbox path
type FileEntry struct {
	Path     string
	Size     int64
	IsDir    bool
	Mode     fs.FileMode
	Modified time.Time
	// Regular reports a regular file, false for directories, devices, FIFOs and sockets
	Regular bool
}

// Summary is one sandbox found by ListOwned
type Summary struct {
	Ref       Ref
	Labels    map[string]string
	CreatedAt time.Time
	// State is empty when the listing does not say
	State State
	// ExpiresAt is the provider-side lifetime, zero when the listing does not say
	ExpiresAt time.Time
}

// State is a provider-neutral reading of one sandbox
type State string

// Sandbox states adapters map their own to
const (
	StateStarting State = "starting"
	StateRunning  State = "running"
	StateDegraded State = "degraded" // runs, but the provider cannot reach it
	StateStopped  State = "stopped"  // stopped, suspended, hibernated, paused or archived
	StateFailed   State = "failed"
	StateGone     State = "gone"
)

// Status is what the provider says about one sandbox now
type Status struct {
	State State
	// Since is when the provider says the state began, zero when unknown
	Since time.Time
	// Reason holds the provider's words, kept as evidence and never shown to users
	Reason string
	// KillReason is why the provider ended a gone sandbox, E2B's kill_reason, else empty
	KillReason string
	// ExpiresAt is the provider-side lifetime, zero when unknown
	ExpiresAt time.Time
}

// Provider is the port the router places sandboxes through
type Provider interface {
	Name() string
	Capabilities() Capabilities
	// SizeFor picks the size this provider would use for a package needing at least floor
	SizeFor(floor Resources) (Size, bool)
	HourlyCostMicros(size Size) int64
	BillingIncrement() time.Duration
	Create(ctx context.Context, req CreateRequest) (Ref, error)
	WaitReady(ctx context.Context, ref Ref) error
	SetLifetime(ctx context.Context, ref Ref, until time.Time) error
	// Destroy removes a sandbox and returns nil when it is already gone
	Destroy(ctx context.Context, ref Ref) error
	Exec(ctx context.Context, ref Ref, spec ExecSpec) (ExecStream, error)
	// ReadFile reads up to length bytes at offset and reports whether they reach the end of file
	ReadFile(
		ctx context.Context, ref Ref, path string, offset int64, length int,
	) ([]byte, bool, error)
	// WriteFile replaces a file with data, creating missing parent directories
	WriteFile(ctx context.Context, ref Ref, path string, data []byte, mode fs.FileMode) error
	// ListFiles lists a directory down to depth levels, one being its children, into a Listing
	ListFiles(ctx context.Context, ref Ref, path string, depth int, into *Listing) error
	// Inspect reads one sandbox, StateGone with a nil error when the provider no longer has it
	Inspect(ctx context.Context, ref Ref) (Status, error)
	// ListOwned finds the sandboxes carrying every given label
	ListOwned(ctx context.Context, labels map[string]string) ([]Summary, error)
	// StatFile describes one path without following a final symlink
	StatFile(ctx context.Context, ref Ref, path string) (FileEntry, error)
	// OpenReader streams a regular file from offset to the size it had when opened
	OpenReader(ctx context.Context, ref Ref, path string, offset int64) (FileReader, error)
	// OpenWriter streams spec.Size bytes into path, creating parents, replacing it only on Commit
	OpenWriter(ctx context.Context, ref Ref, path string, spec WriteSpec) (FileWriter, error)
}

// FileReader streams a regular file from an offset to the size it had when opened
type FileReader interface {
	// Read returns io.EOF exactly at Info().Size, and an error if the provider ends early
	Read(p []byte) (int, error)
	Info() FileEntry
	Close() error
}

// WriteSpec describes the file a FileWriter makes
type WriteSpec struct {
	Size   int64       // exact byte count
	Mode   fs.FileMode // always set, 0644 when the caller gave none
	SHA256 []byte      // expected digest when the caller gave one, nil otherwise
}

// FileWriter streams a new file, no Write makes it visible and only Commit replaces the path
type FileWriter interface {
	Write(p []byte) (int, error)
	// Commit sends sum where the provider checks digests, then replaces the path
	Commit(ctx context.Context, sum []byte) (FileEntry, error)
	// Abort drops what was written and leaves the path as it was, a no-op after Commit
	Abort() error
}

// Pather is a provider that resolves sandbox paths itself and whose file API reaches every path
type Pather interface {
	// Home is the exec user's home directory as a sandbox path
	Home(ref Ref) string
	// RealPath resolves an absolute sandbox path as realpath -m does inside the sandbox
	RealPath(ctx context.Context, ref Ref, path string) (string, error)
}

// StartCoster is a provider that charges a fee per sandbox start beside its hourly price
type StartCoster interface {
	// StartCostMicros is what the next start of size costs, false when it would be refused
	StartCostMicros(size Size) (int64, bool)
}

// Sizer is a provider that lists the sizes it sells, smallest first
type Sizer interface {
	Sizes() []Size
}

// EgressSource is where a provider says a sandbox's outbound connections leave from
type EgressSource struct {
	// Prefix is masked, a /32 for IPv4 and the adapter's width for IPv6
	Prefix netip.Prefix
	// Bind is the reported address itself, which the forwarder dials from
	Bind netip.Addr
}

// EgressSourcer is a provider whose API reports a sandbox's own egress address, Boat only
type EgressSourcer interface {
	// EgressSource reads it from the provider's API, never from the sandbox, and answers
	// false when the provider reports no usable address now
	EgressSource(ctx context.Context, ref Ref) (EgressSource, bool, error)
}

// GateRoute is how the edges reach the bugnet-run gate in one sandbox, never shown to users
type GateRoute struct {
	BaseURL         string            // the gate's origin, its port only when not the default
	Header          map[string]string // provider credential headers
	Cookies         map[string]string // provider credential cookies
	BlockedPrefixes []string          // paths the edges never forward
}

// Gater is a provider whose sandboxes the edges reach through the bugnet-run gate
type Gater interface {
	// PrepareGate opens the provider's route to port and returns Ref.Meta entries to store
	PrepareGate(ctx context.Context, ref Ref, port int) (map[string]string, error)
	// GateRoute builds the route from ref alone, without calling the provider
	GateRoute(ref Ref, port int) (GateRoute, error)
}

// NativeSSH is a provider's own SSH server, used only when the gate path fails there
type NativeSSH struct {
	Address string // host and port, built with net.JoinHostPort
	User    string
	HostKey string // authorized_keys format
}

// NativeSSHer is a provider with an SSH server of its own, Boat only
type NativeSSHer interface {
	// AuthorizeNativeSSH adds gatewayKey, an authorized_keys line, to the sandbox's SSH server
	AuthorizeNativeSSH(ctx context.Context, ref Ref, gatewayKey string) (NativeSSH, error)
}

// HookEvent is a verified provider webhook reduced to what the router acts on
type HookEvent struct {
	ID        string
	Type      string
	SandboxID string
	// Labels are the sandbox's labels when the event carries them, else nil
	Labels map[string]string
	At     time.Time
}

// HookVerifier is a provider whose lifecycle webhooks the router accepts
type HookVerifier interface {
	// VerifyHook checks h and the exact body against secret and parses the body
	VerifyHook(h http.Header, body []byte, secret []byte) (HookEvent, error)
}

// Limits is one reading of a provider account's cap and use, a zero meaning not given
type Limits struct {
	MaxConcurrent int // the account's cap
	Active        int // sandboxes running on the account now, every deployment included
}

// LimitsReader is a provider that reports its account's cap and use
type LimitsReader interface {
	ReadLimits(ctx context.Context) (Limits, error)
}

// Block says why a provider account cannot start sandboxes
type Block string

// Account blocks
const (
	BlockNone     Block = ""
	BlockBanned   Block = "banned"   // banned, locked, suspended or closed by the provider
	BlockUnfunded Block = "unfunded" // out of credit, past due or with no payment method
	BlockAuth     Block = "auth"     // the key no longer works
)

// Account is one reading of a provider account's standing
type Account struct {
	Block  Block
	Detail string // the provider's words, for logs and signals
}

// AccountChecker is a provider that reports whether its account may start sandboxes
type AccountChecker interface {
	CheckAccount(ctx context.Context) (Account, error)
}

// SandboxUsage is what a provider billed for one sandbox, CostMicros -1 when it gives time only
type SandboxUsage struct {
	ID         string
	Seconds    int64
	CostMicros int64
	// Deployment is the bugnet_deployment label the source ties to the sandbox, else empty
	Deployment string
}

// Usage is one page of what a provider billed in a window
type Usage struct {
	Sandboxes []SandboxUsage
	// TotalMicros is the account's bill for the page's window, -1 when the provider gives none
	TotalMicros int64
	// Complete is false while the provider may still add to the window
	Complete bool
	// Attributed is true when the source can name a sandbox's deployment, Deployment then
	// being empty only for a sandbox it ties to none
	Attributed bool
	// Next is the cursor of the next page, empty after the last
	Next string
}

// UsageReader is a provider that reports what it billed
type UsageReader interface {
	// ReadUsage reports use in [from, to) one page at a time, resuming at cursor and asking per
	// sandbox for refs where the provider must
	ReadUsage(ctx context.Context, from, to time.Time, refs []Ref, cursor string) (Usage, error)
}

// ForeignPage is one page of sandboxes found by label
type ForeignPage struct {
	Sandboxes []Summary
	// Next is the cursor of the next page, empty after the last
	Next string
}

// ForeignLister is a provider that lists sandboxes by label in bounded pages
type ForeignLister interface {
	// ListLabelled finds sandboxes carrying every label, resuming at cursor and reading at most
	// limit services from the provider where it needs a read for each
	ListLabelled(
		ctx context.Context, labels map[string]string, cursor string, limit int,
	) (ForeignPage, error)
}
