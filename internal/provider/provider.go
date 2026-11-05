// Package provider defines the port every sandbox provider adapter implements
package provider

import (
	"cmp"
	"context"
	"io/fs"
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
	SSH, Preview, GPU, EgressControl, NeedsRunnerUpload bool
	MaxLifetime                                         time.Duration
	// Arch is the CPU architecture of the provider's sandboxes, amd64 when empty
	Arch string
	// OneSandboxPerKey means a create repeated under one idempotency key never makes a second
	OneSandboxPerKey bool
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
}

// Summary is one sandbox found by ListOwned
type Summary struct {
	Ref       Ref
	Labels    map[string]string
	CreatedAt time.Time
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
	// ListOwned finds the sandboxes carrying every given label
	ListOwned(ctx context.Context, labels map[string]string) ([]Summary, error)
}

// StartCoster is a provider that charges a fee per sandbox start beside its hourly price
type StartCoster interface {
	// StartCostMicros is what the next start of size costs, false when it would be refused
	StartCostMicros(size Size) (int64, bool)
}
