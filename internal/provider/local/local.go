// Package local runs sandboxes as host directories and host processes, for tests and development.
//
// All state lives on disk under Root so several router processes see the same sandboxes.
// Root/<id>/meta.json holds a sandbox's labels, idempotency key, creation time and deadline, and
// Root/<id>/fs is its filesystem, with fs/root as HOME and the default working directory.
// meta.json also holds the two loopback ports Create picks for the sandbox's gate and sshd, since
// every sandbox shares the host's port space.
// A sandbox appears and disappears with one directory rename, built under Root/.staging and
// removed through Root/.trash. An idempotency key derives the sandbox id, and Root/.keys/<id>,
// created with O_EXCL, records that the key was used so it keeps naming that sandbox.
// The runner's root commands, setup, gate, ssh-up, egress apply and forward, get --root at the
// sandbox's fs with their --spec and --config paths mapped there, and run as the host user even
// when the router asks for sudo -n. Each sandbox gets the runner hard-linked at its stable path
// at create, and Destroy kills the services setup started from the pid files under fs/run/bugnet.
package local

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"os/user"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const (
	runnerName        = "bugnet-run"
	runnerSandboxPath = "/tmp/bugnet/bin/bugnet-run"
	runnerJobsDir     = "/tmp/bugnet/jobs"
	jobPidFile        = "pid"
	jobSpidFile       = "spid"
	jobExitFile       = "exit"
	metaFile          = "meta.json"
	metaLockFile      = ".meta.lock"
	idBytes           = 12
	stderrLimit       = 64 << 10
	stderrGrace       = time.Second
)

// Runner subcommands that read or write the jobs directory
var jobCommands = []string{"start", "attach", "ls", "cancel"}

// rootCommands are the runner subcommands that take --root, each as its words
var rootCommands = [][]string{{"setup"}, {"gate"}, {"ssh-up"}, {"egress", "apply"}, {"forward"}}

// Sandbox paths of the runner's stable copy and its service pid files
const (
	stableRunner = "/usr/local/lib/bugnet/bugnet-run"
	servicePids  = "/run/bugnet"
)

// services are the long-running processes setup and ssh-up start, by their pid file names
var services = []string{"gate", "forward", "sshd", "sample"}

// Options configures a local provider, Root must be an absolute path
type Options struct {
	Name             string
	Root             string
	Sizes            []provider.Size
	HourlyCostMicros int64
	Increment        time.Duration
	RunnerPath       string
	CreateHook       func(attempt int, req provider.CreateRequest) error
	ReadyDelay       time.Duration
}

// Provider runs sandboxes on the host and keeps every sandbox's state under Root
type Provider struct {
	opts     Options
	attempts atomic.Int64
	execUser string
}

// Open builds a provider only when enabled, since the local provider runs commands on the host
func Open(enabled bool, opts Options) (*Provider, error) {
	if !enabled {
		return nil, errors.New(
			"the local provider runs commands on the host, set local.enabled = true to use it")
	}
	if !filepath.IsAbs(opts.Root) {
		return nil, fmt.Errorf("local provider root %q is not an absolute path", opts.Root)
	}
	return New(opts), nil
}

// New builds a provider without the enabled gate and panics unless Root is absolute
func New(opts Options) *Provider {
	if !filepath.IsAbs(opts.Root) {
		panic(fmt.Sprintf("local provider root %q is not an absolute path", opts.Root))
	}
	if opts.Name == "" {
		opts.Name = "local"
	}
	opts.Root = filepath.Clean(opts.Root)
	return &Provider{opts: opts, execUser: hostUser()}
}

// hostUser names the user the router runs as, empty for root as Capabilities.ExecUser says
func hostUser() string {
	if os.Geteuid() == 0 {
		return ""
	}
	if u, err := user.Current(); err == nil && u.Username != "" {
		return u.Username
	}
	return os.Getenv("USER")
}

// Name returns the configured provider name
func (p *Provider) Name() string { return p.opts.Name }

// Capabilities reports a host with no isolation, a one-day lifetime cap and Exec as the host user
func (p *Provider) Capabilities() provider.Capabilities {
	return provider.Capabilities{MaxLifetime: 24 * time.Hour, ExecUser: p.execUser}
}

// SizeFor returns the first configured size that covers floor, a size naming no model covering any
func (p *Provider) SizeFor(floor provider.Resources) (provider.Size, bool) {
	for _, s := range p.opts.Sizes {
		r := s.Resources
		if r.VCPU >= floor.VCPU && r.MemoryMiB >= floor.MemoryMiB && r.DiskGiB >= floor.DiskGiB &&
			r.GPUCount >= floor.GPUCount && r.GPUMemoryGiB >= floor.GPUMemoryGiB &&
			(r.GPUModel == "" || floor.GPUModel == "" || r.GPUModel == floor.GPUModel) {
			return s, true
		}
	}
	return provider.Size{}, false
}

// Sizes lists the configured sizes
func (p *Provider) Sizes() []provider.Size { return slices.Clone(p.opts.Sizes) }

// HourlyCostMicros returns the one configured cost for every size
func (p *Provider) HourlyCostMicros(provider.Size) int64 { return p.opts.HourlyCostMicros }

// BillingIncrement returns the configured increment
func (p *Provider) BillingIncrement() time.Duration { return p.opts.Increment }

/* Sandboxes */

type meta struct {
	Provider       string            `json:"provider"`
	Labels         map[string]string `json:"labels,omitempty"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	CreatedAt      time.Time         `json:"created_at"`
	Deadline       time.Time         `json:"deadline,omitzero"`
	// GatePort and SSHDPort are the loopback ports this sandbox's gate and sshd listen on
	GatePort int `json:"gate_port,omitempty"`
	SSHDPort int `json:"sshd_port,omitempty"`
}

type sandbox struct {
	dir  string
	meta meta
}

// Create runs the hook before touching disk so a failing hook leaves nothing behind
func (p *Provider) Create(ctx context.Context, req provider.CreateRequest) (provider.Ref, error) {
	if err := ctx.Err(); err != nil {
		return provider.Ref{}, fmt.Errorf("create a %s sandbox: %w", p.opts.Name, err)
	}
	if req.Image != "" || req.Builder {
		return provider.Ref{}, invalid(errors.New("a local sandbox has no base image"))
	}
	attempt := int(p.attempts.Add(1))
	if p.opts.CreateHook != nil {
		if err := p.opts.CreateHook(attempt, req); err != nil {
			return provider.Ref{}, err
		}
	}

	ref := provider.Ref{Provider: p.opts.Name, ID: p.newID(req.IdempotencyKey)}
	if req.IdempotencyKey != "" {
		_, err := os.Stat(p.keyPath(ref.ID))
		if err == nil {
			return p.withPorts(ref), nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return provider.Ref{}, internalErr(err)
		}
	}
	if err := p.build(ref.ID, req); err != nil {
		return provider.Ref{}, internalErr(err)
	}
	if req.IdempotencyKey != "" {
		if err := p.recordKey(ref.ID, req.IdempotencyKey); err != nil {
			return provider.Ref{}, internalErr(err)
		}
	}
	return p.withPorts(ref), nil
}

// withPorts adds the ports the sandbox on disk holds, which a build that lost a race did not pick
func (p *Provider) withPorts(ref provider.Ref) provider.Ref {
	if s, err := p.open(ref); err == nil {
		ref.Meta = portsMeta(s.meta)
	}
	return ref
}

// newID derives the id from the key so every process creating with that key names one sandbox
func (p *Provider) newID(key string) string {
	if key == "" {
		b := make([]byte, idBytes)
		_, _ = rand.Read(b)
		return hex.EncodeToString(b)
	}
	sum := sha256.Sum256([]byte(p.opts.Name + "\x00" + key))
	return hex.EncodeToString(sum[:idBytes])
}

func validID(id string) bool {
	if len(id) != 2*idBytes {
		return false
	}
	for _, c := range id {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func (p *Provider) keyPath(id string) string {
	return filepath.Join(p.opts.Root, ".keys", id)
}

// build fills a staging directory and renames it into place, yielding to a sandbox already there
func (p *Provider) build(id string, req provider.CreateRequest) error {
	staging, err := p.scratchDir("staging", id)
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(staging) }()

	for _, dir := range []string{"root", "tmp"} {
		if err := os.MkdirAll(filepath.Join(staging, "fs", dir), 0o755); err != nil {
			return err
		}
	}
	now := time.Now().UTC()
	m := meta{
		Provider:       p.opts.Name,
		Labels:         req.Labels,
		IdempotencyKey: req.IdempotencyKey,
		CreatedAt:      now,
	}
	if m.GatePort, m.SSHDPort, err = freePorts(); err != nil {
		return err
	}
	p.linkRunner(filepath.Join(staging, "fs"))
	if req.Lifetime > 0 {
		m.Deadline = now.Add(req.Lifetime)
	}
	if err := writeMeta(staging, m); err != nil {
		return err
	}
	err = os.Rename(staging, filepath.Join(p.opts.Root, id))
	if errors.Is(err, fs.ErrExist) {
		return nil
	}
	return err
}

// linkRunner puts the runner at its stable path without a copy, sparing macOS its first-run check
func (p *Provider) linkRunner(fsDir string) {
	if p.opts.RunnerPath == "" {
		return
	}
	dst := filepath.Join(fsDir, filepath.FromSlash(stableRunner))
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return
	}
	_ = os.Link(p.opts.RunnerPath, dst)
}

func (p *Provider) recordKey(id, key string) error {
	if err := os.MkdirAll(filepath.Dir(p.keyPath(id)), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(p.keyPath(id), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, fs.ErrExist) {
		return nil
	}
	if err != nil {
		return err
	}
	_, err = f.WriteString(key)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

func (p *Provider) scratchDir(kind, id string) (string, error) {
	parent := filepath.Join(p.opts.Root, "."+kind)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return "", err
	}
	return os.MkdirTemp(parent, id+"-*")
}

// open loads a sandbox of this provider, treating any other ref as unknown
func (p *Provider) open(ref provider.Ref) (sandbox, error) {
	if ref.Provider != p.opts.Name || !validID(ref.ID) {
		return sandbox{}, notFound(ref)
	}
	dir := filepath.Join(p.opts.Root, ref.ID)
	m, err := readMeta(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return sandbox{}, notFound(ref)
	}
	if err != nil {
		return sandbox{}, internalErr(err)
	}
	if m.Provider != p.opts.Name {
		return sandbox{}, notFound(ref)
	}
	return sandbox{dir: dir, meta: m}, nil
}

// openLocked opens ref's sandbox holding its meta lock until the caller runs the returned unlock
func (p *Provider) openLocked(ref provider.Ref) (sandbox, func(), error) {
	s, err := p.open(ref)
	if err != nil {
		return sandbox{}, nil, err
	}
	f, err := os.OpenFile(filepath.Join(s.dir, metaLockFile), os.O_RDWR|os.O_CREATE, 0o600)
	if errors.Is(err, fs.ErrNotExist) {
		return sandbox{}, nil, notFound(ref)
	}
	if err != nil {
		return sandbox{}, nil, fileErr(err)
	}
	unlock := func() { _ = f.Close() }
	// A file lock, since a second Provider or process can share the root
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		unlock()
		return sandbox{}, nil, internalErr(err)
	}
	if s, err = p.open(ref); err != nil {
		unlock()
		return sandbox{}, nil, err
	}
	return s, unlock, nil
}

// WaitReady treats a sandbox as ready once ReadyDelay has passed since its creation
func (p *Provider) WaitReady(ctx context.Context, ref provider.Ref) error {
	s, err := p.open(ref)
	if err != nil {
		return err
	}
	if wait := time.Until(s.meta.CreatedAt.Add(p.opts.ReadyDelay)); wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	_, err = p.open(ref)
	return err
}

// SetLifetime records the deadline, which the router enforces rather than this provider
func (p *Provider) SetLifetime(_ context.Context, ref provider.Ref, until time.Time) error {
	s, unlock, err := p.openLocked(ref)
	if err != nil {
		return err
	}
	defer unlock()
	s.meta.Deadline = until.UTC()
	err = writeMeta(s.dir, s.meta)
	if errors.Is(err, fs.ErrNotExist) {
		return notFound(ref)
	}
	return internalErr(err)
}

// Destroy kills the sandbox's runner jobs, then moves it out of sight in one rename and deletes it
func (p *Provider) Destroy(_ context.Context, ref provider.Ref) error {
	s, err := p.open(ref)
	if e, ok := provider.AsError(err); ok && e.Code == provider.CodeNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	if err := killJobs(s.jobsDir()); err != nil {
		return internalErr(err)
	}
	if err := killServices(s.fsDir()); err != nil {
		return internalErr(err)
	}
	trash, err := p.scratchDir("trash", ref.ID)
	if err != nil {
		return internalErr(err)
	}
	err = os.Rename(s.dir, filepath.Join(trash, "sandbox"))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		_ = os.RemoveAll(trash)
		return internalErr(err)
	}
	return internalErr(os.RemoveAll(trash))
}

// killJobs ends runner jobs, whose supervisors left the exec's process group through setsid
func killJobs(jobs string) error {
	entries, err := os.ReadDir(jobs)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var errs []error
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(jobs, e.Name())
		// The supervisor writes exit as its last act, after which its pid may be another process's
		_, err := os.Lstat(filepath.Join(dir, jobExitFile))
		spid, ok := readPid(filepath.Join(dir, jobSpidFile))
		if ok && errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, kill(spid))
		}
		if pid, ok := readPid(filepath.Join(dir, jobPidFile)); ok {
			errs = append(errs, kill(-pid))
		}
	}
	return errors.Join(errs...)
}

// killServices ends the gate, forwarder, sshd and sampler setsid started for this sandbox
func killServices(fsDir string) error {
	var errs []error
	for _, name := range services {
		pidFile := filepath.Join(fsDir, filepath.FromSlash(servicePids), name+".pid")
		pid, ok := readPid(pidFile)
		// A pid the system gave another process since never names this sandbox's root
		if !ok || !strings.Contains(commandLine(pid), fsDir) {
			continue
		}
		// Each service leads its own session, so its group holds what it started
		errs = append(errs, kill(-pid), kill(pid))
	}
	return errors.Join(errs...)
}

// commandLine is a process's command line, from /proc where it exists and ps elsewhere
func commandLine(pid int) string {
	if b, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "cmdline")); err == nil {
		return strings.ReplaceAll(string(b), "\x00", " ")
	}
	out, err := exec.Command("ps", "-ww", "-o", "command=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return ""
	}
	return string(out)
}

// kill sends SIGKILL, and EPERM means the pid now belongs to another user
func kill(pid int) error {
	err := syscall.Kill(pid, syscall.SIGKILL)
	if errors.Is(err, syscall.ESRCH) || errors.Is(err, syscall.EPERM) {
		return nil
	}
	return err
}

func readPid(name string) (int, bool) {
	b, err := os.ReadFile(name)
	if err != nil {
		return 0, false
	}
	return parsePid(b)
}

// parsePid accepts only plain decimals above 1 so kill never reaches every process or our group
func parsePid(b []byte) (int, bool) {
	s := strings.TrimSpace(string(b))
	if s == "" || strings.TrimLeft(s, "0123456789") != "" {
		return 0, false
	}
	pid, err := strconv.ParseInt(s, 10, 32)
	if err != nil || pid <= 1 {
		return 0, false
	}
	return int(pid), true
}

// Inspect reads the sandbox, one whose directory is missing being gone
func (p *Provider) Inspect(_ context.Context, ref provider.Ref) (provider.Status, error) {
	s, err := p.open(ref)
	if e, ok := provider.AsError(err); ok && e.Code == provider.CodeNotFound {
		return provider.Status{State: provider.StateGone}, nil
	}
	if err != nil {
		return provider.Status{}, err
	}
	return provider.Status{State: provider.StateRunning, ExpiresAt: s.meta.Deadline}, nil
}

// ListOwned returns this provider's sandboxes carrying every given label
func (p *Provider) ListOwned(
	_ context.Context, labels map[string]string,
) ([]provider.Summary, error) {
	entries, err := os.ReadDir(p.opts.Root)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, internalErr(err)
	}
	var owned []provider.Summary
	for _, e := range entries {
		if !e.IsDir() || !validID(e.Name()) {
			continue
		}
		m, err := readMeta(filepath.Join(p.opts.Root, e.Name()))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, internalErr(err)
		}
		if m.Provider != p.opts.Name || !hasLabels(m.Labels, labels) {
			continue
		}
		owned = append(owned, provider.Summary{
			Ref:       provider.Ref{Provider: p.opts.Name, ID: e.Name(), Meta: portsMeta(m)},
			Labels:    m.Labels,
			CreatedAt: m.CreatedAt,
		})
	}
	return owned, nil
}

func hasLabels(have, want map[string]string) bool {
	for k, v := range want {
		if got, ok := have[k]; !ok || got != v {
			return false
		}
	}
	return true
}

func readMeta(dir string) (meta, error) {
	var m meta
	raw, err := os.ReadFile(filepath.Join(dir, metaFile))
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return m, fmt.Errorf("%s: %w", filepath.Join(dir, metaFile), err)
	}
	return m, nil
}

func writeMeta(dir string, m meta) error {
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return writeAtomic(filepath.Join(dir, metaFile), raw, 0o644)
}

// writeAtomic replaces a file through a rename so no reader sees it half written
func writeAtomic(name string, data []byte, perm fs.FileMode) error {
	f, err := os.CreateTemp(filepath.Dir(name), ".write-*")
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Chmod(perm)
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(f.Name(), name)
	}
	if err != nil {
		_ = os.Remove(f.Name())
	}
	return err
}

/* Files */

func (s sandbox) fsDir() string { return filepath.Join(s.dir, "fs") }

func (s sandbox) home() string { return filepath.Join(s.dir, "fs", "root") }

func (s sandbox) jobsDir() string {
	return filepath.Join(s.fsDir(), filepath.FromSlash(runnerJobsDir))
}

// hostPath maps a sandbox path under fs, relative paths start at HOME and none may leave fs
func (s sandbox) hostPath(name string) (string, error) { return s.resolve(s.home(), name) }

// resolve is hostPath with relative paths starting at the host directory dir
func (s sandbox) resolve(dir, name string) (string, error) {
	if path.IsAbs(name) {
		dir = s.fsDir()
	}
	host := filepath.Join(dir, filepath.FromSlash(name))
	if host != s.fsDir() && !strings.HasPrefix(host, s.fsDir()+string(filepath.Separator)) {
		return "", invalid(fmt.Errorf("path %q leaves the sandbox", name))
	}
	return host, nil
}

func (s sandbox) sandboxPath(host string) string {
	rel, _ := filepath.Rel(s.fsDir(), host)
	return path.Join("/", filepath.ToSlash(rel))
}

// ReadFile reports eof once the returned bytes reach the file's size
func (p *Provider) ReadFile(
	_ context.Context, ref provider.Ref, name string, offset int64, length int,
) ([]byte, bool, error) {
	s, err := p.open(ref)
	if err != nil {
		return nil, false, err
	}
	if offset < 0 || length < 0 {
		return nil, false, invalid(fmt.Errorf("read of %d bytes at offset %d", length, offset))
	}
	host, err := s.hostPath(name)
	if err != nil {
		return nil, false, err
	}
	f, err := os.Open(host)
	if err != nil {
		return nil, false, fileErr(err)
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return nil, false, fileErr(err)
	}
	if info.IsDir() {
		return nil, false, invalid(fmt.Errorf("%s is a directory", name))
	}
	data, err := io.ReadAll(io.NewSectionReader(f, offset, int64(length)))
	if err != nil {
		return nil, false, fileErr(err)
	}
	return data, offset+int64(len(data)) >= info.Size(), nil
}

// WriteFile creates missing parents and replaces the file atomically with mode's permission bits
func (p *Provider) WriteFile(
	_ context.Context, ref provider.Ref, name string, data []byte, mode fs.FileMode,
) error {
	s, err := p.open(ref)
	if err != nil {
		return err
	}
	host, err := s.hostPath(name)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(host), 0o755); err != nil {
		return fileErr(err)
	}
	return fileErr(writeAtomic(host, data, mode.Perm()))
}

// ListFiles returns sandbox paths without following symlinks
func (p *Provider) ListFiles(
	_ context.Context, ref provider.Ref, name string, depth int, into *provider.Listing,
) error {
	s, err := p.open(ref)
	if err != nil {
		return err
	}
	if depth < 1 {
		return invalid(fmt.Errorf("depth %d is below 1", depth))
	}
	root, err := s.hostPath(name)
	if err != nil {
		return err
	}
	info, err := os.Stat(root)
	if err != nil {
		return fileErr(err)
	}
	if !info.IsDir() {
		return invalid(fmt.Errorf("%s is not a directory", name))
	}

	err = filepath.WalkDir(root, func(host string, d fs.DirEntry, err error) error {
		if host == root {
			if err == nil && !into.Visit() {
				return filepath.SkipAll
			}
			return err
		}
		var info fs.FileInfo
		if err == nil {
			info, err = d.Info()
		}
		// Commands in the sandbox may delete files while the walk runs
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if !into.Add(s.entry(host, info)) {
			return filepath.SkipAll
		}
		rel, _ := filepath.Rel(root, host)
		if d.IsDir() && strings.Count(rel, string(filepath.Separator))+1 >= depth {
			return filepath.SkipDir
		}
		if d.IsDir() && !into.Visit() {
			return filepath.SkipAll
		}
		return nil
	})
	return fileErr(err)
}

/* Exec */

// Exec runs argv on the host inside the sandbox's fs with a minimal environment
func (p *Provider) Exec(
	ctx context.Context, ref provider.Ref, spec provider.ExecSpec,
) (provider.ExecStream, error) {
	s, err := p.open(ref)
	if err != nil {
		return nil, err
	}
	if len(spec.Argv) == 0 {
		return nil, invalid(errors.New("exec needs a command"))
	}
	dir := s.home()
	if spec.Cwd != "" {
		if dir, err = s.hostPath(spec.Cwd); err != nil {
			return nil, err
		}
	}
	argv, err := p.command(s, spec.Argv, dir)
	if err != nil {
		return nil, err
	}
	env := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + s.home()}
	for k, v := range spec.Env {
		env = append(env, k+"="+v)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return startStream(ctx, argv, dir, env)
}

// command points the runner at RunnerPath, its paths at the host and its state at this sandbox
func (p *Provider) command(s sandbox, argv []string, dir string) ([]string, error) {
	argv = unsudo(argv)
	if argv[0] != runnerName && argv[0] != runnerSandboxPath {
		return argv, nil
	}
	if p.opts.RunnerPath == "" {
		return nil, invalid(errors.New("bugnet-run has no RunnerPath on this provider"))
	}
	words := rootWords(argv[1:])
	flags := []string{"cwd"}
	if words > 0 {
		flags = append(flags, "spec", "config")
	}
	args, err := s.mapPaths(argv[1:], dir, flags)
	if err != nil {
		return nil, err
	}
	out := append([]string{p.opts.RunnerPath}, args...)
	if len(args) > 0 && slices.Contains(jobCommands, args[0]) && !hasFlag(args[1:], "dir") {
		out = slices.Insert(out, 2, "--dir", s.jobsDir())
	}
	if words > 0 && !hasFlag(args[words:], "root") {
		out = slices.Insert(out, 1+words, "--root", s.fsDir())
	}
	return out, nil
}

// unsudo runs a root command as the host user, since a local sandbox's root is a host directory
func unsudo(argv []string) []string {
	if len(argv) > 2 && argv[0] == "sudo" && argv[1] == "-n" &&
		(argv[2] == runnerName || argv[2] == runnerSandboxPath) {
		return argv[2:]
	}
	return argv
}

// rootWords is how many words name a root command at the start of args, 0 for another command
func rootWords(args []string) int {
	for _, words := range rootCommands {
		if len(args) >= len(words) && slices.Equal(args[:len(words)], words) {
			return len(words)
		}
	}
	return 0
}

// mapPaths maps the runner's own path flags, which end at "--", as Exec maps a cwd from dir
func (s sandbox) mapPaths(args []string, dir string, names []string) ([]string, error) {
	out := slices.Clone(args)
	for i := 0; i < len(out) && out[i] != "--"; i++ {
		name, value, inline := strings.Cut(out[i], "=")
		if !slices.Contains(names, strings.TrimLeft(name, "-")) || !strings.HasPrefix(name, "-") ||
			strings.HasPrefix(name, "---") {
			continue
		}
		at := i
		if !inline {
			if i+1 == len(out) {
				break
			}
			i++
			at, value = i, out[i]
		}
		if value == "" {
			continue
		}
		host, err := s.resolve(dir, value)
		if err != nil {
			return nil, err
		}
		if inline {
			host = name + "=" + host
		}
		out[at] = host
	}
	return out, nil
}

// hasFlag looks only at the runner's own flags, which end at "--"
func hasFlag(args []string, flag string) bool {
	for _, a := range args {
		if a == "--" {
			return false
		}
		if name, _, _ := strings.Cut(a, "="); name == "-"+flag || name == "--"+flag {
			return true
		}
	}
	return false
}

type execStream struct {
	stdout    *os.File
	pgid      int
	cancel    context.CancelFunc
	done      chan struct{}
	code      int
	err       error
	closeOnce sync.Once
	closeErr  error
}

func startStream(ctx context.Context, argv []string, dir string, env []string) (
	*execStream, error,
) {
	ctx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Dir = dir
	cmd.Env = env
	// Its own process group lets cancel reach children that still hold stdout open
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	// A background child still holding stderr must not keep Wait from returning
	cmd.WaitDelay = stderrGrace
	stderr := &cappedBuffer{limit: stderrLimit}
	cmd.Stderr = stderr

	r, w, err := os.Pipe()
	if err != nil {
		cancel()
		return nil, internalErr(err)
	}
	cmd.Stdout = w
	err = cmd.Start()
	_ = w.Close()
	if err != nil {
		cancel()
		_ = r.Close()
		return nil, invalid(err)
	}

	s := &execStream{stdout: r, pgid: cmd.Process.Pid, cancel: cancel, done: make(chan struct{})}
	go func() {
		waitErr := cmd.Wait()
		s.code, s.err = exitStatus(cmd.ProcessState, waitErr, ctx.Err(), argv[0], stderr.buf)
		cancel()
		close(s.done)
	}()
	return s, nil
}

// exitStatus returns the process's own exit code, and an error only when it never produced one
func exitStatus(
	state *os.ProcessState, waitErr, ctxErr error, name string, stderr []byte,
) (int, error) {
	if state != nil && state.Exited() {
		return state.ExitCode(), nil
	}
	if ctxErr != nil {
		return -1, fmt.Errorf("%s killed: %w", name, ctxErr)
	}
	if len(stderr) > 0 {
		return -1, fmt.Errorf("%s: %w, stderr: %s", name, waitErr, stderr)
	}
	return -1, fmt.Errorf("%s: %w", name, waitErr)
}

func (s *execStream) Read(p []byte) (int, error) { return s.stdout.Read(p) }

func (s *execStream) Wait() (int, error) {
	<-s.done
	return s.code, s.err
}

func (s *execStream) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		// The leader may have exited while children it left behind still hold stdout
		_ = kill(-s.pgid)
		<-s.done
		s.closeErr = s.stdout.Close()
	})
	return s.closeErr
}

// cappedBuffer keeps the first limit bytes and drops the rest so the process never blocks
type cappedBuffer struct {
	buf   []byte
	limit int
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	if room := b.limit - len(b.buf); room > 0 {
		b.buf = append(b.buf, p[:min(room, len(p))]...)
	}
	return len(p), nil
}

/* Errors */

func notFound(ref provider.Ref) error {
	return &provider.Error{
		Code: provider.CodeNotFound,
		Err:  fmt.Errorf("sandbox %s/%s does not exist", ref.Provider, ref.ID),
	}
}

func invalid(err error) error {
	return &provider.Error{Code: provider.CodeInvalid, Err: err}
}

// internalErr classifies an unexpected host failure and keeps errors already classified
func internalErr(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := provider.AsError(err); ok {
		return err
	}
	return &provider.Error{Code: provider.CodeInternal, Err: err}
}

// fileErr turns a missing file into not_found, a full disk into no_space and the rest internal
func fileErr(err error) error {
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return &provider.Error{Code: provider.CodeNotFound, Err: err}
	case errors.Is(err, syscall.ENOSPC):
		return &provider.Error{Code: provider.CodeNoSpace, Err: err}
	}
	return internalErr(err)
}
