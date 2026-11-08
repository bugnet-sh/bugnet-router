package saga

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/oynozan/bugnet-router/internal/provider"
)

// runnerPath is where bugnet-run lives inside every sandbox
const runnerPath = "/tmp/bugnet/bin/bugnet-run"

const (
	readyTimeout  = 60 * time.Second
	uploadTimeout = 60 * time.Second
)

// errNoRunnerBuild means the router has no bugnet-run build for the sandbox's architecture
var errNoRunnerBuild = errors.New("no bugnet-run build")

// BootPlan says what one sandbox is booted for
type BootPlan struct {
	Placement            uuid.UUID
	KeyID                string
	NeedPreview, NeedSSH bool
	Restricted           []string // the package's egress.allow
}

// BootReport is what a boot did, which start samples record
type BootReport struct {
	GateReady        bool
	RunnerMS, GateMS int
	// GateReadyAt is when the gate first answered, zero when it never did
	GateReadyAt time.Time
	// CAVersion is the Bugnet CA version the sandbox trusts, 0 for none
	CAVersion int
}

// errRestricted means a restricted package reached a provider without egress control
var errRestricted = errors.New("a restricted package needs a provider with egress control")

// Boot waits for the sandbox, puts the runner in place, runs setup and harden, and opens the gate
func (s *Saga) Boot(ctx context.Context, prov provider.Provider, ref provider.Ref,
	b BootPlan) (provider.Ref, BootReport, error) {
	var rep BootReport
	if len(b.Restricted) > 0 && !prov.Capabilities().EgressControl {
		return ref, rep, errRestricted
	}
	start := s.now()
	if err := s.boot(ctx, prov, ref); err != nil {
		return ref, rep, err
	}
	rep.RunnerMS = int(s.now().Sub(start).Milliseconds())
	key, err := s.d.Keys.GateKey(b.KeyID, b.Placement)
	if err != nil {
		return ref, rep, fmt.Errorf("gate key: %w", err)
	}
	// The sampler starts last, so harden's and ssh-up's installs never count as the user's disk
	ans, err := s.setup(ctx, prov, ref, key, false)
	if err != nil {
		return ref, rep, fmt.Errorf("setup: %w", err)
	}
	if slices.Contains(caTrusted, ans.CA) {
		rep.CAVersion = s.d.Boot.CAVersion
	}
	s.d.Log.Info("sandbox set up", "placement", b.Placement, "provider", prov.Name(),
		"ca", ans.CA, "init", ans.Init)
	if required, err := s.harden(ctx, prov, ref); err != nil {
		if required {
			return ref, rep, fmt.Errorf("harden: %w", err)
		}
		s.d.Log.Warn("harden failed where it is not required", "placement", b.Placement,
			"provider", prov.Name(), "err", err)
	}
	gateStart := s.now()
	ref, at, err := s.openGate(ctx, prov, ref, key)
	rep.GateMS = int(s.now().Sub(gateStart).Milliseconds())
	switch {
	case err == nil:
		rep.GateReady, rep.GateReadyAt = true, at
	case b.NeedPreview || b.NeedSSH:
		return ref, rep, fmt.Errorf("gate: %w", err)
	default:
		s.d.Log.Warn("gate not ready, the order needs neither previews nor SSH",
			"placement", b.Placement, "provider", prov.Name(), "err", err)
	}
	if _, err := s.setup(ctx, prov, ref, key, true); err != nil {
		s.d.Log.Warn("setup with the sampler", "placement", b.Placement,
			"provider", prov.Name(), "err", err)
	}
	return ref, rep, nil
}

// boot waits for a created sandbox and puts the runner in place when the provider lacks it
func (s *Saga) boot(ctx context.Context, prov provider.Provider, ref provider.Ref) error {
	readyCtx, cancel := context.WithTimeout(ctx, readyTimeout)
	defer cancel()
	if err := prov.WaitReady(readyCtx, ref); err != nil {
		return fmt.Errorf("wait ready: %w", err)
	}
	if !prov.Capabilities().NeedsRunnerUpload {
		return nil
	}
	return s.uploadRunner(ctx, prov, ref)
}

// uploadRunner writes the build for the sandbox's architecture and checks that it runs there
func (s *Saga) uploadRunner(ctx context.Context, prov provider.Provider, ref provider.Ref) error {
	arch := "amd64"
	if ref.Meta["arch"] == "arm64" {
		arch = "arm64"
	}
	data := s.d.Runners[arch]
	if len(data) == 0 {
		return fmt.Errorf("%w for %s", errNoRunnerBuild, arch)
	}
	ctx, cancel := context.WithTimeout(ctx, uploadTimeout)
	defer cancel()
	if err := prov.WriteFile(ctx, ref, runnerPath, data, 0o755); err != nil {
		return fmt.Errorf("upload runner: %w", err)
	}
	stream, err := prov.Exec(ctx, ref, provider.ExecSpec{Argv: []string{runnerPath, "ping"}})
	if err != nil {
		return fmt.Errorf("runner ping: %w", err)
	}
	defer func() { _ = stream.Close() }()
	if _, err := io.Copy(io.Discard, stream); err != nil {
		return fmt.Errorf("runner ping output: %w", err)
	}
	code, err := stream.Wait()
	if err != nil {
		return fmt.Errorf("runner ping: %w", err)
	}
	if code != 0 {
		return fmt.Errorf("runner ping exited %d", code)
	}
	return nil
}
