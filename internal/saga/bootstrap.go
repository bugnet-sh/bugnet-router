package saga

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// runnerPath is where bugnet-run lives inside every sandbox
const runnerPath = "/tmp/bugnet/bin/bugnet-run"

// RunnerPath is runnerPath for a probe that runs the runner's ping
const RunnerPath = runnerPath

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
	// Pool runs setup's pool phase and harden, then stops before every step that needs an id
	Pool bool
}

// BootReport is what a boot did, which start samples record
type BootReport struct {
	GateReady bool
	// ReadyMS is the wait for the provider to answer, RunnerMS the runner upload and ping and
	// GateMS the gate steps, each store.NoStep until its step ran
	ReadyMS, RunnerMS, GateMS int
	// GateReadyAt is when the gate first answered, zero when it never did
	GateReadyAt time.Time
	// CAVersion is the Bugnet CA version the sandbox trusts, 0 for none
	CAVersion int
	// SSH is the sshd ssh-up made ready, nil when the order needs no SSH
	SSH *store.SSHHost
}

// Prepared is what Prepare learned that the ready writes store
type Prepared struct {
	// Ref carries the gate route's Meta entries, kept only in provider_ref
	Ref   provider.Ref
	KeyID string
	// GateReadyAt is zero when the gate never answered
	GateReadyAt time.Time
	SSHHost     *store.SSHHost
	// CAVersion is secrets.ca_version when setup installed, checked or appended it, else 0
	CAVersion int
	// GateMS is the time the gate steps took, store.NoStep when they did not run
	GateMS int
}

// errRestricted means a restricted package reached a provider without egress control
var errRestricted = errors.New("a restricted package needs a provider with egress control")

// Boot waits for a created sandbox, puts the runner in place, runs setup and harden, then Prepare
//
// With Pool set it stops after harden, as setup's pool phase left it, and Prepare runs once the
// sandbox has a placement. Every path that calls Prepare itself runs Boot with Pool set first, or
// none at all for a parked member, so no sandbox runs Prepare twice.
func (s *Saga) Boot(ctx context.Context, prov provider.Provider, ref provider.Ref,
	b BootPlan) (provider.Ref, BootReport, error) {
	rep := BootReport{ReadyMS: store.NoStep, RunnerMS: store.NoStep, GateMS: store.NoStep}
	if len(b.Restricted) > 0 && !prov.Capabilities().EgressControl {
		return ref, rep, errRestricted
	}
	if err := s.boot(ctx, prov, ref, &rep); err != nil {
		return ref, rep, err
	}
	var ans runnerAnswer
	var key string
	var err error
	if b.Pool {
		ans, err = s.setupPool(ctx, prov, ref)
	} else {
		if key, err = s.d.Keys.GateKey(b.KeyID, b.Placement); err != nil {
			return ref, rep, fmt.Errorf("gate key: %w", err)
		}
		ans, err = s.setup(ctx, prov, ref, key, false)
	}
	if err != nil {
		return ref, rep, fmt.Errorf("setup: %w", err)
	}
	if slices.Contains(caTrusted, ans.CA) {
		rep.CAVersion = s.d.Boot.CAVersion
	}
	s.d.Log.Info("sandbox set up", "placement", b.Placement, "provider", prov.Name(),
		"ca", ans.CA, "init", ans.Init, "reseed", ans.Reseed, "pool", b.Pool)
	if required, err := s.harden(ctx, prov, ref); err != nil {
		if required {
			return ref, rep, fmt.Errorf("harden: %w", err)
		}
		s.d.Log.Warn("harden failed where it is not required", "placement", b.Placement,
			"provider", prov.Name(), "err", err)
	}
	if b.Pool {
		return ref, rep, nil
	}
	prep, err := s.prepare(ctx, prov, ref, b.KeyID, b.Placement, placement.Requirements{
		Preview: b.NeedPreview, SSH: b.NeedSSH,
	}, false, rep.CAVersion)
	rep.GateMS = prep.GateMS
	rep.GateReady, rep.GateReadyAt = !prep.GateReadyAt.IsZero(), prep.GateReadyAt
	rep.SSH = prep.SSHHost
	return prep.Ref, rep, err
}

// Prepare runs the start steps that need the placement id, setup's full phase first when pooled
//
// Pooled, setup runs again in its full phase with a fresh seed, the gate key and the CA fields
// of a cold start. Otherwise Boot ran that setup already and Prepare starts at the gate's route.
// Either way its CA answer fills Prepared.CAVersion.
func (s *Saga) Prepare(
	ctx context.Context, prov provider.Provider, ref provider.Ref, id uuid.UUID,
	req placement.Requirements, pooled bool,
) (Prepared, error) {
	return s.prepare(ctx, prov, ref, s.keyID, id, req, pooled, 0)
}

// prepare is Prepare with the key id the placement was created under and, for a sandbox that
// Boot set up, the CA version that setup reported
func (s *Saga) prepare(
	ctx context.Context, prov provider.Provider, ref provider.Ref, keyID string, id uuid.UUID,
	req placement.Requirements, pooled bool, caVersion int,
) (Prepared, error) {
	prep := Prepared{Ref: ref, KeyID: keyID, CAVersion: caVersion, GateMS: store.NoStep}
	key, err := s.d.Keys.GateKey(keyID, id)
	if err != nil {
		return prep, fmt.Errorf("gate key: %w", err)
	}
	if pooled {
		ans, err := s.setup(ctx, prov, ref, key, false)
		if err != nil {
			return prep, fmt.Errorf("setup: %w", err)
		}
		if slices.Contains(caTrusted, ans.CA) {
			prep.CAVersion = s.d.Boot.CAVersion
		}
		s.d.Log.Info("pooled sandbox set up", "placement", id, "provider", prov.Name(),
			"ca", ans.CA, "init", ans.Init, "reseed", ans.Reseed)
	}
	gateStart := s.now()
	ref, at, err := s.openGate(ctx, prov, ref, key)
	prep.GateMS = int(s.now().Sub(gateStart).Milliseconds())
	prep.Ref = ref
	switch {
	case err == nil:
		prep.GateReadyAt = at
	case req.Preview || req.SSH:
		return prep, fmt.Errorf("gate: %w", err)
	default:
		s.d.Log.Warn("gate not ready, the order needs neither previews nor SSH",
			"placement", id, "provider", prov.Name(), "err", err)
	}
	if req.SSH && !s.d.Registry.NativeSSH(prov) {
		host, err := s.sshUp(ctx, prov, ref, id, nil, pooled)
		if err != nil {
			return prep, fmt.Errorf("ssh-up: %w", err)
		}
		prep.SSHHost = &host
	}
	// The sampler starts last, so harden's and ssh-up's installs never count as the user's disk
	if _, err := s.setup(ctx, prov, ref, key, true); err != nil {
		s.d.Log.Warn("setup with the sampler", "placement", id,
			"provider", prov.Name(), "err", err)
	}
	return prep, nil
}

// boot waits for a created sandbox and puts the runner in place when the provider lacks it
func (s *Saga) boot(
	ctx context.Context, prov provider.Provider, ref provider.Ref, rep *BootReport,
) error {
	readyCtx, cancel := context.WithTimeout(ctx, readyTimeout)
	defer cancel()
	start := s.now()
	err := prov.WaitReady(readyCtx, ref)
	rep.ReadyMS = int(s.now().Sub(start).Milliseconds())
	if err != nil {
		return fmt.Errorf("wait ready: %w", err)
	}
	if !prov.Capabilities().NeedsRunnerUpload {
		return nil
	}
	start = s.now()
	err = s.uploadRunner(ctx, prov, ref)
	rep.RunnerMS = int(s.now().Sub(start).Milliseconds())
	return err
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
