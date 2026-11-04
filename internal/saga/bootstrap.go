package saga

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

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
	path := s.d.Runners[arch]
	if path == "" {
		return fmt.Errorf("%w for %s", errNoRunnerBuild, arch)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("%w for %s: %w", errNoRunnerBuild, arch, err)
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
