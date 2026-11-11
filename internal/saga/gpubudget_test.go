package saga

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/gpu"
	"github.com/bugnet-sh/bugnet-router/internal/image"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

// slowestGPU takes each call to the end of its context, failing where Lium's slowest failing boot
// fails, and logs every call with how long it took on the bubble's clock
type slowestGPU struct {
	provider.Provider // nil, so a call the boot should not make panics
	name              string
	manifest          []byte
	mu                *sync.Mutex
	log               *[]string
}

func (p slowestGPU) Name() string { return p.name }

func (p slowestGPU) Capabilities() provider.Capabilities {
	return provider.Capabilities{GPU: true, NeedsRunnerUpload: true, MaxLifetime: 24 * time.Hour}
}

// note logs a call that began at start
func (p slowestGPU) note(call string, start time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	took := time.Since(start).Round(time.Second)
	*p.log = append(*p.log, fmt.Sprintf("%s %s %s", p.name, call, took))
}

// last waits until a millisecond before ctx's deadline, the last moment a step still succeeds
func last(ctx context.Context) {
	if dl, ok := ctx.Deadline(); ok {
		time.Sleep(time.Until(dl) - time.Millisecond)
	}
}

func (p slowestGPU) Create(ctx context.Context, _ provider.CreateRequest) (provider.Ref, error) {
	defer p.note("create", time.Now())
	select {
	case <-time.After(config.GPUCreateBudget):
		return provider.Ref{ID: p.name + "-1", Meta: map[string]string{"arch": "amd64"}}, nil
	case <-ctx.Done():
		return provider.Ref{}, ctx.Err()
	}
}

func (p slowestGPU) WaitReady(ctx context.Context, _ provider.Ref) error {
	defer p.note("ready", time.Now())
	last(ctx)
	return nil
}

func (p slowestGPU) ReadFile(
	_ context.Context, _ provider.Ref, path string, _ int64, _ int,
) ([]byte, bool, error) {
	if path != image.ManifestPath {
		return nil, false, fmt.Errorf("read of %s", path)
	}
	return p.manifest, true, nil
}

func (p slowestGPU) WriteFile(
	context.Context, provider.Ref, string, []byte, fs.FileMode,
) error {
	return nil
}

// Exec never answers the baked runner's ping and fails harden as its timeout ends, while the
// uploaded runner's ping and setup answer at their last moment
func (p slowestGPU) Exec(
	ctx context.Context, _ provider.Ref, spec provider.ExecSpec,
) (provider.ExecStream, error) {
	start := time.Now()
	call := spec.Argv[1]
	if spec.Argv[0] == image.RunnerPath {
		call = "baked " + call
	}
	defer p.note(call, start)
	switch call {
	case "baked ping", "harden":
		<-ctx.Done()
		return nil, ctx.Err()
	case "ping":
		last(ctx)
		return &answered{}, nil
	case "setup":
		last(ctx)
		return &answered{Reader: strings.NewReader(`{"ca":"checked"}`)}, nil
	}
	return nil, fmt.Errorf("exec of %v", spec.Argv)
}

// Destroy takes the whole cleanup step the saga gives it
func (p slowestGPU) Destroy(ctx context.Context, _ provider.Ref) error {
	defer p.note("destroy", time.Now())
	<-ctx.Done()
	return ctx.Err()
}

// answered is a finished command that printed its reader and exited 0
type answered struct{ io.Reader }

func (a *answered) Read(b []byte) (int, error) {
	if a.Reader == nil {
		return 0, io.EOF
	}
	return a.Reader.Read(b)
}

func (*answered) Wait() (int, error) { return 0, nil }
func (*answered) Close() error       { return nil }

// TestGPUBudgetCoversLiumWorstCase runs Lium's slowest boot that fails on the bubble's fake clock,
// every step to its timeout and harden failing last, then the destroy of its sandbox, and shows
// that HF's create still starts and ends inside the GPU placing budget of an empty settings file
func TestGPUBudgetCoversLiumWorstCase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "router.toml")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	runner := []byte("bugnet-run build")
	sum := sha256.Sum256(runner)
	manifest := []byte(`{"v":1,"version":"2026.11.3","channel":"prod","target":"oci",` +
		`"os":"ubuntu-24.04","built_at":"2026-11-03T00:00:00Z","runner_sha256":{"amd64":"` +
		hex.EncodeToString(sum[:]) + `"}}`)
	gpuPkg := catalog.Package{ID: "gpu-t4-10m", Minutes: 10, PriceMicros: 180_000,
		GPUModel: gpu.T4, Resources: provider.Resources{
			VCPU: 3, MemoryMiB: 12 << 10, DiskGiB: 40, GPUCount: 1, GPUMemoryGiB: 16,
			GPUModel: gpu.T4,
		}}
	synctest.Test(t, func(t *testing.T) {
		mu, log := &sync.Mutex{}, &[]string{}
		lium := slowestGPU{name: "lium", manifest: manifest, mu: mu, log: log}
		hf := slowestGPU{name: "hf", mu: mu, log: log}
		reg, err := provider.NewRegistry([]provider.Provider{lium, hf}, provider.Features{
			Images: map[string]string{"lium": "2026.11.3"},
		})
		if err != nil {
			t.Fatal(err)
		}
		_, keys := testenv.DataPlaneKeys(t)
		harden := map[string]config.HardenRule{"lium": {Required: true}}
		s := New(Deps{
			Registry: reg, Keys: keys, Runners: map[string][]byte{"amd64": runner},
			Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Boot: BootSettings{Harden: harden},
			ReadyTimeouts: cfg.ReadyTimeouts, GPUPlacingBudget: cfg.GPUPlacingBudget,
		})
		ctx := t.Context()
		start := time.Now()
		budget, cancel := context.WithTimeout(ctx, s.placingBudgetFor(gpuPkg))
		defer cancel()

		// Lium's attempt as attemptCold runs it, its create under the budget and its boot past it
		ref, err := s.create(budget, lium, provider.CreateRequest{})
		if err != nil {
			t.Fatalf("lium's create: %v", err)
		}
		_, _, err = s.Boot(ctx, lium, ref, BootPlan{
			Placement: uuid.New(), KeyID: keys.Current().ID,
		})
		if err == nil || !strings.HasPrefix(err.Error(), "harden: ") {
			t.Fatalf("lium's boot ended with %v, want harden's failure", err)
		}
		dctx, dcancel := step(ctx)
		_ = destroyRef(dctx, lium, ref)
		dcancel()
		failed := time.Since(start)

		// The next candidate starts only while the budget has room, and creates under it
		if budget.Err() != nil {
			t.Fatalf("the budget ran out %s into placing, before hf's turn", failed)
		}
		if _, err := s.create(budget, hf, provider.CreateRequest{}); err != nil {
			t.Fatalf("hf's create, started %s into a %s budget: %v", failed,
				cfg.GPUPlacingBudget, err)
		}
		want := []string{
			"lium create 20s", "lium ready 6m0s", "lium baked ping 1m0s", "lium ping 1m0s",
			"lium setup 1m30s", "lium harden 2m0s", "lium destroy 30s", "hf create 20s",
		}
		if !slices.Equal(*log, want) {
			t.Fatalf("calls %q, want %q", *log, want)
		}
		t.Logf("lium failed %s into placing and hf had created by %s, of a %s budget", failed,
			time.Since(start), cfg.GPUPlacingBudget)
	})
}
