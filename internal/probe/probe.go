// Package probe starts one sandbox the way a placement does, times each step and removes it, for
// the canary and the benchmark harness.
//
// A probe holds a capacity slot with no margin and records it in probe_slots, takes a create
// token from the provider's bucket, creates the size under labels that mark it a probe with no
// placement, boots it with the saga's own Boot, runs the runner's ping once cold and three times
// warm, writes and reads 4 KiB, destroys it and stores one start sample. A sandbox it leaks is
// destroyed by the sweep at 10 minutes and by its 5-minute lifetime.
package probe

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/health"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

const (
	// Lifetime is how long a probe's sandbox may live at the provider
	Lifetime = 5 * time.Minute
	// stepBudget bounds a probe's steps before its destroy, under the job's 2-minute timeout
	stepBudget = 90 * time.Second
	// destroyBudget bounds the destroy, which runs on past a cancelled probe
	destroyBudget = 30 * time.Second
	// warmRuns is how many warm commands follow the first one
	warmRuns = 3
	// fileSize is what the file write and read move
	fileSize = 4 << 10
	// filePath is where the probe's file lives inside the sandbox
	filePath = "/tmp/bugnet-probe.bin"
)

// Spec is one probe run
type Spec struct {
	Kind     store.SampleKind // canary or bench
	Provider string
	Size     provider.Size
	BenchID  string // empty for canaries
	Run      int
}

// Runner is the part of the saga a probe starts a sandbox with
type Runner interface {
	Boot(
		ctx context.Context, prov provider.Provider, ref provider.Ref, b saga.BootPlan,
	) (provider.Ref, saga.BootReport, error)
	TakeCreate(ctx context.Context, name string) bool
	CreatesPaused(ctx context.Context, name string) bool
	ProviderFailed(ctx context.Context, name string, err error, creating bool) bool
	OwnedLabels() map[string]string
	KeyID() string
}

// Deps are what a probe works with
type Deps struct {
	Registry *provider.Registry
	Store    *store.Store
	Saga     Runner
	// Replica is recorded on each sample
	Replica string
	Now     func() time.Time // time.Now when nil
	Log     *slog.Logger     // slog.Default when nil
}

// Probe runs probes, safe for concurrent use
type Probe struct{ d Deps }

// New builds a probe over d
func New(d Deps) *Probe {
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.Log == nil {
		d.Log = slog.Default()
	}
	return &Probe{d: d}
}

// Run stores and returns one sample. It reports false, and writes nothing, when the provider has
// no free slot, no create token or a paused create, or when a canary would pay for a start.
func (p *Probe) Run(ctx context.Context, s Spec) (store.StartSample, bool, error) {
	prov, ok := p.d.Registry.Get(s.Provider)
	if !ok {
		return store.StartSample{}, false, fmt.Errorf("probe: unknown provider %q", s.Provider)
	}
	if s.Kind == store.SampleCanary && !StartFree(prov, s.Size) {
		return store.StartSample{}, false, nil
	}
	runID := uuid.New()
	slot, err := p.d.Store.ReserveProbeSlot(ctx, s.Provider, runID, p.d.Now())
	if err != nil {
		return store.StartSample{}, false, fmt.Errorf("probe: reserve a slot: %w", err)
	}
	if !slot {
		return store.StartSample{}, false, nil
	}
	defer p.release(ctx, s.Provider, runID)
	if p.d.Saga.CreatesPaused(ctx, s.Provider) || !p.d.Saga.TakeCreate(ctx, s.Provider) {
		return store.StartSample{}, false, nil
	}
	m := p.run(ctx, prov, s, runID)
	if err := p.d.Store.InsertStartSample(context.WithoutCancel(ctx), m); err != nil {
		return m, true, fmt.Errorf("probe: store the sample: %w", err)
	}
	return m, true, nil
}

// StartFree reports whether the next start of size costs nothing
func StartFree(prov provider.Provider, size provider.Size) bool {
	c, ok := prov.(provider.StartCoster)
	if !ok {
		return true
	}
	fee, ok := c.StartCostMicros(size)
	return ok && fee == 0
}

// release frees the probe's slot even when the probe's own context is gone
func (p *Probe) release(ctx context.Context, name string, id uuid.UUID) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), destroyBudget)
	defer cancel()
	if err := p.d.Store.ReleaseProbeSlot(ctx, name, id); err != nil {
		p.d.Log.Error("cannot release a probe slot", "provider", name, "err", err)
	}
}

// run does steps 3 to 6 and builds the sample, its outcome from the first step that failed
func (p *Probe) run(
	ctx context.Context, prov provider.Provider, s Spec, runID uuid.UUID,
) store.StartSample {
	started := p.d.Now()
	m := store.StartSample{
		Kind: s.Kind, BenchID: s.BenchID, Provider: s.Provider, SizeName: s.Size.Name,
		StartedAt: started.UTC(), Replica: p.d.Replica,
		CreateMS: store.NoStep, ReadyMS: store.NoStep, RunnerMS: store.NoStep,
		GateMS: store.NoStep, FirstCmdMS: store.NoStep, WarmCmdMS: store.NoStep,
		DestroyMS: store.NoStep,
	}
	steps, cancel := context.WithTimeout(ctx, stepBudget)
	defer cancel()
	labels := p.d.Saga.OwnedLabels()
	labels["bugnet_probe"] = string(s.Kind)
	labels["probe"] = runID.String()
	ref, err := prov.Create(steps, provider.CreateRequest{
		IdempotencyKey: "probe-" + runID.String(), Size: s.Size, Labels: labels,
		Lifetime: Lifetime,
	})
	m.CreateMS = ms(p.d.Now().Sub(started))
	m.SandboxID = ref.ID
	if err != nil {
		p.d.Saga.ProviderFailed(ctx, s.Provider, err, true)
		return p.finish(m, prov, s.Size, started, err)
	}
	err = p.exercise(steps, prov, ref, &m)
	if err != nil {
		p.d.Saga.ProviderFailed(ctx, s.Provider, err, false)
	}
	destroyed := p.d.Now()
	dctx, dcancel := context.WithTimeout(context.WithoutCancel(ctx), destroyBudget)
	derr := prov.Destroy(dctx, ref)
	dcancel()
	if e, ok := provider.AsError(derr); ok && e.Code == provider.CodeNotFound {
		derr = nil
	}
	m.DestroyMS = ms(p.d.Now().Sub(destroyed))
	if derr != nil {
		p.d.Log.Error("cannot destroy a probe sandbox, the sweep will", "provider", s.Provider,
			"sandbox", ref.ID, "err", derr)
		if err == nil {
			err = fmt.Errorf("destroy: %w", derr)
		}
	}
	return p.finish(m, prov, s.Size, started, err)
}

// finish sets the outcome and the estimated cost of a probe that took the time since started
func (p *Probe) finish(
	m store.StartSample, prov provider.Provider, size provider.Size, started time.Time, err error,
) store.StartSample {
	m.Outcome, m.ErrorCode = health.Outcome(err)
	if err != nil {
		m.Error = err.Error()
	}
	m.CostMicros = health.StartCost(prov, size, p.d.Now().Sub(started))
	return m
}

// exercise boots the sandbox and times its first and warm commands and a 4 KiB round trip
func (p *Probe) exercise(
	ctx context.Context, prov provider.Provider, ref provider.Ref, m *store.StartSample,
) error {
	plan := saga.BootPlan{
		Placement: uuid.New(), KeyID: p.d.Saga.KeyID(),
		NeedPreview: p.d.Registry.Capabilities(prov).Preview,
	}
	ref, rep, err := p.d.Saga.Boot(ctx, prov, ref, plan)
	if rep.ReadyMS >= 0 {
		m.ReadyMS = m.CreateMS + rep.ReadyMS
	}
	m.RunnerMS, m.GateMS = rep.RunnerMS, rep.GateMS
	if err != nil {
		return fmt.Errorf("boot: %w", err)
	}
	var warm []int
	for i := 0; i <= warmRuns; i++ {
		t := p.d.Now()
		if err := ping(ctx, prov, ref); err != nil {
			return fmt.Errorf("ping: %w", err)
		}
		if i == 0 {
			m.FirstCmdMS = ms(p.d.Now().Sub(t))
			continue
		}
		warm = append(warm, ms(p.d.Now().Sub(t)))
	}
	slices.Sort(warm)
	m.WarmCmdMS = warm[len(warm)/2]
	return roundTrip(ctx, prov, ref)
}

// ping runs the runner's ping and waits for it to exit 0
func ping(ctx context.Context, prov provider.Provider, ref provider.Ref) error {
	stream, err := prov.Exec(ctx, ref, provider.ExecSpec{Argv: []string{saga.RunnerPath, "ping"}})
	if err != nil {
		return err
	}
	defer func() { _ = stream.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(stream, fileSize))
	code, err := stream.Wait()
	if err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("exit code %d", code)
	}
	return nil
}

// roundTrip writes 4 KiB and reads them back
func roundTrip(ctx context.Context, prov provider.Provider, ref provider.Ref) error {
	data := make([]byte, fileSize)
	for i := range data {
		data[i] = byte(i)
	}
	if err := prov.WriteFile(ctx, ref, filePath, data, 0o600); err != nil {
		return fmt.Errorf("write file: %w", err)
	}
	got, _, err := prov.ReadFile(ctx, ref, filePath, 0, fileSize)
	if err != nil {
		return fmt.Errorf("read file: %w", err)
	}
	if !slices.Equal(got, data) {
		return errors.New("the file read back differs")
	}
	return nil
}

func ms(d time.Duration) int { return int(d.Milliseconds()) }
