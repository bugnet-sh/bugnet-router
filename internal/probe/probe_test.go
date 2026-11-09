package probe_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/probe"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

// runnerBin is the host build of bugnet-run every local sandbox in these tests runs
var runnerBin string

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	dir, err := os.MkdirTemp("", "bugnet-run-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer func() { _ = os.RemoveAll(dir) }()
	runnerBin = filepath.Join(dir, "bugnet-run")
	build := exec.Command("go", "build", "-o", runnerBin,
		"github.com/bugnet-sh/bugnet-run/cmd/bugnet-run")
	build.Stdout, build.Stderr = os.Stderr, os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "build bugnet-run:", err)
		return 1
	}
	return m.Run()
}

func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

var small = provider.Size{
	Name: "small", Resources: provider.Resources{VCPU: 1, MemoryMiB: 512, DiskGiB: 1},
}

// spy is a local provider that records what the probe asked of it
type spy struct {
	*local.Provider
	mu      sync.Mutex
	creates []provider.CreateRequest
	files   map[string][]byte
	// fee is the start fee StartCostMicros reports, none when negative
	fee int64
	// answers fakes a runner subcommand's JSON answer and exit code
	answers map[string]string
	codes   map[string]int
}

func (s *spy) Create(ctx context.Context, req provider.CreateRequest) (provider.Ref, error) {
	s.mu.Lock()
	s.creates = append(s.creates, req)
	s.mu.Unlock()
	return s.Provider.Create(ctx, req)
}

func (s *spy) WriteFile(
	ctx context.Context, ref provider.Ref, path string, data []byte, mode os.FileMode,
) error {
	s.mu.Lock()
	if s.files == nil {
		s.files = map[string][]byte{}
	}
	s.files[path] = slices.Clone(data)
	s.mu.Unlock()
	return s.Provider.WriteFile(ctx, ref, path, data, mode)
}

func (s *spy) Exec(
	ctx context.Context, ref provider.Ref, spec provider.ExecSpec,
) (provider.ExecStream, error) {
	argv := spec.Argv
	if len(argv) > 2 && argv[0] == "sudo" {
		argv = argv[2:]
	}
	if len(argv) > 1 && argv[0] == saga.RunnerPath {
		if out, ok := s.answers[argv[1]]; ok {
			return s.Provider.Exec(ctx, ref, provider.ExecSpec{Argv: []string{
				"sh", "-c", `printf '%s\n' "$1"; exit "$2"`, "sh", out,
				fmt.Sprint(s.codes[argv[1]]),
			}})
		}
	}
	return s.Provider.Exec(ctx, ref, spec)
}

// costly is a spy that charges a fee for a start, as Boat does
type costly struct{ *spy }

func (c costly) StartCostMicros(provider.Size) (int64, bool) { return c.fee, true }

type rig struct {
	probe *probe.Probe
	store *store.Store
	vk    valkey.Client
	pfx   string
	prov  *spy
	saga  *saga.Saga
}

func newRig(t *testing.T, name string, opts func(*local.Options), boot saga.BootSettings) *rig {
	t.Helper()
	return newRigOf(t, name, opts, boot, func(s *spy) provider.Provider { return s })
}

func newRigOf(
	t *testing.T, name string, opts func(*local.Options), boot saga.BootSettings,
	wrap func(*spy) provider.Provider,
) *rig {
	t.Helper()
	ctx := context.Background()
	url := testenv.DatabaseURL(t)
	if err := store.Migrate(ctx, url); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	vk, prefix := testenv.Valkey(t)
	rc, err := river.NewClient(riverpgxv5.New(pool), &river.Config{})
	if err != nil {
		t.Fatal(err)
	}
	lo := local.Options{
		Name: name, Root: t.TempDir(), RunnerPath: runnerBin, Sizes: []provider.Size{small},
		HourlyCostMicros: 1000, Increment: time.Second,
	}
	if opts != nil {
		opts(&lo)
	}
	sp := &spy{Provider: local.New(lo)}
	t.Cleanup(func() {
		owned, _ := sp.ListOwned(context.Background(), nil)
		for _, o := range owned {
			_ = sp.Destroy(context.Background(), o.Ref)
		}
	})
	reg, err := provider.NewRegistry([]provider.Provider{wrap(sp)}, provider.Features{})
	if err != nil {
		t.Fatal(err)
	}
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, keys := testenv.DataPlaneKeys(t)
	st := store.New(pool, store.WithRefSealer(keys), store.WithLogger(discard))
	tracker := placement.NewTracker(time.Now)
	sg := saga.New(saga.Deps{
		Store: st, Engine: placement.New(reg, tracker), Registry: reg, River: rc, Valkey: vk,
		Prefix: prefix, Tracker: tracker, ReplicaID: "replica-test", Log: discard, Keys: keys,
		Boot: boot,
	})
	t.Cleanup(sg.Wait)
	p := probe.New(probe.Deps{
		Registry: reg, Store: st, Saga: sg, Replica: "replica-test", Log: discard,
	})
	return &rig{probe: p, store: st, vk: vk, pfx: prefix, prov: sp, saga: sg}
}

func (r *rig) samples(t *testing.T, name string) []store.StartSample {
	t.Helper()
	got, err := r.store.StartSamples(t.Context(), name, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func (r *rig) slots(t *testing.T, name string) (rows, inUse int) {
	t.Helper()
	rows, err := r.store.ProbeSlotCount(t.Context(), name)
	if err != nil {
		t.Fatal(err)
	}
	caps, err := r.store.Capacities(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range caps {
		if c.Provider == name {
			inUse = c.InUse
		}
	}
	return rows, inUse
}

func canary(name string) probe.Spec {
	return probe.Spec{Kind: store.SampleCanary, Provider: name, Size: small}
}

// local provider, sandbox id, probe_slots row gone
func TestProbeWritesAFullSample(t *testing.T) {
	r := newRig(t, "local", nil, saga.BootSettings{})
	m, ran, err := r.probe.Run(t.Context(), canary("local"))
	if err != nil || !ran {
		t.Fatalf("Run = %v, %v", ran, err)
	}
	eq(t, m.Outcome, "ok")
	eq(t, m.Kind, store.SampleCanary)
	eq(t, m.SizeName, "small")
	eq(t, m.Replica, "replica-test")
	if m.SandboxID == "" || m.ErrorCode != "" || m.Error != "" {
		t.Fatalf("sample %+v, want a sandbox id and no error", m)
	}
	// The local runner is in place, so only the wait and the gate steps are timed
	for name, ms := range map[string]int{
		"create": m.CreateMS, "destroy": m.DestroyMS, "first": m.FirstCmdMS,
		"warm": m.WarmCmdMS, "gate": m.GateMS,
	} {
		if ms < 0 {
			t.Errorf("%s was not timed: %+v", name, m)
		}
	}
	if m.ReadyMS < m.CreateMS {
		t.Errorf("ready_ms %d is before create_ms %d", m.ReadyMS, m.CreateMS)
	}
	if m.CostMicros <= 0 {
		t.Errorf("cost_micros %d, want the rate for the seconds it ran", m.CostMicros)
	}

	got := r.samples(t, "local")
	if len(got) != 1 {
		t.Fatalf("samples %+v, want one", got)
	}
	m.ID = got[0].ID
	eq(t, got[0], m)

	// Nothing is left: no row, no slot, no sandbox
	rows, inUse := r.slots(t, "local")
	eq(t, rows, 0)
	eq(t, inUse, 0)
	owned, _ := r.prov.ListOwned(t.Context(), nil)
	eq(t, len(owned), 0)
}

func TestProbeCreatesWithTheProbeLabelsAndNoPlacement(t *testing.T) {
	r := newRig(t, "local", nil, saga.BootSettings{})
	if _, ran, err := r.probe.Run(t.Context(), canary("local")); err != nil || !ran {
		t.Fatalf("Run = %v, %v", ran, err)
	}
	if len(r.prov.creates) != 1 {
		t.Fatalf("creates %+v, want one", r.prov.creates)
	}
	req := r.prov.creates[0]
	run := req.Labels["probe"]
	eq(t, req.IdempotencyKey, "probe-"+run)
	eq(t, req.Labels, map[string]string{
		"bugnet": "1", "bugnet_deployment": "main", "bugnet_probe": "canary", "probe": run,
	})
	eq(t, req.Lifetime, 5*time.Minute)
	eq(t, req.Size, small)
}

func TestProbeSkipsWithoutASlotAndWritesNothing(t *testing.T) {
	r := newRig(t, "local", nil, saga.BootSettings{})
	if err := r.store.SetCapacity(t.Context(), "local", 1); err != nil {
		t.Fatal(err)
	}
	if ok, err := r.store.ReserveCapacity(t.Context(), "local", 0); err != nil || !ok {
		t.Fatal(ok, err)
	}
	_, ran, err := r.probe.Run(t.Context(), canary("local"))
	eq(t, ran, false)
	eq(t, err, nil)
	eq(t, len(r.samples(t, "local")), 0)
	eq(t, len(r.prov.creates), 0)
	rows, inUse := r.slots(t, "local")
	eq(t, rows, 0)
	eq(t, inUse, 1)
}

func TestProbeSkipsAPausedProviderAndFreesItsSlot(t *testing.T) {
	r := newRig(t, "local", nil, saga.BootSettings{})
	cmd := r.vk.B().Set().Key(r.pfx + "pause:local:create").Value("1").Ex(time.Minute).Build()
	if err := r.vk.Do(t.Context(), cmd).Error(); err != nil {
		t.Fatal(err)
	}
	_, ran, err := r.probe.Run(t.Context(), canary("local"))
	eq(t, ran, false)
	eq(t, err, nil)
	eq(t, len(r.samples(t, "local")), 0)
	eq(t, len(r.prov.creates), 0)
	rows, inUse := r.slots(t, "local")
	eq(t, rows, 0)
	eq(t, inUse, 0)
}

func TestProbeCanaryNeverPaysForAStart(t *testing.T) {
	r := newRigOf(t, "boat", nil, saga.BootSettings{},
		func(s *spy) provider.Provider { s.fee = 3000; return costly{s} })
	_, ran, err := r.probe.Run(t.Context(), canary("boat"))
	eq(t, ran, false)
	eq(t, err, nil)
	eq(t, len(r.prov.creates), 0)
	// A benchmark pays only when told to
	spec := probe.Spec{
		Kind: store.SampleBench, Provider: "boat", Size: small, BenchID: "b1", Run: 1,
	}
	_, ran, err = r.probe.Run(t.Context(), spec)
	eq(t, ran, false)
	eq(t, err, nil)
	eq(t, len(r.prov.creates), 0)
	spec.AllowPaid = true
	m, ran, err := r.probe.Run(t.Context(), spec)
	if err != nil || !ran {
		t.Fatalf("bench Run = %v, %v", ran, err)
	}
	eq(t, m.BenchID, "b1")
	if m.CostMicros < 3000 {
		t.Errorf("cost_micros %d leaves out the start fee", m.CostMicros)
	}
}

func TestProbeCreateErrorsAreSamplesThatCountOrNot(t *testing.T) {
	for _, tc := range []struct {
		code    provider.Code
		outcome string
	}{
		{provider.CodeCapacity, "capacity"},
		{provider.CodeRateLimited, "rate_limited"},
		{provider.CodeQuota, "quota"},
		{provider.CodeInternal, "failed"},
	} {
		t.Run(string(tc.code), func(t *testing.T) {
			r := newRig(t, "local", func(o *local.Options) {
				o.CreateHook = func(int, provider.CreateRequest) error {
					return &provider.Error{Code: tc.code, Err: errors.New("hook")}
				}
			}, saga.BootSettings{})
			m, ran, err := r.probe.Run(t.Context(), canary("local"))
			if err != nil || !ran {
				t.Fatalf("Run = %v, %v", ran, err)
			}
			eq(t, m.Outcome, tc.outcome)
			eq(t, m.ErrorCode, string(tc.code))
			eq(t, m.SandboxID, "")
			eq(t, m.DestroyMS, store.NoStep)
			eq(t, len(r.samples(t, "local")), 1)
			rows, inUse := r.slots(t, "local")
			eq(t, rows, 0)
			eq(t, inUse, 0)
		})
	}
}

func TestProbeBootFailureIsFailedAndStillDestroysTheSandbox(t *testing.T) {
	r := newRig(t, "local", nil, saga.BootSettings{})
	r.prov.answers = map[string]string{"setup": `{"error":"gate_failed"}`}
	r.prov.codes = map[string]int{"setup": 1}
	m, ran, err := r.probe.Run(t.Context(), canary("local"))
	if err != nil || !ran {
		t.Fatalf("Run = %v, %v", ran, err)
	}
	eq(t, m.Outcome, "failed")
	if m.SandboxID == "" || m.DestroyMS < 0 {
		t.Fatalf("sample %+v, want a sandbox that was destroyed", m)
	}
	owned, _ := r.prov.ListOwned(t.Context(), nil)
	eq(t, len(owned), 0)
	rows, inUse := r.slots(t, "local")
	eq(t, rows, 0)
	eq(t, inUse, 0)
}

// router-worker's spec lists the pools
func TestProbeHardenSpecCarriesThePools(t *testing.T) {
	list := filepath.Join(t.TempDir(), "pools.txt")
	pools := []byte("# pools\n203.0.113.0/24\n\n198.51.100.7\n")
	if err := os.WriteFile(list, pools, 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{Hardening: config.Hardening{
		MailPorts: []int{25}, DenyRanges: []string{"10.0.0.0/8"}, PoolList: list,
		Rules: map[string]config.HardenRule{"boxd": {Required: true}},
	}}
	boot, err := saga.LoadBootSettings(slog.New(slog.NewTextHandler(io.Discard, nil)), cfg)
	if err != nil {
		t.Fatal(err)
	}
	r := newRig(t, "boxd", nil, boot)
	r.prov.answers = map[string]string{"harden": `{"firewall":"nft"}`}
	m, ran, err := r.probe.Run(t.Context(), canary("boxd"))
	if err != nil || !ran || m.Outcome != "ok" {
		t.Fatalf("Run = %+v, %v, %v", m, ran, err)
	}
	raw := r.prov.files["/tmp/bugnet/harden-spec.json"]
	var spec struct {
		MailPorts  []int
		DenyRanges []string `json:"deny_ranges"`
		Pools      []string
	}
	if err := json.Unmarshal(raw, &spec); err != nil {
		t.Fatalf("the harden spec %q: %v", raw, err)
	}
	eq(t, spec.Pools, []string{"203.0.113.0/24", "198.51.100.7/32"})
	eq(t, spec.DenyRanges, []string{"10.0.0.0/8"})
}
