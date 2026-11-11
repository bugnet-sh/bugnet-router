package saga_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

func TestSetupSpecNoGateOnLium(t *testing.T) {
	ca := testCA(t)
	b := saga.BootSettings{CAPEM: ca, CAVersion: 2, PreviewDomain: "preview.bugnet.sh"}
	ref := provider.Ref{ID: "x"}
	for _, sample := range []bool{false, true} {
		spec := saga.SetupSpecFor(b, "lium", ref, "", sample)
		eq(t, spec.Phase, "nogate")
		eq(t, spec.GateKey, "")
		eq(t, spec.GateListen, "")
		eq(t, spec.Sample, sample)
		eq(t, spec.CAPEM, string(ca))
		eq(t, spec.CAVersion, 2)
		eq(t, spec.DenyPorts, []int{22, 2022, 29417})
		eq(t, spec.Seed != "", true)
	}
	// A key a caller passes never reaches a nogate spec
	raw, err := json.Marshal(saga.SetupSpecFor(b, "lium", ref, "a-gate-key", false))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "a-gate-key") {
		t.Fatalf("the lium spec carries the gate key: %s", raw)
	}
	eq(t, saga.PoolSetupSpecFor(b, "lium", ref).Phase, "pool")
}

func TestSetupSpecFullPhaseOnHF(t *testing.T) {
	ca := testCA(t)
	b := saga.BootSettings{CAPEM: ca, CAVersion: 2}
	ref := provider.Ref{ID: "x"}
	hf := saga.SetupSpecFor(b, "hf", ref, "key", false)
	eq(t, hf.Phase, "")
	eq(t, hf.GateKey, "key")
	eq(t, hf.GateListen, ":29417")
	eq(t, hf.DenyPorts, []int{22, 2022, 29417, 49983})
	eq(t, hf.CAPEM, string(ca))
	// The other providers keep the full phase
	for _, name := range []string{"e2b", "boxd", "koyeb", "boat", "local", "a"} {
		spec := saga.SetupSpecFor(b, name, ref, "key", true)
		eq(t, spec.Phase, "")
		eq(t, spec.GateKey, "key")
	}
}

func TestHardenSpecNoGatewayOnLium(t *testing.T) {
	b := saga.BootSettings{
		MailPorts: []int{25}, DenyRanges: []string{"10.0.0.0/8"},
		Harden: map[string]config.HardenRule{
			"lium": {Required: true, NoGateway: true}, "hf": {}, "boat": {Required: true},
		},
	}
	lium, ok := saga.HardenSpecFor(b, "lium")
	eq(t, ok, true)
	eq(t, lium.NoGateway, true)
	raw, _ := json.Marshal(lium)
	eq(t, string(raw), `{"v":1,"mail_ports":[25],"deny_ranges":["10.0.0.0/8"],"pools":[],`+
		`"allow":[],"no_gateway":true}`)
	// Every other provider's spec has no no_gateway field, nor lium's while its rule has none
	b.Harden["lium"] = config.HardenRule{Required: true}
	for _, name := range []string{"hf", "boat", "lium"} {
		spec, ok := saga.HardenSpecFor(b, name)
		eq(t, ok, true)
		eq(t, spec.NoGateway, false)
		raw, _ := json.Marshal(spec)
		eq(t, string(raw), `{"v":1,"mail_ports":[25],"deny_ranges":["10.0.0.0/8"],"pools":[],`+
			`"allow":[]}`)
	}
}

// TestHardenSpecReadsLiumHardenNoGateway follows lium.harden_no_gateway from the settings file
func TestHardenSpecReadsLiumHardenNoGateway(t *testing.T) {
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, doc := range []string{"", "[lium]\nharden_no_gateway = true\n"} {
		path := filepath.Join(t.TempDir(), "settings.toml")
		if err := os.WriteFile(path, []byte(doc), 0o644); err != nil {
			t.Fatal(err)
		}
		cfg, err := config.LoadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		// The settings file cannot turn it off, so a rule without it stands in for one that could
		for _, r := range []struct {
			rule config.HardenRule
			want bool
		}{{cfg.Hardening.Rules["lium"], true}, {config.HardenRule{Required: true}, false}} {
			cfg.Hardening.Rules["lium"] = r.rule
			b, err := saga.LoadBootSettings(discard, cfg)
			if err != nil {
				t.Fatal(err)
			}
			spec, ok := saga.HardenSpecFor(b, "lium")
			eq(t, ok, true)
			eq(t, spec.NoGateway, r.want)
			hf, ok := saga.HardenSpecFor(b, "hf")
			eq(t, ok, true)
			eq(t, hf.NoGateway, false)
		}
	}
}

// writtenFile is one file the saga wrote into a sandbox
type writtenFile struct {
	path string
	data []byte
}

// gateSpy is a scripted provider that records its file writes and gate route calls
type gateSpy struct {
	scripted
	files     *[]writtenFile
	gateCalls *atomic.Int32
}

func newGateSpy(t *testing.T, opts local.Options, answers map[string]answer) gateSpy {
	t.Helper()
	return gateSpy{
		scripted: newScripted(newLocal(t, opts), answers), files: &[]writtenFile{},
		gateCalls: &atomic.Int32{},
	}
}

func (g gateSpy) WriteFile(
	ctx context.Context, ref provider.Ref, path string, data []byte, mode fs.FileMode,
) error {
	g.mu.Lock()
	*g.files = append(*g.files, writtenFile{path: path, data: bytes.Clone(data)})
	g.mu.Unlock()
	return g.scripted.WriteFile(ctx, ref, path, data, mode)
}

func (g gateSpy) OpenWriter(
	ctx context.Context, ref provider.Ref, path string, spec provider.WriteSpec,
) (provider.FileWriter, error) {
	g.mu.Lock()
	*g.files = append(*g.files, writtenFile{path: path})
	g.mu.Unlock()
	return g.scripted.OpenWriter(ctx, ref, path, spec)
}

func (g gateSpy) PrepareGate(
	ctx context.Context, ref provider.Ref, port int,
) (map[string]string, error) {
	g.gateCalls.Add(1)
	return g.scripted.PrepareGate(ctx, ref, port)
}

func (g gateSpy) GateRoute(ref provider.Ref, port int) (provider.GateRoute, error) {
	g.gateCalls.Add(1)
	return g.scripted.GateRoute(ref, port)
}

func (g gateSpy) written() []writtenFile {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]writtenFile(nil), *g.files...)
}

// setupSpecs are the setup specs written into g's sandboxes, field by field
func (g gateSpy) setupSpecs(t *testing.T) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, f := range g.written() {
		if f.path != "/tmp/bugnet/setup-spec.json" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal(f.data, &m); err != nil {
			t.Fatal(err)
		}
		out = append(out, m)
	}
	return out
}

// keyHolders lists each file written into g's sandboxes or left in fsRoot that holds key
func keyHolders(t *testing.T, g gateSpy, fsRoot, key string) []string {
	t.Helper()
	if key == "" {
		t.Fatal("no key to look for")
	}
	var out []string
	for _, f := range g.written() {
		if bytes.Contains(f.data, []byte(key)) {
			out = append(out, "written "+f.path)
		}
	}
	err := filepath.WalkDir(fsRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil || !d.Type().IsRegular() {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(data, []byte(key)) {
			out = append(out, "left "+path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// holdsNoKey fails t when a file written into g's sandboxes or left in fsRoot holds key
func holdsNoKey(t *testing.T, g gateSpy, fsRoot, key string) {
	t.Helper()
	if got := keyHolders(t, g, fsRoot, key); len(got) > 0 {
		t.Fatalf("the gate key is in %v", got)
	}
	for _, name := range []string{"etc/bugnet/gate.key", "etc/bugnet/gate.json"} {
		if exists(filepath.Join(fsRoot, name)) {
			t.Errorf("the sandbox holds %s", name)
		}
	}
}

func TestBootNoGateWritesNoGateKey(t *testing.T) {
	root := t.TempDir()
	lium := newGateSpy(t, local.Options{Name: "lium", Root: root},
		map[string]answer{"harden": {`{"firewall":"nft"}`, 0}})
	e := newEnvWith(t, func(d *saga.Deps) {
		d.Boot.Harden = map[string]config.HardenRule{"lium": {Required: true, NoGateway: true}}
	}, lium)
	id := uuid.New()
	keyID := e.keys.Current().ID
	ref, rep, err := e.saga.Boot(t.Context(), lium, created(t, lium),
		saga.BootPlan{Placement: id, KeyID: keyID})
	eq(t, err, nil)
	eq(t, rep.GateReady, false)
	eq(t, rep.GateMS, store.NoStep)
	eq(t, lium.calls(), []string{"setup sample=false", "harden", "setup sample=true"})
	eq(t, lium.gateCalls.Load(), int32(0))
	specs := lium.setupSpecs(t)
	eq(t, len(specs), 2)
	for i, spec := range specs {
		eq(t, spec["phase"], any("nogate"))
		eq(t, spec["gate_key"], any(""))
		eq(t, spec["gate_listen"], any(""))
		eq(t, spec["sample"], any(i == 1))
	}
	hardenSpecs := 0
	for _, f := range lium.written() {
		if f.path == "/tmp/bugnet/harden-spec.json" {
			hardenSpecs++
			var h struct {
				NoGateway bool `json:"no_gateway"`
			}
			if err := json.Unmarshal(f.data, &h); err != nil {
				t.Fatal(err)
			}
			eq(t, h.NoGateway, true)
		}
	}
	eq(t, hardenSpecs, 1)
	key, err := e.keys.GateKey(keyID, id)
	if err != nil {
		t.Fatal(err)
	}
	holdsNoKey(t, lium, sandboxFS(root, ref), key)

	// The same look finds the key a gated boot writes, in its spec and its gate.key
	a := newGateSpy(t, local.Options{Name: "a", Root: root}, nil)
	ga := newEnv(t, a)
	aref, _, err := ga.saga.Boot(t.Context(), a, created(t, a),
		saga.BootPlan{Placement: id, KeyID: ga.keys.Current().ID})
	if err != nil {
		t.Fatal(err)
	}
	akey, err := ga.keys.GateKey(ga.keys.Current().ID, id)
	if err != nil {
		t.Fatal(err)
	}
	if got := keyHolders(t, a, sandboxFS(root, aref), akey); len(got) < 2 {
		t.Fatalf("a gated boot's key found only in %v", got)
	}
}

func TestBootNoGateDerivesNoGateKey(t *testing.T) {
	lium := newLocal(t, local.Options{Name: "lium"})
	a := newLocal(t, local.Options{Name: "a"})
	e := newEnv(t, lium, a)
	// No root has this id, so any gate key derivation fails the boot
	plan := saga.BootPlan{Placement: uuid.New(), KeyID: "no-such-root"}
	if _, _, err := e.saga.Boot(t.Context(), lium, created(t, lium), plan); err != nil {
		t.Fatalf("a lium boot under an unknown key id: %v", err)
	}
	if _, _, err := e.saga.Boot(t.Context(), a, created(t, a), plan); err == nil ||
		!strings.Contains(err.Error(), "gate key") {
		t.Fatalf("a gated boot under an unknown key id: %v", err)
	}
}

func TestBootNoGateRefusesWhatNeedsTheGate(t *testing.T) {
	lium := newGateSpy(t, local.Options{Name: "lium"}, nil)
	e := newEnv(t, lium)
	for _, need := range []saga.BootPlan{{NeedPreview: true}, {NeedSSH: true}} {
		need.Placement, need.KeyID = uuid.New(), e.keys.Current().ID
		_, _, err := e.saga.Boot(t.Context(), lium, created(t, lium), need)
		if err == nil || !strings.Contains(err.Error(), "no gate route") {
			t.Fatalf("a lium boot for %+v: %v", need, err)
		}
	}
	eq(t, lium.gateCalls.Load(), int32(0))
}

func TestPrepareNoGateAfterAPoolBoot(t *testing.T) {
	root := t.TempDir()
	lium := newGateSpy(t, local.Options{Name: "lium", Root: root}, nil)
	e := newEnv(t, lium)
	ref, _, err := e.saga.Boot(t.Context(), lium, created(t, lium), saga.BootPlan{Pool: true})
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	prep, err := e.saga.Prepare(t.Context(), lium, ref, id, placement.Requirements{}, true)
	eq(t, err, nil)
	eq(t, prep.GateMS, store.NoStep)
	eq(t, prep.GateReadyAt.IsZero(), true)
	eq(t, lium.calls(), []string{"setup pool", "setup sample=false", "setup sample=true"})
	eq(t, lium.gateCalls.Load(), int32(0))
	specs := lium.setupSpecs(t)
	eq(t, len(specs), 3)
	for _, spec := range specs[1:] {
		eq(t, spec["phase"], any("nogate"))
		eq(t, spec["gate_key"], any(""))
	}
	key, err := e.keys.GateKey(e.keys.Current().ID, id)
	if err != nil {
		t.Fatal(err)
	}
	holdsNoKey(t, lium, sandboxFS(root, ref), key)
}

func TestNoGatePlacementTakesNoGateSteps(t *testing.T) {
	root := t.TempDir()
	lium := newGateSpy(t, local.Options{Name: "lium", Root: root}, nil)
	e := newEnv(t, lium)
	p, _ := e.placeReady(t, "order-1")
	eq(t, p.GateReadyAt, (*time.Time)(nil))
	before := len(lium.argvs())
	if _, err := e.saga.RepairGate(t.Context(), p); !errors.Is(err, saga.ErrGateUnavailable) {
		t.Fatalf("RepairGate on a lium placement: %v", err)
	}
	if _, err := e.saga.OpenGate(t.Context(), p); !errors.Is(err, saga.ErrGateUnavailable) {
		t.Fatalf("OpenGate on a lium placement: %v", err)
	}
	_, key, err := e.saga.Gate(p)
	if !errors.Is(err, saga.ErrGateUnavailable) || key != "" {
		t.Fatalf("Gate on a lium placement: %v with a key %t", err, key != "")
	}
	// Nothing ran in the sandbox, setup --repair least of all
	if ran := lium.argvs()[before:]; len(ran) > 0 {
		t.Fatalf("gate steps on a lium placement ran %v", ran)
	}
	eq(t, lium.gateCalls.Load(), int32(0))
	eq(t, e.reload(t, p.ID).GateReadyAt, (*time.Time)(nil))
	want, err := e.keys.GateKey(p.KeyID, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	holdsNoKey(t, lium, sandboxFS(root, *p.ProviderRef), want)
}
