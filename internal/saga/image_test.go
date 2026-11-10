package saga_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io/fs"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/image"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/pool"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

var runnerBuild = []byte("the router's bugnet-run build")

// imaged is a provider whose sandboxes hold a base image manifest, or none, and a baked runner
type imaged struct {
	*local.Provider
	manifest []byte
	pingFail bool
	mu       *sync.Mutex
	log      *[]string
}

func newImaged(p *local.Provider, manifest []byte) imaged {
	return imaged{Provider: p, manifest: manifest, mu: &sync.Mutex{}, log: &[]string{}}
}

func (i imaged) note(s string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	*i.log = append(*i.log, s)
}

func (i imaged) calls() []string {
	i.mu.Lock()
	defer i.mu.Unlock()
	return slices.Clone(*i.log)
}

func (i imaged) Capabilities() provider.Capabilities {
	c := i.Provider.Capabilities()
	c.NeedsRunnerUpload = true
	return c
}

func (i imaged) WaitReady(ctx context.Context, ref provider.Ref) error {
	i.note("WaitReady")
	return i.Provider.WaitReady(ctx, ref)
}

func (i imaged) ReadFile(
	ctx context.Context, ref provider.Ref, path string, off int64, n int,
) ([]byte, bool, error) {
	if path != image.ManifestPath {
		return i.Provider.ReadFile(ctx, ref, path, off, n)
	}
	i.note("ReadFile " + path)
	if i.manifest == nil {
		return nil, false, &provider.Error{Code: provider.CodeNotFound}
	}
	return i.manifest, true, nil
}

func (i imaged) WriteFile(
	ctx context.Context, ref provider.Ref, path string, data []byte, mode fs.FileMode,
) error {
	i.note("WriteFile " + path)
	return i.Provider.WriteFile(ctx, ref, path, data, mode)
}

func (i imaged) Exec(
	ctx context.Context, ref provider.Ref, spec provider.ExecSpec,
) (provider.ExecStream, error) {
	argv := slices.Clone(spec.Argv)
	switch {
	case len(argv) == 2 && argv[0] == "cat" && argv[1] == image.ManifestPath:
		i.note("Exec cat " + image.ManifestPath)
		if i.manifest == nil {
			return i.Provider.Exec(ctx, ref, provider.ExecSpec{Argv: []string{"false"}})
		}
		return i.Provider.Exec(ctx, ref, provider.ExecSpec{
			Argv: []string{"sh", "-c", `printf '%s' "$1"`, "sh", string(i.manifest)}})
	case len(argv) == 2 && argv[0] == image.RunnerPath && argv[1] == "ping":
		i.note("Exec " + image.RunnerPath + " ping")
		if i.pingFail {
			return i.Provider.Exec(ctx, ref, provider.ExecSpec{Argv: []string{"false"}})
		}
		return i.Provider.Exec(ctx, ref, provider.ExecSpec{Argv: []string{"true"}})
	}
	for k, a := range argv {
		if a == image.RunnerPath {
			argv[k] = image.UploadedRunnerPath
		}
	}
	spec.Argv = argv
	return i.Provider.Exec(ctx, ref, spec)
}

func (i imaged) wrote(path string) bool {
	return slices.Contains(i.calls(), "WriteFile "+path)
}

// manifestFor is a manifest of the given version and channel for runner build, with the CA listed
func manifestFor(t *testing.T, version, channel string, runner []byte, cas ...image.CA) []byte {
	t.Helper()
	sum := sha256.Sum256(runner)
	m := image.Manifest{
		V: 1, Version: version, Channel: channel, Target: "oci", OS: "ubuntu-24.04",
		RunnerSHA256: map[string]string{"amd64": hex.EncodeToString(sum[:])}, CAs: cas,
		Tools: map[string]string{"git": "2.43", "node": "24.22", "docker": "none"},
	}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func imageEnv(t *testing.T, name, version string, p imaged, adjust func(*saga.Deps)) *env {
	t.Helper()
	return newEnvFeatures(t, func(d *saga.Deps) {
		d.Runners = map[string][]byte{"amd64": runnerBuild}
		d.ImageChannel = "dev"
		if adjust != nil {
			adjust(d)
		}
	}, provider.Features{Images: map[string]string{name: version}}, p)
}

func TestBootReadsManifestBeforeAnyWrite(t *testing.T) {
	p := newImaged(newLocal(t, local.Options{Name: "a"}),
		manifestFor(t, "2026.11.1", "dev", runnerBuild))
	e := imageEnv(t, "a", "2026.11.1", p, nil)
	ref, rep, err := e.saga.Boot(t.Context(), p, created(t, p), saga.BootPlan{Pool: true})
	eq(t, err, nil)
	calls := p.calls()
	eq(t, calls[0], "WaitReady")
	eq(t, calls[1], "ReadFile "+image.ManifestPath)
	eq(t, rep.RunnerBaked, true)
	eq(t, rep.RunnerMS, store.NoStep)
	eq(t, p.wrote(image.UploadedRunnerPath), false)
	eq(t, ref.Meta["runner"], image.RunnerPath)
	eq(t, rep.Image.Version, "2026.11.1")
}

func TestBootReadsBoatManifestThroughExec(t *testing.T) {
	p := newImaged(newLocal(t, local.Options{Name: "boat"}),
		manifestFor(t, "2026.11.1", "dev", runnerBuild))
	e := imageEnv(t, "boat", "2026.11.1", p, nil)
	_, rep, err := e.saga.Boot(t.Context(), p, created(t, p), saga.BootPlan{Pool: true})
	eq(t, err, nil)
	eq(t, slices.Contains(p.calls(), "Exec cat "+image.ManifestPath), true)
	eq(t, slices.Contains(p.calls(), "ReadFile "+image.ManifestPath), false)
	eq(t, rep.Image != nil, true)
}

func TestBootUploadsWhenRunnerDiffers(t *testing.T) {
	p := newImaged(newLocal(t, local.Options{Name: "a"}),
		manifestFor(t, "2026.11.1", "dev", []byte("an older build")))
	e := imageEnv(t, "a", "2026.11.1", p, nil)
	ref, rep, err := e.saga.Boot(t.Context(), p, created(t, p), saga.BootPlan{Pool: true})
	eq(t, err, nil)
	eq(t, rep.RunnerBaked, false)
	eq(t, p.wrote(image.UploadedRunnerPath), true)
	eq(t, ref.Meta["runner"], "")
	eq(t, rep.Image != nil, true)
}

func TestBootUploadsWhenBakedPingFails(t *testing.T) {
	p := newImaged(newLocal(t, local.Options{Name: "a"}),
		manifestFor(t, "2026.11.1", "dev", runnerBuild))
	p.pingFail = true
	e := imageEnv(t, "a", "2026.11.1", p, nil)
	ref, rep, err := e.saga.Boot(t.Context(), p, created(t, p), saga.BootPlan{Pool: true})
	eq(t, err, nil)
	eq(t, rep.RunnerBaked, false)
	eq(t, p.wrote(image.UploadedRunnerPath), true)
	eq(t, ref.Meta["runner"], "")
}

func TestBootWithoutManifestRunsAsToday(t *testing.T) {
	p := newImaged(newLocal(t, local.Options{Name: "a"}), nil)
	e := imageEnv(t, "a", "", p, nil)
	ref, rep, err := e.saga.Boot(t.Context(), p, created(t, p), saga.BootPlan{Pool: true})
	eq(t, err, nil)
	eq(t, rep.Image == nil, true)
	eq(t, rep.RunnerBaked, false)
	eq(t, p.wrote(image.UploadedRunnerPath), true)
	eq(t, ref.Meta["runner"], "")
}

func TestBootImageMismatchFailsOver(t *testing.T) {
	accept := func(d *saga.Deps) { d.ImageAccept = []string{"2026.11.1"} }
	for _, c := range []struct {
		name     string
		manifest []byte
		version  string
		adjust   func(*saga.Deps)
	}{
		{"version", manifestFor(t, "2026.10.1", "dev", runnerBuild), "2026.11.1", nil},
		{"channel", manifestFor(t, "2026.11.1", "prod", runnerBuild), "2026.11.1", nil},
		{"missing under accept", nil, "2026.11.1", accept},
		{"malformed", []byte(`{"v":1,"surprise":true}`), "2026.11.1", nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			bad := newImaged(newLocal(t, local.Options{Name: "bad"}), c.manifest)
			e := newEnvFeatures(t, func(d *saga.Deps) {
				d.Runners = map[string][]byte{"amd64": runnerBuild}
				d.ImageChannel = "dev"
				if c.adjust != nil {
					c.adjust(d)
				}
			}, provider.Features{Images: map[string]string{"bad": c.version}}, bad)
			p, _ := e.place(t, "order-1")
			eq(t, p.State, store.PlacementFailed)
			eq(t, p.FailureReason, "image_mismatch")
			eq(t, bad.wrote(image.UploadedRunnerPath), false)
		})
	}
}

func TestBootRefusesAVersionOffAccept(t *testing.T) {
	p := newImaged(newLocal(t, local.Options{Name: "a"}),
		manifestFor(t, "2026.11.1", "dev", runnerBuild))
	e := imageEnv(t, "a", "2026.11.1", p, func(d *saga.Deps) {
		d.ImageAccept = []string{"2026.12.1"}
	})
	_, _, err := e.saga.Boot(t.Context(), p, created(t, p), saga.BootPlan{Pool: true})
	eq(t, err != nil, true)
	eq(t, p.wrote(image.UploadedRunnerPath), false)
}

func TestReadyRecordsImage(t *testing.T) {
	p := newImaged(newLocal(t, local.Options{Name: "a"}),
		manifestFor(t, "2026.11.1", "dev", runnerBuild))
	e := imageEnv(t, "a", "2026.11.1", p, nil)
	ready, _ := e.placeReady(t, "order-1")
	eq(t, ready.ImageVersion, "2026.11.1")
	var kept struct {
		OS    string
		Tools map[string]string
	}
	eq(t, json.Unmarshal(ready.ImageManifest, &kept), nil)
	eq(t, kept.OS, "ubuntu-24.04")
	eq(t, kept.Tools["node"], "24.22")
	eq(t, ready.ProviderRef.Meta["runner"], image.RunnerPath)
	uses, err := e.store.ImageUse(t.Context(), "")
	eq(t, err, nil)
	eq(t, len(uses), 1)
	eq(t, uses[0].Version, "2026.11.1")
	eq(t, uses[0].Live, 1)
	eq(t, uses[0].LastReadyAt.IsZero(), false)
}

func imagedPool(t *testing.T, p imaged) *poolEnv {
	t.Helper()
	return newPoolEnvFeatures(t, provider.Features{Images: map[string]string{"a": "2026.11.1"}},
		func(d *saga.Deps) {
			d.Runners = map[string][]byte{"amd64": runnerBuild}
			d.ImageChannel = "dev"
		}, p)
}

func TestReadyRecordsImageOnThePooledPath(t *testing.T) {
	p := newImaged(newLocal(t, local.Options{Name: "a"}),
		manifestFor(t, "2026.11.1", "dev", runnerBuild))
	e := imagedPool(t, p)
	members := e.fill(t)
	eq(t, len(members), 1)
	eq(t, members[0].Image != nil, true)
	eq(t, members[0].Image.Version, "2026.11.1")
	eq(t, members[0].ProviderRef.Meta["runner"], image.RunnerPath)
	ready, _ := e.placeReady(t, "order-1")
	eq(t, ready.StartPath, "pooled")
	eq(t, ready.ImageVersion, "2026.11.1")
	eq(t, strings.Contains(string(ready.ImageManifest), "ubuntu-24.04"), true)
	eq(t, ready.ProviderRef.Meta["runner"], image.RunnerPath)
	var version string
	err := e.pool.QueryRow(t.Context(), `select coalesce(image_version, '') from start_samples
		where placement_id = $1`, ready.ID).Scan(&version)
	eq(t, err, nil)
	eq(t, version, "2026.11.1")
}

func TestPoolTickRetiresAnImageOffAccept(t *testing.T) {
	p := newImaged(newLocal(t, local.Options{Name: "a"}),
		manifestFor(t, "2026.11.1", "dev", runnerBuild))
	e := imagedPool(t, p)
	members := e.fill(t)
	eq(t, len(members), 1)
	settings := config.DefaultPools()
	settings.Headroom = 0
	settings.Pool = []config.Pool{{Provider: "a", Size: "small", Min: 1, Max: 3,
		MaxAge: time.Hour}}
	mgr, err := pool.New(pool.Deps{
		Store: e.store, Registry: mustRegistry(t, p), Saga: e.saga, River: e.river,
		Valkey: e.vk, Prefix: e.prefix, Settings: settings,
		ImageAccept: []string{"2026.12.1"},
	})
	eq(t, err, nil)
	eq(t, mgr.Tick(t.Context()), nil)
	got := e.member(t, members[0].ID)
	eq(t, got.State, store.MemberRetiring)
	eq(t, got.EndReason, store.EndImage)
}

func TestReadyRecordsImageOnTheReservedPath(t *testing.T) {
	p := newImaged(newLocal(t, local.Options{Name: "a"}),
		manifestFor(t, "2026.11.1", "dev", runnerBuild))
	e := newEnvFeatures(t, func(d *saga.Deps) {
		d.Runners = map[string][]byte{"amd64": runnerBuild}
		d.ImageChannel = "dev"
		d.Reserve = reserveSettings("a")
	}, provider.Features{Images: map[string]string{"a": "2026.11.1"}}, p)
	eq(t, e.reserve(t, "q2.i", smallPkg).Skip, saga.Skip(""))
	e.waitReservation(t, "q2.i", store.ReservationReady)
	ready := e.createWithQuote(t, "order-1", "q2.i", placement.Requirements{})
	eq(t, ready.StartPath, "reserved")
	eq(t, ready.ImageVersion, "2026.11.1")
	eq(t, ready.ProviderRef.Meta["runner"], image.RunnerPath)
}
