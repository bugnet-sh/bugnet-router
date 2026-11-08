package providers_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/providers"
)

// load reads a settings file with the given content, the keys stand in for the environment
func load(t *testing.T, content string, keys map[string]string) config.Config {
	t.Helper()
	path := filepath.Join(t.TempDir(), "settings.toml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ProviderKeys = keys
	return cfg
}

func localConfig(t *testing.T) config.Config {
	t.Helper()
	return load(t, `providers = ["local"]
[local]
enabled = true
root = "`+t.TempDir()+`"
runner = "/opt/bugnet-run"
hourly_micros = 2500
`, nil)
}

func TestBuildLocal(t *testing.T) {
	reg, err := providers.Build(localConfig(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	ordered := reg.Ordered()
	if len(ordered) != 1 || ordered[0].Name() != "local" {
		t.Fatalf("registry %v, want only local", ordered)
	}
	p := ordered[0]
	huge := provider.Resources{
		VCPU: 256, MemoryMiB: 1 << 20, DiskGiB: 1 << 14, GPUCount: 8, GPUMemoryGiB: 640,
	}
	size, ok := p.SizeFor(huge)
	if !ok || size.Name != "local" {
		t.Fatalf("size for a large package: %+v, %v", size, ok)
	}
	if p.HourlyCostMicros(size) != 2500 || p.BillingIncrement() != time.Second {
		t.Fatalf("cost %d per hour in %v increments", p.HourlyCostMicros(size),
			p.BillingIncrement())
	}
}

func TestBuildLocalRefusesUnlessEnabled(t *testing.T) {
	cfg := localConfig(t)
	cfg.LocalEnabled = false
	_, err := providers.Build(cfg, nil)
	if err == nil || !strings.Contains(err.Error(), "local.enabled") {
		t.Fatalf("build while disabled: %v", err)
	}
}

func TestBuildBoxd(t *testing.T) {
	cfg := load(t, `providers = ["boxd"]`, map[string]string{"boxd": "bxd_test"})
	reg, err := providers.Build(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	p, ok := reg.Get("boxd")
	if !ok {
		t.Fatalf("registry %v lacks boxd", reg.Ordered())
	}
	t.Cleanup(func() { _ = p.(io.Closer).Close() })
	if !p.Capabilities().NeedsRunnerUpload || p.BillingIncrement() != time.Second {
		t.Fatalf("boxd capabilities %+v, increment %v", p.Capabilities(), p.BillingIncrement())
	}
}

func TestBuildBoxdEndpointOverride(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lis.Close() })
	cfg := load(t, `providers = ["boxd"]
[boxd]
region = "nyc"
endpoint = "`+lis.Addr().String()+`"
`, map[string]string{"boxd": "bxd_test"})
	reg, err := providers.Build(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	p, _ := reg.Get("boxd")
	t.Cleanup(func() { _ = p.(io.Closer).Close() })

	ctx, cancel := context.WithCancel(t.Context())
	listed := make(chan struct{})
	go func() {
		defer close(listed)
		_, _ = p.ListOwned(ctx, map[string]string{"bugnet": "1"})
	}()
	defer func() { cancel(); <-listed }()
	_ = lis.(*net.TCPListener).SetDeadline(time.Now().Add(5 * time.Second))
	conn, err := lis.Accept()
	if err != nil {
		t.Fatalf("boxd never dialled boxd.endpoint: %v", err)
	}
	_ = conn.Close()
}

func TestBuildBoxdNeedsKey(t *testing.T) {
	_, err := providers.Build(load(t, `providers = ["boxd"]`, nil), nil)
	if err == nil || !strings.Contains(err.Error(), "BOXD_API_KEY") {
		t.Fatalf("build boxd without a key: %v", err)
	}
}

// e2bTemplates are the two sizes deploy/e2b builds
const e2bTemplates = `
[[e2b.templates]]
size = "2x2"
template = "bugnet-2x2"
vcpu = 2
memory_mib = 2048
disk_gib = 10

[[e2b.templates]]
size = "4x8"
template = "bugnet-4x8"
vcpu = 4
memory_mib = 8192
disk_gib = 10
`

func TestBuildE2B(t *testing.T) {
	cfg := load(t, `providers = ["e2b"]
[e2b]
domain = "e2b-juliett.dev"
max_lifetime = "24h"
`+e2bTemplates, map[string]string{"e2b": "e2b_test"})
	reg, err := providers.Build(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	p, ok := reg.Get("e2b")
	if !ok {
		t.Fatalf("registry %v lacks e2b", reg.Ordered())
	}
	caps := p.Capabilities()
	if !caps.NeedsRunnerUpload || caps.MaxLifetime != 24*time.Hour ||
		p.BillingIncrement() != time.Second {
		t.Fatalf("e2b capabilities %+v, increment %v", caps, p.BillingIncrement())
	}
	size, ok := p.SizeFor(provider.Resources{VCPU: 4})
	if !ok || size.Name != "4x8" || p.HourlyCostMicros(size) != 331200 {
		t.Fatalf("size for 4 vCPU: %+v, %v", size, ok)
	}
}

func TestBuildE2BDefaults(t *testing.T) {
	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	cfg := load(t, `providers = ["e2b"]`, map[string]string{"e2b": "e2b_test"})
	reg, err := providers.Build(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	p, _ := reg.Get("e2b")
	if p.Capabilities().MaxLifetime != time.Hour {
		t.Errorf("max lifetime %v, want the Hobby plan's hour", p.Capabilities().MaxLifetime)
	}
	if s, ok := p.SizeFor(provider.Resources{}); ok {
		t.Errorf("e2b without templates offers %+v", s)
	}
	if got := logs.String(); !strings.Contains(got, "level=WARN") ||
		!strings.Contains(got, "e2b.templates") {
		t.Errorf("log %q, want a warning naming e2b.templates", got)
	}
}

func TestBuildE2BRejectsBadConfig(t *testing.T) {
	_, err := providers.Build(load(t, `providers = ["e2b"]`+e2bTemplates, nil), nil)
	if err == nil || !strings.Contains(err.Error(), "E2B_API_KEY") {
		t.Fatalf("build e2b without a key: %v", err)
	}
	for name, table := range map[string]string{
		"no vCPU": "size = \"2x2\"\ntemplate = \"bugnet-2x2\"\nmemory_mib = 2048\n" +
			"disk_gib = 10",
		"no template": "size = \"2x2\"\nvcpu = 2\nmemory_mib = 2048\ndisk_gib = 10",
		"no size":     "template = \"bugnet-2x2\"\nvcpu = 2\nmemory_mib = 2048\ndisk_gib = 10",
		"no disk":     "size = \"2x2\"\ntemplate = \"bugnet-2x2\"\nvcpu = 2\nmemory_mib = 2048",
	} {
		cfg := load(t, "providers = [\"e2b\"]\n[[e2b.templates]]\n"+table,
			map[string]string{"e2b": "e2b_test"})
		_, err := providers.Build(cfg, nil)
		if err == nil || !strings.Contains(err.Error(), "e2b.templates") {
			t.Errorf("%s: build gave %v, want an error naming e2b.templates", name, err)
		}
	}
	again := "[[e2b.templates]]\nsize = \"2x2\"\ntemplate = \"other\"\nvcpu = 2\n" +
		"memory_mib = 2048\ndisk_gib = 10"
	twice := load(t, "providers = [\"e2b\"]\n"+e2bTemplates+again,
		map[string]string{"e2b": "e2b_test"})
	if _, err := providers.Build(twice, nil); err == nil ||
		!strings.Contains(err.Error(), "e2b.templates") || !strings.Contains(err.Error(), "two") {
		t.Errorf("a size twice: build gave %v, want an error naming e2b.templates", err)
	}
}

func TestBuildKoyeb(t *testing.T) {
	cfg := load(t, `providers = ["koyeb"]`, map[string]string{"koyeb": "koyeb_test"})
	reg, err := providers.Build(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	p, ok := reg.Get("koyeb")
	if !ok {
		t.Fatalf("registry %v lacks koyeb", reg.Ordered())
	}
	caps := p.Capabilities()
	if !caps.NeedsRunnerUpload || !caps.GPU || caps.MaxLifetime != 24*time.Hour-10*time.Minute ||
		p.BillingIncrement() != time.Second {
		t.Fatalf("koyeb capabilities %+v, increment %v", caps, p.BillingIncrement())
	}
	size, ok := p.SizeFor(provider.Resources{VCPU: 2, MemoryMiB: 2048, DiskGiB: 10})
	if !ok || size.Name != "medium" || p.HourlyCostMicros(size) != 28800 {
		t.Fatalf("size for 2 vCPU: %+v, %v", size, ok)
	}
}

func TestBuildKoyebInstanceTypes(t *testing.T) {
	cfg := load(t, `providers = ["koyeb"]
[koyeb]
region = "par"
[koyeb.instance_types]
cpu = "large"
`, map[string]string{"koyeb": "koyeb_test"})
	reg, err := providers.Build(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	p, _ := reg.Get("koyeb")
	size, ok := p.SizeFor(provider.Resources{})
	if !ok || size.Name != "cpu" || size.Resources.VCPU != 4 {
		t.Fatalf("size from koyeb.instance_types: %+v, %v", size, ok)
	}
	if s, ok := p.SizeFor(provider.Resources{GPUCount: 1}); ok || p.Capabilities().GPU {
		t.Fatalf("configured sizes without a GPU offer %+v and capabilities %+v", s,
			p.Capabilities())
	}
}

func TestBuildKoyebRejectsBadConfig(t *testing.T) {
	_, err := providers.Build(load(t, `providers = ["koyeb"]`, nil), nil)
	if err == nil || !strings.Contains(err.Error(), "KOYEB_API_TOKEN") {
		t.Fatalf("build koyeb without a token: %v", err)
	}
	cfg := load(t, "providers = [\"koyeb\"]\n[koyeb.instance_types]\ncpu = \"nano\"",
		map[string]string{"koyeb": "koyeb_test"})
	_, err = providers.Build(cfg, nil)
	if err == nil || !strings.Contains(err.Error(), "koyeb.instance_types") {
		t.Fatalf("build koyeb with an unknown instance type: %v", err)
	}
}

func TestBuildBoat(t *testing.T) {
	cfg := load(t, `providers = ["boat"]`, map[string]string{"boat": "boat_test"})
	reg, err := providers.Build(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	p, ok := reg.Get("boat")
	if !ok {
		t.Fatalf("registry %v lacks boat", reg.Ordered())
	}
	caps := p.Capabilities()
	if !caps.NeedsRunnerUpload || caps.SSH || caps.Preview || caps.GPU ||
		caps.MaxLifetime != 30*24*time.Hour || p.BillingIncrement() != time.Second {
		t.Fatalf("boat capabilities %+v, increment %v", caps, p.BillingIncrement())
	}
	size, ok := p.SizeFor(provider.Resources{VCPU: 4, MemoryMiB: 8192})
	if !ok || size.Name != "default" || p.HourlyCostMicros(size) != 36000 {
		t.Fatalf("size for 4 vCPU: %+v, %v", size, ok)
	}
}

func TestBuildBoatTypes(t *testing.T) {
	cfg := load(t, `providers = ["boat"]
[boat.types]
cpu2 = "small"
cpu8 = "large"
`, map[string]string{"boat": "boat_test"})
	reg, err := providers.Build(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	p, _ := reg.Get("boat")
	size, ok := p.SizeFor(provider.Resources{VCPU: 3})
	if !ok || size.Name != "cpu8" || size.Resources.VCPU != 8 {
		t.Fatalf("size from boat.types: %+v, %v", size, ok)
	}
	if s, ok := p.SizeFor(provider.Resources{VCPU: 16}); ok {
		t.Fatalf("configured sizes offer %+v", s)
	}
}

func TestBuildBoatRejectsBadConfig(t *testing.T) {
	_, err := providers.Build(load(t, `providers = ["boat"]`, nil), nil)
	if err == nil || !strings.Contains(err.Error(), "BOAT_API_KEY") {
		t.Fatalf("build boat without a key: %v", err)
	}
	cfg := load(t, "providers = [\"boat\"]\n[boat.types]\ncpu = \"tiny\"",
		map[string]string{"boat": "boat_test"})
	_, err = providers.Build(cfg, nil)
	if err == nil || !strings.Contains(err.Error(), "boat.types") {
		t.Fatalf("build boat with an unknown type: %v", err)
	}
}

func TestBuildRejectsUnknownProviders(t *testing.T) {
	_, err := providers.Build(config.Config{Providers: []string{"nimbus"}}, nil)
	if err == nil || !strings.Contains(err.Error(), "nimbus") {
		t.Fatalf("build nimbus: %v", err)
	}
}

// uploading is a local provider that needs bugnet-run uploaded, like every remote adapter
type uploading struct {
	*local.Provider
	arch string
}

func (u uploading) Capabilities() provider.Capabilities {
	c := u.Provider.Capabilities()
	c.NeedsRunnerUpload, c.Arch = true, u.arch
	return c
}

func TestRunnersReadsTheBuildsUploadingProvidersNeed(t *testing.T) {
	dir := t.TempDir()
	amd := filepath.Join(dir, "bugnet-run-linux-amd64")
	if err := os.WriteFile(amd, []byte("amd64 build"), 0o755); err != nil {
		t.Fatal(err)
	}
	newLocal := func(name string) *local.Provider {
		return local.New(local.Options{Name: name, Root: t.TempDir()})
	}
	reg := registry(t, newLocal("plain"), uploading{newLocal("up"), ""})
	runners, err := providers.Runners(config.Config{RunnerLinuxAMD64: amd}, reg)
	if err != nil || string(runners["amd64"]) != "amd64 build" || len(runners) != 1 {
		t.Fatalf("runners %v, %v, want the amd64 build alone", runners, err)
	}

	_, err = providers.Runners(config.Config{}, reg)
	if err == nil || !strings.Contains(err.Error(), "runner.linux_amd64") ||
		!strings.Contains(err.Error(), "up") {
		t.Fatalf("runners without the build gave %v, want an error naming the key and up", err)
	}
	missing := config.Config{RunnerLinuxAMD64: filepath.Join(dir, "missing")}
	if _, err := providers.Runners(missing, reg); err == nil {
		t.Fatal("runners with an unreadable build gave no error")
	}
	arm := registry(t, uploading{newLocal("arm"), "arm64"})
	_, err = providers.Runners(config.Config{RunnerLinuxAMD64: amd}, arm)
	if err == nil || !strings.Contains(err.Error(), "runner.linux_arm64") {
		t.Fatalf("runners for an arm64 provider gave %v, want the arm64 key named", err)
	}
	none, err := providers.Runners(config.Config{}, registry(t, newLocal("plain")))
	if err != nil || len(none) != 0 {
		t.Fatalf("runners with no uploading provider gave %v, %v, want none", none, err)
	}
}

// registry registers ps with no data-plane features
func registry(t *testing.T, ps ...provider.Provider) *provider.Registry {
	t.Helper()
	reg, err := provider.NewRegistry(ps, provider.Features{})
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

func TestBuildPassesTheFeatureLists(t *testing.T) {
	cfg := load(t, `providers = ["e2b", "boat"]
[secrets]
providers = ["e2b"]
`+e2bTemplates, map[string]string{"e2b": "e2b_test", "boat": "boat_test"})
	reg, err := providers.Build(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	e2b, _ := reg.Get("e2b")
	boat, _ := reg.Get("boat")
	if !reg.Capabilities(e2b).Secrets || reg.Capabilities(boat).Secrets {
		t.Fatalf("secrets on e2b %v and boat %v, want e2b alone",
			reg.Capabilities(e2b).Secrets, reg.Capabilities(boat).Secrets)
	}
	cfg = load(t, `providers = ["e2b", "boat"]
[previews]
providers = ["e2b", "boat"]
[ssh]
providers = ["e2b", "boat"]
`+e2bTemplates, map[string]string{"e2b": "e2b_test", "boat": "boat_test"})
	reg, err = providers.Build(cfg, nil)
	if err != nil {
		t.Fatalf("previews and ssh on two gated adapters gave %v", err)
	}
	for _, name := range []string{"e2b", "boat"} {
		p, _ := reg.Get(name)
		if c := reg.Capabilities(p); !c.Preview || !c.SSH {
			t.Errorf("%s got preview %v and ssh %v, want both", name, c.Preview, c.SSH)
		}
	}
	cfg = load(t, "providers = [\"e2b\", \"boat\"]\n[ssh]\nproviders = [\"boat\"]\n"+
		"[boat]\nssh_path = \"native\"\n"+e2bTemplates,
		map[string]string{"e2b": "e2b_test", "boat": "boat_test"})
	_, err = providers.Build(cfg, nil)
	if want := "boat is listed for ssh on its native path"; err == nil ||
		!strings.Contains(err.Error(), want) {
		t.Errorf("boat's native path gave %v, want an error with %q", err, want)
	}
}
