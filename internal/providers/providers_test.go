package providers_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/oynozan/bugnet-router/internal/config"
	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/providers"
)

func localConfig(t *testing.T) config.Config {
	t.Helper()
	return config.Config{
		Providers:         []string{"local"},
		LocalEnabled:      true,
		LocalRoot:         t.TempDir(),
		LocalRunnerPath:   "/opt/bugnet-run",
		LocalHourlyMicros: 2500,
	}
}

func TestBuildLocal(t *testing.T) {
	t.Setenv("BUGNET_ENABLE_LOCAL_PROVIDER", "1")
	reg, err := providers.Build(localConfig(t))
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

func TestBuildLocalRefusesWithoutFlag(t *testing.T) {
	t.Setenv("BUGNET_ENABLE_LOCAL_PROVIDER", "")
	_, err := providers.Build(localConfig(t))
	if err == nil || !strings.Contains(err.Error(), "BUGNET_ENABLE_LOCAL_PROVIDER") {
		t.Fatalf("build without the flag: %v", err)
	}
}

func TestBuildBoxd(t *testing.T) {
	reg, err := providers.Build(config.Config{
		Providers:    []string{"boxd"},
		ProviderKeys: map[string]string{"boxd": "bxd_test"},
	})
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
	t.Setenv("BUGNET_BOXD_ENDPOINT", lis.Addr().String())
	t.Setenv("BUGNET_BOXD_REGION", "nyc")
	reg, err := providers.Build(config.Config{
		Providers:    []string{"boxd"},
		ProviderKeys: map[string]string{"boxd": "bxd_test"},
	})
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
		t.Fatalf("boxd never dialled BUGNET_BOXD_ENDPOINT: %v", err)
	}
	_ = conn.Close()
}

func TestBuildBoxdNeedsKey(t *testing.T) {
	_, err := providers.Build(config.Config{Providers: []string{"boxd"}})
	if err == nil || !strings.Contains(err.Error(), "BOXD_API_KEY") {
		t.Fatalf("build boxd without a key: %v", err)
	}
}

func TestBuildE2B(t *testing.T) {
	t.Setenv("BUGNET_E2B_TEMPLATES", "2x2=bugnet-2x2:2:2048:10,4x8=bugnet-4x8:4:8192:10")
	t.Setenv("BUGNET_E2B_DOMAIN", "e2b-juliett.dev")
	t.Setenv("BUGNET_E2B_MAX_LIFETIME", "24h")
	reg, err := providers.Build(config.Config{
		Providers:    []string{"e2b"},
		ProviderKeys: map[string]string{"e2b": "e2b_test"},
	})
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
	for _, env := range []string{
		"BUGNET_E2B_TEMPLATES", "BUGNET_E2B_DOMAIN", "BUGNET_E2B_MAX_LIFETIME",
	} {
		t.Setenv(env, "")
	}
	reg, err := providers.Build(config.Config{
		Providers:    []string{"e2b"},
		ProviderKeys: map[string]string{"e2b": "e2b_test"},
	})
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
		!strings.Contains(got, "BUGNET_E2B_TEMPLATES") {
		t.Errorf("log %q, want a warning naming BUGNET_E2B_TEMPLATES", got)
	}
}

func TestBuildE2BRejectsBadConfig(t *testing.T) {
	cases := []struct {
		name, env, value string
	}{
		{"no key", "", ""},
		{"bad templates", "BUGNET_E2B_TEMPLATES", "2x2=bugnet-2x2:2:2048"},
		{"bad lifetime", "BUGNET_E2B_MAX_LIFETIME", "an hour"},
		{"negative lifetime", "BUGNET_E2B_MAX_LIFETIME", "-1h"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			keys := map[string]string{"e2b": "e2b_test"}
			want := c.env
			if c.env == "" {
				keys, want = nil, "E2B_API_KEY"
			} else {
				t.Setenv(c.env, c.value)
			}
			_, err := providers.Build(config.Config{Providers: []string{"e2b"}, ProviderKeys: keys})
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("build gave %v, want an error naming %s", err, want)
			}
		})
	}
}

func TestBuildKoyeb(t *testing.T) {
	t.Setenv("BUGNET_KOYEB_REGION", "")
	t.Setenv("BUGNET_KOYEB_INSTANCE_TYPES", "")
	reg, err := providers.Build(config.Config{
		Providers:    []string{"koyeb"},
		ProviderKeys: map[string]string{"koyeb": "koyeb_test"},
	})
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
	t.Setenv("BUGNET_KOYEB_REGION", "par")
	t.Setenv("BUGNET_KOYEB_INSTANCE_TYPES", "cpu=large")
	reg, err := providers.Build(config.Config{
		Providers:    []string{"koyeb"},
		ProviderKeys: map[string]string{"koyeb": "koyeb_test"},
	})
	if err != nil {
		t.Fatal(err)
	}
	p, _ := reg.Get("koyeb")
	size, ok := p.SizeFor(provider.Resources{})
	if !ok || size.Name != "cpu" || size.Resources.VCPU != 4 {
		t.Fatalf("size from BUGNET_KOYEB_INSTANCE_TYPES: %+v, %v", size, ok)
	}
	if s, ok := p.SizeFor(provider.Resources{GPUCount: 1}); ok || p.Capabilities().GPU {
		t.Fatalf("configured sizes without a GPU offer %+v and capabilities %+v", s,
			p.Capabilities())
	}
}

func TestBuildKoyebRejectsBadConfig(t *testing.T) {
	_, err := providers.Build(config.Config{Providers: []string{"koyeb"}})
	if err == nil || !strings.Contains(err.Error(), "KOYEB_API_TOKEN") {
		t.Fatalf("build koyeb without a token: %v", err)
	}
	t.Setenv("BUGNET_KOYEB_INSTANCE_TYPES", "cpu=nano")
	_, err = providers.Build(config.Config{
		Providers:    []string{"koyeb"},
		ProviderKeys: map[string]string{"koyeb": "koyeb_test"},
	})
	if err == nil || !strings.Contains(err.Error(), "BUGNET_KOYEB_INSTANCE_TYPES") {
		t.Fatalf("build koyeb with an unknown instance type: %v", err)
	}
}

func TestBuildBoat(t *testing.T) {
	t.Setenv("BUGNET_BOAT_TYPES", "")
	reg, err := providers.Build(config.Config{
		Providers:    []string{"boat"},
		ProviderKeys: map[string]string{"boat": "boat_test"},
	})
	if err != nil {
		t.Fatal(err)
	}
	p, ok := reg.Get("boat")
	if !ok {
		t.Fatalf("registry %v lacks boat", reg.Ordered())
	}
	caps := p.Capabilities()
	if !caps.NeedsRunnerUpload || !caps.SSH || caps.GPU || caps.MaxLifetime != 30*24*time.Hour ||
		p.BillingIncrement() != time.Second {
		t.Fatalf("boat capabilities %+v, increment %v", caps, p.BillingIncrement())
	}
	size, ok := p.SizeFor(provider.Resources{VCPU: 4, MemoryMiB: 8192})
	if !ok || size.Name != "default" || p.HourlyCostMicros(size) != 36000 {
		t.Fatalf("size for 4 vCPU: %+v, %v", size, ok)
	}
}

func TestBuildBoatTypes(t *testing.T) {
	t.Setenv("BUGNET_BOAT_TYPES", "cpu2=small,cpu8=large")
	reg, err := providers.Build(config.Config{
		Providers:    []string{"boat"},
		ProviderKeys: map[string]string{"boat": "boat_test"},
	})
	if err != nil {
		t.Fatal(err)
	}
	p, _ := reg.Get("boat")
	size, ok := p.SizeFor(provider.Resources{VCPU: 3})
	if !ok || size.Name != "cpu8" || size.Resources.VCPU != 8 {
		t.Fatalf("size from BUGNET_BOAT_TYPES: %+v, %v", size, ok)
	}
	if s, ok := p.SizeFor(provider.Resources{VCPU: 16}); ok {
		t.Fatalf("configured sizes offer %+v", s)
	}
}

func TestBuildBoatRejectsBadConfig(t *testing.T) {
	_, err := providers.Build(config.Config{Providers: []string{"boat"}})
	if err == nil || !strings.Contains(err.Error(), "BOAT_API_KEY") {
		t.Fatalf("build boat without a key: %v", err)
	}
	t.Setenv("BUGNET_BOAT_TYPES", "cpu=tiny")
	_, err = providers.Build(config.Config{
		Providers:    []string{"boat"},
		ProviderKeys: map[string]string{"boat": "boat_test"},
	})
	if err == nil || !strings.Contains(err.Error(), "BUGNET_BOAT_TYPES") {
		t.Fatalf("build boat with an unknown type: %v", err)
	}
}

func TestBuildRejectsUnknownProviders(t *testing.T) {
	_, err := providers.Build(config.Config{Providers: []string{"nimbus"}})
	if err == nil || !strings.Contains(err.Error(), "nimbus") {
		t.Fatalf("build nimbus: %v", err)
	}
}
