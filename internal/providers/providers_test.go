package providers_test

import (
	"context"
	"io"
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

func TestBuildRejectsProvidersWithoutAdapter(t *testing.T) {
	for _, name := range []string{"koyeb", "boat", "e2b"} {
		_, err := providers.Build(config.Config{Providers: []string{name}})
		if err == nil || !strings.Contains(err.Error(), name) {
			t.Errorf("build %s: %v", name, err)
		}
	}
}
