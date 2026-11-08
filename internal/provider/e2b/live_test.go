package e2b

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/contract"
	"github.com/bugnet-sh/bugnet-router/internal/provider/livetest"
)

const liveTimeout = 3 * time.Minute

// liveProvider talks to the real E2B with the config/dev.toml settings, skipping without a key
func liveProvider(t *testing.T) *Provider {
	t.Helper()
	key := os.Getenv("E2B_API_KEY")
	if key == "" {
		t.Skip("E2B_API_KEY is not set")
	}
	cfg := livetest.Settings(t).E2B
	if len(cfg.Templates) == 0 {
		t.Skip("config/dev.toml has no e2b.templates")
	}
	var templates []Template
	for _, tm := range cfg.Templates {
		templates = append(templates, Template{
			Name: tm.Size, TemplateID: tm.Template, VCPU: tm.VCPU, MemoryMiB: tm.MemoryMiB,
			DiskGiB: tm.DiskGiB,
		})
	}
	p, err := New(Options{
		APIKey: key, Domain: cfg.Domain, Templates: templates, MaxLifetime: cfg.MaxLifetime,
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestE2BLiveContract(t *testing.T) {
	p := liveProvider(t)
	contract.Run(t, func(*testing.T) provider.Provider { return p })
}

// liveSandbox creates a ready sandbox for one live test and destroys it when the test ends
func liveSandbox(t *testing.T, p *Provider) provider.Ref {
	t.Helper()
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	labels := map[string]string{"bugnet-live": hex.EncodeToString(b)}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), liveTimeout)
		defer cancel()
		owned, err := p.ListOwned(ctx, labels)
		if err != nil {
			t.Errorf("list sandboxes left by the live run: %v", err)
		}
		for _, s := range owned {
			if err := p.Destroy(ctx, s.Ref); err != nil {
				t.Errorf("destroy %s: %v", s.Ref.ID, err)
			}
		}
	})

	ctx, cancel := context.WithTimeout(t.Context(), liveTimeout)
	defer cancel()
	size, _ := p.SizeFor(provider.Resources{})
	ref, err := p.Create(ctx, provider.CreateRequest{
		IdempotencyKey: labels["bugnet-live"], Size: size, Labels: labels,
		Lifetime: 15 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.WaitReady(ctx, ref); err != nil {
		t.Fatal(err)
	}
	return ref
}

func TestE2BLiveArch(t *testing.T) {
	p := liveProvider(t)
	ref := liveSandbox(t, p)
	if ref.Meta["envd_token"] == "" {
		t.Errorf("ref meta %v has no envd_token, v2 sandboxes are always secured", ref.Meta)
	}
	ctx, cancel := context.WithTimeout(t.Context(), liveTimeout)
	defer cancel()
	out, code, err := runExec(ctx, p, ref, provider.ExecSpec{Argv: []string{"uname", "-m"}})
	if err != nil || code != 0 {
		t.Fatalf("uname -m gave exit %d, %v", code, err)
	}
	machine := strings.TrimSpace(string(out))
	t.Logf("e2b sandbox architecture: uname -m = %s, Ref.Meta[arch] = %s", machine,
		ref.Meta["arch"])
	arch := map[string]string{"x86_64": "amd64", "aarch64": "arm64"}[machine]
	if arch != ref.Meta["arch"] {
		t.Errorf("uname -m is %q but the ref says arch %q", machine, ref.Meta["arch"])
	}
}

func TestLiveE2BFileStreams(t *testing.T) {
	p := liveProvider(t)
	ref := liveSandbox(t, p)
	dir := "/root/bugnet-streams"
	t.Run("Contract", func(t *testing.T) { contract.Streams(t, p, ref, dir) })
	livetest.RoundTrips(t, p, ref, dir)
	t.Run("DevUrandom", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), liveTimeout)
		defer cancel()
		e, err := p.StatFile(ctx, ref, "/dev/urandom")
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("/dev/urandom stats as %+v", e)
		if e.Regular {
			t.Error("/dev/urandom stats as a regular file")
		}
		if r, err := p.OpenReader(ctx, ref, "/dev/urandom", 0); err == nil {
			_ = r.Close()
			t.Error("OpenReader opened /dev/urandom")
		}
	})
}
