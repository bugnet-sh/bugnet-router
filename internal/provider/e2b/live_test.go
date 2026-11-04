package e2b

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/provider/contract"
)

const liveTimeout = 3 * time.Minute

// liveProvider talks to the real E2B and skips the test without a key and built templates
func liveProvider(t *testing.T) *Provider {
	t.Helper()
	key, list := os.Getenv("E2B_API_KEY"), os.Getenv("BUGNET_E2B_TEMPLATES")
	if key == "" || list == "" {
		t.Skip("E2B_API_KEY and BUGNET_E2B_TEMPLATES are not both set")
	}
	templates, err := ParseTemplates(list)
	if err != nil {
		t.Fatal(err)
	}
	p, err := New(Options{
		APIKey: key, Domain: os.Getenv("BUGNET_E2B_DOMAIN"), Templates: templates,
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

func TestE2BLiveArch(t *testing.T) {
	p := liveProvider(t)
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
