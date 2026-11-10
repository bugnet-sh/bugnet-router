package koyeb

import (
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const testDigest = "ghcr.io/bugnet-sh/base@sha256:" +
	"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func imageProvider(t *testing.T, f *fakeKoyeb, image string) *Provider {
	t.Helper()
	p, err := New(Options{APIToken: fakeToken, BaseURL: f.api.URL, Image: image})
	if err != nil {
		t.Fatal(err)
	}
	f.trust(p)
	return p
}

// dockerImage is docker.image of the n-th service body the fake saw
func dockerImage(f *fakeKoyeb, n int) any {
	return field(f.serviceBodies()[n], "definition.docker.image")
}

func TestKoyebCreateUsesImage(t *testing.T) {
	f := newFake(t)
	p := imageProvider(t, f, testDigest)
	size, _ := p.SizeFor(provider.Resources{VCPU: 2, MemoryMiB: 2048, DiskGiB: 10})
	if _, err := p.Create(t.Context(), provider.CreateRequest{
		IdempotencyKey: "k1", Size: size, Lifetime: 15 * time.Minute,
	}); err != nil {
		t.Fatal(err)
	}
	if got := dockerImage(f, 0); got != testDigest {
		t.Errorf("docker.image %v, want the digest", got)
	}
	other := testDigest[:len(testDigest)-1] + "e"
	if _, err := p.Create(t.Context(), provider.CreateRequest{
		IdempotencyKey: "k2", Size: size, Lifetime: 15 * time.Minute, Image: other,
	}); err != nil {
		t.Fatal(err)
	}
	if got := dockerImage(f, 1); got != other {
		t.Errorf("docker.image %v, want the request's image", got)
	}
}

func TestKoyebRefusesBuilder(t *testing.T) {
	p := imageProvider(t, newFake(t), "")
	size, _ := p.SizeFor(provider.Resources{VCPU: 2})
	_, err := p.Create(t.Context(), provider.CreateRequest{
		Size: size, Lifetime: time.Minute, Builder: true,
	})
	if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeInvalid {
		t.Fatalf("Create gave %v, want invalid", err)
	}
}

func TestKoyebMissingImageCanFailOver(t *testing.T) {
	f := newFake(t)
	f.missingImages = map[string]bool{testDigest: true}
	p := imageProvider(t, f, testDigest)
	size, _ := p.SizeFor(provider.Resources{VCPU: 2, MemoryMiB: 2048, DiskGiB: 10})
	_, err := p.Create(t.Context(), provider.CreateRequest{
		IdempotencyKey: "k1", Size: size, Lifetime: 15 * time.Minute,
	})
	if e, ok := provider.AsError(err); !ok || !e.CanFailOver {
		t.Fatalf("Create gave %v, want a failover error", err)
	}
}
