package e2b

import (
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

func TestE2BImageOverridesTemplate(t *testing.T) {
	f := newFake(t)
	f.images["bugnet-dev-2026-11-1-2x2"] = true
	p := f.provider(t, fakeKey)
	size, _ := p.SizeFor(provider.Resources{VCPU: 2})
	_, err := p.Create(t.Context(), provider.CreateRequest{
		Size: size, Lifetime: 10 * time.Minute, Image: "bugnet-dev-2026-11-1-2x2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := f.lastCreate().body["templateID"]; got != "bugnet-dev-2026-11-1-2x2" {
		t.Errorf("templateID %v, want the image", got)
	}
	if _, err := p.Create(t.Context(), provider.CreateRequest{
		Size: size, Lifetime: 10 * time.Minute,
	}); err != nil {
		t.Fatal(err)
	}
	if got := f.lastCreate().body["templateID"]; got != "bugnet-2x2" {
		t.Errorf("templateID %v, want the size's template", got)
	}
}

func TestE2BRefusesBuilder(t *testing.T) {
	p := newFake(t).provider(t, fakeKey)
	size, _ := p.SizeFor(provider.Resources{VCPU: 2})
	_, err := p.Create(t.Context(), provider.CreateRequest{
		Size: size, Lifetime: time.Minute, Builder: true,
	})
	if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeInvalid {
		t.Fatalf("Create gave %v, want invalid", err)
	}
}

func TestE2BMissingTemplateCanFailOver(t *testing.T) {
	p := newFake(t).provider(t, fakeKey)
	size, _ := p.SizeFor(provider.Resources{VCPU: 2})
	_, err := p.Create(t.Context(), provider.CreateRequest{
		Size: size, Lifetime: time.Minute, Image: "bugnet-gone",
	})
	if e, ok := provider.AsError(err); !ok || !e.CanFailOver {
		t.Fatalf("Create gave %v, want a failover error", err)
	}
}

func TestE2BDeleteImage(t *testing.T) {
	f := newFake(t)
	f.images["bugnet-dev-2026-11-1-2x2"] = true
	p := f.provider(t, fakeKey)
	var _ provider.ImageDeleter = p
	if err := p.DeleteImage(t.Context(), "bugnet-dev-2026-11-1-2x2"); err != nil {
		t.Fatal(err)
	}
	if len(f.deletes) != 1 {
		t.Fatalf("deleted %v", f.deletes)
	}
	if err := p.DeleteImage(t.Context(), "bugnet-dev-2026-11-1-2x2"); err != nil {
		t.Errorf("a second delete gave %v, want nil", err)
	}
}
