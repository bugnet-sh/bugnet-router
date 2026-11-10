package boat

import (
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

func fromProvider(t *testing.T, f *fakeBoat, from string) *Provider {
	t.Helper()
	p, err := New(Options{APIKey: fakeKey, BaseURL: f.srv.URL + "/api/v1", From: from})
	if err != nil {
		t.Fatal(err)
	}
	p.pollInterval = 10 * time.Millisecond
	p.imagePoll = 5 * time.Millisecond
	return p
}

func TestBoatCreateFrom(t *testing.T) {
	f := newFake(t)
	f.addSaved("bugnet-dev-2026-11-1")
	p := fromProvider(t, f, "bugnet-dev-2026-11-1")
	size, _ := p.SizeFor(provider.Resources{VCPU: 2})
	if _, err := p.Create(t.Context(), provider.CreateRequest{
		Size: size, Lifetime: 10 * time.Minute, Labels: map[string]string{"bugnet": "1"},
	}); err != nil {
		t.Fatal(err)
	}
	body := f.lastCreate().body
	eq(t, body["from"], any("bugnet-dev-2026-11-1"))
	eq(t, body["noEnv"], any(true))
	eq(t, body["failFast"], any(true))
	eq(t, body["snapshots"], any(false))
	eq(t, body["monitoring"], any(false))
}

func TestBoatRequestImageWinsOverSettings(t *testing.T) {
	f := newFake(t)
	f.addSaved("bugnet-dev-2026-11-2")
	p := fromProvider(t, f, "bugnet-dev-2026-11-1")
	size, _ := p.SizeFor(provider.Resources{VCPU: 2})
	if _, err := p.Create(t.Context(), provider.CreateRequest{
		Size: size, Lifetime: time.Minute, Image: "bugnet-dev-2026-11-2",
	}); err != nil {
		t.Fatal(err)
	}
	eq(t, f.lastCreate().body["from"], any("bugnet-dev-2026-11-2"))
}

func TestBoatBuilderHasSnapshotsOn(t *testing.T) {
	f := newFake(t)
	p := fromProvider(t, f, "")
	size, _ := p.SizeFor(provider.Resources{VCPU: 1})
	if _, err := p.Create(t.Context(), provider.CreateRequest{
		Size: size, Lifetime: time.Hour, Builder: true,
	}); err != nil {
		t.Fatal(err)
	}
	body := f.lastCreate().body
	eq(t, body["snapshots"], any(true))
	eq(t, body["noEnv"], any(true))
	eq(t, body["ttlSeconds"], any(float64(3600)))
	eq(t, body["type"], any("small"))
	if _, has := body["labels"]; has {
		t.Errorf("a builder carries labels %v", body["labels"])
	}
	if _, has := body["from"]; has {
		t.Errorf("a builder starts from %v", body["from"])
	}
	_, err := p.Create(t.Context(), provider.CreateRequest{
		Size: size, Lifetime: time.Hour, Builder: true, Labels: map[string]string{"a": "b"},
	})
	if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeInvalid {
		t.Errorf("a labelled builder gave %v, want invalid", err)
	}
}

func TestBoatSaveImagePollsUntilReady(t *testing.T) {
	f := newFake(t)
	f.savedPolls = 3
	p := fromProvider(t, f, "")
	var _ provider.ImageSaver = p
	size, _ := p.SizeFor(provider.Resources{VCPU: 1})
	ref, err := p.Create(t.Context(), provider.CreateRequest{
		Size: size, Lifetime: time.Hour, Builder: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	name, err := p.SaveImage(t.Context(), ref, "bugnet-dev-2026-11-1")
	if err != nil || name != "bugnet-dev-2026-11-1" {
		t.Fatalf("SaveImage gave %q, %v", name, err)
	}
	if n := f.callCount("GET /api/v1/saved-snapshots/{name}"); n < 4 {
		t.Errorf("GET ran %d times, want the existence check and the polls", n)
	}
	// A name that exists is refused before any save
	_, err = p.SaveImage(t.Context(), ref, "bugnet-dev-2026-11-1")
	if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeInvalid {
		t.Errorf("a second save gave %v, want invalid", err)
	}
	eq(t, f.callCount("POST /api/v1/saved-snapshots"), 1)
}

func TestBoatSaveImageNeedsSnapshotsOn(t *testing.T) {
	f := newFake(t)
	p := fromProvider(t, f, "")
	ref, err := p.Create(t.Context(), provider.CreateRequest{
		Size: provider.Size{Name: "small"}, Lifetime: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.SaveImage(t.Context(), ref, "x"); err == nil {
		t.Fatal("a sandbox made without snapshots was saved")
	}
}

func TestBoatMissingSavedSnapshotCanFailOver(t *testing.T) {
	f := newFake(t)
	p := fromProvider(t, f, "bugnet-gone")
	size, _ := p.SizeFor(provider.Resources{VCPU: 2})
	_, err := p.Create(t.Context(), provider.CreateRequest{Size: size, Lifetime: time.Minute})
	if e, ok := provider.AsError(err); !ok || !e.CanFailOver {
		t.Fatalf("Create gave %v, want a failover error", err)
	}
}

func TestBoatDeleteImage(t *testing.T) {
	f := newFake(t)
	f.addSaved("s")
	p := fromProvider(t, f, "")
	for range 2 {
		if err := p.DeleteImage(t.Context(), "s"); err != nil {
			t.Fatal(err)
		}
	}
}
