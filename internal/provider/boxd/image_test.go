package boxd

import (
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

var size2x8 = provider.Size{Name: "2x8", Resources: provider.Resources{
	VCPU: 2, MemoryMiB: 8 << 10, DiskGiB: 100,
}}

func snapshotProvider(t *testing.T, f *fakeBoxd, snaps map[string]string) *Provider {
	t.Helper()
	p, err := New(Options{
		APIKey: fakeKey, Endpoint: f.addr, Insecure: true, WakeCheck: []string{"true"},
		Snapshots: snaps,
	})
	if err != nil {
		t.Fatal(err)
	}
	p.pollInterval = 20 * time.Millisecond
	p.imagePoll = 5 * time.Millisecond
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func TestBoxdCreateFromSnapshot(t *testing.T) {
	f := newFake(t)
	f.addSnapshot("bugnet-dev-2026-11-1-2x8", "local")
	p := snapshotProvider(t, f, map[string]string{"2x8": "bugnet-dev-2026-11-1-2x8"})
	_, err := p.Create(t.Context(), provider.CreateRequest{
		IdempotencyKey: "k1", Size: size2x8, Lifetime: 10 * time.Minute,
		Labels: map[string]string{"bugnet": "1"},
		Egress: &provider.EgressAllow{Hosts: []string{"example.com"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	req := f.lastCreate()
	if got := req.GetSpec().GetBase().GetSnapshot(); got != "bugnet-dev-2026-11-1-2x8" {
		t.Errorf("spec.base.snapshot %q", got)
	}
	if !req.GetSpec().GetIsolated() {
		t.Error("the spec is not isolated")
	}
	if len(req.GetSpec().GetEgress()) == 0 {
		t.Error("a restricted package lost its egress list")
	}
	if req.GetDeleteAfterSecs() == 0 {
		t.Error("no delete-after timer")
	}
	if req.GetSpec().GetLabels()["bugnet"] != "1" {
		t.Errorf("labels %v", req.GetSpec().GetLabels())
	}
}

func TestBoxdRequestImageWinsOverSettings(t *testing.T) {
	f := newFake(t)
	f.addSnapshot("bugnet-dev-2026-11-2-2x8", "local")
	p := snapshotProvider(t, f, map[string]string{"2x8": "bugnet-dev-2026-11-1-2x8"})
	if _, err := p.Create(t.Context(), provider.CreateRequest{
		Size: size2x8, Lifetime: time.Minute, Image: "bugnet-dev-2026-11-2-2x8",
	}); err != nil {
		t.Fatal(err)
	}
	if got := f.lastCreate().GetSpec().GetBase().GetSnapshot(); got != "bugnet-dev-2026-11-2-2x8" {
		t.Errorf("spec.base.snapshot %q", got)
	}
}

func TestBoxdBuilderHasNoLabels(t *testing.T) {
	f := newFake(t)
	p := snapshotProvider(t, f, nil)
	_, err := p.Create(t.Context(), provider.CreateRequest{
		IdempotencyKey: "k1", Size: size2x8, Lifetime: time.Hour, Builder: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	req := f.lastCreate()
	if len(req.GetSpec().GetLabels()) != 0 || req.GetName() != "" {
		t.Errorf("builder labels %v name %q", req.GetSpec().GetLabels(), req.GetName())
	}
	if req.GetSpec().GetBase() != nil || !req.GetSpec().GetIsolated() {
		t.Errorf("builder spec %v", req.GetSpec())
	}
	if req.GetDeleteAfterSecs() != 3600 {
		t.Errorf("timer %d, want 3600", req.GetDeleteAfterSecs())
	}
	_, err = p.Create(t.Context(), provider.CreateRequest{
		Size: size2x8, Lifetime: time.Hour, Builder: true, Labels: map[string]string{"a": "b"},
	})
	if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeInvalid {
		t.Errorf("a labelled builder gave %v, want invalid", err)
	}
}

func TestBoxdOCIBuilderHasNoSpec(t *testing.T) {
	f := newFake(t)
	p := snapshotProvider(t, f, nil)
	_, err := p.Create(t.Context(), provider.CreateRequest{
		Size: size2x8, Lifetime: time.Hour, Builder: true,
		Image: "oci:ghcr.io/bugnet-sh/base@sha256:abc",
	})
	if err != nil {
		t.Fatal(err)
	}
	req := f.lastCreate()
	if req.GetSpec() != nil || len(req.GetLabels()) != 0 || !req.GetIsolated() {
		t.Errorf("request %v, want an isolated image_ref create with no spec or labels", req)
	}
	if req.GetImageRef() != "ghcr.io/bugnet-sh/base@sha256:abc" {
		t.Errorf("image_ref %q", req.GetImageRef())
	}
	if req.GetConfig().GetVcpu() != 2 || req.GetConfig().GetMemoryBytes() != 8<<30 {
		t.Errorf("size %v", req.GetConfig())
	}
	_, err = p.Create(t.Context(), provider.CreateRequest{
		Size: size2x8, Lifetime: time.Hour, Image: "oci:x",
	})
	if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeInvalid {
		t.Errorf("an oci image on a sold sandbox gave %v, want invalid", err)
	}
}

func TestBoxdSaveImageWaitsReady(t *testing.T) {
	f := newFake(t)
	f.snapshotPolls = 3
	p := snapshotProvider(t, f, nil)
	var _ provider.ImageSaver = p
	ref, err := p.Create(t.Context(), provider.CreateRequest{
		Size: size2x8, Lifetime: time.Hour, Builder: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	name, err := p.SaveImage(t.Context(), ref, "bugnet-dev-2026-11-1-2x8")
	if err != nil || name != "bugnet-dev-2026-11-1-2x8" {
		t.Fatalf("SaveImage gave %q, %v", name, err)
	}
	if n := f.callCount("GetSnapshot"); n < 4 {
		t.Errorf("GetSnapshot ran %d times, want the existence check and the polls", n)
	}
	p.imageWait = 20 * time.Millisecond
	f.snapshotPolls = 1 << 20
	if _, err := p.SaveImage(t.Context(), ref, "bugnet-dev-2026-11-2-2x8"); err == nil {
		t.Error("a snapshot that never turns ready gave no error")
	}
}

func TestBoxdSaveImageRefusesExistingName(t *testing.T) {
	f := newFake(t)
	f.addSnapshot("bugnet-dev-2026-11-1-2x8", "local")
	p := snapshotProvider(t, f, nil)
	ref, err := p.Create(t.Context(), provider.CreateRequest{
		Size: size2x8, Lifetime: time.Hour, Builder: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.SaveImage(t.Context(), ref, "bugnet-dev-2026-11-1-2x8")
	if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeInvalid {
		t.Fatalf("SaveImage gave %v, want invalid", err)
	}
	if n := f.callCount("CreateSnapshot"); n != 0 {
		t.Errorf("CreateSnapshot ran %d times", n)
	}
}

func TestBoxdMissingSnapshotCanFailOver(t *testing.T) {
	f := newFake(t)
	f.addSnapshot("archived", "archived")
	p := snapshotProvider(t, f, nil)
	for _, image := range []string{"gone", "archived"} {
		_, err := p.Create(t.Context(), provider.CreateRequest{
			Size: size2x8, Lifetime: time.Minute, Image: image,
		})
		if e, ok := provider.AsError(err); !ok || !e.CanFailOver {
			t.Errorf("Create from %s gave %v, want a failover error", image, err)
		}
	}
}

func TestBoxdSnapshotsNeedEverySize(t *testing.T) {
	f := newFake(t)
	p := snapshotProvider(t, f, map[string]string{"1x4": "a"})
	_, err := p.Create(t.Context(), provider.CreateRequest{Size: size2x8, Lifetime: time.Minute})
	if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeInvalid {
		t.Fatalf("Create gave %v, want invalid", err)
	}
}

func TestBoxdDeleteImage(t *testing.T) {
	f := newFake(t)
	f.addSnapshot("s", "local")
	p := snapshotProvider(t, f, nil)
	for range 2 {
		if err := p.DeleteImage(t.Context(), "s"); err != nil {
			t.Fatal(err)
		}
	}
}

func TestBoxdSnapshotStorage(t *testing.T) {
	f := newFake(t)
	f.addSnapshot("s", "archived")
	p := snapshotProvider(t, f, nil)
	got, err := p.SnapshotStorage(t.Context(), "s")
	if err != nil || got != "archived" {
		t.Fatalf("SnapshotStorage gave %q, %v", got, err)
	}
}
