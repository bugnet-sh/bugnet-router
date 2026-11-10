package main

import (
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

func TestMonitorImageLines(t *testing.T) {
	p := migrated(t)
	st := store.New(p)
	ctx := t.Context()
	prov := local.New(local.Options{Name: "a", Root: t.TempDir()})
	reg, err := provider.NewRegistry([]provider.Provider{prov},
		provider.Features{Images: map[string]string{"a": "2026.10.1"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := imageLines(discard, st, reg, nil)(ctx); len(got) != 0 {
		t.Fatalf("lines %v, want none while accept is empty", got)
	}
	lines := imageLines(discard, st, reg, []string{"2026.11.1"})(ctx)
	if len(lines) != 1 || !strings.Contains(lines[0], "a is kept out of placements") {
		t.Fatalf("lines %v, want the kept-out provider", lines)
	}
	sample := func(code, reseed string, age time.Duration) {
		t.Helper()
		m := store.StartSample{
			Kind: store.SamplePlacement, Provider: "a", SizeName: "s",
			StartedAt: time.Now().Add(-age), CreateMS: 1, ReadyMS: 1, RunnerMS: -1, GateMS: -1,
			FirstCmdMS: -1, WarmCmdMS: -1, DestroyMS: -1, WakeMS: -1, Outcome: "ok",
			ErrorCode: code, Reseed: reseed,
		}
		if err := st.InsertStartSample(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	sample("image_mismatch", "", time.Minute)
	sample("image_mismatch", "", 2*time.Hour)
	sample("", "write", time.Minute)
	sample("", "ioctl", time.Minute)
	lines = imageLines(discard, st, reg, nil)(ctx)
	if len(lines) != 2 || !strings.Contains(lines[0], "a had 1 image_mismatch") ||
		!strings.Contains(lines[1], "a had 1 sandboxes whose seed") {
		t.Fatalf("lines %v, want one mismatch and one reseed line", lines)
	}
}
