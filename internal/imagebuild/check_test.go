package imagebuild

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
)

// bootWith is a BootFunc that leaves the given host key per sandbox
func bootWith(host func(n int) string) (BootFunc, *int) {
	n := new(int)
	return func(_ context.Context, _ provider.Provider, ref provider.Ref) (provider.Ref, error) {
		*n++
		ref.Meta = map[string]string{saga.MetaSSHHostKey: host(*n), saga.MetaReseed: "ioctl"}
		return ref, nil
	}, n
}

func distinctKeys(n int) string { return "ssh-ed25519 K" + strconv.Itoa(n) }

func startCheck(
	t *testing.T, f *fakeProvider, host func(int) string, rnd ...string,
) (CheckReport, error) {
	t.Helper()
	boot, _ := bootWith(host)
	f.preset = rnd
	return Check(t.Context(), f, boot, "bugnet-dev-2026-11-1-2x2", small, needles())
}

func TestCheckPassesCleanDistinctSandboxes(t *testing.T) {
	f := newFakeProvider("e2b")
	rep, err := startCheck(t, f, distinctKeys, strings.Repeat("11", 32), strings.Repeat("22", 32))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(rep.Problems()), 0)
	eq(t, len(rep.ReadyMS), 2)
	eq(t, rep.FreeBytes, int64(123456789))
	eq(t, rep.Manifest.Version, "2026.11.1")
	eq(t, strings.Join(rep.Reseed, ","), "ioctl,ioctl")
	if len(f.creates) != 2 || f.creates[0].Image != "bugnet-dev-2026-11-1-2x2" ||
		f.creates[0].Builder {
		t.Errorf("creates %+v", f.creates)
	}
	if len(f.destroyed) != 2 {
		t.Errorf("destroyed %v, want both check sandboxes", f.destroyed)
	}
}

func TestCheckComparesJobProbeWithManifest(t *testing.T) {
	f := newFakeProvider("boat")
	f.probeTools = func(*fakeBox) map[string]string {
		return map[string]string{"git": "2.43", "python3": "3.12", "node": "22.1"}
	}
	rep, err := startCheck(t, f, distinctKeys, strings.Repeat("11", 32), strings.Repeat("22", 32))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(rep.ToolDiffs), 1)
	if !strings.Contains(rep.ToolDiffs[0], "node") {
		t.Errorf("tool diffs %v, want node", rep.ToolDiffs)
	}
	eq(t, len(rep.Problems()), 1)
}

func TestCheckFlagsSameRandomness(t *testing.T) {
	same, other := strings.Repeat("11", 32), strings.Repeat("22", 32)
	for name, c := range map[string]struct {
		host func(int) string
		rnd  []string
	}{
		"same bytes":    {distinctKeys, []string{same, same}},
		"same host key": {func(int) string { return "ssh-ed25519 SAME" }, []string{same, other}},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFakeProvider("boxd")
			rep, err := startCheck(t, f, c.host, c.rnd...)
			if err != nil {
				t.Fatal(err)
			}
			eq(t, rep.SameRand, true)
			eq(t, len(rep.Problems()), 1)
		})
	}
}

func TestCheckScansBeforeAnyStartStep(t *testing.T) {
	f := newFakeProvider("e2b")
	f.leakFor = func(string) bool { return true }
	var scanned, booted bool
	boot := func(_ context.Context, _ provider.Provider, ref provider.Ref) (provider.Ref, error) {
		booted = true
		scanned = scanned || len(f.boxes[ref.ID].files["/tmp/leak/archive.tar"]) > 0
		ref.Meta = map[string]string{saga.MetaSSHHostKey: "k" + ref.ID}
		return ref, nil
	}
	f.preset = []string{strings.Repeat("11", 32), strings.Repeat("22", 32)}
	rep, err := Check(t.Context(), f, boot, "art", small, needles())
	if err != nil {
		t.Fatal(err)
	}
	if !booted || !scanned {
		t.Errorf("booted %v scanned %v, want the scan to precede the boot", booted, scanned)
	}
	eq(t, len(rep.Findings) > 0, true)
	if strings.Contains(strings.Join(rep.Problems(), " "), testSecret) {
		t.Error("a problem printed a secret")
	}
}

// readyFake is the fake provider with ReadyMeta, which pins a host key per sandbox
type readyFake struct{ *fakeProvider }

func (readyFake) ReadyMeta(_ context.Context, ref provider.Ref) (map[string]string, error) {
	return map[string]string{provider.MetaHostKey: "pinned-" + ref.ID}, nil
}

func TestCheckBootSeesReadyMeta(t *testing.T) {
	f := newFakeProvider("e2b")
	f.preset = []string{strings.Repeat("11", 32), strings.Repeat("22", 32)}
	var seen []string
	boot := func(_ context.Context, _ provider.Provider, ref provider.Ref) (provider.Ref, error) {
		seen = append(seen, ref.Meta[provider.MetaHostKey])
		ref.Meta = map[string]string{saga.MetaSSHHostKey: "k" + ref.ID}
		return ref, nil
	}
	_, err := Check(t.Context(), readyFake{f}, boot, "art", small, needles())
	if err != nil {
		t.Fatal(err)
	}
	eq(t, strings.Join(seen, ","), "pinned-box-1,pinned-box-2")
}
