package saga_test

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/image"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/sshkeys/sshkeystest"
)

var checkManifest = `{"v":1,"version":"2026.11.3","channel":"prod","target":"oci",` +
	`"os":"ubuntu-24.04","built_at":"2026-11-03T00:00:00Z","runner_sha256":{"amd64":"` +
	strings.Repeat("ab", 32) + `"}}`

// checkBox is one check sandbox that answers the baked runner's commands itself
type checkBox struct {
	provider.Provider // nil, so a call CheckBoot should not make panics
	name              string
	upKey             string // the host key ssh-up reports
	mu                *sync.Mutex
	ran               *[]string
}

func newCheckBox(t *testing.T, name string) checkBox {
	return checkBox{name: name, upKey: sshkeystest.ED25519(t), mu: &sync.Mutex{}, ran: &[]string{}}
}

func (b checkBox) Name() string { return b.name }

func (b checkBox) Capabilities() provider.Capabilities { return provider.Capabilities{} }

func (b checkBox) ReadFile(
	_ context.Context, _ provider.Ref, path string, _ int64, _ int,
) ([]byte, bool, error) {
	if path != image.ManifestPath {
		return nil, false, fmt.Errorf("read of %s", path)
	}
	return []byte(checkManifest), true, nil
}

func (b checkBox) WriteFile(context.Context, provider.Ref, string, []byte, fs.FileMode) error {
	return nil
}

func (b checkBox) Exec(
	_ context.Context, _ provider.Ref, spec provider.ExecSpec,
) (provider.ExecStream, error) {
	if len(spec.Argv) == 2 && spec.Argv[0] == "cat" && spec.Argv[1] == image.ManifestPath {
		return printed(checkManifest), nil
	}
	if len(spec.Argv) < 2 || spec.Argv[0] != image.RunnerPath {
		return nil, fmt.Errorf("exec of %v", spec.Argv)
	}
	b.mu.Lock()
	*b.ran = append(*b.ran, spec.Argv[1])
	b.mu.Unlock()
	switch spec.Argv[1] {
	case "ping":
		return printed(""), nil
	case "setup":
		return printed(`{"reseed":"ioctl"}`), nil
	case "ssh-up":
		return printed(`{"port":2022,"host_key":"` + b.upKey + ` root@sandbox"}`), nil
	}
	return nil, fmt.Errorf("exec of %v", spec.Argv)
}

func (b checkBox) calls() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(*b.ran)
}

// printedStream is a finished command that printed its reader and exited 0
type printedStream struct{ io.Reader }

func printed(out string) provider.ExecStream { return printedStream{strings.NewReader(out)} }

func (printedStream) Wait() (int, error) { return 0, nil }
func (printedStream) Close() error       { return nil }

// readyBox is a check sandbox whose provider has ReadyMeta, which pins its own sshd's host key
type readyBox struct{ checkBox }

func (readyBox) ReadyMeta(context.Context, provider.Ref) (map[string]string, error) {
	return nil, nil
}

func pinned(key string) provider.Ref {
	return provider.Ref{ID: "pod", Meta: map[string]string{provider.MetaHostKey: key + " root@pod"}}
}

func TestCheckBootNativeReadsPinnedHostKey(t *testing.T) {
	boot := saga.CheckBoot(saga.BootSettings{}, saga.SSHSettings{NativeSSH: []string{"lium"}})
	ctx := t.Context()

	// Two pods give the two host keys their adapter pinned, and ssh-up never runs
	var keys []string
	for range 2 {
		box, key := newCheckBox(t, "lium"), sshkeystest.ED25519(t)
		got, err := boot(ctx, readyBox{box}, pinned(key))
		eq(t, err, nil)
		eq(t, box.calls(), []string{"ping", "setup"})
		eq(t, got.Meta[saga.MetaSSHHostKey], key)
		eq(t, got.Meta[saga.MetaReseed], "ioctl")
		keys = append(keys, got.Meta[saga.MetaSSHHostKey])
	}
	if keys[0] == keys[1] {
		t.Fatal("two pods gave one host key")
	}

	// A native pod without a pinned key, or with a broken one, fails the check
	for _, ref := range []provider.Ref{{ID: "pod"}, pinned("ssh-ed25519 not-base64!")} {
		box := newCheckBox(t, "lium")
		_, err := boot(ctx, readyBox{box}, ref)
		if err == nil || !strings.Contains(err.Error(), "host_key") {
			t.Fatalf("a pod pinned %q: %v, want an error naming host_key",
				ref.Meta[provider.MetaHostKey], err)
		}
		eq(t, box.calls(), []string{"ping", "setup"})
	}

	// A provider on the gate path still runs ssh-up, whatever its Meta holds
	box := newCheckBox(t, "e2b")
	got, err := boot(ctx, readyBox{box}, pinned(sshkeystest.ED25519(t)))
	eq(t, err, nil)
	eq(t, box.calls(), []string{"ping", "setup", "ssh-up"})
	eq(t, got.Meta[saga.MetaSSHHostKey], box.upKey)
}

func TestCheckBootNativeFromSettings(t *testing.T) {
	for path, native := range map[string][]string{"native": {"boat"}, "gate": nil} {
		t.Run(path, func(t *testing.T) {
			ssh := saga.SSHSettingsOf(config.Config{Boat: config.Boat{SSHPath: path}})
			eq(t, ssh.NativeSSH, native)
			// Boat has no ReadyMeta, so its check runs ssh-up whatever its ssh_path
			box := newCheckBox(t, "boat")
			got, err := saga.CheckBoot(saga.BootSettings{}, ssh)(t.Context(), box, provider.Ref{})
			eq(t, err, nil)
			eq(t, box.calls(), []string{"ping", "setup", "ssh-up"})
			eq(t, got.Meta[saga.MetaSSHHostKey], box.upKey)
		})
	}
}
