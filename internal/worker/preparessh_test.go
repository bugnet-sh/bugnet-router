package worker_test

import (
	"context"
	"encoding/json"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/worker"
)

// hostKey is an ed25519 public key line, as ssh-up prints the sandbox's host key
const hostKey = "ssh-ed25519 " +
	"AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl"

// fakeSSHUp answers ssh-up on the first port of its spec and runs every other command
type fakeSSHUp struct {
	*local.Provider
	runs *atomic.Int32
}

func (f fakeSSHUp) Exec(
	ctx context.Context, ref provider.Ref, spec provider.ExecSpec,
) (provider.ExecStream, error) {
	if !slices.Contains(spec.Argv, "ssh-up") {
		return f.Provider.Exec(ctx, ref, spec)
	}
	f.runs.Add(1)
	raw, _, err := f.ReadFile(ctx, ref, "/tmp/bugnet/ssh-spec.json", 0, 1<<20)
	var got struct{ Ports []int }
	if err != nil || json.Unmarshal(raw, &got) != nil || len(got.Ports) == 0 {
		got.Ports = []int{0}
	}
	out := `{"port":` + strconv.Itoa(got.Ports[0]) + `,"host_key":"` + hostKey + `"}`
	return f.Provider.Exec(ctx, ref, provider.ExecSpec{
		Argv: []string{"sh", "-c", `printf '%s\n' "$1"`, "sh", out},
	})
}

func TestPrepareSSHJobMakesSSHReady(t *testing.T) {
	runs := &atomic.Int32{}
	e := newEnv(t, options{providers: []provider.Provider{
		fakeSSHUp{Provider: newLocal(t, "local"), runs: runs},
	}})
	p, _ := e.placeReady(t, "order-ssh")
	e.start(t)
	e.run(t, jobargs.PrepareSSH{PlacementID: p.ID})
	host, err := e.store.SSHHost(t.Context(), p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if host.HostKey != hostKey || runs.Load() != 1 {
		t.Fatalf("host %+v after %d runs, want the key ssh-up printed after one", host,
			runs.Load())
	}
	e.run(t, jobargs.PrepareSSH{PlacementID: p.ID})
	if runs.Load() != 1 {
		t.Fatalf("a second job ran ssh-up again, %d runs", runs.Load())
	}
}

func TestPrepareSSHTimeoutIsUnderTheRescue(t *testing.T) {
	if got := worker.PrepareSSHTimeout(); got != 150*time.Second {
		t.Fatalf("prepare_ssh timeout %v, want 150 s", got)
	}
}
