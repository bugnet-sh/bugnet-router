package contract_test

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/gpu"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/contract"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
)

var rtx3090 = provider.Size{Name: "rtx3090", Resources: provider.Resources{
	VCPU: 2, MemoryMiB: 2048, DiskGiB: 10, GPUCount: 1, GPUMemoryGiB: 24, GPUModel: gpu.RTX3090,
}}

// timed is a local provider that records each SizeFor floor and the time each WaitReady was
// given, and whose sandboxes are ready only after need
type timed struct {
	*local.Provider
	need   time.Duration
	mu     sync.Mutex
	floors []provider.Resources
	given  []time.Duration
}

func newTimed(t *testing.T, need time.Duration) *timed {
	t.Helper()
	return &timed{need: need, Provider: local.New(local.Options{
		Name: "local", Root: t.TempDir(), Sizes: []provider.Size{rtx3090},
	})}
}

func (p *timed) SizeFor(floor provider.Resources) (provider.Size, bool) {
	p.mu.Lock()
	p.floors = append(p.floors, floor)
	p.mu.Unlock()
	return p.Provider.SizeFor(floor)
}

// WaitReady fails at once when its context ends before need, as a real wait would at the deadline
func (p *timed) WaitReady(ctx context.Context, ref provider.Ref) error {
	if err := p.Provider.WaitReady(ctx, ref); err != nil {
		return err
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil
	}
	given := time.Until(deadline)
	p.mu.Lock()
	p.given = append(p.given, given)
	p.mu.Unlock()
	if given < p.need {
		return context.DeadlineExceeded
	}
	return nil
}

func (p *timed) record() ([]provider.Resources, []time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.floors, p.given
}

// within fails unless got is at most want and less than 10 s under it
func within(t *testing.T, got, want time.Duration) {
	t.Helper()
	if got > want || got < want-10*time.Second {
		t.Fatalf("WaitReady was given %s, want %s", got, want)
	}
}

func TestRunDefaults(t *testing.T) {
	p := newTimed(t, 0)
	contract.Run(t, func(*testing.T) provider.Provider { return p })
	floors, given := p.record()
	if !reflect.DeepEqual(floors, []provider.Resources{{}}) {
		t.Fatalf("SizeFor was asked for %+v, want one empty floor", floors)
	}
	if len(given) != 1 {
		t.Fatalf("WaitReady readied %d times, want 1", len(given))
	}
	within(t, given[0], 3*time.Minute)
}

func TestRunReadyTimeout(t *testing.T) {
	// A sandbox ready only after 4 minutes fails under the default 3
	p := newTimed(t, 4*time.Minute)
	contract.Run(t, func(*testing.T) provider.Provider { return p },
		contract.ReadyTimeout(6*time.Minute))
	_, given := p.record()
	if len(given) != 1 {
		t.Fatalf("WaitReady readied %d times, want 1", len(given))
	}
	within(t, given[0], 6*time.Minute)
}

func TestRunZeroReadyTimeoutIsTheDefault(t *testing.T) {
	p := newTimed(t, 0)
	contract.Run(t, func(*testing.T) provider.Provider { return p }, contract.ReadyTimeout(0))
	_, given := p.record()
	if len(given) != 1 {
		t.Fatalf("WaitReady readied %d times, want 1", len(given))
	}
	within(t, given[0], 3*time.Minute)
}

func TestRunFloor(t *testing.T) {
	p := newTimed(t, 0)
	floor := provider.Resources{GPUCount: 1, GPUModel: gpu.RTX3090}
	contract.Run(t, func(*testing.T) provider.Provider { return p }, contract.Floor(floor))
	floors, _ := p.record()
	if !reflect.DeepEqual(floors, []provider.Resources{floor}) {
		t.Fatalf("SizeFor was asked for %+v, want %+v", floors, floor)
	}
}
