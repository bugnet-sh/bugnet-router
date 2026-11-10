package boxd

import (
	"context"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

func parkProvider(t *testing.T) (*fakeBoxd, *Provider, provider.Ref) {
	t.Helper()
	f, p := newTestProvider(t)
	return f, p, mustCreate(t, p, "park")
}

func TestBoxdParkHibernatesAndUnparkWakes(t *testing.T) {
	f, p, ref := parkProvider(t)
	must(t, p.Park(context.Background(), ref))
	vm, err := f.vm(ref.ID)
	must(t, err)
	eq(t, vm.status, "hibernated")
	must(t, p.Unpark(context.Background(), ref))
	eq(t, vm.status, "running")
	st, err := p.Inspect(context.Background(), ref)
	must(t, err)
	eq(t, st.State, provider.StateRunning)
}

func TestBoxdParkTwiceAndUnparkRunningAreFine(t *testing.T) {
	_, p, ref := parkProvider(t)
	ctx := context.Background()
	must(t, p.Unpark(ctx, ref))
	must(t, p.Park(ctx, ref))
	must(t, p.Park(ctx, ref))
	must(t, p.Unpark(ctx, ref))
}

func TestBoxdUnparkWaitsForTheAgent(t *testing.T) {
	f, p, ref := parkProvider(t)
	must(t, p.Park(context.Background(), ref))
	vm, err := f.vm(ref.ID)
	must(t, err)
	f.mu.Lock()
	vm.agentDownExecs = 3
	f.mu.Unlock()
	must(t, p.Unpark(context.Background(), ref))
	if n := f.callCount("Exec"); n < 4 {
		t.Fatalf("unpark pinged %d times, want at least 4", n)
	}
}

func TestBoxdUnparkGivesUpAfterTenSeconds(t *testing.T) {
	f, p, ref := parkProvider(t)
	must(t, p.Park(context.Background(), ref))
	vm, err := f.vm(ref.ID)
	must(t, err)
	f.mu.Lock()
	vm.agentDownExecs = 1 << 30
	f.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err = p.Unpark(ctx, ref)
	e, ok := provider.AsError(err)
	if !ok || !e.CanFailOver {
		t.Fatalf("unpark of a dead agent gave %v", err)
	}
}

func TestBoxdUnparkUnknownMachine(t *testing.T) {
	_, p, _ := parkProvider(t)
	err := p.Unpark(context.Background(), provider.Ref{Provider: "boxd", ID: "vm-9999"})
	e, ok := provider.AsError(err)
	if !ok || e.Code != provider.CodeNotFound {
		t.Fatalf("got %v, want not found", err)
	}
}

func TestBoxdParkedCostFollowsGiB(t *testing.T) {
	_, p, _ := parkProvider(t)
	eq(t, p.ParkedHourlyCostMicros(provider.Size{Name: "1x4"}), int64(460))
	p.parkedGiB = 10
	eq(t, p.ParkedHourlyCostMicros(provider.Size{Name: "1x4"}), int64(920))
}

func TestBoxdIsAParker(t *testing.T) {
	var _ provider.Parker = (*Provider)(nil)
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
