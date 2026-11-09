package boxd

import (
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

func TestBoxdInspectMapsStatuses(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	deleteAt := time.Now().Add(time.Hour).UTC().Truncate(time.Millisecond)

	for status, want := range map[string]provider.State{
		"pending": provider.StateStarting, "starting": provider.StateStarting,
		"running": provider.StateRunning, "migrating": provider.StateRunning,
		"suspended": provider.StateStopped, "hibernated": provider.StateStopped,
		"stopped": provider.StateStopped, "failed": provider.StateFailed,
	} {
		t.Run(status, func(t *testing.T) {
			vm := f.add("m-"+status, map[string]string{"bugnet": "1"})
			f.mu.Lock()
			vm.status, vm.deleteAt = status, deleteAt
			f.mu.Unlock()
			got, err := p.Inspect(t.Context(), newRef(vm.id))
			if err != nil {
				t.Fatal(err)
			}
			eq(t, got, provider.Status{State: want, Reason: status, ExpiresAt: deleteAt})
		})
	}
}

func TestBoxdInspectGoneOnNotFoundAndErrorsOnANewStatus(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	got, err := p.Inspect(t.Context(), newRef("vm-9999"))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, got.State, provider.StateGone)

	vm := f.add("odd", map[string]string{"bugnet": "1"})
	f.mu.Lock()
	vm.status = "teleporting"
	f.mu.Unlock()
	if _, err := p.Inspect(t.Context(), newRef(vm.id)); err == nil {
		t.Fatal("a status boxd added since read as a definite state")
	}
}

func TestBoxdListingCarriesStateAndExpiry(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	vm := f.add("m", map[string]string{"bugnet": "1"})
	f.mu.Lock()
	vm.status, vm.deleteAt = "hibernated", time.Now().Add(time.Hour)
	f.mu.Unlock()
	owned, err := p.ListOwned(t.Context(), map[string]string{"bugnet": "1"})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(owned), 1)
	eq(t, owned[0].State, provider.StateStopped)
	if owned[0].ExpiresAt.IsZero() {
		t.Fatal("the listing dropped delete_at_ms")
	}
}
