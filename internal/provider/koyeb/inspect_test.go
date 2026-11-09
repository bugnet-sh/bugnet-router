package koyeb

import (
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

func TestKoyebInspectMapsDeploymentStatuses(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	ref, err := createKey(t, p, "key-inspect", nil)
	if err != nil {
		t.Fatal(err)
	}
	for status, want := range map[string]provider.State{
		"HEALTHY": provider.StateRunning, "DEGRADED": provider.StateRunning,
		"UNHEALTHY": provider.StateDegraded, "ALLOCATING": provider.StateStarting,
		"STARTING": provider.StateStarting, "STOPPED": provider.StateStopped,
		"ERROR": provider.StateFailed,
	} {
		t.Run(status, func(t *testing.T) {
			f.setDeployment(ref.ID, func(d *fakeDeployment) { d.status = status })
			got, err := p.Inspect(t.Context(), ref)
			if err != nil {
				t.Fatal(err)
			}
			eq(t, got.State, want)
			eq(t, got.Reason, status)
			// The service's backstop is its creation plus the 86400 s life cycle
			if d := time.Until(got.ExpiresAt); d < 23*time.Hour || d > 25*time.Hour {
				t.Fatalf("expires in %v, want about a day", d)
			}
		})
	}
	f.setDeployment(ref.ID, func(d *fakeDeployment) { d.status = "WARMING_UP_SOMEHOW" })
	if _, err := p.Inspect(t.Context(), ref); err == nil {
		t.Fatal("a status Koyeb added since read as a definite state")
	}
}

func TestKoyebInspectReadsTheNewestInstanceEventOfAFailure(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	ref, err := createKey(t, p, "key-event", nil)
	if err != nil {
		t.Fatal(err)
	}
	f.setDeployment(ref.ID, func(d *fakeDeployment) { d.status = "ERROR" })
	_, dep, _ := f.box(ref.ID)
	f.mu.Lock()
	f.instanceEvents[dep.id] = "evicted for node maintenance"
	f.mu.Unlock()
	got, err := p.Inspect(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, got.State, provider.StateFailed)
	eq(t, got.Reason, "INSTANCE_EVENT evicted for node maintenance")

	// A failure with no event keeps the status word
	f.mu.Lock()
	delete(f.instanceEvents, dep.id)
	f.mu.Unlock()
	got, err = p.Inspect(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, got.Reason, "ERROR")
}

func TestKoyebInspectGoneOnADeletedApp(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	ref, err := createKey(t, p, "key-gone", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Destroy(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	got, err := p.Inspect(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, got.State, provider.StateGone)
}

func TestKoyebListingCarriesStateAndExpiry(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	ref, err := createKey(t, p, "key-list", nil)
	if err != nil {
		t.Fatal(err)
	}
	f.setService(ref.ID, func(s *fakeService) { s.status = "UNHEALTHY" })
	for range 2 {
		// The second listing answers from what the first remembered, with the state read anew
		owned, err := p.ListOwned(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		eq(t, len(owned), 1)
		eq(t, owned[0].State, provider.StateDegraded)
		if owned[0].ExpiresAt.IsZero() {
			t.Fatal("the listing dropped the life cycle")
		}
	}
	f.setService(ref.ID, func(s *fakeService) { s.status = "HEALTHY" })
	owned, err := p.ListOwned(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, owned[0].State, provider.StateRunning)
}
