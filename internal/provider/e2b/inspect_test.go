package e2b

import (
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

func TestE2BInspectMapsStates(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := f.add(map[string]string{"bugnet": "1"})
	box, _ := f.box(ref.ID)

	got, err := p.Inspect(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, got, provider.Status{State: provider.StateRunning, ExpiresAt: box.endAt.UTC().Round(0)})

	f.setBox(ref.ID, func(b *fakeSandbox) { b.state = "paused" })
	got, err = p.Inspect(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, got.State, provider.StateStopped)

	f.setBox(ref.ID, func(b *fakeSandbox) { b.state = "exploded" })
	if _, err := p.Inspect(t.Context(), ref); err == nil {
		t.Fatal("a state E2B added since read as a definite state")
	}
}

func TestE2BInspectReadsTheKillReasonOfAGoneSandbox(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	for _, reason := range []string{"admin", "timeout", "orphaned", ""} {
		ref := f.add(map[string]string{"bugnet": "1"})
		f.killedBy(ref.ID, reason)
		got, err := p.Inspect(t.Context(), ref)
		if err != nil {
			t.Fatal(err)
		}
		eq(t, got, provider.Status{State: provider.StateGone, KillReason: reason})
	}

	// Our own kill is E2B's request reason
	ref := f.add(map[string]string{"bugnet": "1"})
	if err := p.Destroy(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	got, err := p.Inspect(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, got, provider.Status{State: provider.StateGone, KillReason: "request"})
}

func TestE2BInspectErrorsWhenTheEventsCannotBeRead(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := f.add(map[string]string{"bugnet": "1"})
	f.killedBy(ref.ID, "admin")
	f.inject("GET /events/sandboxes/{id}", fault{status: 500, times: 10})
	if _, err := p.Inspect(t.Context(), ref); err == nil {
		t.Fatal("a failed events read answered gone")
	}
}

func TestE2BListingCarriesStateAndExpiry(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	ref := f.add(map[string]string{"bugnet": "1"})
	f.setBox(ref.ID, func(b *fakeSandbox) { b.state = "paused" })
	owned, err := p.ListOwned(t.Context(), map[string]string{"bugnet": "1"})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(owned), 1)
	eq(t, owned[0].State, provider.StateStopped)
	if owned[0].ExpiresAt.IsZero() {
		t.Fatal("the listing dropped endAt")
	}
}
