package boat

import (
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

func TestBoatInspectMapsStates(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	labels := map[string]string{"bugnet": "1"}
	since := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)

	for _, c := range []struct {
		name  string
		setup func(id string)
		want  provider.Status
	}{
		{"ready", func(string) {}, provider.Status{State: provider.StateRunning}},
		{"running", func(id string) { f.setState(id, "running") },
			provider.Status{State: provider.StateRunning}},
		{"degraded", func(id string) { f.degrade(id, since) },
			provider.Status{
				State: provider.StateDegraded, Since: since, Reason: "tunnel unreachable",
			}},
		{"init", func(id string) { f.setState(id, "init") },
			provider.Status{State: provider.StateStarting}},
		{"archived", func(id string) { f.setState(id, "archived") },
			provider.Status{State: provider.StateStopped}},
		{"stopped with text", func(id string) { f.stopWith(id, "stopped", "plan limit") },
			provider.Status{State: provider.StateStopped, Reason: "plan limit"}},
		{"cancelled", func(id string) { f.setState(id, "cancelled") },
			provider.Status{State: provider.StateStopped, Reason: "no machine was ready"}},
		{"error", func(id string) { f.stopWith(id, "error", "kernel panic") },
			provider.Status{State: provider.StateFailed, Reason: "kernel panic"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			sb := f.add(labels)
			c.setup(sb.id)
			got, err := p.Inspect(t.Context(), newRef(sb.id, sb.created))
			if err != nil {
				t.Fatal(err)
			}
			// Boat's ttl moves archiveAfter, which the test does not pin
			got.ExpiresAt = time.Time{}
			eq(t, got, c.want)
		})
	}
}

func TestBoatInspectGoneOn404AndErrorsOtherwise(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	st, err := p.Inspect(t.Context(), newRef("bx_nobody", time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, st.State, provider.StateGone)

	sb := f.add(map[string]string{"bugnet": "1"})
	f.setState(sb.id, "something_new")
	if _, err := p.Inspect(t.Context(), newRef(sb.id, sb.created)); err == nil {
		t.Fatal("a state Boat added since read as a definite state")
	}
}

func TestBoatListingCarriesStateAndExpiry(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	sb := f.add(map[string]string{"bugnet": "1"})
	f.degrade(sb.id, time.Now())
	owned, err := p.ListOwned(t.Context(), map[string]string{"bugnet": "1"})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(owned), 1)
	eq(t, owned[0].State, provider.StateDegraded)
	if owned[0].ExpiresAt.IsZero() {
		t.Fatal("the listing dropped archiveAfter")
	}
}
