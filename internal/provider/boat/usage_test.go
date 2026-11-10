package boat

import (
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

func TestBoatReadUsageAsksPerSandboxThenListsTheRest(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	mine := f.add(map[string]string{"bugnet": "1", "bugnet_deployment": "main"})
	gone := f.add(map[string]string{"bugnet": "1", "bugnet_deployment": "main"})
	dev := f.add(map[string]string{"bugnet": "1", "bugnet_deployment": "dev"})
	nobody := f.add(map[string]string{"bugnet": "1"})
	f.add(map[string]string{"other": "1"})
	f.usages = map[string]string{
		mine.id: `{"seconds":120.4,"dollars":0.0012,"running":false}`,
	}
	from, to := time.Now().Add(-time.Hour), time.Now().Add(time.Hour)
	refs := []provider.Ref{
		{Provider: "boat", ID: mine.id}, {Provider: "boat", ID: gone.id},
		{Provider: "e2b", ID: "not-boats"},
	}
	u, err := p.ReadUsage(t.Context(), from, to, refs, "")
	if err != nil {
		t.Fatal(err)
	}
	// A sandbox Boat answers 404 for has no line, and the listing phase comes next
	eq(t, u.Sandboxes, []provider.SandboxUsage{{ID: mine.id, Seconds: 120, CostMicros: 1200}})
	eq(t, u.Complete, true)
	eq(t, u.Next, listCursor)
	eq(t, u.TotalMicros, int64(-1))

	u, err = p.ReadUsage(t.Context(), from, to, refs, u.Next)
	if err != nil {
		t.Fatal(err)
	}
	// The sandboxes that are not in refs and carry the bugnet label, with their deployment
	eq(t, u.Attributed, true)
	eq(t, u.Next, "")
	got := map[string]string{}
	for _, s := range u.Sandboxes {
		got[s.ID] = s.Deployment
		eq(t, s.CostMicros, int64(-1))
	}
	eq(t, got, map[string]string{dev.id: "dev", nobody.id: ""})
}

func TestBoatReadUsageHoldsBackARunningSandbox(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	s := f.add(map[string]string{"bugnet": "1"})
	f.usages = map[string]string{s.id: `{"seconds":5,"dollars":0.00005,"running":true}`}
	u, err := p.ReadUsage(t.Context(), time.Now().Add(-time.Hour), time.Now(),
		[]provider.Ref{{Provider: "boat", ID: s.id}}, "")
	if err != nil {
		t.Fatal(err)
	}
	eq(t, u.Complete, false)
}

func TestBoatListLabelledResumesAtItsCursor(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	for range 5 {
		f.add(map[string]string{"bugnet": "1"})
	}
	f.add(map[string]string{"other": "1"})
	labels := map[string]string{"bugnet": "1"}
	first, err := p.ListLabelled(t.Context(), labels, "", 3)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(first.Sandboxes), 3)
	if first.Next == "" {
		t.Fatal("the first page of three out of five names no next page")
	}
	rest, err := p.ListLabelled(t.Context(), labels, first.Next, 3)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(rest.Sandboxes), 2)
	eq(t, rest.Next, "")
}
