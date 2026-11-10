package e2b

import (
	"strconv"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/e2b/api"
)

func killed(id, template string, at time.Time, ms float64, meta map[string]any) api.SandboxEvent {
	data := map[string]interface{}{"execution_time": ms}
	if meta != nil {
		data["sandbox_metadata"] = meta
	}
	return api.SandboxEvent{
		SandboxId: id, SandboxTemplateId: template, Type: killedType, Timestamp: at,
		Version: "v1", EventData: &data,
	}
}

func TestE2BReadUsagePricesKilledEventsAndPages(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	at := time.Now().Add(-time.Hour)
	mine := map[string]any{deploymentLabel: "main"}
	// Newest first, as the call asks for them
	for i := range eventsPage + 1 {
		f.teamEvents = append(f.teamEvents,
			killed("s"+strconv.Itoa(i), "bugnet-2x2", at, 3_600_000, mine))
	}
	f.teamEvents = append(f.teamEvents, killed("old", "bugnet-2x2", at.Add(-48*time.Hour), 1, mine))
	from, to := at.Add(-time.Minute), time.Now()
	first, err := p.ReadUsage(t.Context(), from, to, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(first.Sandboxes), eventsPage)
	eq(t, first.Next, strconv.Itoa(eventsPage))
	eq(t, first.Attributed, true)
	rate := hourlyMicros(provider.Resources{VCPU: 2, MemoryMiB: 2048})
	eq(t, first.Sandboxes[0], provider.SandboxUsage{
		ID: "s0", Seconds: 3600, CostMicros: rate, Deployment: "main",
	})
	// The next page reaches an event older than the window, which ends the read
	second, err := p.ReadUsage(t.Context(), from, to, nil, first.Next)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(second.Sandboxes), 1)
	eq(t, second.Next, "")
}

func TestE2BReadUsageNamesAnEventWithoutMetadataUnattributed(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	at := time.Now().Add(-time.Minute)
	f.teamEvents = []api.SandboxEvent{killed("s1", "someone-elses-template", at, 1000, nil)}
	u, err := p.ReadUsage(t.Context(), at.Add(-time.Hour), time.Now(), nil, "")
	if err != nil {
		t.Fatal(err)
	}
	eq(t, u.Attributed, false)
	// A template that is not ours cannot be priced
	eq(t, u.Sandboxes, []provider.SandboxUsage{{ID: "s1", Seconds: 1, CostMicros: -1}})
}

func TestE2BReadUsageSkipsAWindowOlderThanTheEventsKept(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	old := time.Now().Add(-10 * 24 * time.Hour)
	u, err := p.ReadUsage(t.Context(), old.Add(-24*time.Hour), old, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	eq(t, u.Sandboxes == nil, true)
	eq(t, u.Complete, true)
	eq(t, f.requests.Load(), int64(0))
}
