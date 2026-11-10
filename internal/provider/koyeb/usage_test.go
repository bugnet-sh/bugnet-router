package koyeb

import (
	"strconv"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

func TestKoyebReadUsagePagesAndPricesOurInstanceTypes(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	for i := range 101 {
		f.usageDetails = append(f.usageDetails, map[string]any{
			"app_id": "app-" + strconv.Itoa(i), "instance_type": "small", "duration_seconds": "30",
		})
	}
	from := time.Now().Add(-48 * time.Hour)
	first, err := p.ReadUsage(t.Context(), from, from.Add(24*time.Hour), nil, "")
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(first.Sandboxes), 100)
	eq(t, first.Next, "100")
	eq(t, first.Complete, true)
	eq(t, first.TotalMicros, int64(-1))
	eq(t, first.Sandboxes[0], provider.SandboxUsage{ID: "app-0", Seconds: 30, CostMicros: 120})
	last, err := p.ReadUsage(t.Context(), from, from.Add(24*time.Hour), nil, first.Next)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(last.Sandboxes), 1)
	eq(t, last.Next, "")
}

func TestKoyebReadUsageOfADayThatRunsToNowIsIncomplete(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	now := time.Now()
	u, err := p.ReadUsage(t.Context(), now.Add(-time.Hour), now.Add(time.Hour), nil, "")
	if err != nil {
		t.Fatal(err)
	}
	eq(t, u.Complete, false)
}

func TestKoyebListLabelledReadsAtMostLimitServicesFromItsCursor(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeToken)
	labels := map[string]string{"bugnet": "1"}
	made := 0
	for range 5 {
		if _, err := p.Create(t.Context(), provider.CreateRequest{
			IdempotencyKey: "k" + strconv.Itoa(made), Size: p.Sizes()[0], Labels: labels,
			Lifetime: time.Hour,
		}); err != nil {
			t.Fatal(err)
		}
		made++
	}
	first, err := p.ListLabelled(t.Context(), labels, "", 3)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(first.Sandboxes), 3)
	eq(t, first.Next, "3")
	rest, err := p.ListLabelled(t.Context(), labels, first.Next, 3)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(rest.Sandboxes), 2)
	eq(t, rest.Next, "")
}
