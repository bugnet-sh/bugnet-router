package boxd

import (
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/boxd/boxdpb"
)

func TestBoxdReadUsageGivesTheOrgsWindowInOneCall(t *testing.T) {
	f := newFake(t)
	f.orgUsage = &boxdpb.GetOrgUsageResponse{
		Currency: "eur", TotalCostMicro: 1000, Complete: false,
		Vms: []*boxdpb.VmUsage{{
			VmId: "vm-1", CostMicro: 400, Seconds: &boxdpb.StateSeconds{Running: 60, Standby: 30},
		}},
	}
	from := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	u, err := f.provider(t, fakeKey).ReadUsage(t.Context(), from, from.Add(24*time.Hour),
		nil, "")
	if err != nil {
		t.Fatal(err)
	}
	// The euro cost is priced up at the adapter's fixed rate
	eq(t, u.TotalMicros, int64(1150))
	eq(t, u.Complete, false)
	eq(t, u.Attributed, false)
	eq(t, u.Next, "")
	eq(t, u.Sandboxes, []provider.SandboxUsage{{ID: "vm-1", Seconds: 90, CostMicros: 460}})
	f.mu.Lock()
	asked := f.usageAsked
	f.mu.Unlock()
	eq(t, asked.GetSince(), from.Unix())
	eq(t, asked.GetUntil(), from.Add(24*time.Hour).Unix())
}
