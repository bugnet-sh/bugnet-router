package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/pricing"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

func TestSpendCheckFlagsLossOnly(t *testing.T) {
	pool := migrated(t)
	st := store.New(pool)
	vk, _ := testenv.Valkey(t)
	ctx := t.Context()
	rule := pricing.Rule{MarkupBP: 1000}
	ask := func(c spendCheck) (int, string) {
		rec := httptest.NewRecorder()
		monitor(discard, st, vk, testPrefix, healthy, testPools, feedCheck{}, nil,
			limitsCheck{}, nil, canaryCheck{}, lossCheck{}, c)(rec,
			httptest.NewRequest(http.MethodGet, "/monitor", nil))
		return rec.Code, rec.Body.String()
	}
	ended := func(cost, paid int64) {
		p, _, err := st.CreatePlacement(ctx, store.NewPlacement{
			OrderID: uuid.NewString(), HandleSHA256: []byte(uuid.NewString()), PackageID: "p",
			AmountMicros: paid,
		})
		if err != nil {
			t.Fatal(err)
		}
		_, err = pool.Exec(ctx, `update placements set state = 'terminated', cost_micros = $2,
			terminated_at = now() where id = $1`, p.ID, cost)
		if err != nil {
			t.Fatal(err)
		}
	}
	ended(3050, 3355)
	ended(3050, 3354)
	code, body := ask(spendCheck{rule: rule, costAlertFraction: 0.1})
	if code != http.StatusOK || body != "ok\n" {
		t.Fatalf("answered %d %q with only an under-markup run, want ok", code, body)
	}
	ended(3050, 3000)
	code, body = ask(spendCheck{rule: rule, costAlertFraction: 0.1})
	want := "1 placements ended costing more than they paid in the last 24 hours, " +
		"1 more cost over their markup\n"
	if code != http.StatusServiceUnavailable || body != want {
		t.Fatalf("answered %d %q, want %q", code, body, want)
	}
	if err := st.SetForeignSandboxes(ctx, "e2b", 3, time.Now()); err != nil {
		t.Fatal(err)
	}
	day := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -1)
	for _, row := range []store.CostDay{
		{Provider: "boat", Day: day, OursMicros: 100, TheirsMicros: 200, TheirsComplete: true},
		{Provider: "koyeb", Day: day, OursMicros: 100, TheirsMicros: 105, TheirsComplete: true},
		{Provider: "boxd", Day: day, OursMicros: 100, TheirsMicros: -1, UnknownIDs: 2},
	} {
		row.CheckedAt, row.FirstCheckedAt = time.Now(), time.Now()
		if err := st.SaveCostDay(ctx, row); err != nil {
			t.Fatal(err)
		}
	}
	_, body = ask(spendCheck{costAlertFraction: 0.1})
	for _, line := range []string{
		"provider e2b: 3 sandboxes no known deployment owns",
		"provider boat billed 200 micros on " + day.Format("2006-01-02") + " against our 100",
		"provider boxd billed 2 sandboxes that are not ours on " + day.Format("2006-01-02"),
	} {
		if !strings.Contains(body, line) {
			t.Errorf("the answer lacks %q:\n%s", line, body)
		}
	}
	if strings.Contains(body, "koyeb") {
		t.Errorf("koyeb is 5%% over, under the 10%% alert:\n%s", body)
	}
}
