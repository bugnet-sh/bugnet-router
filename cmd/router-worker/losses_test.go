package main

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

func TestMonitorShowsLostEndsOverTheHourlyAlert(t *testing.T) {
	pool := migrated(t)
	st := store.New(pool)
	vk, _ := testenv.Valkey(t)
	ask := func(c lossCheck) (int, string) {
		rec := httptest.NewRecorder()
		monitor(discard, st, vk, testPrefix, healthy, testPools, feedCheck{}, nil,
			limitsCheck{}, nil, canaryCheck{}, c, spendCheck{})(rec,
			httptest.NewRequest(http.MethodGet, "/monitor", nil))
		return rec.Code, rec.Body.String()
	}
	insert := func(prov, reason string, n int, ago time.Duration) {
		for i := range n {
			p, _, err := st.CreatePlacement(t.Context(), store.NewPlacement{
				OrderID:      prov + reason + strconv.Itoa(i) + uuid.NewString(),
				HandleSHA256: []byte(uuid.NewString()), PackageID: "p",
			})
			if err != nil {
				t.Fatal(err)
			}
			_, err = pool.Exec(t.Context(), `update placements set state = 'terminated',
				provider = $2, termination_reason = $3, terminating_at = $4 where id = $1`,
				p.ID, prov, reason, time.Now().Add(-ago))
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	insert("boat", "provider_failed", 3, time.Minute)
	insert("e2b", "provider_failed", 4, 2*time.Minute)
	insert("boxd", "provider_failed", 9, 2*time.Hour)
	insert("koyeb", "sandbox_exited", 9, time.Minute)

	code, body := ask(lossCheck{alertPerHour: 3})
	want := "provider e2b: 4 placements ended as provider_failed in the last hour\n"
	if code != http.StatusServiceUnavailable || body != want {
		t.Fatalf("answered %d %q, want only e2b over three an hour", code, body)
	}
	if code, body := ask(lossCheck{}); code != http.StatusOK || body != "ok\n" {
		t.Fatalf("answered %d %q with no threshold set, want ok", code, body)
	}
}

func TestMonitorShowsAKillNoRouterAskedFor(t *testing.T) {
	pool := migrated(t)
	st := store.New(pool)
	vk, _ := testenv.Valkey(t)
	add := func(prov, alert string, ago time.Duration) {
		t.Helper()
		err := st.InsertSignal(t.Context(), store.Signal{
			Class: "provider_failed", Provider: prov, PlacementID: uuid.New(), Source: "hook",
			Observed: []byte(`{"alert":"` + alert + `"}`), Action: "ended",
			CreatedAt: time.Now().Add(-ago),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	add("e2b", store.AlertUnaskedKill, time.Minute)
	add("e2b", store.AlertUnaskedKill, 2*time.Minute)
	add("e2b", store.AlertUnaskedKill, 3*time.Hour)
	add("boat", "other", time.Minute)
	rec := httptest.NewRecorder()
	monitor(discard, st, vk, testPrefix, healthy, testPools, feedCheck{}, nil, limitsCheck{},
		nil, canaryCheck{}, lossCheck{alertPerHour: 3}, spendCheck{})(rec,
		httptest.NewRequest(http.MethodGet, "/monitor", nil))
	want := "provider e2b: 2 sandboxes killed that no router asked for\n"
	if rec.Code != http.StatusServiceUnavailable || rec.Body.String() != want {
		t.Fatalf("answered %d %q, want %q", rec.Code, rec.Body.String(), want)
	}
}
