package e2e

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// statsTx is a base58 signature of 64 bytes
const statsTx = "36CpwVLKDRH7UU96nxCVFzCR8DNoDFCxNu19SKnFiTWW" +
	"TgJUs3HQbSHTb2zHgh2uo8yqUVZz2MjWanciehRuemgr"

// snapshotWait covers the 30 s period of the snapshot job and a slow run
const snapshotWait = 75 * time.Second

const totalsJSON = `{"version": 1,
	"daily": [{"day": "2026-11-07", "earnedMicros": 50000, "refundedMicros": 0,
		"allowanceReturnedMicros": 0, "x402SpentMicros": 0}],
	"totals": {"earnedMicros": 50000, "refundedMicros": 0, "allowanceReturnedMicros": 0,
		"x402SpentMicros": 0},
	"byKind": [{"kind": "sandbox", "count": 1, "micros": 50000}],
	"refunds": {"count": 0, "micros": 0},
	"byMethod": [],
	"limits": {"order.max_micros": 100000000}}`

// feedSummary is the part of the summary this test reads
type feedSummary struct {
	Version int `json:"version"`
	Now     struct {
		Active int `json:"active"`
	} `json:"now"`
	Totals struct {
		Sandboxes    int    `json:"sandboxes"`
		EarnedMicros int    `json:"earnedMicros"`
		SpentMicros  *int64 `json:"spentMicros"`
	} `json:"totals"`
	Packages []struct {
		ID  string `json:"id"`
		GPU any    `json:"gpu"`
	} `json:"packages"`
	Limits map[string]int64 `json:"limits"`
}

type feedSessions struct {
	Items []struct {
		Provider *string `json:"provider"`
		Status   string  `json:"status"`
		Wallet   *string `json:"wallet"`
		Tx       *string `json:"tx"`
	} `json:"items"`
	Next *string `json:"next"`
}

func getFeed(t *testing.T, url string, into any) http.Header {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s answered %d %s", url, resp.StatusCode, body)
	}
	if err := json.Unmarshal(body, into); err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	return resp.Header
}

func TestE2EStatsFeed(t *testing.T) {
	t.Parallel()
	var bearer atomic.Value
	bearer.Store("")
	payments := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/stats/totals" ||
			r.Header.Get("Authorization") != bearer.Load().(string) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = io.WriteString(w, totalsJSON)
	}))
	defer payments.Close()
	s := startStackWith(t, stackOptions{payments: payments.URL, extra: `
[wallets]
use_payer = true
emit_events = true
facilitator_addresses = ["` + e2eFacilitator + `"]
`})
	bearer.Store("Bearer " + s.keys["BUGNET_PAYMENTS_ROUTER_TOKEN"])
	out := parseKV(s.devOK(t, s.a, "create", "--package", smallPkg, "--payer", e2ePayer,
		"--fee-payer", e2eFacilitator, "--tx", statsTx))
	if out["state"] != "ready" {
		t.Fatalf("create printed %v", out)
	}

	var sum feedSummary
	deadline := time.Now().Add(snapshotWait)
	for {
		resp, err := http.Get(s.a.stats + "/api/stats")
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK && json.Unmarshal(body, &sum) == nil &&
				sum.Totals.Sandboxes == 1 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no snapshot with the sandbox within %s: %+v", snapshotWait, sum)
		}
		time.Sleep(time.Second)
	}
	header := getFeed(t, s.b.stats+"/api/stats", &sum)
	if header.Get("Cache-Control") != "public, max-age=30" ||
		header.Get("Content-Type") != "application/json" {
		t.Fatalf("summary headers %v", header)
	}
	if sum.Version != 1 || sum.Now.Active != 1 || sum.Totals.EarnedMicros != 50000 ||
		sum.Totals.SpentMicros != nil {
		t.Fatalf("summary %+v", sum)
	}
	if len(sum.Packages) != 2 || sum.Packages[0].ID != "e2e-small-1m" ||
		sum.Packages[1].ID != smallPkg || sum.Packages[0].GPU != nil {
		t.Fatalf("packages %+v, want the two start packages by price", sum.Packages)
	}
	if sum.Limits["wallet.max_concurrent"] != 5 || sum.Limits["order.max_micros"] != 100000000 {
		t.Fatalf("limits %v", sum.Limits)
	}

	var page feedSessions
	getFeed(t, s.b.stats+"/api/stats/sessions", &page)
	if len(page.Items) != 1 || page.Next != nil {
		t.Fatalf("sessions %+v", page)
	}
	item := page.Items[0]
	if item.Status != "running" || item.Provider == nil || *item.Provider != "local" ||
		item.Wallet == nil || *item.Wallet != e2ePayer || item.Tx == nil || *item.Tx != statsTx {
		t.Fatalf("session %+v", item)
	}
}
