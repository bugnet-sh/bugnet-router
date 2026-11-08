package stats

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

var t0 = date(8, 12, 0, 0)

func seededFigures() store.StatsFigures {
	return store.StatsFigures{
		At:        t0,
		Active:    map[string]int64{"boat": 2, "e2b": 1, "koyeb": 0},
		Sandboxes: 40, ComputeSeconds: 51234,
		Days: []store.StatsDay{
			{Day: date(6, 0, 0, 0), Sandboxes: 10, ComputeSeconds: 12000, PeakActive: 3,
				MedianSeconds: 540},
			{Day: date(7, 0, 0, 0), Sandboxes: 24, ComputeSeconds: 21000, PeakActive: 4,
				MedianSeconds: 600},
			{Day: date(8, 0, 0, 0), Sandboxes: 6, ComputeSeconds: 18234, PeakActive: 3},
		},
		Providers: []store.StatsProvider{
			{Provider: "boat", Sandboxes7d: 30, Active: 2},
			{Provider: "e2b", Sandboxes7d: 10, Active: 1},
		},
	}
}

func totals(t *testing.T, file string) PaymentsPart {
	t.Helper()
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	p, err := ParseTotals(raw, t0)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

var routerLimits = map[string]int64{
	"sandbox.max_running_jobs": 16, "transfer.max_file_bytes": 1 << 30,
	"wallet.max_concurrent": 5,
}

func TestSummaryGolden(t *testing.T) {
	s := Merge(seededFigures(), totals(t, "testdata/totals.json"), t0.Add(-20*time.Second),
		testCatalog(t).All(), routerLimits)
	golden(t, s, "testdata/summary.json")
}

// sessionRows are one row of each kind the page must show
func sessionRows() []store.StatsSessionRow {
	payer, fee, other := key(32, 7), key(32, 9), key(32, 11)
	sig := key(64, 5)
	return []store.StatsSessionRow{
		{ID: uuid.MustParse("00000000-0000-4000-8000-000000000005"),
			CreatedAt: date(8, 11, 50, 2), ReadyAt: ptr(date(8, 11, 50, 4)), State: "ready",
			Provider: "boat", PackageID: "cpu-small-10m", Commands: 2, PriceMicros: 50000,
			Payer: payer, FeePayer: fee, Tx: sig},
		{ID: uuid.MustParse("00000000-0000-4000-8000-000000000004"),
			CreatedAt: date(8, 10, 0, 0), ReadyAt: ptr(date(8, 10, 0, 12)),
			EndedAt: ptr(date(8, 10, 40, 12)), State: "terminated", Provider: "e2b",
			PackageID: "gpu-l4-30m", TerminationReason: "deadline", Commands: 7,
			PriceMicros: 1545000, Payer: fee, FeePayer: fee, Tx: sig},
		{ID: uuid.MustParse("00000000-0000-4000-8000-000000000003"),
			CreatedAt: date(8, 9, 0, 0), EndedAt: ptr(date(8, 9, 0, 30)), State: "failed",
			PackageID: "retired-pkg", FailureReason: "provider_quota_exceeded",
			PriceMicros: 50000, Payer: other, FeePayer: fee},
		{ID: uuid.MustParse("00000000-0000-4000-8000-000000000002"),
			CreatedAt: date(7, 20, 0, 0), ReadyAt: ptr(date(7, 20, 0, 3)),
			EndedAt: ptr(date(7, 20, 5, 3)), State: "terminating", Provider: "boat",
			PackageID: "cpu-small-10m", TerminationReason: "stop", Commands: 1,
			PriceMicros: 50000, Payer: other, Tx: key(63, 5)},
		{ID: uuid.MustParse("00000000-0000-4000-8000-000000000001"),
			CreatedAt: date(7, 19, 0, 0), ReadyAt: ptr(date(7, 19, 0, 3)),
			EndedAt: ptr(date(7, 19, 1, 3)), State: "terminated", Provider: "koyeb",
			PackageID: "cpu-small-10m", TerminationReason: "abuse", PriceMicros: 50000,
			Payer: other, FeePayer: fee, PayerWasFeePayer: true},
	}
}

func TestSessionsGolden(t *testing.T) {
	s := Sessions{Catalog: testCatalog(t)}
	golden(t, s.Page(sessionRows(), t0), "testdata/sessions.json")
}

// ss7 are SS 7's shapes, gpu with its model, every array holding one element of its kind
const ss7 = `{
	"summary": {
		"version": 1, "generatedAt": "2026-11-07T14:02:31Z",
		"now": {"active": 3, "activeByProvider": [{"provider": "boat", "active": 2}]},
		"totals": {"sandboxes": 1, "computeSeconds": 1, "earnedMicros": 1, "refundedMicros": 1,
			"allowanceReturnedMicros": 1, "x402SpentMicros": 1, "spentMicros": 1},
		"daily": [{"day": "2026-11-07", "earnedMicros": 1, "refundedMicros": 0,
			"allowanceReturnedMicros": 0, "x402SpentMicros": 1, "spentMicros": 1,
			"sandboxes": 1, "computeSeconds": 1, "peakActive": 1, "medianSeconds": 1}],
		"providers": [{"provider": "boat", "sandboxes7d": 1, "active": 1, "startP50Ms": 1,
			"startP95Ms": 1, "failed7d": 1}],
		"payments": {"byKind": [{"kind": "sandbox", "count": 1, "micros": 1}],
			"byMethod": [{"method": "checkout", "count": 1, "micros": 1}],
			"refunds": {"count": 1, "micros": 1}},
		"packages": [{"id": "cpu-small-10m", "vcpu": 2, "memoryMiB": 2048, "diskGiB": 10,
			"gpu": {"count": 1, "memoryGiB": 24, "model": "NVIDIA L4"}, "minutes": 10,
			"priceMicros": 1}],
		"limits": {"wallet.max_concurrent": 5}
	},
	"sessions": {
		"items": [{"startedAt": "2026-11-07T13:50:02Z", "endedAt": "2026-11-07T14:00:02Z",
			"provider": "boat", "vcpu": 2, "memoryMiB": 2048,
			"gpu": {"count": 1, "memoryGiB": 24, "model": "NVIDIA L4"}, "seconds": 600,
			"commands": 7, "priceMicros": 50000, "status": "ended", "endReason": "deadline",
			"wallet": "w", "tx": "t"}],
		"next": "c"
	}
}`

// nullable are the keys SS 7 lets go out as null, and those this task's spec edit adds
var nullable = map[string]bool{
	"startP50Ms": true, "startP95Ms": true, "provider": true, "vcpu": true, "memoryMiB": true,
	"gpu": true, "wallet": true, "tx": true, "endedAt": true, "endReason": true, "next": true,
	// Spec edit while Tasks 35 and 33 are not merged
	"spentMicros": true, "failed7d": true,
}

// conforms checks got against SS 7's shape, the same keys and kinds at every level
func conforms(path string, want, got any) error {
	if got == nil {
		key := path[strings.LastIndex(path, ".")+1:]
		if !nullable[key] {
			return fmt.Errorf("%s is null", path)
		}
		return nil
	}
	switch w := want.(type) {
	case map[string]any:
		g, ok := got.(map[string]any)
		if !ok {
			return fmt.Errorf("%s is not an object", path)
		}
		if strings.HasSuffix(path, ".limits") {
			return nil
		}
		for k := range g {
			if _, ok := w[k]; !ok {
				return fmt.Errorf("%s.%s is not in SS 7", path, k)
			}
		}
		for k, v := range w {
			gv, ok := g[k]
			if !ok {
				return fmt.Errorf("%s.%s is missing", path, k)
			}
			if err := conforms(path+"."+k, v, gv); err != nil {
				return err
			}
		}
	case []any:
		g, ok := got.([]any)
		if !ok {
			return fmt.Errorf("%s is not a list", path)
		}
		for i, v := range g {
			if err := conforms(fmt.Sprintf("%s[%d]", path, i), w[0], v); err != nil {
				return err
			}
		}
	default:
		if fmt.Sprintf("%T", want) != fmt.Sprintf("%T", got) {
			return fmt.Errorf("%s is %T, SS 7 has %T", path, got, want)
		}
	}
	return nil
}

func TestGoldenFilesFollowSectionSeven(t *testing.T) {
	var shapes map[string]any
	if err := json.Unmarshal([]byte(ss7), &shapes); err != nil {
		t.Fatal(err)
	}
	for name, file := range map[string]string{
		"summary": "testdata/summary.json", "sessions": "testdata/sessions.json",
	} {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		var got any
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		if err := conforms(name, shapes[name], got); err != nil {
			t.Errorf("%s: %v", file, err)
		}
	}
}

func TestGeneratedAtIsTheOlderPart(t *testing.T) {
	f := store.StatsFigures{At: t0.Add(500 * time.Millisecond)}
	s := Merge(f, PaymentsPart{}, t0.Add(-5*time.Minute), nil, nil)
	eq(t, time.Time(s.GeneratedAt), t0.Add(-5*time.Minute))
	s = Merge(f, PaymentsPart{}, t0.Add(time.Minute), nil, nil)
	eq(t, time.Time(s.GeneratedAt), t0)
}

func TestDaysZeroFilledFromTheEarlierFirstDay(t *testing.T) {
	f := store.StatsFigures{At: t0, Days: []store.StatsDay{
		{Day: date(7, 0, 0, 0), Sandboxes: 2},
		{Day: date(8, 0, 0, 0), Sandboxes: 1},
	}}
	p := PaymentsPart{Daily: []PaymentsDay{
		{Day: "2026-11-04", Money: Money{EarnedMicros: 10}},
		{Day: "2026-11-07", Money: Money{EarnedMicros: 20}},
		// A day ahead of the router's clock leaves the daily rows
		{Day: "2026-11-09", Money: Money{EarnedMicros: 30}},
	}}
	s := Merge(f, p, t0, nil, nil)
	var days []string
	for _, d := range s.Daily {
		days = append(days, d.Day)
	}
	eq(t, days, []string{"2026-11-04", "2026-11-05", "2026-11-06", "2026-11-07", "2026-11-08"})
	eq(t, s.Daily[0].EarnedMicros, int64(10))
	eq(t, s.Daily[1], Day{Day: "2026-11-05"})
	eq(t, s.Daily[3].EarnedMicros, int64(20))
	eq(t, s.Daily[3].Sandboxes, int64(2))
	eq(t, s.Daily[4].Sandboxes, int64(1))

	// With the meter known, a day before the router's first one spent nothing
	spent := int64(70)
	f.SpentMicros = &spent
	f.Days[0].SpentMicros, f.Days[1].SpentMicros = ptr(int64(30)), ptr(int64(40))
	s = Merge(f, p, t0, nil, nil)
	eq(t, *s.Daily[0].SpentMicros, int64(0))
	eq(t, *s.Daily[3].SpentMicros, int64(30))
	eq(t, *s.Totals.SpentMicros, int64(70))

	s = Merge(store.StatsFigures{At: t0}, PaymentsPart{}, t0, nil, nil)
	eq(t, s.Daily, []Day{})
}

func TestUnknownSourcesGoOutNull(t *testing.T) {
	s := Merge(seededFigures(), PaymentsPart{}, t0, nil, nil)
	body, err := Encode(s)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"spentMicros":null`, `"failed7d":null`,
		`"startP50Ms":null`, `"startP95Ms":null`} {
		if !bytes.Contains(body, []byte(want)) {
			t.Errorf("summary lacks %s", want)
		}
	}
	for _, empty := range []string{`"byKind":[]`, `"byMethod":[]`, `"packages":[]`,
		`"limits":{}`} {
		if !bytes.Contains(body, []byte(empty)) {
			t.Errorf("summary lacks %s", empty)
		}
	}
}

func TestPackagesAreTheStartPackagesWithTheirGPU(t *testing.T) {
	got := Packages(testCatalog(t).All())
	eq(t, got, []Package{
		{ID: "cpu-small-10m", VCPU: 2, MemoryMiB: 2048, DiskGiB: 10, Minutes: 10,
			PriceMicros: 50000},
		{ID: "gpu-l4-30m", VCPU: 8, MemoryMiB: 32768, DiskGiB: 100,
			GPU: &GPU{Count: 1, MemoryGiB: 24, Model: "NVIDIA L4"}, Minutes: 30,
			PriceMicros: 1500000},
	})
}

func TestTotalsSendsTheBearer(t *testing.T) {
	raw, err := os.ReadFile("testdata/totals.json")
	if err != nil {
		t.Fatal(err)
	}
	var got *http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r
		_, _ = w.Write(raw)
	}))
	defer srv.Close()
	p := NewPayments(srv.URL+"/", "router-token-0123456789abcdef0123456789", time.Second)
	p.Now = func() time.Time { return t0 }
	part, err := p.Totals(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	eq(t, got.Method, http.MethodGet)
	eq(t, got.URL.Path, "/internal/v1/stats/totals")
	eq(t, got.Header.Get("Authorization"), "Bearer router-token-0123456789abcdef0123456789")
	eq(t, part.Totals.EarnedMicros, int64(1300000))
	eq(t, len(part.Daily), 3)
}

func TestTotalsTokenAndStatusAnswers(t *testing.T) {
	calls := 0
	status := http.StatusNotFound
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if status == http.StatusFound {
			http.Redirect(w, r, "http://203.0.113.9/steal", status)
			return
		}
		w.WriteHeader(status)
	}))
	defer srv.Close()
	p := NewPayments(srv.URL, "", time.Second)
	_, err := p.Totals(t.Context())
	eq(t, errors.Is(err, ErrNoToken), true)
	eq(t, calls, 0)
	p.Token = "router-token-0123456789abcdef0123456789"
	for _, c := range []struct {
		status int
		want   error
	}{
		{http.StatusNotFound, ErrToken}, {http.StatusUnauthorized, ErrToken},
		{http.StatusMethodNotAllowed, errStatus}, {http.StatusInternalServerError, errStatus},
		{http.StatusFound, errStatus},
	} {
		status = c.status
		_, err := p.Totals(t.Context())
		if !errors.Is(err, c.want) {
			t.Errorf("status %d: %v, want %v", c.status, err, c.want)
		}
	}
}

func TestTotalsTimesOut(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		<-release
	}))
	defer srv.Close()
	defer close(release)
	p := NewPayments(srv.URL, "router-token-0123456789abcdef0123456789", 50*time.Millisecond)
	start := time.Now()
	if _, err := p.Totals(t.Context()); err == nil {
		t.Fatal("a hung Payments answered")
	}
	if time.Since(start) > 5*time.Second {
		t.Fatalf("the call took %v", time.Since(start))
	}
}

// part is a good totals answer with one field replaced by a raw JSON value, or removed
func part(field, raw string) string {
	var m map[string]any
	body, _ := os.ReadFile("testdata/totals.json")
	_ = json.Unmarshal(body, &m)
	out, _ := json.Marshal(m)
	s := string(out)
	if field == "" {
		return s
	}
	var parsed map[string]json.RawMessage
	_ = json.Unmarshal(out, &parsed)
	if raw == "" {
		delete(parsed, field)
	} else {
		parsed[field] = json.RawMessage(raw)
	}
	out, _ = json.Marshal(parsed)
	return string(out)
}

func TestTotalsChecks(t *testing.T) {
	day := func(d string) string {
		return `{"day": "` + d + `", "earnedMicros": 1, "refundedMicros": 0,
			"allowanceReturnedMicros": 0, "x402SpentMicros": 0}`
	}
	money := `{"earnedMicros": 1, "refundedMicros": 0, "allowanceReturnedMicros": 0,
		"x402SpentMicros": 0}`
	cases := []struct {
		name, body string
		want       error
	}{
		{"not json", "{", errShape},
		{"a list", "[]", errShape},
		{"trailing data", part("", "") + "{}", errShape},
		{"version 2", part("version", "2"), errVersion},
		{"no version", part("version", ""), errMissing},
		{"no totals", part("totals", ""), errMissing},
		{"no daily", part("daily", ""), errMissing},
		{"no byKind", part("byKind", ""), errMissing},
		{"no refunds", part("refunds", ""), errMissing},
		{"no byMethod", part("byMethod", ""), errMissing},
		{"a missing total", part("totals", `{"earnedMicros": 1}`), errMissing},
		{"negative", part("totals", strings.Replace(money, `"earnedMicros": 1`,
			`"earnedMicros": -1`, 1)), errNumber},
		{"a fraction", part("totals", strings.Replace(money, `"earnedMicros": 1`,
			`"earnedMicros": 1.5`, 1)), errNumber},
		{"an exponent", part("totals", strings.Replace(money, `"earnedMicros": 1`,
			`"earnedMicros": 1e3`, 1)), errNumber},
		{"over 2^53 - 1", part("totals", strings.Replace(money, `"earnedMicros": 1`,
			`"earnedMicros": 9007199254740992`, 1)), errNumber},
		{"a string", part("totals", strings.Replace(money, `"earnedMicros": 1`,
			`"earnedMicros": "1"`, 1)), errNumber},
		{"null", part("totals", strings.Replace(money, `"earnedMicros": 1`,
			`"earnedMicros": null`, 1)), errNumber},
		{"bad day", part("daily", "["+day("2026-11-7")+"]"), errDay},
		{"impossible day", part("daily", "["+day("2026-02-30")+"]"), errDay},
		{"days fall", part("daily", "["+day("2026-11-07")+","+day("2026-11-06")+"]"), errDay},
		{"days repeat", part("daily", "["+day("2026-11-07")+","+day("2026-11-07")+"]"),
			errDay},
		{"two days ahead", part("daily", "["+day("2026-11-10")+"]"), errDay},
		{"a day without money", part("daily", `[{"day": "2026-11-07"}]`), errMissing},
		{"unknown kind", part("byKind", `[{"kind": "tip", "count": 1, "micros": 1}]`),
			errKind},
		{"upper case method", part("byMethod", `[{"method": "Pay", "count": 1, "micros": 1}]`),
			errMethod},
		{"long method", part("byMethod", `[{"method": "`+strings.Repeat("a", 33)+
			`", "count": 1, "micros": 1}]`), errMethod},
		{"bad limit", part("limits", `{"order.max_micros": -5}`), errNumber},
	}
	for _, c := range cases {
		_, err := ParseTotals([]byte(c.body), t0)
		if !errors.Is(err, c.want) {
			t.Errorf("%s: %v, want %v", c.name, err, c.want)
		}
	}
	// A day after today absorbs skew at midnight UTC, and an unknown limit key goes unread
	for _, ok := range []string{
		part("daily", "["+day("2026-11-09")+"]"),
		part("limits", `{"order.max_micros": 1, "future.key": -1.5}`),
		part("limits", ""),
	} {
		if _, err := ParseTotals([]byte(ok), t0); err != nil {
			t.Errorf("%s refused: %v", ok, err)
		}
	}
	big := `{"pad": "` + strings.Repeat("x", maxTotalsBytes) + `"}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(big))
	}))
	defer srv.Close()
	_, err := NewPayments(srv.URL, "router-token-0123456789abcdef0123456789", time.Second).
		Totals(t.Context())
	eq(t, errors.Is(err, errSize), true)
}

func TestUnknownPaymentsLimitKeyDropped(t *testing.T) {
	p := totals(t, "testdata/totals.json")
	eq(t, p.Limits, map[string]int64{
		"order.max_micros": 100000000, "x402.max_allowance_micros": 5000000,
		"x402.max_price_per_call_micros": 500000,
	})
	s := Merge(store.StatsFigures{At: t0}, p, t0, nil, map[string]int64{"wallet.x": 1})
	_, ok := s.Limits["gateway.quotes_per_minute"]
	eq(t, ok, false)
	eq(t, s.Limits["wallet.x"], int64(1))
	eq(t, s.Limits["order.max_micros"], int64(100000000))
}

func TestRouterLimits(t *testing.T) {
	lim := RouterLimits(cfg(t, "wallets.use_payer = true", `ssh.providers = ["e2b"]`))
	eq(t, lim, map[string]int64{
		"wallet.max_concurrent": 5, "wallet.starts_per_hour": 30, "wallet.starts_per_day": 200,
		"wallet.failed_refunds_per_day": 2, "sandbox.max_running_jobs": 16,
		"transfer.max_file_bytes": 1 << 30, "sandbox.max_ssh_connections": 8,
	})
	// Task 29 adds its key at its switch point
	if _, ok := lim["wallet.bytes_per_day"]; ok {
		t.Error("wallet.bytes_per_day sent before its source exists")
	}
	lim = RouterLimits(cfg(t, "wallets.use_payer = true", "wallets.failed_refunds_per_day = 7"))
	eq(t, lim["wallet.failed_refunds_per_day"], int64(7))
	for k := range lim {
		if slices.Contains(paymentsLimits, k) {
			t.Errorf("the router sends Payments' key %s", k)
		}
	}
	lim = RouterLimits(cfg(t, `previews.providers = ["boat"]`, `secrets.providers = ["e2b"]`,
		`transfers.max_bytes = "2GiB"`, "limits.max_running_jobs = 3",
		"previews.max_per_placement = 4", "secrets.max_per_placement = 9"))
	eq(t, lim, map[string]int64{
		"sandbox.max_running_jobs": 3, "transfer.max_file_bytes": 2 << 30,
		"sandbox.max_previews": 4, "sandbox.max_secrets": 9,
	})
}

func TestRouterLimitsLeaveOutWhatNoOneMeets(t *testing.T) {
	eq(t, RouterLimits(cfg(t)), map[string]int64{
		"sandbox.max_running_jobs": 16, "transfer.max_file_bytes": 1 << 30,
	})
}

func TestEndReasonWords(t *testing.T) {
	e := NewEnds(cfg(t))
	for _, c := range []struct{ state, term, fail, want string }{
		{"terminated", "stop", "", "stop"},
		{"terminating", "deadline", "", "deadline"},
		{"terminated", "abuse", "", "stopped_by_bugnet"},
		{"terminated", "provider_failed", "", "provider_failed"},
		{"failed", "", "provider_quota_exceeded", "start_failed"},
		{"failed", "", "no_candidate", "no_capacity"},
		{"failed", "", "capacity_full", "no_capacity"},
		{"failed", "", "candidates_exhausted", "no_capacity"},
		{"failed", "", "wallet_busy", "wallet_limit"},
		{"failed", "", "wallet_rate_limited", "wallet_limit"},
		{"failed", "", "boot_failed", "start_failed"},
		{"failed", "", "create_failed", "start_failed"},
		{"failed", "", "runner_unavailable", "start_failed"},
		{"failed", "", "internal_error", "start_failed"},
		{"failed", "", "placing_stalled", "start_failed"},
		{"terminated", "a_new_reason", "", "other"},
		{"terminated", "", "", "other"},
		{"failed", "stop", "", "stop"},
		// Task 28B's reasons, abuse and blocks unnamed until the founder answers CP 20.12
		{"terminated", "wallet_blocked", "", "stopped_by_bugnet"},
		{"terminating", "control_lost", "", "stopped_by_bugnet"},
		{"terminated", "sandbox_exited", "", "sandbox_exited"},
		{"terminated", "operator", "", "stopped_by_bugnet"},
	} {
		got := e.EndReason(c.state, c.term, c.fail)
		if got == nil || *got != c.want {
			t.Errorf("%s %q %q gave %v, want %s", c.state, c.term, c.fail, got, c.want)
		}
	}
	eq(t, e.EndReason("ready", "", ""), (*string)(nil))
	// The reasons as the saga writes them into termination_reason
	for reason, want := range map[string]string{
		saga.ReasonAbuse: "stopped_by_bugnet", saga.ReasonControlLost: "stopped_by_bugnet",
		saga.ReasonProviderFailed: "provider_failed", saga.ReasonSandboxExited: "sandbox_exited",
		saga.ReasonWalletBlocked: "stopped_by_bugnet", saga.ReasonOperator: "stopped_by_bugnet",
	} {
		eq(t, *e.EndReason("terminated", reason, ""), want)
	}
}

func TestCursorRoundTripAndRefusals(t *testing.T) {
	at := time.Date(2026, 11, 7, 13, 50, 2, 123456000, time.UTC)
	id := uuid.MustParse("8d3f0c6e-1b2a-4c5d-9e8f-0a1b2c3d4e5f")
	c := EncodeCursor(at, id)
	if !strings.HasPrefix(c, "s1.") || len(c) != 3+32 {
		t.Fatalf("cursor %q", c)
	}
	gotAt, gotID, err := DecodeCursor(c)
	eq(t, err, nil)
	eq(t, gotAt, at)
	eq(t, gotID, id)
	raw := func(n int) string { return "s1." + cursorEncoding.EncodeToString(make([]byte, n)) }
	for _, bad := range []string{
		"", "s2." + c[3:], c[3:], c + "=", "s1.!!!!", raw(23), raw(25), c[:len(c)-1],
		"s1." + strings.Repeat("+", 32),
	} {
		if _, _, err := DecodeCursor(bad); !errors.Is(err, ErrBadCursor) {
			t.Errorf("%q decoded", bad)
		}
	}
}

// cursorOf is a cursor of these 8 time bytes and a fixed id
func cursorOf(t8 []byte) string {
	b := append(slices.Clone(t8), bytes.Repeat([]byte{7}, 16)...)
	return "s1." + cursorEncoding.EncodeToString(b)
}

// allOnes is 24 bytes of 0xff, whose time is a microsecond before 1970
var allOnes = "s1." + cursorEncoding.EncodeToString(bytes.Repeat([]byte{0xff}, 24))

func TestCursorTimeMustBeOneTheRouterWrote(t *testing.T) {
	micros := func(at time.Time) []byte {
		return binary.BigEndian.AppendUint64(nil, uint64(at.UnixMicro()))
	}
	for name, c := range map[string]string{
		"all 0xff":         allOnes,
		"just before 1970": cursorOf(micros(time.Unix(0, 0).Add(-time.Microsecond))),
		"far past":         cursorOf([]byte{0x80, 0, 0, 0, 0, 0, 0, 0}),
		"far future":       cursorOf([]byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}),
		"the year 10000":   cursorOf(micros(time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC))),
	} {
		if _, _, err := DecodeCursor(c); !errors.Is(err, ErrBadCursor) {
			t.Errorf("%s: %q decoded", name, c)
		}
	}
	for _, at := range []time.Time{
		time.Unix(0, 0).UTC(), time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC),
	} {
		got, _, err := DecodeCursor(cursorOf(micros(at)))
		eq(t, err, nil)
		eq(t, got, at)
	}
}

func TestWalletAndTxNeedTheirDecodedLength(t *testing.T) {
	// One zero byte and a small next byte give a 64-byte signature in 86 characters
	short := base58(append([]byte{0, 1}, bytes.Repeat([]byte{0xff}, 62)...))
	eq(t, len(short), 86)
	eq(t, *Tx(short), short)
	eq(t, Tx(key(63, 5)), (*string)(nil))
	eq(t, Tx(key(65, 5)), (*string)(nil))
	eq(t, Tx(""), (*string)(nil))
	fee := key(32, 9)
	row := func(payer string) store.StatsSessionRow {
		return store.StatsSessionRow{Payer: payer, FeePayer: fee}
	}
	eq(t, Wallet(row(key(33, 7)), nil), (*string)(nil))
	eq(t, Wallet(row(key(31, 7)), nil), (*string)(nil))
	eq(t, *Wallet(row(key(32, 7)), nil), key(32, 7))
}

func TestFacilitatorNeverShownAsAWallet(t *testing.T) {
	payer, fee, listed := key(32, 7), key(32, 9), key(32, 11)
	cases := []struct {
		name string
		row  store.StatsSessionRow
		list []string
	}{
		{"its own fee payer", store.StatsSessionRow{Payer: fee, FeePayer: fee}, nil},
		{"any order's fee payer", store.StatsSessionRow{Payer: payer, FeePayer: fee,
			PayerWasFeePayer: true}, nil},
		{"the list", store.StatsSessionRow{Payer: listed, FeePayer: fee}, []string{listed}},
		{"no fee payer and no list", store.StatsSessionRow{Payer: payer}, nil},
		{"no payer", store.StatsSessionRow{FeePayer: fee}, nil},
	}
	for _, c := range cases {
		if got := Wallet(c.row, c.list); got != nil {
			t.Errorf("%s: shown %s", c.name, *got)
		}
	}
	eq(t, *Wallet(store.StatsSessionRow{Payer: payer}, []string{listed}), payer)
}

func TestSessionsPageNext(t *testing.T) {
	var rows []store.StatsSessionRow
	for i := range PageSize + 1 {
		rows = append(rows, store.StatsSessionRow{ID: uuid.New(),
			CreatedAt: t0.Add(-time.Duration(i) * time.Minute), State: "terminated"})
	}
	s := Sessions{Catalog: testCatalog(t)}
	page := s.Page(rows, t0)
	eq(t, len(page.Items), PageSize)
	at, id, err := DecodeCursor(*page.Next)
	eq(t, err, nil)
	eq(t, at, rows[PageSize-1].CreatedAt)
	eq(t, id, rows[PageSize-1].ID)
	page = s.Page(rows[:PageSize], t0)
	eq(t, page.Next, (*string)(nil))
	page = s.Page(nil, t0)
	eq(t, page.Items, []Session{})
}
