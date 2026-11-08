package sessions

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

var t0 = time.Date(2026, 11, 6, 10, 0, 0, 0, time.UTC)

const allowanceTx = "5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8u" +
	"irBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW"

var smallPkg = catalog.Package{
	ID: "dev-small-10m", Minutes: 10,
	Resources: provider.Resources{VCPU: 2, MemoryMiB: 2048, DiskGiB: 10},
}

type fixture struct {
	st   *store.Store
	pool *pgxpool.Pool
	w    *Writer
	p    store.Placement
	rec  store.SessionRecord
	now  time.Time
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	url := testenv.DatabaseURL(t)
	if err := store.Migrate(t.Context(), url); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(t.Context(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	_, keys := testenv.DataPlaneKeys(t)
	st := store.New(pool)
	p, _, err := st.CreatePlacement(t.Context(), store.NewPlacement{
		OrderID: "order-1", HandleSHA256: []byte("h1"), PackageID: smallPkg.ID,
		KeyID: keys.Current().ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := st.CreateSessionRecord(t.Context(), p.ID)
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{st: st, pool: pool, p: p, rec: rec, now: t0}
	f.w = NewWriter(st, keys, nil)
	f.w.SetClock(func() time.Time { return f.now })
	f.exec(t, `update session_records set created_at = $2 where id = $1`, rec.ID, t0)
	return f
}

func (f *fixture) exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	if _, err := f.pool.Exec(t.Context(), sql, args...); err != nil {
		t.Fatal(err)
	}
}

// at writes the next event at the clock's time
func (f *fixture) at(t *testing.T, d time.Duration, write func(ctx context.Context)) {
	t.Helper()
	f.now = t0.Add(d)
	write(t.Context())
	f.exec(t, `update session_events set at = $1 where id = (select max(id) from session_events)`,
		f.now)
}

func (f *fixture) build(t *testing.T) []byte {
	t.Helper()
	raw, err := BuildRecord(t.Context(), f.st, f.p.ID)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// filled writes SP 7.3.1's example run, filled in with one event of every served kind
func filled(t *testing.T) *fixture {
	t.Helper()
	f := newFixture(t)
	id := f.p.ID
	deadline := t0.Add(10 * time.Minute)
	f.at(t, time.Second, func(ctx context.Context) {
		f.w.Placement(ctx, id, PhasePlaced, smallPkg, nil, "")
	})
	f.at(t, 2*time.Second, func(ctx context.Context) {
		f.w.Payment(ctx, id, "start", 50000, "")
	})
	f.at(t, 30*time.Second, func(ctx context.Context) {
		f.w.Placement(ctx, id, PhaseReady, smallPkg, &deadline, "")
	})
	f.now = t0.Add(time.Minute)
	job := store.NewJob{ID: "j_1", PlacementID: id}
	if _, _, err := f.st.CreateJob(t.Context(), job); err != nil {
		t.Fatal(err)
	}
	if err := f.st.FinishJob(t.Context(), "j_1", store.JobOutcome{State: store.JobExited,
		StdoutBytes: 13}); err != nil {
		t.Fatal(err)
	}
	f.w.StartJob(t.Context(), f.p, "j_1", []string{"npm", "test"})
	f.exec(t, `update session_events set at = $1 where kind = 'job'`, f.now)
	f.now = t0.Add(2 * time.Minute)
	code := 0
	f.w.EndJob(t.Context(), id, "j_1", JobEnd{ExitCode: &code, PeakMiB: 512, OutBytes: 13})
	f.exec(t, `update session_events set at = $1 where kind = 'job' and data->>'phase' = 'end'`,
		f.now)
	f.at(t, 3*time.Minute, func(ctx context.Context) {
		f.w.File(ctx, f.p, "write", "/root/app/main.py", 120)
	})
	f.at(t, 4*time.Minute, func(ctx context.Context) {
		if _, err := f.w.Store(ctx, id, KindMCP, Fields{
			"at": t0.Add(4 * time.Minute).Format(time.RFC3339), "tool": "bugnet_exec",
			"outcome": "ok", "duration_ms": 900,
		}); err != nil {
			t.Fatal(err)
		}
	})
	f.at(t, 5*time.Minute, func(ctx context.Context) {
		if err := f.w.Allowance(ctx, id, Fields{
			"phase": "paid", "amount_micros": 1000000, "asset": usdcMint,
			"network": "solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp", "tx": allowanceTx,
		}); err != nil {
			t.Fatal(err)
		}
	})
	// A kind with no serve function, such as egress's x402 rows, is stored and never served
	f.at(t, 6*time.Minute, func(ctx context.Context) {
		if _, _, err := f.st.InsertSessionEvent(ctx, id, "x402",
			[]byte(`{"ref":"aaaaaaaaaaaaaaaa","pay_to":"11111111111111111111111111111111"}`),
			true); err != nil {
			t.Fatal(err)
		}
	})
	f.at(t, 10*time.Minute, func(ctx context.Context) {
		f.w.Placement(ctx, id, PhaseEnded, smallPkg, &deadline, "deadline")
	})
	f.exec(t, `update placements set state = 'terminated', termination_reason = 'deadline',
		terminated_at = $2 where id = $1`, id, deadline)
	if err := f.st.EndSessionRecord(t.Context(), id, store.SessionEnd{EndedAt: deadline,
		Keep: 48 * time.Hour}); err != nil {
		t.Fatal(err)
	}
	frames, _ := json.Marshal([]Frame{{S: Stdout, T: "ok 12 tests\n"}})
	if err := f.st.SetSessionJobFrames(t.Context(), f.rec.ID, 1, frames, 0); err != nil {
		t.Fatal(err)
	}
	if err := f.st.SetSessionCopyState(t.Context(), f.rec.ID, store.CopyComplete); err != nil {
		t.Fatal(err)
	}
	return f
}

func TestRecordGolden(t *testing.T) {
	got := filled(t).build(t)
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, got, "", "  "); err != nil {
		t.Fatal(err)
	}
	pretty.WriteByte('\n')
	if os.Getenv("BUGNET_UPDATE_GOLDEN") == "1" {
		if err := os.WriteFile("testdata/record.json", pretty.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile("testdata/record.json")
	if err != nil {
		t.Fatal(err)
	}
	eq(t, pretty.String(), string(want))
}

func TestUnservedKindStaysHidden(t *testing.T) {
	raw := string(filled(t).build(t))
	for _, s := range []string{"x402", "aaaaaaaaaaaaaaaa", "1111111111111111", "pay_to"} {
		if strings.Contains(raw, s) {
			t.Errorf("record_json holds %q: %s", s, raw)
		}
	}
}

func TestRecordWhilePending(t *testing.T) {
	f := newFixture(t)
	f.w.StartJob(t.Context(), f.p, "j_1", []string{"ls"})
	f.exec(t, `update placements set state = 'failed', terminated_at = now() where id = $1`,
		f.p.ID)
	var rec struct {
		EndedBecause string           `json:"endedBecause"`
		Output       string           `json:"output"`
		Jobs         []map[string]any `json:"jobs"`
	}
	if err := json.Unmarshal(f.build(t), &rec); err != nil {
		t.Fatal(err)
	}
	eq(t, rec.EndedBecause, "failed")
	eq(t, rec.Output, "pending")
	_, hasFrames := rec.Jobs[0]["frames"]
	eq(t, hasFrames, false)
	_, hasCode := rec.Jobs[0]["exitCode"]
	eq(t, hasCode, false)
	eq(t, rec.Jobs[0]["state"], any("lost"))
}

func TestEndedBecause(t *testing.T) {
	for reason, want := range map[string]string{
		"stop": "stopped", "deadline": "deadline", "": "deadline", "sandbox_exited": "exited",
		"provider_failed": "fault", "control_lost": "fault", "abuse": "ended",
		"something_new": "ended",
	} {
		eq(t, EndedBecause(store.PlacementTerminated, reason), want)
	}
	eq(t, EndedBecause(store.PlacementFailed, ""), "failed")
	for reason, by := range map[string]string{
		"abuse": "abuse", "wallet_blocked": "abuse", "operator": "operator",
	} {
		got, ok := ClosingEnd(reason)
		eq(t, ok, true)
		eq(t, got, by)
	}
	_, ok := ClosingEnd("stop")
	eq(t, ok, false)
}

func TestTrafficFoldsPerMinute(t *testing.T) {
	f := newFixture(t)
	minute := t0.Add(5 * time.Minute)
	f.w.PreviewTraffic(t.Context(), f.p.ID, 3000, minute.Add(5*time.Second), 3, 300)
	f.w.PreviewTraffic(t.Context(), f.p.ID, 3000, minute.Add(50*time.Second), 2, 200)
	f.w.PreviewTraffic(t.Context(), f.p.ID, 3000, minute.Add(65*time.Second), 1, 1)
	f.w.SecretHost(t.Context(), f.p, "api.openai.com", minute, 4, 40)
	f.w.SecretHost(t.Context(), f.p, "api.openai.com", minute.Add(time.Second), 1, 1)
	f.w.SSH(t.Context(), f.p.ID, "end", 10, 20)
	events, err := f.st.SessionEvents(t.Context(), f.rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	served := served2(events)
	eq(t, len(served), 4)
	eq(t, served[0], map[string]any{"at": "2026-11-06T10:05:00Z", "kind": "preview",
		"port": 3000.0, "phase": "traffic", "requests": 5.0, "bytes": 500.0})
	eq(t, served[1], map[string]any{"at": "2026-11-06T10:05:00Z", "kind": "secret_host",
		"host": "api.openai.com", "requests": 5.0, "bytes": 41.0})
	eq(t, served[2]["requests"], any(1.0))
	eq(t, served[3]["bytesOut"], any(20.0))
}

// served2 serves the events and decodes each as a map
func served2(events []store.SessionEvent) []map[string]any {
	var out []map[string]any
	for _, o := range served(events) {
		raw, _ := json.Marshal(o)
		var m map[string]any
		_ = json.Unmarshal(raw, &m)
		out = append(out, m)
	}
	return out
}

func TestWriterCleansAndCutsStrings(t *testing.T) {
	f := newFixture(t)
	long := strings.Repeat("d/", 150)
	f.w.File(t.Context(), f.p, "read", "/root/\x1b[31m"+long, 1)
	f.w.Transfer(t.Context(), f.p, "in", "/root/"+strings.Repeat("n", 150), 1)
	events, _ := f.st.SessionEvents(t.Context(), f.rec.ID)
	got := served2(events)
	eq(t, len([]rune(got[0]["path"].(string))), 200)
	eq(t, strings.Contains(got[0]["path"].(string), "\x1b"), false)
	eq(t, len(got[1]["name"].(string)), 100)
	// An unknown placement's event is dropped quietly
	f.w.File(t.Context(), store.Placement{ID: uuid.New()}, "read", "/x", 1)
}

func TestWriterRedactsThePlacementsSecretsInEvents(t *testing.T) {
	f := newFixture(t)
	gate, err := f.w.keys.GateKey(f.p.KeyID, f.p.ID)
	if err != nil {
		t.Fatal(err)
	}
	const envd = "plain-envd-token-value"
	p := f.p
	p.ProviderRef = &provider.Ref{Meta: map[string]string{"envd_token": envd}}
	f.w.File(t.Context(), p, "read", "/root/"+gate+"/a.txt", 1)
	f.w.Transfer(t.Context(), p, "in", "/root/"+envd+".tar", 1)
	f.w.SecretHost(t.Context(), p, envd+".example.com", t0, 1, 1)
	events, err := f.st.SessionEvents(t.Context(), f.rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	got := map[any]map[string]any{}
	for _, ev := range served2(events) {
		got[ev["kind"]] = ev
	}
	eq(t, len(got), 3)
	eq(t, got[KindFile]["path"], any("/root/[redacted secret]/a.txt"))
	eq(t, got[KindTransfer]["name"], any("[redacted secret].tar"))
	eq(t, got[KindSecretHost]["host"], any("[redacted secret].example.com"))
}

func TestKeeper(t *testing.T) {
	k := NewKeeper(10, 0)
	k.Add(Frame{S: Stdout, T: "0123456789"}, Frame{S: Stderr, T: "err\n"},
		Frame{S: Stdout, T: "abcde"})
	eq(t, k.Frames(), []Frame{{S: Stdout, T: "56789"}, {S: Stderr, T: "err\n"},
		{S: Stdout, T: "abcde"}})
	eq(t, k.Cut(), int64(5))
	eq(t, k.Kept(), int64(14))
	// A replacing frame takes the place of the line start it replaces
	p := NewKeeper(100, 0)
	p.Add(Frame{S: Stdout, T: "start\n"}, Frame{S: Stdout, T: "10%"},
		Frame{S: Stdout, T: "50%", CR: true}, Frame{S: Stdout, T: "100%\n", CR: true})
	eq(t, p.Frames(), []Frame{{S: Stdout, T: "start\n10%"}, {S: Stdout, T: "100%\n", CR: true}})
	both := NewKeeper(10, 12)
	both.Add(Frame{S: Stdout, T: "0123456789"}, Frame{S: Stderr, T: "abcdef"})
	eq(t, both.Frames(), []Frame{{S: Stdout, T: "456789"}, {S: Stderr, T: "abcdef"}})
	eq(t, both.Kept(), int64(12))
	eq(t, both.Cut(), int64(4))
}

func TestServedPaymentAndAllowanceKeys(t *testing.T) {
	served := func(fn serveFunc, data string) string {
		t.Helper()
		var d map[string]json.RawMessage
		if err := json.Unmarshal([]byte(data), &d); err != nil {
			t.Fatal(err)
		}
		o, ok := fn(d)
		eq(t, ok, true)
		raw, err := json.Marshal(o)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	// The page reads tx on every payment, so it is served empty until the order token has one
	eq(t, served(servePayment, `{"pay_kind":"start","amount_micros":50000}`),
		`{"payKind":"start","amountMicros":50000,"tx":""}`)
	eq(t, served(serveAllowance, `{"phase":"refunded","amount_micros":5,"asset":"`+usdcMint+
		`","network":"solana:x","tx":"sig"}`),
		`{"phase":"refunded","amountMicros":5,"asset":"USDC","network":"solana:x","tx":"sig"}`)
	// Any other mint is left out, since a base58 key could be a wallet's address (D-c)
	const mint = "So11111111111111111111111111111111111111112"
	eq(t, served(serveAllowance, `{"phase":"refunded","amount_micros":5,"asset":"`+mint+
		`","network":"solana:x","tx":"sig"}`),
		`{"phase":"refunded","amountMicros":5,"network":"solana:x","tx":"sig"}`)
	eq(t, served(serveAllowance, `{"phase":"kept","amount_micros":0,"kept_micros":4000,`+
		`"asset":"`+usdcMint+`","network":"solana:x"}`),
		`{"phase":"kept","amountMicros":0,"keptMicros":4000,"asset":"USDC","network":"solana:x"}`)
}
