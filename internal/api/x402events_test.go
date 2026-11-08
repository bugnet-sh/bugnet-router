package api_test

import (
	"log/slog"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/x402"
)

// xev is a valid paid event with ref, which a test changes into the case it needs
func xev(ref string) *routerv1.X402Event {
	return &routerv1.X402Event{
		At: timestamppb.Now(), Ref: ref, Host: "agent402.tools", Method: "POST",
		Path: "/v1/chat", Status: "paid", AskedMicros: 10000, AmountMicros: 10000,
		Asset: usdc, Network: solana, Scheme: "exact", X402Version: 2, PayTo: usdc,
		HttpStatus: 200, Count: 1,
	}
}

func refused(ref, reason string, count uint32) *routerv1.X402Event {
	ev := xev(ref)
	ev.Status, ev.Reason, ev.AmountMicros, ev.Count = "refused_cap", reason, 0, count
	return ev
}

func (e *env) appendX402(
	t *testing.T, id string, events ...*routerv1.X402Event,
) (*routerv1.AppendX402EventsResponse, error) {
	t.Helper()
	res, err := e.egress(t).AppendX402Events(t.Context(), connect.NewRequest(
		&routerv1.AppendX402EventsRequest{PlacementId: id, Events: events}))
	if err != nil {
		return nil, err
	}
	return res.Msg, nil
}

// x402Placement is a ready placement with one x402 service row
func (e *env) x402Placement(t *testing.T) (*routerv1.Placement, string) {
	t.Helper()
	_, p := e.readyPlacement(t)
	spec := x402Spec("agent402.tools", true)
	err := e.store.PutX402Service(t.Context(), store.X402Service{
		PlacementID: uuid.MustParse(p.GetPlacementId()), ServiceID: idOf(spec),
		Spec: x402.Spec{Host: spec.GetHost(), Prefix: spec.GetPrefix(), Method: "GET",
			MaxPriceMicros: 10000, PayTo: usdc, Indexed: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	return p, idOf(spec)
}

func (e *env) service(t *testing.T, p *routerv1.Placement) store.X402Service {
	t.Helper()
	rows, err := e.store.X402Services(t.Context(), uuid.MustParse(p.GetPlacementId()))
	if err != nil || len(rows) != 1 {
		t.Fatalf("services %v: %v", rows, err)
	}
	return rows[0]
}

func TestAppendX402EventsStoresOnceWithCounters(t *testing.T) {
	var logs syncBuffer
	e := newEnv(t, options{log: slog.New(slog.NewTextHandler(&logs, nil))})
	p, sid := e.x402Placement(t)
	bad := xev("bbbbbbbbbbbbbbbb")
	bad.Method = "post-secret-value"
	good, other := xev("aaaaaaaaaaaaaaaa"), xev("cccccccccccccccc")
	good.ServiceId, other.ServiceId = sid, sid
	other.Status, other.Reason, other.AmountMicros = "uncertain", "paid_no_answer", 10000
	for range 2 {
		res, err := e.appendX402(t, p.GetPlacementId(), good, bad, other)
		if err != nil {
			t.Fatal(err)
		}
		eq(t, res.GetRejected(), []uint32{1})
	}
	rows := e.sessionEvents(t, p)["x402"]
	eq(t, len(rows), 2)
	eq(t, rows[0]["service_id"], any(sid))
	eq(t, rows[0]["ref"], any("aaaaaaaaaaaaaaaa"))
	svc := e.service(t, p)
	eq(t, [3]int{svc.Requests, svc.Paid, svc.Uncertain}, [3]int{2, 1, 1})
	log := logs.String()
	if !strings.Contains(log, "index=1") || !strings.Contains(log, "rule=method") ||
		strings.Contains(log, "secret-value") {
		t.Fatalf("log does not name index 1 and the method rule only:\n%s", log)
	}
}

func TestAppendX402EventsFoldedRefusalsAndPathCut(t *testing.T) {
	e := newEnv(t, options{})
	p, sid := e.x402Placement(t)
	later := time.Now().Add(-time.Second)
	fold := refused("dddddddddddddddd", "allowance_spent", 7)
	fold.ServiceId, fold.At = sid, timestamppb.New(later)
	fold.Path = "/" + strings.Repeat("a", 200)
	host := &routerv1.X402Event{
		At: timestamppb.Now(), Ref: "eeeeeeeeeeeeeeee", Host: "evil.example",
		Status: "refused_host", Count: 3,
	}
	failed := xev("ffffffffffffffff")
	failed.Status, failed.Reason = "payment_failed", "payment_rejected"
	res, err := e.appendX402(t, p.GetPlacementId(), fold, host, failed)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(res.GetRejected()), 0)
	svc := e.service(t, p)
	eq(t, svc.Requests, 7)
	got, err := e.store.X402Refusals(t.Context(), uuid.MustParse(p.GetPlacementId()))
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, r := range got {
		counts[r.Reason] = r.Count
	}
	eq(t, counts, map[string]int{"allowance_spent": 7, "refused_host": 3})
	for _, row := range e.sessionEvents(t, p)["x402"] {
		if row["ref"] == "dddddddddddddddd" {
			eq(t, len(row["path"].(string)), 80)
		}
	}
}

func TestAppendX402EventsShapesAndFaults(t *testing.T) {
	e := newEnv(t, options{})
	p, _ := e.x402Placement(t)
	cases := map[string]func(ev *routerv1.X402Event){
		"count zero":        func(ev *routerv1.X402Event) { ev.Count = 0 },
		"count two on paid": func(ev *routerv1.X402Event) { ev.Count = 2 },
		"unknown reason":    func(ev *routerv1.X402Event) { ev.Status, ev.Reason = "free", "nope" },
		"payto not a key":   func(ev *routerv1.X402Event) { ev.PayTo = "abc" },
		"other network":     func(ev *routerv1.X402Event) { ev.Network = "eip155:1" },
		"empty method":      func(ev *routerv1.X402Event) { ev.Method = "" },
		"bad service":       func(ev *routerv1.X402Event) { ev.ServiceId = "xs_nope" },
	}
	for name, mod := range cases {
		ev := xev("gggggggggggggggg")
		mod(ev)
		res, err := e.appendX402(t, p.GetPlacementId(), ev)
		if err != nil || len(res.GetRejected()) != 1 {
			t.Errorf("%s: rejected %v, err %v", name, res.GetRejected(), err)
		}
	}
	host := &routerv1.X402Event{Ref: "hhhhhhhhhhhhhhhh", Status: "refused_host", Count: 1}
	res, err := e.appendX402(t, p.GetPlacementId(), host)
	if err != nil || len(res.GetRejected()) != 0 {
		t.Errorf("overflow refused_host row: %v %v", res.GetRejected(), err)
	}
	_, err = e.appendX402(t, uuid.NewString())
	eq(t, connect.CodeOf(err), connect.CodeInvalidArgument)
	_, err = e.appendX402(t, p.GetPlacementId(), make([]*routerv1.X402Event, 51)...)
	eq(t, connect.CodeOf(err), connect.CodeInvalidArgument)
}

func TestAppendX402EventsPastTheCapAndTheEnd(t *testing.T) {
	sess := sessionSettings
	sess.MaxEvents = 1
	e := newEnv(t, options{sessions: &sess})
	p, _ := e.x402Placement(t)
	id := p.GetPlacementId()
	_, err := e.appendX402(t, id, xev("iiiiiiiiiiiiiiii"),
		refused("jjjjjjjjjjjjjjjj", "rate_cap", 2))
	if err != nil {
		t.Fatal(err)
	}
	rows := e.sessionEvents(t, p)["x402"]
	eq(t, len(rows), 1)
	eq(t, rows[0]["status"], any("paid"))
	got, _ := e.store.X402Refusals(t.Context(), uuid.MustParse(id))
	eq(t, len(got), 1)
	eq(t, got[0].Count, 2)

	end := time.Now().Add(-9 * time.Minute)
	if err := e.store.EndSessionRecord(t.Context(), uuid.MustParse(id),
		store.SessionEnd{EndedAt: end, Keep: time.Hour}); err != nil {
		t.Fatal(err)
	}
	_, err = e.appendX402(t, id, xev("kkkkkkkkkkkkkkkk"))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(e.sessionEvents(t, p)["x402"]), 2)
	end = time.Now().Add(-11 * time.Minute)
	if err := e.store.EndSessionRecord(t.Context(), uuid.MustParse(id),
		store.SessionEnd{EndedAt: end, Keep: time.Hour}); err != nil {
		t.Fatal(err)
	}
	// ended_at was set by the first end and is kept, so move it back directly
	if _, err := e.pool.Exec(t.Context(),
		`update session_records set ended_at = $2 where placement_id = $1`,
		uuid.MustParse(id), end); err != nil {
		t.Fatal(err)
	}
	if _, err = e.appendX402(t, id, xev("llllllllllllllll")); err != nil {
		t.Fatal(err)
	}
	eq(t, len(e.sessionEvents(t, p)["x402"]), 2)
}

func TestMCPToolListHasTheX402Tools(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	res, err := e.appendEvents(t, h, mcp("bugnet_x402_find", "ok"), mcp("bugnet_x402_budget", "ok"))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(res.GetRejected()), 0)
	eq(t, len(e.sessionEvents(t, p)["mcp"]), 2)
}
