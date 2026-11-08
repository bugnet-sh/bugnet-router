package api_test

import (
	"encoding/json"
	"log/slog"
	"regexp"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

const sessionsBase = "https://sessions.test.example"

// sessionSettings are the [sessions] defaults with a test origin and a few tool names
var sessionSettings = config.Sessions{
	BaseURL: sessionsBase, Keep: 48 * time.Hour, AbuseHold: 720 * time.Hour, MaxEvents: 5000,
	JobOutput: 64 << 10, PlacementOutput: 4 << 20, DiskBudget: 8 << 30,
	MCPTools: []string{"bugnet_start", "bugnet_exec", "bugnet_stop", "bugnet_x402_find",
		"bugnet_x402_budget"},
	Live: config.SessionsLive{
		Enabled: true, MaxReaders: 8, PlacementBytes: 256 << 20, BytesPerSecond: 1 << 20,
		Replay: 256 << 10,
	},
}

var sessionLink = regexp.MustCompile(
	`^https://sessions\.test\.example/s/([a-z2-7]{16})#k=(bnk_[A-Za-z0-9_-]{43})$`)

// usdc is the USDC mint and sig a valid base58 signature of 64 bytes
const (
	usdc   = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
	solana = "solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp"
	sig    = "5JLwPBCJVyAxNnBYk9Ut7pRpCGVFXSoHZpfbTGhJn5gfU5jepsSJaSFBZqMn2Bap" +
		"VA9JNRM2hpBrfemJJyA8pFyh"
)

func (e *env) sessionPage(t *testing.T) routerv1connect.SessionPageServiceClient {
	return routerv1connect.NewSessionPageServiceClient(e.httpClient(t, sessionsToken), e.url)
}

// link splits a session link into its id and key
func link(t *testing.T, url string) (string, string) {
	t.Helper()
	m := sessionLink.FindStringSubmatch(url)
	if m == nil {
		t.Fatalf("session link %q is not the contract's shape", url)
	}
	return m[1], m[2]
}

func (e *env) record(t *testing.T, url string) (*routerv1.GetSessionRecordResponse, error) {
	t.Helper()
	id, key := link(t, url)
	res, err := e.sessionPage(t).GetSessionRecord(t.Context(), connect.NewRequest(
		&routerv1.GetSessionRecordRequest{Id: id, Key: key}))
	if err != nil {
		return nil, err
	}
	return res.Msg, nil
}

func (e *env) sessionRecord(t *testing.T, p *routerv1.Placement) store.SessionRecord {
	t.Helper()
	rec, err := e.store.SessionRecordByPlacement(t.Context(), uuid.MustParse(p.GetPlacementId()))
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

// sessionEvents are the record's stored events by kind, each as its stored fields
func (e *env) sessionEvents(t *testing.T, p *routerv1.Placement) map[string][]map[string]any {
	t.Helper()
	events, err := e.store.SessionEvents(t.Context(), e.sessionRecord(t, p).ID)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string][]map[string]any{}
	for _, ev := range events {
		var d map[string]any
		if err := json.Unmarshal(ev.Data, &d); err != nil {
			t.Fatal(err)
		}
		out[ev.Kind] = append(out[ev.Kind], d)
	}
	return out
}

// eventually retries check for up to 10 s, for events written after a call returned
func eventually(t *testing.T, check func() bool) {
	t.Helper()
	for range 200 {
		if check() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("condition never held")
}

func (e *env) exec2(t *testing.T, h string, argv ...string) *routerv1.ExecResponse {
	t.Helper()
	return e.exec(t, &routerv1.ExecRequest{Handle: h, Argv: argv,
		Until: &routerv1.Until{Exit: true}, WaitMs: 20000})
}

func TestCreateMakesTheRecord(t *testing.T) {
	hold := make(chan struct{})
	e := newEnv(t, options{holdReady: hold})
	h, p := e.create(t)
	eq(t, p.GetState(), routerv1.PlacementState_PLACEMENT_STATE_PLACING)
	id, _ := link(t, p.GetSessionUrl())
	eq(t, e.sessionRecord(t, p).ID, id)
	eq(t, e.get(t, h, 0, 0).GetSessionUrl(), p.GetSessionUrl())
	close(hold)
	eq(t, e.waitReady(t, h).GetSessionUrl(), p.GetSessionUrl())
	got := e.sessionEvents(t, p)
	eq(t, got["placement"][0]["phase"], "placed")
	eq(t, got["placement"][0]["package_id"], smallPkg)
	eq(t, got["payment"][0]["pay_kind"], "start")
}

func TestSnapshotWhilePlacing(t *testing.T) {
	hold := make(chan struct{})
	e := newEnv(t, options{holdReady: hold})
	defer close(hold)
	_, p := e.create(t)
	before := time.Now().Truncate(time.Second)
	res, err := e.record(t, p.GetSessionUrl())
	if err != nil {
		t.Fatal(err)
	}
	after := time.Now()
	eq(t, res.GetState(), "running")
	var snap map[string]any
	if err := json.Unmarshal(res.GetLiveJson(), &snap); err != nil {
		t.Fatal(err)
	}
	// The page refuses a snapshot without a deadline, so placing serves the paid minutes from now
	raw, _ := snap["deadline"].(string)
	deadline, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatalf("deadline %v: %v", snap["deadline"], err)
	}
	if deadline.Before(before.Add(10*time.Minute)) || deadline.After(after.Add(10*time.Minute)) {
		t.Fatalf("deadline %s, want 10 minutes after %s", deadline, before)
	}
}

func TestSessionURLShape(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	id, key := link(t, p.GetSessionUrl())
	rec := e.sessionRecord(t, p)
	eq(t, id, rec.ID)
	sp := e.placementOf(t, p)
	eq(t, e.dpKeys.CheckSessionKey(sp.KeyID, id, 0, key), true)
	// The link is the same on every answer, never derived from the handle or the order
	eq(t, e.stop(t, h).GetSessionUrl(), p.GetSessionUrl())
	eq(t, strings.Contains(p.GetSessionUrl(), p.GetPlacementId()), false)
}

func TestSessionURLOnEveryAnswer(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	url := p.GetSessionUrl()
	link(t, url)
	check, err := e.checkExtend(t, h, morePkg)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, check.Msg.GetPlacement().GetSessionUrl(), url)
	ext, err := e.client.ExtendPlacement(t.Context(), connect.NewRequest(
		&routerv1.ExtendPlacementRequest{
			Handle: h, OrderToken: e.extendToken(t, h, morePkg, morePrice),
		}))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, ext.Msg.GetPlacement().GetSessionUrl(), url)
	stream, err := e.client.WatchPlacement(timeout(t), connect.NewRequest(
		&routerv1.WatchPlacementRequest{Handle: h}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()
	sent := 0
	for stream.Receive() {
		eq(t, stream.Msg().GetPlacement().GetSessionUrl(), url)
		if sent++; sent == 1 {
			e.stop(t, h)
		}
	}
	eq(t, stream.Err(), nil)
	if sent < 2 {
		t.Fatalf("watch sent %d placements, want the ready one and the stop's", sent)
	}
}

func TestSessionURLEmptyAfterExpiry(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	stopped := e.stop(t, h)
	eq(t, stopped.GetSessionUrl() != "", true)
	rec := e.sessionRecord(t, p)
	eq(t, rec.ExpiresAt.Sub(*rec.EndedAt), 48*time.Hour)
	_, err := e.pool.Exec(t.Context(), `update session_records
		set expires_at = now() - interval '1 second' where id = $1`, rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, e.get(t, h, 0, 0).GetSessionUrl(), "")
	other, _ := e.readyPlacement(t)
	if _, err := e.pool.Exec(t.Context(), `update session_records set closed_at = now()
		where placement_id = $1`, e.placementOf(t, e.get(t, other, 0, 0)).ID); err != nil {
		t.Fatal(err)
	}
	eq(t, e.get(t, other, 0, 0).GetSessionUrl(), "")
}

func TestNoBaseURLGivesNoLink(t *testing.T) {
	none := sessionSettings
	none.BaseURL = ""
	e := newEnv(t, options{sessions: &none})
	_, p := e.readyPlacement(t)
	eq(t, p.GetSessionUrl(), "")
	// The record is still kept, the link only waits for the page's origin
	e.sessionRecord(t, p)
}

func TestNoKeyIDGivesNoLink(t *testing.T) {
	var logs syncBuffer
	e := newEnv(t, options{log: slog.New(slog.NewTextHandler(&logs, nil))})
	h, p := e.readyPlacement(t)
	id, _ := link(t, p.GetSessionUrl())
	if _, err := e.pool.Exec(t.Context(), `update placements set key_id = null where id = $1`,
		e.placementOf(t, p).ID); err != nil {
		t.Fatal(err)
	}
	eq(t, e.get(t, h, 0, 0).GetSessionUrl(), "")
	eq(t, strings.Count(logs.String(), "no session link"), 1)
	// A key under the current root opens nothing, as a root rotation would change it
	key, err := e.dpKeys.SessionKey(e.dpKeys.Current().ID, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.sessionPage(t).GetSessionRecord(t.Context(), connect.NewRequest(
		&routerv1.GetSessionRecordRequest{Id: id, Key: key}))
	mustCode(t, err, connect.CodeNotFound, "SESSION_UNKNOWN")
}

func TestEveryRouterKindWritten(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	e.exec2(t, h, "sh", "-c", "echo hi; exit 3")
	if _, err := e.client.WriteFile(t.Context(), connect.NewRequest(&routerv1.WriteFileRequest{
		Handle: h, Path: "/root/a.txt", Data: []byte("abc")})); err != nil {
		t.Fatal(err)
	}
	if _, err := e.client.ReadFile(t.Context(), connect.NewRequest(&routerv1.ReadFileRequest{
		Handle: h, Path: "/root/a.txt"})); err != nil {
		t.Fatal(err)
	}
	if _, err := e.client.ListFiles(t.Context(), connect.NewRequest(&routerv1.ListFilesRequest{
		Handle: h, Path: "/root"})); err != nil {
		t.Fatal(err)
	}
	pv := e.mustCreatePreview(t, h, 5173)
	e.mustCreatePreview(t, h, 5173)
	e.revoke(t, pv.GetPreviewId())
	o := e.open(t, e.mintOK(t, upload(h, "/root/up.bin", 2)), toSandbox)
	if _, err := e.write(timeout(t), t, o, "ab"); err != nil {
		t.Fatal(err)
	}
	d := e.open(t, e.mintOK(t, download(h, "/root/a.txt")), fromSandbox)
	if _, err := e.read(timeout(t), t, d, 0, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := e.client.ExtendPlacement(t.Context(), connect.NewRequest(
		&routerv1.ExtendPlacementRequest{Handle: h,
			OrderToken: e.extendToken(t, h, "small-10m-more", 45000)})); err != nil {
		t.Fatal(err)
	}
	e.stop(t, h)
	var got map[string][]map[string]any
	eventually(t, func() bool {
		got = e.sessionEvents(t, p)
		return len(got["job"]) == 2 && len(got["placement"]) == 4 && len(got["transfer"]) == 2
	})
	var phases []any
	for _, ev := range got["placement"] {
		phases = append(phases, ev["phase"])
	}
	eq(t, phases, []any{"placed", "ready", "extended", "ended"})
	eq(t, got["placement"][2]["minutes"], 10.0)
	eq(t, got["placement"][2]["package_id"], "small-10m-more")
	eq(t, got["placement"][3]["ended_because"], "stopped")
	eq(t, got["payment"], []map[string]any{
		{"pay_kind": "start", "amount_micros": float64(smallPrice), "tx": ""},
		{"pay_kind": "extend", "amount_micros": float64(45000), "tx": ""},
	})
	end := got["job"][1]
	eq(t, end["phase"], "end")
	eq(t, end["exit_code"], 3.0)
	eq(t, end["out_bytes"], 3.0)
	eq(t, got["file"], []map[string]any{
		{"op": "write", "path": "/root/a.txt", "bytes": float64(3)},
		{"op": "read", "path": "/root/a.txt", "bytes": float64(3)},
		{"op": "list", "path": "/root", "bytes": float64(0)},
	})
	eq(t, got["preview"], []map[string]any{
		{"port": float64(5173), "phase": "issued"}, {"port": float64(5173), "phase": "revoked"},
	})
	eq(t, got["transfer"], []map[string]any{
		{"direction": "in", "name": "up.bin", "bytes": float64(2)},
		{"direction": "out", "name": "a.txt", "bytes": float64(3)},
	})
}

func TestEventFailureNeverFailsTheCall(t *testing.T) {
	e := newEnv(t, options{})
	h, _ := e.readyPlacement(t)
	if _, err := e.pool.Exec(t.Context(),
		`alter table session_events rename to session_events_gone`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = e.pool.Exec(t.Context(), `alter table session_events_gone rename to session_events`)
	})
	if _, err := e.client.WriteFile(t.Context(), connect.NewRequest(&routerv1.WriteFileRequest{
		Handle: h, Path: "/root/a.txt", Data: []byte("abc")})); err != nil {
		t.Fatalf("a failed event write failed the call: %v", err)
	}
	eq(t, e.exec2(t, h, "true").GetState(), routerv1.JobState_JOB_STATE_EXITED)
	e.stop(t, h)
}

func TestCommandFirstKiBRedacted(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	tok := "bnsec_" + strings.Repeat("a", 26)
	long := strings.Repeat("x", 2000)
	e.exec2(t, h, "echo", "KEY="+tok, "\x1b[31mred\x1b[0m", long)
	jobs, err := e.store.SessionJobs(t.Context(), e.sessionRecord(t, p).ID)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(jobs), 1)
	cmd := jobs[0].Command
	eq(t, strings.HasPrefix(cmd, "echo KEY=[redacted token] red xxx"), true)
	eq(t, len(cmd), 1024)
	eq(t, strings.Contains(cmd, "bnsec_"), false)
}

func TestRecordNamesNoIDs(t *testing.T) {
	e := newEnv(t, options{})
	order, tok := e.startOrder(t, smallPkg, smallPrice)
	res, err := e.createWith(t, tok)
	if err != nil {
		t.Fatal(err)
	}
	h, p := res.Msg.GetHandle(), e.waitReady(t, res.Msg.GetHandle())
	run := e.exec2(t, h, "echo", "done")
	e.stop(t, h)
	var rec *routerv1.GetSessionRecordResponse
	eventually(t, func() bool {
		rec, err = e.record(t, p.GetSessionUrl())
		return err == nil && rec.GetState() == "ended"
	})
	raw := string(rec.GetRecordJson())
	sp := e.placementOf(t, p)
	for _, id := range []string{
		p.GetPlacementId(), order, h, run.GetJobId(), sp.ProviderRef.ID, sp.Provider,
		"127.0.0.1",
	} {
		if strings.Contains(raw, id) {
			t.Errorf("record_json names %q: %s", id, raw)
		}
	}
}

func TestGetSessionRecordStates(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	e.exec2(t, h, "echo", "hi")
	res, err := e.record(t, p.GetSessionUrl())
	if err != nil {
		t.Fatal(err)
	}
	eq(t, res.GetState(), "running")
	eq(t, len(res.GetRecordJson()), 0)
	var snap struct {
		V        int    `json:"v"`
		Deadline string `json:"deadline"`
		Jobs     []struct {
			Seq      int    `json:"seq"`
			Command  string `json:"command"`
			State    string `json:"state"`
			OutBytes int64  `json:"outBytes"`
		} `json:"jobs"`
	}
	if err := json.Unmarshal(res.GetLiveJson(), &snap); err != nil {
		t.Fatal(err)
	}
	eq(t, snap.V, 1)
	eq(t, snap.Deadline, p.GetDeadline().AsTime().UTC().Format(time.RFC3339))
	eq(t, len(snap.Jobs), 1)
	eq(t, snap.Jobs[0].Command, "echo hi")
	eq(t, snap.Jobs[0].OutBytes, int64(3))
	e.stop(t, h)
	res, err = e.record(t, p.GetSessionUrl())
	if err != nil {
		t.Fatal(err)
	}
	eq(t, res.GetState(), "ended")
	var record struct {
		V            int    `json:"v"`
		EndedBecause string `json:"endedBecause"`
		DeletesAt    string `json:"deletesAt"`
	}
	if err := json.Unmarshal(res.GetRecordJson(), &record); err != nil {
		t.Fatal(err)
	}
	eq(t, record.EndedBecause, "stopped")
	eq(t, record.DeletesAt != "", true)
	rec := e.sessionRecord(t, p)
	if _, err := e.pool.Exec(t.Context(), `update session_records set expires_at = now()
		where id = $1`, rec.ID); err != nil {
		t.Fatal(err)
	}
	res, err = e.record(t, p.GetSessionUrl())
	if err != nil {
		t.Fatal(err)
	}
	eq(t, res.GetState(), "expired")
	eq(t, len(res.GetRecordJson())+len(res.GetLiveJson()), 0)
}

func TestWrongKeyLikeUnknownID(t *testing.T) {
	e := newEnv(t, options{})
	_, p := e.readyPlacement(t)
	id, key := link(t, p.GetSessionUrl())
	wrong := "bnk_" + strings.Repeat("A", 43)
	other := "abcdefghijklmnop"
	for _, c := range [][2]string{
		{id, wrong}, {other, key}, {id, key[:46]}, {id, ""}, {"", key}, {id, key[4:]},
		{strings.ToUpper(id), key},
	} {
		_, err := e.sessionPage(t).GetSessionRecord(t.Context(), connect.NewRequest(
			&routerv1.GetSessionRecordRequest{Id: c[0], Key: c[1]}))
		mustCode(t, err, connect.CodeNotFound, "SESSION_UNKNOWN")
		_, err = e.sessionPage(t).DeleteSessionRecord(t.Context(), connect.NewRequest(
			&routerv1.DeleteSessionRecordRequest{Id: c[0], Key: c[1]}))
		mustCode(t, err, connect.CodeNotFound, "SESSION_UNKNOWN")
	}
}

func TestDeleteNow(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	e.exec2(t, h, "echo", "hi")
	id, key := link(t, p.GetSessionUrl())
	del := func() error {
		_, err := e.sessionPage(t).DeleteSessionRecord(t.Context(), connect.NewRequest(
			&routerv1.DeleteSessionRecordRequest{Id: id, Key: key}))
		return err
	}
	eq(t, del(), nil)
	rec := e.sessionRecord(t, p)
	eq(t, rec.ClosedBy, store.ClosedByUser)
	events, _ := e.store.SessionEvents(t.Context(), rec.ID)
	jobs, _ := e.store.SessionJobs(t.Context(), rec.ID)
	eq(t, len(events)+len(jobs), 0)
	// A closed record answers OK again, and its page is expired
	eq(t, del(), nil)
	res, err := e.record(t, p.GetSessionUrl())
	eq(t, err, nil)
	eq(t, res.GetState(), "expired")
	eq(t, e.get(t, h, 0, 0).GetSessionUrl(), "")
}

func TestAbuseEndClosesAndHolds(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	e.exec2(t, h, "echo", "evidence")
	sp := e.placementOf(t, p)
	if _, err := e.pool.Exec(t.Context(), `update placements set state = 'terminating',
		termination_reason = 'abuse', terminating_at = now() where id = $1`, sp.ID); err != nil {
		t.Fatal(err)
	}
	if err := e.saga.Terminate(t.Context(), sp.ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	rec := e.sessionRecord(t, p)
	eq(t, rec.ClosedBy, store.ClosedByAbuse)
	eq(t, rec.Held(time.Now()), true)
	eq(t, time.Until(*rec.HoldUntil) > 719*time.Hour, true)
	res, err := e.record(t, p.GetSessionUrl())
	eq(t, err, nil)
	eq(t, res.GetState(), "expired")
	// Delete now keeps a held record, and the operator still reads it
	id, key := link(t, p.GetSessionUrl())
	_, err = e.sessionPage(t).DeleteSessionRecord(t.Context(), connect.NewRequest(
		&routerv1.DeleteSessionRecordRequest{Id: id, Key: key}))
	eq(t, err, nil)
	events, _ := e.store.SessionEvents(t.Context(), rec.ID)
	eq(t, len(events) > 0, true)
	hold, err := e.operator(t, operatorToken).GetSessionHold(t.Context(), connect.NewRequest(
		&routerv1.GetSessionHoldRequest{PlacementId: p.GetPlacementId()}))
	eq(t, err, nil)
	eq(t, hold.Msg.GetClosedBy(), "abuse")
	eq(t, strings.Contains(string(hold.Msg.GetRecordJson()), `"command":"echo evidence"`), true)
}

func TestGetSessionHold(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	op := e.operator(t, operatorToken)
	ask := func(id string) error {
		_, err := op.GetSessionHold(t.Context(), connect.NewRequest(
			&routerv1.GetSessionHoldRequest{PlacementId: id}))
		return err
	}
	mustCode(t, ask(p.GetPlacementId()), connect.CodeNotFound, "SESSION_UNKNOWN")
	mustCode(t, ask(uuid.NewString()), connect.CodeNotFound, "SESSION_UNKNOWN")
	mustCode(t, ask("not-a-uuid"), connect.CodeInvalidArgument, "")
	e.stop(t, h)
	sp := e.placementOf(t, p)
	if err := e.store.CloseSessionRecord(t.Context(), sp.ID, store.ClosedByOperator,
		time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	res, err := op.GetSessionHold(t.Context(), connect.NewRequest(
		&routerv1.GetSessionHoldRequest{PlacementId: p.GetPlacementId()}))
	eq(t, err, nil)
	var record struct {
		V            int    `json:"v"`
		EndedBecause string `json:"endedBecause"`
	}
	eq(t, json.Unmarshal(res.Msg.GetRecordJson(), &record), nil)
	eq(t, record.V, 1)
	eq(t, record.EndedBecause, "stopped")
	_, err = e.sessionPage(t).GetSessionRecord(t.Context(), connect.NewRequest(
		&routerv1.GetSessionRecordRequest{Id: e.sessionRecord(t, p).ID, Key: "x"}))
	mustCode(t, err, connect.CodeNotFound, "SESSION_UNKNOWN")
}

func TestRotateEndsTheOldLink(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	old := p.GetSessionUrl()
	rotate := func(url string) string {
		t.Helper()
		res, err := e.client.RotateSessionLink(t.Context(), connect.NewRequest(
			&routerv1.RotateSessionLinkRequest{Handle: h, SessionUrl: url}))
		if err != nil {
			t.Fatal(err)
		}
		return res.Msg.GetSessionUrl()
	}
	fresh := rotate(old)
	eq(t, fresh != old, true)
	oldID, _ := link(t, old)
	newID, _ := link(t, fresh)
	eq(t, newID, oldID)
	_, err := e.record(t, old)
	mustCode(t, err, connect.CodeNotFound, "SESSION_UNKNOWN")
	res, err := e.record(t, fresh)
	eq(t, err, nil)
	eq(t, res.GetState(), "running")
	// A resent call naming the old link changes nothing, so it cannot kill the new one
	eq(t, rotate(old), fresh)
	eq(t, rotate("anything"), fresh)
	eq(t, e.get(t, h, 0, 0).GetSessionUrl(), fresh)
	_, err = e.client.RotateSessionLink(t.Context(), connect.NewRequest(
		&routerv1.RotateSessionLinkRequest{Handle: "sbx_unknown", SessionUrl: fresh}))
	mustCode(t, err, connect.CodeNotFound, "HANDLE_UNKNOWN")
}

func mcp(tool, outcome string) *routerv1.McpEvent {
	return &routerv1.McpEvent{At: timestamppb.Now(), Tool: tool, Outcome: outcome,
		DurationMs: 12}
}

func (e *env) appendEvents(
	t *testing.T, h string, events ...*routerv1.McpEvent,
) (*routerv1.AppendSessionEventsResponse, error) {
	t.Helper()
	res, err := e.client.AppendSessionEvents(t.Context(), connect.NewRequest(
		&routerv1.AppendSessionEventsRequest{Handle: h, Events: events}))
	if err != nil {
		return nil, err
	}
	return res.Msg, nil
}

func TestAppendSessionEvents(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	many := make([]*routerv1.McpEvent, 51)
	for i := range many {
		many[i] = mcp("bugnet_exec", "ok")
	}
	_, err := e.appendEvents(t, h, many...)
	mustCode(t, err, connect.CodeInvalidArgument, "")
	res, err := e.appendEvents(t, h, mcp("bugnet_exec", "ok"), mcp("rm_rf", "ok"),
		mcp("bugnet_stop", "Not Ok"), mcp("bugnet_x402_find", "denied"),
		mcp("bugnet_exec", strings.Repeat("a", 33)))
	eq(t, err, nil)
	eq(t, res.GetRejected(), []uint32{1, 2, 4})
	got := e.sessionEvents(t, p)["mcp"]
	eq(t, len(got), 2)
	eq(t, got[0]["tool"], "bugnet_exec")
	eq(t, got[1]["outcome"], "denied")
	eq(t, got[1]["duration_ms"], 12.0)
	_, err = e.appendEvents(t, "sbx_unknown", mcp("bugnet_exec", "ok"))
	mustCode(t, err, connect.CodeNotFound, "HANDLE_UNKNOWN")
}

func TestAppendWindowAfterEnd(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	e.stop(t, h)
	rec := e.sessionRecord(t, p)
	for _, c := range []struct {
		ago  time.Duration
		kept int
	}{{9 * time.Minute, 1}, {11 * time.Minute, 1}} {
		if _, err := e.pool.Exec(t.Context(), `update session_records set ended_at = $2
			where id = $1`, rec.ID, time.Now().Add(-c.ago)); err != nil {
			t.Fatal(err)
		}
		res, err := e.appendEvents(t, h, mcp("bugnet_stop", "ok"))
		eq(t, err, nil)
		eq(t, len(res.GetRejected()), 0)
		eq(t, len(e.sessionEvents(t, p)["mcp"]), c.kept)
	}
}

func (e *env) allowance(
	t *testing.T, order, phase string, amount int64, tx string,
) error {
	t.Helper()
	return e.allowanceEvent(t, order, &routerv1.AllowanceEvent{
		Phase: phase, AmountMicros: amount, Asset: usdc, Network: solana, Tx: tx,
	})
}

func (e *env) allowanceEvent(t *testing.T, order string, ev *routerv1.AllowanceEvent) error {
	t.Helper()
	_, err := e.payments(t).AppendAllowanceEvent(t.Context(), connect.NewRequest(
		&routerv1.AppendAllowanceEventRequest{OrderId: order, Event: ev}))
	return err
}

func TestAppendAllowanceEventKept(t *testing.T) {
	e := newEnv(t, options{})
	order, tok := e.startOrder(t, smallPkg, smallPrice)
	res, err := e.createWith(t, tok)
	if err != nil {
		t.Fatal(err)
	}
	h := res.Msg.GetHandle()
	p := e.waitReady(t, h)
	eq(t, e.allowance(t, order, "paid", 1_000_000, sig), nil)
	eq(t, e.allowance(t, order, "refunded", 400_000, sig), nil)
	e.stop(t, h)
	kept := func(amount, keptMicros int64, tx string) error {
		return e.allowanceEvent(t, order, &routerv1.AllowanceEvent{Phase: "kept",
			AmountMicros: amount, KeptMicros: keptMicros, Asset: usdc, Network: solana, Tx: tx})
	}
	for _, c := range []struct {
		amount, kept int64
		tx           string
	}{{0, 4000, sig}, {0, 0, ""}, {0, -1, ""}, {1, 4000, ""}} {
		mustCode(t, kept(c.amount, c.kept, c.tx), connect.CodeInvalidArgument, "")
	}
	eq(t, kept(0, 4000, ""), nil)
	eq(t, kept(0, 4000, ""), nil)
	got := e.sessionEvents(t, p)["allowance"]
	eq(t, len(got), 3)
	eq(t, got[2], map[string]any{"phase": "kept", "amount_micros": 0.0,
		"kept_micros": 4000.0, "asset": usdc, "network": solana})
	rec, err := e.record(t, p.GetSessionUrl())
	if err != nil {
		t.Fatal(err)
	}
	var record struct {
		Events []map[string]any `json:"events"`
	}
	eq(t, json.Unmarshal(rec.GetRecordJson(), &record), nil)
	var served []map[string]any
	for _, ev := range record.Events {
		if ev["kind"] == "allowance" {
			delete(ev, "at")
			served = append(served, ev)
		}
	}
	eq(t, len(served), 3)
	eq(t, served[2], map[string]any{"kind": "allowance", "phase": "kept", "amountMicros": 0.0,
		"keptMicros": 4000.0, "asset": "USDC", "network": solana})
	eq(t, served[1]["tx"], sig)
}

func TestAppendAllowanceEvent(t *testing.T) {
	e := newEnv(t, options{})
	order, tok := e.startOrder(t, smallPkg, smallPrice)
	res, err := e.createWith(t, tok)
	if err != nil {
		t.Fatal(err)
	}
	p := e.waitReady(t, res.Msg.GetHandle())
	eq(t, e.allowance(t, order, "paid", 1_000_000, sig), nil)
	eq(t, e.allowance(t, order, "paid", 1_000_000, sig), nil)
	eq(t, e.allowance(t, order, "refunded", 400_000, sig), nil)
	got := e.sessionEvents(t, p)["allowance"]
	eq(t, len(got), 2)
	eq(t, got[0], map[string]any{"phase": "paid", "amount_micros": float64(1_000_000),
		"asset": usdc, "network": solana, "tx": sig})
	mustCode(t, e.allowance(t, "ord_unknown", "paid", 1, sig), connect.CodeNotFound,
		"ORDER_UNKNOWN")
	for _, c := range []struct {
		phase  string
		amount int64
		tx     string
	}{{"kept", 1, sig}, {"paid", -1, sig}, {"paid", 1, sig[:40]}, {"paid", 1, ""}} {
		mustCode(t, e.allowance(t, order, c.phase, c.amount, c.tx),
			connect.CodeInvalidArgument, "")
	}
	// An extend order is no start order
	if _, err := e.client.ExtendPlacement(t.Context(), connect.NewRequest(
		&routerv1.ExtendPlacementRequest{Handle: res.Msg.GetHandle(), OrderToken: e.extendToken(t,
			res.Msg.GetHandle(), "small-10m-more", 45000)})); err != nil {
		t.Fatal(err)
	}
	rows, err := e.pool.Query(t.Context(), `select order_id from orders where kind = 'extend'`)
	if err != nil {
		t.Fatal(err)
	}
	var extend string
	for rows.Next() {
		_ = rows.Scan(&extend)
	}
	rows.Close()
	mustCode(t, e.allowance(t, extend, "paid", 1, sig), connect.CodeNotFound, "ORDER_UNKNOWN")
	// Past expires_at an event is dropped with OK
	rec := e.sessionRecord(t, p)
	if _, err := e.pool.Exec(t.Context(), `update session_records set expires_at = now()
		where id = $1`, rec.ID); err != nil {
		t.Fatal(err)
	}
	eq(t, e.allowance(t, order, "refunded", 1, sig), nil)
	eq(t, len(e.sessionEvents(t, p)["allowance"]), 2)
}

func TestSessionsTokenOpensOnlyItsService(t *testing.T) {
	e := newEnv(t, options{})
	_, p := e.readyPlacement(t)
	id, key := link(t, p.GetSessionUrl())
	for name, tok := range map[string]string{
		"Internal": internalToken, "Edge": edgeToken, "Operator": operatorToken,
		"Payments": paymentsToken, "empty": "",
	} {
		c := routerv1connect.NewSessionPageServiceClient(e.httpClient(t, tok), e.url)
		_, err := c.GetSessionRecord(t.Context(), connect.NewRequest(
			&routerv1.GetSessionRecordRequest{Id: id, Key: key}))
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("GetSessionRecord with the %s token gave %v", name, err)
		}
	}
	res, err := e.record(t, p.GetSessionUrl())
	eq(t, err, nil)
	eq(t, res.GetState(), "running")
	// The sessions token opens nothing else
	_, err = routerv1connect.NewSandboxRouterServiceClient(e.httpClient(t, sessionsToken),
		e.url).PricePackage(t.Context(), connect.NewRequest(&routerv1.PricePackageRequest{}))
	mustCode(t, err, connect.CodeUnauthenticated, "")
	// The page never spends the sandbox's operation bucket
	before := len(e.opsKeys(t))
	for range 30 {
		if _, err := e.record(t, p.GetSessionUrl()); err != nil {
			t.Fatal(err)
		}
	}
	eq(t, len(e.opsKeys(t)), before)
}
