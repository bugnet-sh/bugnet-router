package api_test

import (
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// speculativeLocal lets the test provider start a sandbox before payment
func speculativeLocal() config.Reserve {
	r := config.DefaultReserve()
	r.SpeculativeProviders = []string{"local"}
	return r
}

func (e *env) reserve(
	t *testing.T, quote string, ip string,
) (*routerv1.ReserveResponse, error) {
	t.Helper()
	res, err := e.client.Reserve(t.Context(), connect.NewRequest(&routerv1.ReserveRequest{
		QuoteId: quote, PackageId: smallPkg, ClientIp: ip,
	}))
	if err != nil {
		return nil, err
	}
	return res.Msg, nil
}

func TestReserveAnswerNamesNoProvider(t *testing.T) {
	e := newEnv(t, options{reserve: speculativeLocal()})
	res, err := e.reserve(t, "q2.api", "203.0.113.7")
	if err != nil {
		t.Fatal(err)
	}
	eq(t, res.GetOutcome(), routerv1.ReserveOutcome_RESERVE_OUTCOME_HELD)
	eq(t, res.GetSkip(), routerv1.ReserveSkip_RESERVE_SKIP_UNSPECIFIED)
	eq(t, res.GetExpiresAt().AsTime().After(time.Now()), true)
	// The answer is an outcome, a skip and a time, and names no provider, sandbox or id
	raw, err := proto.Marshal(res)
	eq(t, err, nil)
	for _, name := range []string{"local", "sbx", "reservation"} {
		eq(t, strings.Contains(string(raw), name), false)
	}
	eq(t, res.ProtoReflect().Descriptor().Fields().Len(), 3)
	var id string
	err = e.pool.QueryRow(t.Context(), `select id::text from reservations`).Scan(&id)
	eq(t, err, nil)
	eq(t, strings.Contains(string(raw), id), false)

	// A skip answers OK too, with its reason and no time
	off := newEnv(t, options{reserve: func() config.Reserve {
		r := speculativeLocal()
		r.Enabled = false
		return r
	}()})
	res, err = off.reserve(t, "q2.api", "")
	eq(t, err, nil)
	eq(t, res.GetOutcome(), routerv1.ReserveOutcome_RESERVE_OUTCOME_SKIPPED)
	eq(t, res.GetSkip(), routerv1.ReserveSkip_RESERVE_SKIP_DISABLED)
	eq(t, res.GetExpiresAt() == nil, true)
	// An address that does not parse is no refusal either
	res, err = e.reserve(t, "q2.api.2", "not an address")
	eq(t, err, nil)
	eq(t, res.GetOutcome(), routerv1.ReserveOutcome_RESERVE_OUTCOME_HELD)
}

func TestReserveRefusesABadQuoteAndAnUnknownPackage(t *testing.T) {
	e := newEnv(t, options{reserve: speculativeLocal()})
	_, err := e.reserve(t, strings.Repeat("q", 129), "")
	mustCode(t, err, connect.CodeInvalidArgument, "QUOTE_INVALID")
	_, err = e.reserve(t, "q2.\x01", "")
	mustCode(t, err, connect.CodeInvalidArgument, "QUOTE_INVALID")
	_, err = e.client.Reserve(t.Context(), connect.NewRequest(&routerv1.ReserveRequest{
		QuoteId: "q2.ok", PackageId: "nope",
	}))
	mustCode(t, err, connect.CodeInvalidArgument, "")
	_, err = e.client.ReleaseReservation(t.Context(), connect.NewRequest(
		&routerv1.ReleaseReservationRequest{QuoteId: ""}))
	mustCode(t, err, connect.CodeInvalidArgument, "QUOTE_INVALID")
	_, err = e.client.ReleaseReservation(t.Context(), connect.NewRequest(
		&routerv1.ReleaseReservationRequest{QuoteId: "q2.never"}))
	eq(t, err, nil)
}

func TestCreatePlacementBindsTheQuotesReservation(t *testing.T) {
	e := newEnv(t, options{reserve: speculativeLocal()})
	res, err := e.reserve(t, "q2.bind", "")
	eq(t, err, nil)
	eq(t, res.GetOutcome(), routerv1.ReserveOutcome_RESERVE_OUTCOME_HELD)
	waitFor(t, "the reservation to be ready", func() bool {
		var n int
		err := e.pool.QueryRow(t.Context(),
			`select count(*) from reservations where state = 'ready'`).Scan(&n)
		return err == nil && n == 1
	})
	created, err := e.client.CreatePlacement(t.Context(), connect.NewRequest(
		&routerv1.CreatePlacementRequest{
			OrderToken: e.startToken(t, smallPkg, smallPrice), QuoteId: "q2.bind",
		}))
	eq(t, err, nil)
	// The bind is in the order's own transaction, so the answer is ready already
	eq(t, created.Msg.GetPlacement().GetState(), routerv1.PlacementState_PLACEMENT_STATE_READY)
	var path string
	err = e.pool.QueryRow(t.Context(), `select start_path from placements`).Scan(&path)
	eq(t, err, nil)
	eq(t, path, store.PathReserved)

	// A quote the router never saw, and a malformed one, start cold and are no error
	for _, q := range []string{"q2.unknown", "q2.\x01bad"} {
		got, err := e.client.CreatePlacement(t.Context(), connect.NewRequest(
			&routerv1.CreatePlacementRequest{
				OrderToken: e.startToken(t, smallPkg, smallPrice), QuoteId: q,
			}))
		eq(t, err, nil)
		e.waitReady(t, got.Msg.GetHandle())
	}
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
		if ok() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestReportStartsByPath(t *testing.T) {
	e := newEnv(t, options{})
	add := func(path string, ready, bind, paid, first int, outcome string) {
		t.Helper()
		id := e.placingPlacementID(t)
		if first > 0 {
			_, err := e.pool.Exec(t.Context(), `insert into jobs (id, placement_id, state,
				first_frame_ms) values ($1, $2, 'exited', $3)`, "j_"+id.String(), id, first)
			eq(t, err, nil)
		}
		err := e.store.InsertStartSample(t.Context(), store.StartSample{
			Kind: store.SamplePlacement, PlacementID: id, Provider: "local", SizeName: "small",
			StartedAt: time.Now().Add(-time.Minute), Outcome: outcome, StartPath: path,
			CreateMS: store.NoStep, ReadyMS: ready, RunnerMS: store.NoStep, GateMS: store.NoStep,
			FirstCmdMS: store.NoStep, WarmCmdMS: store.NoStep, DestroyMS: store.NoStep,
			WakeMS: map[string]int{"pooled": 300}[path], BindMS: bind, PaidToReadyMS: paid,
		})
		eq(t, err, nil)
	}
	add("cold", 3000, 3100, 3200, 100, "ok")
	add("cold", 5000, 5100, 5200, 200, "ok")
	add("pooled", store.NoStep, 700, 900, 50, "ok")
	add("reserved", store.NoStep, 10, 8010, 40, "ok")

	op := e.operator(t, operatorToken)
	got, err := op.ReportStarts(t.Context(), connect.NewRequest(&routerv1.ReportStartsRequest{
		Window: &routerv1.ReportStartsRequest_Since{Since: hourAgo()},
	}))
	eq(t, err, nil)
	byPath := map[string]*routerv1.StartReport{}
	for _, r := range got.Msg.GetReports() {
		byPath[r.GetStartPath()] = r
	}
	eq(t, len(byPath), 3)
	cold := byPath["cold"]
	eq(t, cold.GetOk(), uint32(2))
	eq(t, [2]uint32{cold.GetBind().GetSamples(), cold.GetBind().GetP90Ms()}, [2]uint32{2, 5100})
	eq(t, cold.GetPaidToReady().GetP50Ms(), uint32(3200))
	eq(t, cold.GetFirstFrame().GetP50Ms(), uint32(100))
	// Payment to first frame adds the two per run: 3300 and 5400
	eq(t, [2]uint32{cold.GetPaidToFirstFrame().GetP50Ms(), cold.GetPaidToFirstFrame().GetP90Ms()},
		[2]uint32{3300, 5400})
	pooled := byPath["pooled"]
	eq(t, pooled.GetWake().GetP50Ms(), uint32(300))
	eq(t, pooled.GetPaidToFirstFrame().GetP50Ms(), uint32(950))
	eq(t, byPath["reserved"].GetBind().GetP50Ms(), uint32(10))
	eq(t, byPath["reserved"].GetPaidToFirstFrame().GetP50Ms(), uint32(8050))

	// A path filter keeps the paths asked for
	only, err := op.ReportStarts(t.Context(), connect.NewRequest(&routerv1.ReportStartsRequest{
		Window:     &routerv1.ReportStartsRequest_Since{Since: hourAgo()},
		StartPaths: []string{"pooled", "reserved"},
	}))
	eq(t, err, nil)
	eq(t, len(only.Msg.GetReports()), 2)
}

// placingPlacementID stores a placing placement for a sample to name
func (e *env) placingPlacementID(t *testing.T) uuid.UUID {
	t.Helper()
	p, _, err := e.store.CreatePlacement(t.Context(), store.NewPlacement{
		OrderID: "o-" + uuid.NewString(), HandleSHA256: []byte(uuid.NewString()),
		PackageID: smallPkg,
	})
	eq(t, err, nil)
	return p.ID
}
