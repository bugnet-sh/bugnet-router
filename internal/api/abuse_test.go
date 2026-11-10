package api_test

import (
	"crypto/sha256"
	"slices"
	"strconv"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bugnet-sh/bugnet-router/internal/store"
)

func byPlacement(id string) *routerv1.PlacementTarget {
	return &routerv1.PlacementTarget{Target: &routerv1.PlacementTarget_PlacementId{PlacementId: id}}
}

func byPreview(id string) *routerv1.PlacementTarget {
	return &routerv1.PlacementTarget{Target: &routerv1.PlacementTarget_PreviewId{PreviewId: id}}
}

func byTransfer(raw string) *routerv1.PlacementTarget {
	return &routerv1.PlacementTarget{Target: &routerv1.PlacementTarget_Transfer{Transfer: raw}}
}

func bySandbox(prov, id string) *routerv1.PlacementTarget {
	return &routerv1.PlacementTarget{Target: &routerv1.PlacementTarget_ProviderSandbox{
		ProviderSandbox: &routerv1.ProviderSandbox{Provider: prov, SandboxId: id},
	}}
}

func (e *env) find(
	t *testing.T, tg *routerv1.PlacementTarget,
) (*routerv1.FindPlacementResponse, error) {
	t.Helper()
	res, err := e.operator(t, operatorToken).FindPlacement(t.Context(), connect.NewRequest(
		&routerv1.FindPlacementRequest{Target: tg}))
	if err != nil {
		return nil, err
	}
	return res.Msg, nil
}

func (e *env) mustFind(t *testing.T, tg *routerv1.PlacementTarget) *routerv1.OperatorPlacement {
	t.Helper()
	res, err := e.find(t, tg)
	if err != nil {
		t.Fatalf("find %v: %v", tg, err)
	}
	return res.GetPlacement()
}

func (e *env) endPlacement(
	t *testing.T, req *routerv1.EndPlacementRequest,
) (*routerv1.EndPlacementResponse, error) {
	t.Helper()
	res, err := e.operator(t, operatorToken).EndPlacement(t.Context(), connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return res.Msg, nil
}

// sandboxIDOf reads the provider's id of a placement's sandbox
func (e *env) sandboxIDOf(t *testing.T, id string) string {
	t.Helper()
	var sid string
	err := e.pool.QueryRow(t.Context(),
		`select provider_ref ->> 'id' from placements where id = $1`, uuid.MustParse(id)).
		Scan(&sid)
	eq(t, err, nil)
	return sid
}

type endRow struct {
	State, Reason string
	Refundable    bool
}

func (e *env) endRow(t *testing.T, id string) endRow {
	t.Helper()
	var r endRow
	err := e.pool.QueryRow(t.Context(), `select state, coalesce(termination_reason, ''),
		refundable from placements where id = $1`, uuid.MustParse(id)).
		Scan(&r.State, &r.Reason, &r.Refundable)
	eq(t, err, nil)
	return r
}

func (e *env) signalActions(t *testing.T, id string) []string {
	t.Helper()
	sigs, err := e.store.Signals(t.Context(), uuid.MustParse(id))
	eq(t, err, nil)
	var out []string
	for _, s := range sigs {
		out = append(out, s.Source+":"+s.Action)
	}
	return out
}

func TestFindPlacementByEveryTarget(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	id := p.GetPlacementId()
	pv := e.mustCreatePreview(t, h, 5173)
	tok := tokenOf(t, e.mintOK(t, upload(h, "/root/f", 3)))
	sid := e.sandboxIDOf(t, id)
	for name, tg := range map[string]*routerv1.PlacementTarget{
		"placement": byPlacement(id), "preview": byPreview(pv.GetPreviewId()),
		"transfer URL":              byTransfer("https://" + transferHost + "/" + tok),
		"transfer URL with a query": byTransfer("https://" + transferHost + "/" + tok + "?x=1"),
		"transfer token":            byTransfer(tok), "sandbox": bySandbox("local", sid),
	} {
		got := e.mustFind(t, tg)
		eq(t, got.GetPlacementId(), id)
		if got.GetProviderSandboxId() != sid || got.GetProvider() != "local" ||
			got.GetState() != routerv1.PlacementState_PLACEMENT_STATE_READY {
			t.Fatalf("%s found %v", name, got)
		}
	}
	mustCode(t, ignore(e.find(t, byPreview("zzzzzzzzzzzzzzzz"))), connect.CodeNotFound,
		"TARGET_UNKNOWN")
	mustCode(t, ignore(e.find(t, byPreview("aaaaaaaaaaaaaaaa"))), connect.CodeNotFound,
		"TARGET_UNKNOWN")
	mustCode(t, ignore(e.find(t, byTransfer("bnx_unknown"))), connect.CodeNotFound,
		"TARGET_UNKNOWN")
	mustCode(t, ignore(e.find(t, byTransfer("not a token"))), connect.CodeNotFound,
		"TARGET_UNKNOWN")
	mustCode(t, ignore(e.find(t, bySandbox("local", "nope"))), connect.CodeNotFound,
		"TARGET_UNKNOWN")
	mustCode(t, ignore(e.find(t, byPlacement(uuid.NewString()))), connect.CodeNotFound,
		"TARGET_UNKNOWN")
	mustCode(t, ignore(e.find(t, byPlacement("x"))), connect.CodeInvalidArgument, "")
	mustCode(t, ignore(e.find(t, &routerv1.PlacementTarget{})), connect.CodeInvalidArgument, "")
	mustCode(t, ignore(e.find(t, bySandbox("local", ""))), connect.CodeInvalidArgument, "")
}

func ignore[T any](_ T, err error) error { return err }

func TestFindPlacementTransferWhileOnlyOneRecordLives(t *testing.T) {
	e := newEnv(t, options{})
	h, p := e.readyPlacement(t)
	tok := tokenOf(t, e.mintOK(t, upload(h, "/root/f", 3)))
	// The index row goes, so the Valkey record alone finds the placement
	sum := sha256.Sum256([]byte(tok))
	_, err := e.pool.Exec(t.Context(), `delete from transfer_index where token_sha256 = $1`,
		sum[:])
	eq(t, err, nil)
	eq(t, e.mustFind(t, byTransfer(tok)).GetPlacementId(), p.GetPlacementId())
}

func TestFindPlacementBySandboxOfAnAttemptNotKept(t *testing.T) {
	e := newEnv(t, options{})
	_, p := e.readyPlacement(t)
	_, err := e.pool.Exec(t.Context(), `update placements set provider_ref = null
		where id = $1`, uuid.MustParse(p.GetPlacementId()))
	eq(t, err, nil)
	_, err = e.pool.Exec(t.Context(), `update placement_attempts set sandbox_id = 'gone-1'
		where placement_id = $1`, uuid.MustParse(p.GetPlacementId()))
	eq(t, err, nil)
	eq(t, e.mustFind(t, bySandbox("local", "gone-1")).GetPlacementId(), p.GetPlacementId())
}

func TestFindPlacementNamesBothPayers(t *testing.T) {
	e := newEnv(t, options{wallets: payerWallets(nil)})
	h := e.readyPaid(t, payerA)
	e.extendPaid(t, h, payerB)
	p := e.get(t, h, routerv1.PlacementState_PLACEMENT_STATE_UNSPECIFIED, 0)
	got := e.mustFind(t, byPlacement(p.GetPlacementId()))
	eq(t, got.GetPayer(), payerA)
	eq(t, got.GetExtendPayers(), []string{payerB})
}

func TestEndPlacementAbuseBlocksEveryPayer(t *testing.T) {
	e := newEnv(t, options{wallets: payerWallets(nil)})
	h := e.readyPaid(t, payerA)
	e.extendPaid(t, h, payerB)
	id := e.get(t, h, routerv1.PlacementState_PLACEMENT_STATE_UNSPECIFIED, 0).GetPlacementId()
	res, err := e.endPlacement(t, &routerv1.EndPlacementRequest{
		Target: byPlacement(id), Actor: "ann", Note: "phishing page",
		Reason: routerv1.OperatorEndReason_OPERATOR_END_REASON_ABUSE, Refundable: true,
	})
	eq(t, err, nil)
	eq(t, slices.Sorted(slices.Values(res.GetBlockedPayers())),
		slices.Sorted(slices.Values([]string{payerA, payerB})))
	eq(t, res.GetPlacement().GetTerminationReason(), "abuse")
	got := e.endRow(t, id)
	eq(t, got.Reason+" "+strconv.FormatBool(got.Refundable), "abuse false")
	eq(t, e.signalActions(t, id), []string{"operator:blocked"})
	for _, payer := range []string{payerA, payerB} {
		w, err := e.operator(t, operatorToken).GetWallet(t.Context(), connect.NewRequest(
			&routerv1.GetWalletRequest{Payer: payer}))
		eq(t, err, nil)
		eq(t, w.Msg.GetWallet().GetBlocked(), true)
		eq(t, w.Msg.GetWallet().GetBlockedBy(), "operator:ann")
	}
	_, err = e.createPaid(t, payerA)
	mustCode(t, err, connect.CodePermissionDenied, "WALLET_BLOCKED")
}

func TestEndPlacementAbuseOnAStoppedPlacement(t *testing.T) {
	e := newEnv(t, options{wallets: payerWallets(nil)})
	h := e.readyPaid(t, payerA)
	id := e.stop(t, h).GetPlacementId()
	e.awaitState(t, h, "terminated")
	before := e.endRow(t, id)
	eq(t, before.Reason, "stop")
	res, err := e.endPlacement(t, &routerv1.EndPlacementRequest{
		Target: byPlacement(id), Actor: "ann", Note: "reported late",
		Reason: routerv1.OperatorEndReason_OPERATOR_END_REASON_ABUSE,
	})
	eq(t, err, nil)
	eq(t, res.GetBlockedPayers(), []string{payerA})
	// The reason and the refund stay as the stop left them
	eq(t, e.endRow(t, id), before)
	eq(t, res.GetPlacement().GetState(), routerv1.PlacementState_PLACEMENT_STATE_TERMINATED)
	eq(t, e.signalActions(t, id), []string{"operator:flagged"})
	_, err = e.createPaid(t, payerA)
	mustCode(t, err, connect.CodePermissionDenied, "WALLET_BLOCKED")
	// A second report adds a note and blocks nobody new
	res, err = e.endPlacement(t, &routerv1.EndPlacementRequest{
		Target: byPlacement(id), Actor: "bob",
		Reason: routerv1.OperatorEndReason_OPERATOR_END_REASON_ABUSE,
	})
	eq(t, err, nil)
	eq(t, len(res.GetBlockedPayers()), 0)
}

func TestEndPlacementOperatorRefundable(t *testing.T) {
	e := newEnv(t, options{wallets: payerWallets(nil)})
	h := e.readyPaid(t, payerA)
	id := e.get(t, h, routerv1.PlacementState_PLACEMENT_STATE_UNSPECIFIED, 0).GetPlacementId()
	res, err := e.endPlacement(t, &routerv1.EndPlacementRequest{
		Target: byPlacement(id), Actor: "ann", Refundable: true, Note: "asked by the owner",
		Reason: routerv1.OperatorEndReason_OPERATOR_END_REASON_OPERATOR,
	})
	eq(t, err, nil)
	eq(t, len(res.GetBlockedPayers()), 0)
	got := e.endRow(t, id)
	eq(t, got.Reason+" "+strconv.FormatBool(got.Refundable), "operator true")
	// No wallet action
	_, err = e.createPaid(t, payerA)
	eq(t, err, nil)
	// A retry of the end in force changes nothing, a placement ended another way is refused
	e.awaitState(t, h, "terminated")
	end := &routerv1.EndPlacementRequest{
		Target: byPlacement(id), Actor: "ann",
		Reason: routerv1.OperatorEndReason_OPERATOR_END_REASON_OPERATOR,
	}
	_, err = e.endPlacement(t, end)
	eq(t, err, nil)
	h2 := e.readyPaid(t, payerB)
	id2 := e.stop(t, h2).GetPlacementId()
	e.awaitState(t, h2, "terminated")
	end.Target = byPlacement(id2)
	_, err = e.endPlacement(t, end)
	mustCode(t, err, connect.CodeFailedPrecondition, "NOT_READY")
}

func TestEndPlacementRefusals(t *testing.T) {
	e := newEnv(t, options{})
	_, p := e.readyPlacement(t)
	abuse := routerv1.OperatorEndReason_OPERATOR_END_REASON_ABUSE
	id := p.GetPlacementId()
	for name, req := range map[string]*routerv1.EndPlacementRequest{
		"no actor":  {Target: byPlacement(id), Reason: abuse},
		"no reason": {Target: byPlacement(id), Actor: "ann"},
		"no target": {Actor: "ann", Reason: abuse},
	} {
		_, err := e.endPlacement(t, req)
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("%s gave %v", name, err)
		}
	}
	_, err := e.endPlacement(t, &routerv1.EndPlacementRequest{
		Target: byPreview("zzzzzzzzzzzzzzzz"), Actor: "ann", Reason: abuse,
	})
	mustCode(t, err, connect.CodeNotFound, "TARGET_UNKNOWN")
	eq(t, e.endRow(t, id).State, "ready")
	_, err = e.operator(t, "wrong-token").EndPlacement(t.Context(), connect.NewRequest(
		&routerv1.EndPlacementRequest{Target: byPlacement(id), Actor: "ann", Reason: abuse}))
	mustCode(t, err, connect.CodeUnauthenticated, "")
}

func TestListSignalsHeldOnly(t *testing.T) {
	e := newEnv(t, options{})
	_, p := e.readyPlacement(t)
	at := time.Now().UTC().Truncate(time.Millisecond)
	pid := uuid.MustParse(p.GetPlacementId())
	for _, sg := range []store.Signal{
		{Class: "provider_failed", Provider: "local", PlacementID: pid, Source: "sweep",
			Action: "held", HeldClass: "control_lost", CreatedAt: at.Add(-time.Hour)},
		{Class: "abuse", Provider: "local", PlacementID: pid, Source: "operator",
			Action: "blocked", CreatedAt: at.Add(-2 * time.Hour)},
		{Class: "account", Provider: "e2b", Source: "account_check", Action: "provider_blocked",
			Observed: []byte(`{"blocked":"auth"}`), CreatedAt: at},
		{Class: "provider_failed", Provider: "e2b", Source: "hook", Action: "held",
			CreatedAt: at.Add(-72 * time.Hour)},
	} {
		eq(t, e.store.InsertSignal(t.Context(), sg), nil)
	}
	list := func(req *routerv1.ListSignalsRequest) []*routerv1.Signal {
		t.Helper()
		res, err := e.operator(t, operatorToken).ListSignals(t.Context(), connect.NewRequest(req))
		eq(t, err, nil)
		return res.Msg.GetSignals()
	}
	actions := func(sigs []*routerv1.Signal) []string {
		var out []string
		for _, s := range sigs {
			out = append(out, s.GetProvider()+":"+s.GetAction())
		}
		return out
	}
	eq(t, actions(list(&routerv1.ListSignalsRequest{HeldOnly: true})),
		[]string{"local:held", "e2b:held"})
	held := list(&routerv1.ListSignalsRequest{
		HeldOnly: true, Since: timestamppb.New(at.Add(-24 * time.Hour)),
	})
	eq(t, actions(held), []string{"local:held"})
	eq(t, held[0].GetPlacementId(), p.GetPlacementId())
	eq(t, held[0].GetSignalClass(), routerv1.SignalClass_SIGNAL_CLASS_PROVIDER_FAILED)
	eq(t, actions(list(&routerv1.ListSignalsRequest{})),
		[]string{"e2b:provider_blocked", "local:held", "local:blocked", "e2b:held"})
	eq(t, actions(list(&routerv1.ListSignalsRequest{Provider: "e2b"})),
		[]string{"e2b:provider_blocked", "e2b:held"})
	acct := list(&routerv1.ListSignalsRequest{
		SignalClass: routerv1.SignalClass_SIGNAL_CLASS_ACCOUNT,
	})
	eq(t, actions(acct), []string{"e2b:provider_blocked"})
	eq(t, acct[0].GetPlacementId(), "")
	eq(t, acct[0].GetObservedJson(), `{"blocked": "auth"}`)
	eq(t, len(list(&routerv1.ListSignalsRequest{Limit: 2})), 2)
}

func TestListProvidersJoinsHealthAndCapacity(t *testing.T) {
	e := newEnv(t, options{})
	at := time.Now().UTC().Truncate(time.Millisecond)
	eq(t, e.store.WriteLimits(t.Context(), "e2b", 20, store.LimitFromProvider, 5, at), nil)
	_, err := e.store.WriteHealth(t.Context(), "e2b", store.HealthState{
		State: store.HealthSlow, Samples: 40, SuccessLB: 0.8, ReadyP95: 2500 * time.Millisecond,
	}, at)
	eq(t, err, nil)
	_, err = e.store.RecordAccount(t.Context(), "boat", "unfunded", "no credit", at)
	eq(t, err, nil)
	res, err := e.operator(t, operatorToken).ListProviders(t.Context(), connect.NewRequest(
		&routerv1.ListProvidersRequest{}))
	eq(t, err, nil)
	by := map[string]*routerv1.ProviderStatus{}
	for _, p := range res.Msg.GetProviders() {
		by[p.GetProvider()] = p
	}
	got := by["e2b"]
	if got == nil || got.GetState() != "slow" || got.GetSamples() != 40 ||
		got.GetSuccessLowerBound() < 0.79 || got.GetSuccessLowerBound() > 0.81 ||
		got.GetReadyP95Ms() != 2500 || got.GetMaxConcurrent() != 20 ||
		got.GetExternalInUse() != 5 || got.GetLimitSource() != "provider" ||
		!got.GetPolledAt().AsTime().Equal(at) {
		t.Fatalf("e2b is %v", got)
	}
	eq(t, by["boat"].GetBlocked(), "unfunded")
	// The registry's own provider shows with the defaults before any row
	eq(t, by["local"].GetState(), "ok")
	eq(t, res.Msg.GetProviders()[0].GetProvider(), "boat")
}
