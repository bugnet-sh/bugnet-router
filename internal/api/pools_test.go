package api_test

import (
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

func poolSettings() *config.Pools {
	s := config.DefaultPools()
	s.Pool = []config.Pool{{Provider: "local", Size: "small", Min: 2, Max: 5,
		MaxAge: time.Hour}}
	return &s
}

// warmMembers makes n warm members as pool_fill leaves them
func (e *env) warmMembers(t *testing.T, n int) {
	t.Helper()
	ctx := t.Context()
	for range n {
		m, err := e.store.StartFill(ctx, "local", "small", 1, nil)
		if err != nil {
			t.Fatal(err)
		}
		ref := provider.Ref{Provider: "local", ID: "sb-" + m.ID.String()[:8]}
		if err := e.store.MarkMemberWarm(ctx, m.ID, ref, "", nil, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestListPoolsCounts(t *testing.T) {
	e := newEnv(t, options{pools: poolSettings()})
	op := e.operator(t, operatorToken)
	res, err := op.ListPools(t.Context(), connect.NewRequest(&routerv1.ListPoolsRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(res.Msg.GetPools()), 1)
	p := res.Msg.GetPools()[0]
	eq(t, [3]string{p.GetProvider(), p.GetSize(), p.GetKeptBy()},
		[3]string{"local", "small", "router"})
	eq(t, [3]uint32{p.GetMin(), p.GetMax(), p.GetTarget()}, [3]uint32{2, 5, 2})
	eq(t, p.GetWarm(), uint32(0))

	e.warmMembers(t, 3)
	if _, err := e.store.StartFill(t.Context(), "local", "small", 1, nil); err != nil {
		t.Fatal(err)
	}
	eq(t, e.store.RecordMiss(t.Context(), "local", "small", time.Now()), nil)
	eq(t, e.store.PutTarget(t.Context(), store.PoolTarget{
		Provider: "local", SizeName: "small", Target: 4, ComputedAt: time.Now()}), nil)
	eq(t, e.store.PutPause(t.Context(), store.Pause{Provider: "local", SizeName: "small",
		Until: time.Now().Add(time.Hour), Reason: store.PauseSpend}), nil)
	res, err = op.ListPools(t.Context(), connect.NewRequest(&routerv1.ListPoolsRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	p = res.Msg.GetPools()[0]
	eq(t, [5]uint32{p.GetWarm(), p.GetFilling(), p.GetHeld(), p.GetTarget(), p.GetMissesLastHour()},
		[5]uint32{3, 1, 0, 4, 1})
	eq(t, p.GetPausedReason(), "spend")
	eq(t, p.GetPausedUntil() != nil, true)
}

func TestListPoolsWithoutAnyIsEmpty(t *testing.T) {
	e := newEnv(t, options{})
	res, err := e.operator(t, operatorToken).ListPools(t.Context(),
		connect.NewRequest(&routerv1.ListPoolsRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(res.Msg.GetPools()), 0)
}

func drainReq(
	name, size string, pause time.Duration,
) *connect.Request[routerv1.DrainPoolRequest] {
	m := &routerv1.DrainPoolRequest{Provider: name, Size: size, Actor: "ops"}
	if pause > 0 {
		m.PauseFor = durationpb.New(pause)
	}
	return connect.NewRequest(m)
}

func TestDrainPool(t *testing.T) {
	e := newEnv(t, options{pools: poolSettings()})
	op := e.operator(t, operatorToken)
	e.warmMembers(t, 2)
	res, err := op.DrainPool(t.Context(), drainReq("local", "small", time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, res.Msg.GetRetired(), uint32(2))
	var retiring int
	err = e.pool.QueryRow(t.Context(),
		`select count(*) from pool_members where state = 'retiring'`).Scan(&retiring)
	eq(t, err, nil)
	eq(t, retiring, 2)
	pauses, err := e.store.Pauses(t.Context())
	eq(t, err, nil)
	eq(t, len(pauses), 1)
	eq(t, [3]string{pauses[0].Reason, pauses[0].Actor, pauses[0].Provider},
		[3]string{store.PauseDrained, "ops", "local"})
	var jobs int
	err = e.pool.QueryRow(t.Context(),
		`select count(*) from river_job where kind = 'pool_retire'`).Scan(&jobs)
	eq(t, err, nil)
	eq(t, jobs, 2)
	// A second drain finds nothing warm
	res, err = op.DrainPool(t.Context(), drainReq("local", "small", 0))
	eq(t, err, nil)
	eq(t, res.Msg.GetRetired(), uint32(0))

	_, err = op.DrainPool(t.Context(), drainReq("local", "9x9", 0))
	mustCode(t, err, connect.CodeNotFound, "POOL_UNKNOWN")
}

func TestDrainPoolRefusals(t *testing.T) {
	e := newEnv(t, options{pools: poolSettings()})
	op := e.operator(t, operatorToken)
	for name, req := range map[string]*connect.Request[routerv1.DrainPoolRequest]{
		"no provider": drainReq("", "small", 0),
		"no size":     drainReq("local", "", 0),
		"no actor": connect.NewRequest(&routerv1.DrainPoolRequest{
			Provider: "local", Size: "small"}),
		"long pause": drainReq("local", "small", 30*24*time.Hour),
	} {
		_, err := op.DrainPool(t.Context(), req)
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("%s: got %v, want invalid argument", name, err)
		}
	}
	// A router with no pool manager knows no pool
	plain := newEnv(t, options{})
	_, err := plain.operator(t, operatorToken).DrainPool(t.Context(),
		drainReq("local", "small", 0))
	mustCode(t, err, connect.CodeNotFound, "POOL_UNKNOWN")
}

func TestFindPlacementShowsAHedgedStart(t *testing.T) {
	e := newEnv(t, options{})
	_, p := e.readyPlacement(t)
	find := func() *routerv1.OperatorPlacement {
		res, err := e.operator(t, operatorToken).FindPlacement(t.Context(), connect.NewRequest(
			&routerv1.FindPlacementRequest{Target: &routerv1.PlacementTarget{
				Target: &routerv1.PlacementTarget_PlacementId{PlacementId: p.GetPlacementId()}}}))
		if err != nil {
			t.Fatal(err)
		}
		return res.Msg.GetPlacement()
	}
	eq(t, find().GetHedged(), false)
	id, err := uuid.Parse(p.GetPlacementId())
	if err != nil {
		t.Fatal(err)
	}
	if err := e.store.SetPlacementHedged(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	eq(t, find().GetHedged(), true)
}

func TestFindPlacementShowsTheStartPath(t *testing.T) {
	e := newEnv(t, options{})
	_, p := e.readyPlacement(t)
	res, err := e.operator(t, operatorToken).FindPlacement(t.Context(), connect.NewRequest(
		&routerv1.FindPlacementRequest{Target: &routerv1.PlacementTarget{
			Target: &routerv1.PlacementTarget_PlacementId{PlacementId: p.GetPlacementId()}}}))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, res.Msg.GetPlacement().GetStartPath(), "cold")
}
