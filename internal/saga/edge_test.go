package saga_test

import (
	"encoding/base64"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	routerv1 "github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1"
	"google.golang.org/protobuf/proto"

	"github.com/oynozan/bugnet-router/internal/budget"
	"github.com/oynozan/bugnet-router/internal/config"
	"github.com/oynozan/bugnet-router/internal/dpkeys"
	"github.com/oynozan/bugnet-router/internal/placement"
	"github.com/oynozan/bugnet-router/internal/provider/local"
	"github.com/oynozan/bugnet-router/internal/saga"
	"github.com/oynozan/bugnet-router/internal/store"
)

// eq fails the test when got and want differ
func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

// budgetDefaults are the [edge_budget] settings of an empty settings file
var budgetDefaults = config.EdgeBudget{
	BytesPer10Minutes: 2 << 30, BytesPerSecond: 5 << 20, RequestsPerSecond: 100,
	RequestsBurst: 200, QueuedRequests: 64, WebSockets: 32, EgressBytesPer10Minutes: 1 << 30,
	EgressBytesPerSecond: 2 << 20, EgressRequestsInFlight: 32,
}

// edgeEvents decodes every entry of the edge event stream, oldest first
func (e *env) edgeEvents(t *testing.T) []*routerv1.EdgeEvent {
	t.Helper()
	cmd := e.vk.B().Xrange().Key(e.prefix + "edge:events").Start("-").End("+").Build()
	entries, err := e.vk.Do(t.Context(), cmd).AsXRange()
	if err != nil {
		t.Fatal(err)
	}
	var out []*routerv1.EdgeEvent
	for _, en := range entries {
		ev := &routerv1.EdgeEvent{}
		if err := proto.Unmarshal([]byte(en.FieldValues["e"]), ev); err != nil {
			t.Fatal(err)
		}
		out = append(out, ev)
	}
	return out
}

func b64(n int, fill byte) string {
	return base64.StdEncoding.EncodeToString([]byte(strings.Repeat(string(fill), n)))
}

func keys(t *testing.T, env string) dpkeys.Keys {
	t.Helper()
	k, err := dpkeys.Parse(env)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func TestNewPlacementCarriesCurrentKeyID(t *testing.T) {
	k := keys(t, "k2:"+b64(32, 2)+",k1:"+b64(32, 1))
	e := newEnvWith(t, func(d *saga.Deps) { d.Keys = k }, newLocal(t, local.Options{Name: "a"}))
	p, _ := e.placeReady(t, "order-1")
	eq(t, p.KeyID, "k2")

	// A placement no provider can take records its root too
	none := newEnvWith(t, func(d *saga.Deps) { d.Keys = k })
	p, _, err := none.saga.Create(t.Context(), none.startToken(t, "order-2"),
		placement.Requirements{})
	eq(t, errors.Is(err, placement.ErrNoCandidate), true)
	eq(t, none.reload(t, p.ID).KeyID, "k2")
}

func TestCreateNeedsDataPlaneKeys(t *testing.T) {
	e := newEnvWith(t, func(d *saga.Deps) { d.Keys = dpkeys.Keys{} },
		newLocal(t, local.Options{Name: "a"}))
	_, _, err := e.saga.Create(t.Context(), e.startToken(t, "order-1"),
		placement.Requirements{})
	if err == nil || !strings.Contains(err.Error(), "data-plane keys") {
		t.Fatalf("create without data-plane keys gave %v", err)
	}
	eq(t, e.placementCount(t), 0)
}

func TestBudgetsAtReadyGrowWithEachExtension(t *testing.T) {
	e := newEnv(t, newLocal(t, local.Options{Name: "a"}))
	p, h := e.placeReady(t, "order-1")
	eq(t, p.EdgeBudgetBytes, int64(2<<30))
	eq(t, p.EgressBudgetBytes, int64(1<<30))
	if _, err := e.saga.Extend(t.Context(), h,
		e.extendToken(t, "ext-1", shortPkg, shortPrice, h)); err != nil {
		t.Fatal(err)
	}
	got := e.reload(t, p.ID)
	eq(t, got.EdgeBudgetBytes, int64(3<<30))
	eq(t, got.EgressBudgetBytes, int64(3<<29))
	// A repeated order neither moves the deadline nor adds budget
	if _, err := e.saga.Extend(t.Context(), h,
		e.extendToken(t, "ext-1", shortPkg, shortPrice, h)); err != nil {
		t.Fatal(err)
	}
	eq(t, e.reload(t, p.ID).EdgeBudgetBytes, int64(3<<30))
}

func TestStopPublishesPlacementEnded(t *testing.T) {
	e := newEnv(t, newLocal(t, local.Options{Name: "a"}))
	p, h := e.placeReady(t, "order-1")
	eq(t, len(e.edgeEvents(t)), 0)
	for range 2 {
		if _, err := e.saga.Stop(t.Context(), h); err != nil {
			t.Fatal(err)
		}
	}
	evs := e.edgeEvents(t)
	eq(t, len(evs), 1)
	eq(t, evs[0].GetPlacementEnded().GetPlacementId(), p.ID.String())
	eq(t, evs[0].GetPlacementEnded().GetReason(), routerv1.EndReason_END_REASON_STOPPED)

	q, _ := e.placeReady(t, "order-2")
	for range 2 {
		if err := e.saga.Terminate(t.Context(), q.ID, *q.Deadline); err != nil {
			t.Fatal(err)
		}
	}
	evs = e.edgeEvents(t)
	eq(t, len(evs), 2)
	eq(t, evs[1].GetPlacementEnded().GetPlacementId(), q.ID.String())
	eq(t, evs[1].GetPlacementEnded().GetReason(), routerv1.EndReason_END_REASON_EXPIRED)
}

func TestNoEndEventWhenThePlacementStaysReady(t *testing.T) {
	e := newEnv(t, newLocal(t, local.Options{Name: "a"}))
	p, _ := e.placeReady(t, "order-1")
	err := e.saga.Terminate(t.Context(), p.ID, p.Deadline.Add(-time.Minute))
	eq(t, errors.Is(err, saga.ErrNotDue), true)
	eq(t, len(e.edgeEvents(t)), 0)
}

// remaining = new budget - used
func TestExtendClearsSpentAndPublishesDeadlineMoved(t *testing.T) {
	e := newEnv(t, newLocal(t, local.Options{Name: "a"}))
	p, h := e.placeReady(t, "order-1")
	m := budget.New(e.vk, e.prefix, budgetDefaults)
	limits := func(p store.Placement, kind budget.Kind) map[uuid.UUID]budget.Limit {
		b := p.EdgeBudgetBytes
		if kind == budget.Egress {
			b = p.EgressBudgetBytes
		}
		return map[uuid.UUID]budget.Limit{p.ID: {Placement: p.ID, Budget: b, Deadline: *p.Deadline}}
	}
	used := map[budget.Kind]uint64{budget.Edge: 5 << 29, budget.Egress: 3 << 29}
	for kind, n := range used {
		_, spent, err := m.Report(t.Context(), kind, budget.ByEdge,
			[]budget.Usage{{Placement: p.ID, Out: n}}, limits(p, kind))
		if err != nil || len(spent) != 1 {
			t.Fatalf("%s report spent %v, %v, want the placement", kind, spent, err)
		}
	}
	got, err := e.saga.Extend(t.Context(), h, e.extendToken(t, "ext-1", morePkg, morePrice, h))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"edge:spent:", "egress:spent:"} {
		n, err := e.vk.Do(t.Context(), e.vk.B().Exists().Key(e.prefix+key+p.ID.String()).
			Build()).AsInt64()
		if err != nil || n != 0 {
			t.Fatalf("%s is still set after the extension (%d, %v)", key, n, err)
		}
	}
	evs := e.edgeEvents(t)
	eq(t, len(evs), 1)
	moved := evs[0].GetDeadlineMoved()
	eq(t, moved.GetPlacementId(), p.ID.String())
	eq(t, moved.GetDeadline().AsTime().Equal(*got.Deadline), true)
	eq(t, moved.GetEdgeBytesRemaining(), uint64(4<<30-5<<29))
	eq(t, moved.GetEgressBytesRemaining(), uint64(2<<30-3<<29))

	// The guards are clear, so the next crossing writes BudgetSpent again
	cur := e.reload(t, p.ID)
	_, spent, err := m.Report(t.Context(), budget.Edge, budget.BySSH,
		[]budget.Usage{{Placement: p.ID, In: 3 << 30}}, limits(cur, budget.Edge))
	eq(t, err == nil && len(spent) == 1, true)
}

func TestPlacementEndedGoesOutBeforeTheDestroy(t *testing.T) {
	f := &failingDestroy{Provider: newLocal(t, local.Options{Name: "a"})}
	e := newEnv(t, f)
	p, h := e.placeReady(t, "order-1")
	f.fail.Store(true)
	defer f.fail.Store(false)
	if _, err := e.saga.Stop(t.Context(), h); err == nil {
		t.Fatal("a stop whose destroy fails succeeded")
	}
	eq(t, e.reload(t, p.ID).State, store.PlacementTerminating)
	evs := e.edgeEvents(t)
	eq(t, len(evs), 1)
	eq(t, evs[0].GetPlacementEnded().GetPlacementId(), p.ID.String())
}
