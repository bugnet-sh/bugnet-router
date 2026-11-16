package saga_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// refuser is a billed provider whose first creates answer the errors given, in order
type refuser struct {
	billed
	mu   *sync.Mutex
	errs *[]error
}

func newRefuser(t *testing.T, name string, hourly, fee int64, errs ...error) refuser {
	t.Helper()
	return refuser{billed: newBilled(t, name, hourly, fee), mu: &sync.Mutex{}, errs: &errs}
}

func (r refuser) Create(ctx context.Context, req provider.CreateRequest) (provider.Ref, error) {
	r.mu.Lock()
	if len(*r.errs) > 0 {
		err := (*r.errs)[0]
		*r.errs = (*r.errs)[1:]
		r.mu.Unlock()
		return provider.Ref{}, err
	}
	r.mu.Unlock()
	return r.billed.Create(ctx, req)
}

// refusedFor is a capacity refusal that allows fail over, unmade when the adapter says so
func refusedFor(unmade bool) error {
	return &provider.Error{Code: provider.CodeCapacity, CanFailOver: true, Unmade: unmade,
		Err: errors.New("no capacity")}
}

// spendPair is a, which bills 3,000 for 10 minutes and a 30,000 start fee, priced at 36,300, and
// b, which bills 33,000 for them, also priced at 36,300 and listed after a
func spendPair(t *testing.T, errs ...error) (billed, *env) {
	t.Helper()
	a := newRefuser(t, "a", boatRate, 30_000, errs...)
	b := newBilled(t, "b", 198_000, 0)
	return b, newEnvWith(t, live(t), a, b)
}

// TestCapacityRefusalSpendsItsFee keeps a CPU capacity refusal's start fee spent, as for Boat's
// cancelled sandbox before GPU prices, so b no longer fits what is left
func TestCapacityRefusalSpendsItsFee(t *testing.T) {
	b, e := spendPair(t, refusedFor(false))
	p, err := e.startPaid(t, "order-1", 36_300, uuid.Nil, placement.Requirements{})
	if err != nil || p.State != store.PlacementFailed || !p.Refundable {
		t.Fatalf("placement %+v %v, want failed refundable", p, err)
	}
	eq(t, len(e.attempts(t, p.ID)), 1)
	eq(t, len(owned(t, b)), 0)
}

// TestUnmadeRefusalSpendsNothing fails over with the whole payment after a refusal the adapter
// says made nothing
func TestUnmadeRefusalSpendsNothing(t *testing.T) {
	_, e := spendPair(t, refusedFor(true))
	p, err := e.startPaid(t, "order-1", 36_300, uuid.Nil, placement.Requirements{})
	if err != nil || p.State != store.PlacementReady || p.Provider != "b" {
		t.Fatalf("placement %+v %v, want ready on b", p, err)
	}
}

// TestUnmadeAfterAnUnknownCreateSpends counts an attempt whose create had an unknown outcome
// before its refusal, since that first create may have made a sandbox
func TestUnmadeAfterAnUnknownCreateSpends(t *testing.T) {
	unknown := &provider.Error{Code: provider.CodeOutcomeUnknown, Err: errors.New("lost answer")}
	b, e := spendPair(t, unknown, refusedFor(true))
	p, err := e.startPaid(t, "order-1", 36_300, uuid.Nil, placement.Requirements{})
	if err != nil || p.State != store.PlacementFailed || !p.Refundable {
		t.Fatalf("placement %+v %v, want failed refundable", p, err)
	}
	eq(t, len(owned(t, b)), 0)
}

// TestGPUCapRefusalLeavesLiumHealthy records no failure for refusals the order's own cap caused
func TestGPUCapRefusalLeavesLiumHealthy(t *testing.T) {
	lium := newNoded(t, 350_000, 300_000)
	var tracker *placement.Tracker
	e := gpuEnvOf(t, gpuLiveJSON, func(d *saga.Deps) { tracker = d.Tracker },
		lium, newHF(t, 240_000))
	for _, order := range []string{"order-1", "order-2", "order-3"} {
		est := e.holdGPU(t, quotedCost, quotedPrice, quotedHourly)
		p, _, err := e.startGPU(t, order, quotedPrice, est)
		if err != nil || p.State != store.PlacementReady || p.Provider != "hf" {
			t.Fatalf("%s %+v %v, want ready on hf", order, p, err)
		}
	}
	eq(t, lium.caps(), []int64{quotedHourly, quotedHourly, quotedHourly})
	eq(t, tracker.Healthy("lium"), true)
}

// finder answers one candidate, its provider pricing per node or not
type finder struct {
	c       placement.Candidate
	perNode bool
}

func (f finder) Candidates(placement.Inputs) ([]placement.Candidate, error) {
	return []placement.Candidate{f.c}, nil
}

func (f finder) PricesPerNode(string) bool { return f.perNode }

// TestHeldServerNeedsAnHourOnAPerNodeServer holds a per-node server only with the quote's hour
func TestHeldServerNeedsAnHourOnAPerNodeServer(t *testing.T) {
	c := placement.Candidate{Provider: newBilled(t, "lium", 280_000, 0), Size: small,
		PriceMicros: 1000}
	in := placement.Inputs{Package: catalog.Package{ID: smallPkg, Minutes: 10,
		Resources: small.Resources}}
	q := store.Quote{PackageID: smallPkg, Provider: "lium", SizeName: small.Name,
		PriceMicros: 1000}
	eq(t, saga.HeldServer(finder{c, true}, in, q, "order-1", 1000, 0) == nil, true)
	eq(t, saga.HeldServer(finder{c, false}, in, q, "order-1", 1000, 0) != nil, true)
	q.HourlyMicros = 280_000
	eq(t, saga.HeldServer(finder{c, true}, in, q, "order-1", 1000, 0) != nil, true)
}
