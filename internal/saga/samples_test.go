package saga_test

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// startSamples returns the samples of name, once the saga's own writes are done
func (e *env) startSamples(t *testing.T, name string) []store.StartSample {
	t.Helper()
	e.saga.Wait()
	got, err := e.store.StartSamples(t.Context(), name, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestPlacementWritesASampleForEachAttemptThatCalledTheProvider(t *testing.T) {
	var calls atomic.Int64
	a := newLocal(t, local.Options{Name: "a", CreateHook: func(int, provider.CreateRequest) error {
		calls.Add(1)
		return &provider.Error{Code: provider.CodeInternal, CanFailOver: true, Err: errors.New("x")}
	}})
	b := newLocal(t, local.Options{Name: "b"})
	e := newEnv(t, a, b)
	p, _ := e.placeReady(t, "order-1")
	eq(t, p.Provider, "b")

	failed := e.startSamples(t, "a")
	eq(t, len(failed), 1)
	f := failed[0]
	eq(t, f.Kind, store.SamplePlacement)
	eq(t, f.PlacementID, p.ID)
	eq(t, f.Outcome, "failed")
	eq(t, f.ErrorCode, "internal")
	eq(t, f.SandboxID, "")
	eq(t, f.Replica, "replica-test")
	if f.CreateMS < 0 || f.ReadyMS != store.NoStep || f.DestroyMS != store.NoStep {
		t.Errorf("failed sample %+v, want a timed create and nothing after it", f)
	}

	ok := e.startSamples(t, "b")
	eq(t, len(ok), 1)
	s := ok[0]
	eq(t, s.Outcome, "ok")
	eq(t, s.PlacementID, p.ID)
	eq(t, s.SizeName, "small")
	eq(t, s.SandboxID, p.ProviderRef.ID)
	// The local runner is in place, so no upload was timed
	eq(t, s.RunnerMS, store.NoStep)
	if s.CreateMS < 0 || s.ReadyMS < s.CreateMS || s.GateMS < 0 || s.CostMicros <= 0 {
		t.Errorf("ok sample %+v, want create, ready and gate timed and a cost", s)
	}
}

func TestPlacementSamplesNameWhatDoesNotCount(t *testing.T) {
	for code, outcome := range map[provider.Code]string{
		provider.CodeCapacity: "capacity", provider.CodeQuota: "quota",
		provider.CodeRateLimited: "rate_limited",
	} {
		t.Run(string(code), func(t *testing.T) {
			a := newLocal(t, local.Options{Name: "a", CreateHook: failWith(code, true)})
			b := newLocal(t, local.Options{Name: "b"})
			e := newEnv(t, a, b)
			e.placeReady(t, "order-1")
			got := e.startSamples(t, "a")
			eq(t, len(got), 1)
			eq(t, got[0].Outcome, outcome)
		})
	}
}

func TestBucketSkipWritesNoSample(t *testing.T) {
	saga.SetCreateWait(t, 50*time.Millisecond)
	a := newLocal(t, local.Options{Name: "a"})
	b := newLocal(t, local.Options{Name: "b"})
	e := newEnvWith(t, func(d *saga.Deps) { d.CreatesPerSecond = map[string]int{"a": 1} }, a, b)
	e.drain(t, "a", 1, 1)
	p, _ := e.placeReady(t, "order-1")
	eq(t, p.Provider, "b")
	// The attempt on a never called the provider
	eq(t, len(e.startSamples(t, "a")), 0)
	eq(t, len(e.startSamples(t, "b")), 1)
}

func TestSampleWriteFailureNeverFailsThePlacement(t *testing.T) {
	a := newLocal(t, local.Options{Name: "a"})
	e := newEnv(t, a)
	if _, err := e.pool.Exec(t.Context(), `drop table start_samples`); err != nil {
		t.Fatal(err)
	}
	p, _ := e.placeReady(t, "order-1")
	eq(t, p.State, store.PlacementReady)
}
