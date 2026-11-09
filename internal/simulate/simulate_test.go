package simulate_test

import (
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/simulate"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

func start(name, outcome string, ready int) store.StartSample {
	return store.StartSample{
		Provider: name, Outcome: outcome, CreateMS: ready / 2, ReadyMS: ready,
		RunnerMS: store.NoStep, GateMS: store.NoStep, FirstCmdMS: store.NoStep,
		WarmCmdMS: store.NoStep, DestroyMS: store.NoStep,
	}
}

func trace(name, outcome string, ready, n int) []store.StartSample {
	var out []store.StartSample
	for range n {
		out = append(out, start(name, outcome, ready))
	}
	return out
}

func TestSimulatorAlwaysDownProvider(t *testing.T) {
	samples := append(trace("down", "failed", 2000, 20), trace("up", "ok", 1500, 20)...)
	got := simulate.Run(samples, simulate.Settings{
		Providers: []string{"down", "up"}, Budget: 30 * time.Second, Orders: 500, Seed: 1,
	})
	if got.FailedOver != 500 || got.Ready != 500 || got.Refunded != 0 {
		t.Fatalf("got %+v, want every order failed over and ready", got)
	}
	if got.FailoverShare() != 1 || got.ReadyShare() != 1 {
		t.Fatalf("shares %v %v", got.FailoverShare(), got.ReadyShare())
	}
	// 2 s lost on the down provider, then 1.5 s on the good one
	if got.ReadyP50 != 3500*time.Millisecond || got.ReadyP90 != 3500*time.Millisecond {
		t.Fatalf("times %v %v", got.ReadyP50, got.ReadyP90)
	}
}

func TestSimulatorRefundsWhenTheBudgetRunsOut(t *testing.T) {
	samples := append(trace("a", "failed", 20000, 5), trace("b", "ok", 15000, 5)...)
	got := simulate.Run(samples, simulate.Settings{
		Providers: []string{"a", "b"}, Budget: 30 * time.Second, Orders: 100, Seed: 2,
	})
	// 20 s lost, then 15 s more is past 30 s
	if got.Refunded != 100 || got.Ready != 0 {
		t.Fatalf("got %+v", got)
	}
	longer := simulate.Run(samples, simulate.Settings{
		Providers: []string{"a", "b"}, Budget: 40 * time.Second, Orders: 100, Seed: 2,
	})
	if longer.Ready != 100 || longer.Refunded != 0 {
		t.Fatalf("got %+v", longer)
	}
}

func TestSimulatorSendsSlowProvidersLast(t *testing.T) {
	samples := append(trace("slow", "ok", 12000, 10), trace("fast", "ok", 1000, 10)...)
	s := simulate.Settings{Providers: []string{"slow", "fast"}, Budget: time.Minute, Orders: 50}
	if got := simulate.Run(samples, s); got.ReadyP50 != 12*time.Second {
		t.Fatalf("without the rule %v", got.ReadyP50)
	}
	s.SlowReadyP95 = 10 * time.Second
	if got := simulate.Run(samples, s); got.ReadyP50 != time.Second || got.FailedOver != 0 {
		t.Fatalf("with the rule %+v", got)
	}
}

func TestSimulatorCapacityRefusalCostsNoAttempt(t *testing.T) {
	samples := append(trace("full", "capacity", 0, 5), trace("ok", "ok", 1000, 5)...)
	got := simulate.Run(samples, simulate.Settings{
		Providers: []string{"full", "ok"}, Budget: 30 * time.Second, MaxAttempts: 1, Orders: 20,
	})
	if got.Ready != 20 || got.FailedOver != 0 {
		t.Fatalf("got %+v", got)
	}
}

func TestSimulatorWithNoSamplesRefundsEverything(t *testing.T) {
	got := simulate.Run(nil, simulate.Settings{Providers: []string{"a"}, Orders: 10})
	if got.Refunded != 10 || got.RefundShare() != 1 {
		t.Fatalf("got %+v", got)
	}
}
