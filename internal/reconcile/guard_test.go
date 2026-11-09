package reconcile_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/reconcile"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/testenv"
)

var now = time.Now().UTC()

type guardEnv struct {
	pool  *pgxpool.Pool
	store *store.Store
}

func newGuardEnv(t *testing.T) guardEnv {
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
	return guardEnv{pool: pool, store: store.New(pool)}
}

// signal records an earlier signal of a placement on a provider
func (g guardEnv) signal(t *testing.T, prov, class, action string, ago time.Duration) {
	t.Helper()
	err := g.store.InsertSignal(t.Context(), store.Signal{
		Class: class, Provider: prov, PlacementID: uuid.New(), Source: "sweep", Action: action,
		CreatedAt: now.Add(-ago),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func (g guardEnv) guard(t *testing.T, prov string, c reconcile.Class) bool {
	t.Helper()
	var held bool
	err := g.store.WithTx(t.Context(), func(tx pgx.Tx) error {
		var err error
		held, err = reconcile.Guard(t.Context(), tx, prov, c, now, config.DefaultAbuse())
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return held
}

func TestGuardTriggers(t *testing.T) {
	t.Run("a fourth control_lost in ten minutes", func(t *testing.T) {
		g := newGuardEnv(t)
		for range 2 {
			g.signal(t, "boat", "control_lost", "struck", time.Minute)
		}
		if g.guard(t, "boat", reconcile.ClassControlLost) {
			t.Fatal("the third control_lost was held")
		}
		g.signal(t, "boat", "control_lost", "struck", time.Minute)
		if !g.guard(t, "boat", reconcile.ClassControlLost) {
			t.Fatal("the fourth control_lost in ten minutes was not held")
		}
	})
	t.Run("the classes together and old signals leave out", func(t *testing.T) {
		g := newGuardEnv(t)
		g.signal(t, "boat", "control_lost", "struck", time.Minute)
		g.signal(t, "boat", "abuse", "blocked", 2*time.Minute)
		g.signal(t, "boat", "sandbox_exited", "ended", 3*time.Minute)
		g.signal(t, "boat", "control_lost", "struck", 11*time.Minute)
		if g.guard(t, "boat", reconcile.ClassAbuse) != true {
			t.Fatal("three recent signals and this one did not trip the guard")
		}
		h := newGuardEnv(t)
		h.signal(t, "boat", "control_lost", "struck", 11*time.Minute)
		h.signal(t, "boat", "control_lost", "struck", 12*time.Minute)
		h.signal(t, "boat", "control_lost", "struck", 13*time.Minute)
		if h.guard(t, "boat", reconcile.ClassControlLost) {
			t.Fatal("signals older than the window were counted")
		}
	})
	t.Run("held ends and operator ends do not count", func(t *testing.T) {
		g := newGuardEnv(t)
		for range 3 {
			g.signal(t, "boat", "abuse", "held", time.Minute)
		}
		err := g.store.InsertSignal(t.Context(), store.Signal{
			Class: "abuse", Provider: "boat", PlacementID: uuid.New(), Source: "operator",
			Action: "blocked", CreatedAt: now,
		})
		if err != nil {
			t.Fatal(err)
		}
		if g.guard(t, "boat", reconcile.ClassAbuse) {
			t.Fatal("held or operator signals tripped the guard")
		}
	})
	t.Run("a blocked account", func(t *testing.T) {
		g := newGuardEnv(t)
		if g.guard(t, "boat", reconcile.ClassSandboxExited) {
			t.Fatal("held with a working account")
		}
		_, err := g.pool.Exec(t.Context(), `
			insert into provider_health (provider, state, state_since, updated_at, blocked)
			values ('boat', 'ok', now(), now(), 'banned')`)
		if err != nil {
			t.Fatal(err)
		}
		if !g.guard(t, "boat", reconcile.ClassSandboxExited) {
			t.Fatal("a blocked account did not hold")
		}
	})
	t.Run("a failed canary in the window", func(t *testing.T) {
		g := newGuardEnv(t)
		sample := func(outcome string, ago time.Duration) {
			err := g.store.InsertStartSample(t.Context(), store.StartSample{
				Kind: store.SampleCanary, Provider: "boat", StartedAt: now.Add(-ago),
				Outcome: outcome, CreateMS: store.NoStep, ReadyMS: store.NoStep,
				RunnerMS: store.NoStep, GateMS: store.NoStep, FirstCmdMS: store.NoStep,
				WarmCmdMS: store.NoStep, DestroyMS: store.NoStep,
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		sample(store.OutcomeFailed, 30*time.Minute)
		if g.guard(t, "boat", reconcile.ClassControlLost) {
			t.Fatal("a canary outside the window held")
		}
		sample(store.OutcomeFailed, 2*time.Minute)
		if !g.guard(t, "boat", reconcile.ClassControlLost) {
			t.Fatal("a failed canary in the window did not hold")
		}
		sample(store.OutcomeOK, time.Minute)
		if g.guard(t, "boat", reconcile.ClassControlLost) {
			t.Fatal("a newer good canary still held")
		}
	})
	t.Run("provider_failed is never held", func(t *testing.T) {
		g := newGuardEnv(t)
		for range 5 {
			g.signal(t, "boat", "control_lost", "struck", time.Minute)
		}
		if g.guard(t, "boat", reconcile.ClassProviderFailed) {
			t.Fatal("provider_failed was held")
		}
	})
}

func TestGuardOnAnotherProviderActs(t *testing.T) {
	g := newGuardEnv(t)
	for range 4 {
		g.signal(t, "boat", "control_lost", "struck", time.Minute)
	}
	if !g.guard(t, "boat", reconcile.ClassControlLost) {
		t.Fatal("the incident on boat did not hold")
	}
	if g.guard(t, "e2b", reconcile.ClassControlLost) {
		t.Fatal("boat's incident held an e2b end")
	}
}
