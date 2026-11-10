package saga_test

import (
	"testing"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/x402"
)

const x402PayTo = "11111111111111111111111111111111"

// placeStoringX402 stores one x402 row while the sandbox boots, then lets the placement finish
func placeStoringX402(
	t *testing.T, e *env, g gatedReady, secrets, withRow bool,
) store.Placement {
	t.Helper()
	p, _, err := e.saga.Create(t.Context(), saga.CreateInput{Token: e.startToken(t, "order-x402"), Requirements: placement.Requirements{Secrets: secrets}})
	if err != nil {
		t.Fatal(err)
	}
	<-g.waiting
	if withRow {
		err = e.store.PutX402Service(t.Context(), store.X402Service{
			PlacementID: p.ID, ServiceID: x402.ServiceID("GET", "api.example.com", "/v1/"),
			Spec: x402.Spec{
				Host: "api.example.com", Prefix: "/v1/", Method: "GET", MaxPriceMicros: 10_000,
				PayTo: x402PayTo, Indexed: true,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	close(g.gate)
	e.saga.Wait()
	return e.reload(t, p.ID)
}

func x402Env(t *testing.T, secrets bool) (*env, gatedReady) {
	t.Helper()
	g := gatedReady{
		Provider: newLocal(t, local.Options{Name: "a"}),
		waiting:  make(chan struct{}), gate: make(chan struct{}),
	}
	var feats provider.Features
	if secrets {
		feats.Secrets = []string{"a"}
	}
	return newEnvWith(t, func(d *saga.Deps) {
		reg, err := provider.NewRegistry([]provider.Provider{g}, feats)
		if err != nil {
			t.Fatal(err)
		}
		d.Registry, d.Engine = reg, placement.New(reg, d.Tracker, nil)
	}, g), g
}

func applyJobs(t *testing.T, e *env, id uuid.UUID) int {
	t.Helper()
	var n int
	err := e.pool.QueryRow(t.Context(), `select count(*) from river_job
		where kind = 'apply_secrets' and args->>'placement_id' = $1`, id.String()).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestReadyAppliesRowsStoredWhilePlacing(t *testing.T) {
	e, g := x402Env(t, true)
	p := placeStoringX402(t, e, g, true, true)
	eq(t, p.State, store.PlacementReady)
	eq(t, applyJobs(t, e, p.ID), 1)
}

func TestReadyFailsRowsWithoutSecrets(t *testing.T) {
	e, g := x402Env(t, false)
	p := placeStoringX402(t, e, g, false, true)
	eq(t, p.State, store.PlacementReady)
	eq(t, applyJobs(t, e, p.ID), 0)
	rows, err := e.store.X402Services(t.Context(), p.ID)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(rows), 1)
	eq(t, rows[0].State, store.X402Failed)
	eq(t, rows[0].FailureReason, "FEATURE_UNAVAILABLE")
}

func TestReadyWithoutRowsInsertsNoJob(t *testing.T) {
	e, g := x402Env(t, true)
	p := placeStoringX402(t, e, g, true, false)
	eq(t, p.State, store.PlacementReady)
	eq(t, applyJobs(t, e, p.ID), 0)
}
