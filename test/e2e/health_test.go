package e2e

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/bugnet-sh/bugnet-router/internal/ordertoken"
)

const downWait = 240 * time.Second

// breakLocalCreates makes every create of the local provider fail, since a file stands where its
// staging directory would go
func breakLocalCreates(t *testing.T, s *stack) {
	t.Helper()
	path := filepath.Join(s.root, ".staging")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestE2ECanaryMarksAFailingProviderDown(t *testing.T) {
	t.Parallel()
	s := startStackWith(t, stackOptions{
		health: `canary_every = "1s"
canary_every_down = "1s"
window = "15m"`,
		beforeWorker: breakLocalCreates,
	})
	c := s.client(t, s.a)
	pool, err := pgxpool.New(t.Context(), s.db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	// The canary tick runs at start and every minute, so two failed canaries take about one
	deadline := time.Now().Add(downWait)
	for {
		var state string
		err := pool.QueryRow(t.Context(),
			`select state from provider_health where provider = 'local'`).Scan(&state)
		if err == nil && state == "down" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("local is %q (%v) after %s, want down", state, err, downWait)
		}
		time.Sleep(2 * time.Second)
	}
	var failed int
	err = pool.QueryRow(t.Context(), `select count(*) from start_samples
		where provider = 'local' and kind = 'canary' and outcome = 'failed'`).Scan(&failed)
	if err != nil || failed < 2 {
		t.Fatalf("%d failed canary samples (%v), want at least 2", failed, err)
	}

	// The replicas read the row within 10 s, then the placement finds no candidate at all
	deadline = time.Now().Add(30 * time.Second)
	for {
		token, err := ordertoken.Sign(s.priv, ordertoken.Claims{
			OrderID: "e2e-" + uuid.NewString(), Kind: ordertoken.KindStart, PackageID: smallPkg,
			AmountMicros: smallPrice, ExpiresAt: time.Now().Add(10 * time.Minute),
		})
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.CreatePlacement(t.Context(), connect.NewRequest(
			&routerv1.CreatePlacementRequest{OrderToken: token}))
		if connect.CodeOf(err) == connect.CodeResourceExhausted {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("CreatePlacement answered %v, want no candidate while local is down", err)
		}
		time.Sleep(2 * time.Second)
	}
}

// The canary of a healthy stack creates a sandbox, removes it and leaves a sample
func TestE2ECanaryLeavesASample(t *testing.T) {
	t.Parallel()
	s := startStackWith(t, stackOptions{health: `canary_every = "1s"`})
	pool, err := pgxpool.New(t.Context(), s.db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	deadline := time.Now().Add(30 * time.Second)
	for {
		var n int
		err := pool.QueryRow(t.Context(), `select count(*) from start_samples
			where provider = 'local' and kind = 'canary' and outcome = 'ok'
				and destroy_ms is not null and sandbox_id <> ''`).Scan(&n)
		if err == nil && n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no ok canary sample after 30 s (%v)", err)
		}
		time.Sleep(time.Second)
	}
}
