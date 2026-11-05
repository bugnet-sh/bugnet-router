package main

import (
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/oynozan/bugnet-router/internal/config"
	"github.com/oynozan/bugnet-router/internal/jobs"
	"github.com/oynozan/bugnet-router/internal/testenv"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

// serveOnly are the variables serving requires and migrating does not
var serveOnly = []string{
	"BUGNET_INTERNAL_TOKEN", "BUGNET_ORDER_TOKEN_PUBLIC_KEY", "BUGNET_PACKAGES_FILE",
	"BUGNET_HANDLE_KEY",
}

func TestMigrateNeedsOnlyDatabaseURL(t *testing.T) {
	for _, name := range serveOnly {
		t.Setenv(name, "")
	}
	url := testenv.DatabaseURL(t)
	t.Setenv("BUGNET_DATABASE_URL", url)
	if err := run(discard, []string{"migrate"}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := pgxpool.New(t.Context(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for _, table := range []string{"placements", "river_job"} {
		var n int
		if err := pool.QueryRow(t.Context(), "select count(*) from "+table).Scan(&n); err != nil {
			t.Fatalf("table %s after migrate: %v", table, err)
		}
	}

	err = run(discard, nil)
	if err == nil || !strings.Contains(err.Error(), "BUGNET_INTERNAL_TOKEN is required") {
		t.Fatalf("serve with only the database url: %v, want the full configuration", err)
	}
}

func TestMigrateWithoutDatabaseURL(t *testing.T) {
	t.Setenv("BUGNET_DATABASE_URL", "")
	err := run(discard, []string{"migrate"})
	if err == nil || !strings.Contains(err.Error(), "BUGNET_DATABASE_URL is required") {
		t.Fatalf("migrate without a database url: %v, want an error naming it", err)
	}
}

func TestJobDepsCarryTheOutputLimits(t *testing.T) {
	d := jobDeps(config.Config{
		ReplicaID: "api-a", JobOutputBudget: 1 << 20, PlacementOutputBudget: 4 << 20,
		PlacementTailReserve: 512 << 10, MaxRunningJobs: 3,
	}, nil, nil, nil, discard)
	want := jobs.Deps{
		ReplicaID: "api-a", Log: discard, JobBudget: 1 << 20, PlacementBudget: 4 << 20,
		TailReserve: 512 << 10, MaxRunning: 3,
	}
	if d != want {
		t.Fatalf("jobs deps %+v, want %+v", d, want)
	}
}
