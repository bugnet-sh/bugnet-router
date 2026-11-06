package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/valkey-io/valkey-go"

	"github.com/oynozan/bugnet-router/internal/config"
	"github.com/oynozan/bugnet-router/internal/jobs"
	"github.com/oynozan/bugnet-router/internal/testenv"
	"github.com/oynozan/bugnet-router/internal/vkinfo"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

// serveOnly are the secrets serving requires and migrating does not
var serveOnly = []string{"BUGNET_INTERNAL_TOKEN", "BUGNET_HANDLE_KEY", "BUGNET_DATA_PLANE_KEYS"}

// settingsFile writes a settings file router-api can serve with
func settingsFile(t *testing.T) string {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "settings.toml")
	content := `order_token_public_key = "` + base64.StdEncoding.EncodeToString(pub) + `"
packages_file = "packages.json"
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
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

	err = run(discard, []string{"-config", settingsFile(t)})
	if err == nil || !strings.Contains(err.Error(), "BUGNET_INTERNAL_TOKEN is required") {
		t.Fatalf("serve with only the database url: %v, want the full configuration", err)
	}
}

func TestServeNeedsASettingsFile(t *testing.T) {
	for _, args := range [][]string{
		nil, {"-config"}, {"-config", ""}, {"serve"}, {"-config", "a.toml", "extra"},
		{"migrate", "-config", "a.toml"}, {"-listen", ":8080"},
	} {
		if err := run(discard, args); !errors.Is(err, errUsage) ||
			!strings.Contains(err.Error(), "-config FILE") {
			t.Errorf("run %q gave %v, want the usage", args, err)
		}
	}
	missing := filepath.Join(t.TempDir(), "absent.toml")
	if err := run(discard, []string{"--config", missing}); err == nil ||
		!strings.Contains(err.Error(), missing) {
		t.Fatalf("run with a missing settings file gave %v, want an error naming it", err)
	}
}

func TestServeRefusesAnEvictingValkey(t *testing.T) {
	cfg := config.Config{DatabaseURL: "postgres://localhost:1/unused",
		ValkeyAddr: testenv.ValkeyAddr()}
	read := func(context.Context, valkey.Client) (vkinfo.Memory, error) {
		return vkinfo.Memory{Max: 1 << 30, Policy: "volatile-ttl"}, nil
	}
	err := serve(t.Context(), discard, cfg, read)
	want := "valkey: maxmemory-policy is volatile-ttl, the router needs noeviction"
	if err == nil || err.Error() != want {
		t.Fatalf("serve on an evicting Valkey gave %v, want %q", err, want)
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
