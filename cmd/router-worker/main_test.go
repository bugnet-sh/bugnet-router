package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/valkey-io/valkey-go"

	"github.com/oynozan/bugnet-router/internal/config"
	"github.com/oynozan/bugnet-router/internal/provider"
	"github.com/oynozan/bugnet-router/internal/providers"
	"github.com/oynozan/bugnet-router/internal/store"
	"github.com/oynozan/bugnet-router/internal/testenv"
	"github.com/oynozan/bugnet-router/internal/vkinfo"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

const packagesJSON = `{"packages": [
	{"id": "small-10m", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10, "price_micros": 50000}
]}`

func b64(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(b)
}

// setup writes the worker's settings file, local provider and no events URL, and sets its secrets
func setup(t *testing.T, databaseURL, prefix, extra string) string {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	packages := filepath.Join(dir, "packages.json")
	if err := os.WriteFile(packages, []byte(packagesJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	valkeyAddr := os.Getenv("BUGNET_TEST_VALKEY_ADDR")
	if valkeyAddr == "" {
		valkeyAddr = "localhost:6379"
	}
	settings := fmt.Sprintf(`order_token_public_key = %q
packages_file = %q
providers = ["local"]
%s
[worker]
listen_addr = "127.0.0.1:0"

[valkey]
addr = %q
prefix = %q

[local]
enabled = true
root = %q
runner = %q
`, base64.StdEncoding.EncodeToString(pub), packages, extra, valkeyAddr, prefix,
		t.TempDir(), filepath.Join(dir, "bugnet-run"))
	path := filepath.Join(dir, "settings.toml")
	if err := os.WriteFile(path, []byte(settings), 0o644); err != nil {
		t.Fatal(err)
	}
	dataPlaneKeys, _ := testenv.DataPlaneKeys(t)
	for k, v := range map[string]string{
		"BUGNET_DATABASE_URL":      databaseURL,
		"BUGNET_INTERNAL_TOKEN":    strings.Repeat("t", 32),
		"BUGNET_HANDLE_KEY":        b64(t, 32),
		"BUGNET_EVENT_SIGNING_KEY": b64(t, ed25519.SeedSize),
		"BUGNET_DATA_PLANE_KEYS":   dataPlaneKeys,
	} {
		t.Setenv(k, v)
	}
	return path
}

func TestRunRequiresEventSigningKey(t *testing.T) {
	path := setup(t, "postgres://localhost:1/unused", "bn:unused:", "")
	t.Setenv("BUGNET_EVENT_SIGNING_KEY", "")
	err := run(discard, "-config", path)
	if err == nil || !strings.Contains(err.Error(), "BUGNET_EVENT_SIGNING_KEY") {
		t.Fatalf("run without a signing key: %v, want an error naming it", err)
	}
}

func TestRunNeedsASettingsFile(t *testing.T) {
	for _, args := range [][]string{
		nil, {"-config"}, {"work"}, {"-config", "a.toml", "extra"},
		{"outbox-redrive", "-config", "a.toml"},
	} {
		if err := run(discard, args...); !errors.Is(err, errUsage) ||
			!strings.Contains(err.Error(), "-config FILE") {
			t.Errorf("run %q gave %v, want the usage", args, err)
		}
	}
	missing := filepath.Join(t.TempDir(), "absent.toml")
	if err := run(discard, "-config", missing); err == nil ||
		!strings.Contains(err.Error(), missing) {
		t.Fatalf("run with a missing settings file gave %v, want an error naming it", err)
	}
}

// fixed is a Valkey memory reader that always answers m
func fixed(m vkinfo.Memory) vkinfo.Reader {
	return func(context.Context, valkey.Client) (vkinfo.Memory, error) { return m, nil }
}

func TestServeRefusesAnEvictingValkey(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{DatabaseURL: "postgres://localhost:1/unused",
		ValkeyAddr: testenv.ValkeyAddr()}
	err = serve(t.Context(), discard, cfg, ln, fixed(vkinfo.Memory{Policy: "allkeys-lru"}))
	want := "valkey: maxmemory-policy is allkeys-lru, the router needs noeviction"
	if err == nil || err.Error() != want {
		t.Fatalf("serve on an evicting Valkey gave %v, want %q", err, want)
	}
}

func get(url string) int {
	resp, err := http.Get(url)
	if err != nil {
		return 0
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

func TestServeAnswersHealthWorksJobsAndDrains(t *testing.T) {
	url := testenv.DatabaseURL(t)
	if err := store.Migrate(t.Context(), url); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	_, prefix := testenv.Valkey(t)
	path := setup(t, url, prefix, `deployment_id = "blue"`)
	cfg, err := config.Load(path, os.LookupEnv)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := providers.Build(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	prov, _ := reg.Get("local")
	orphan := func(deployment string) provider.Ref {
		size, _ := prov.SizeFor(provider.Resources{VCPU: 1})
		ref, err := prov.Create(t.Context(), provider.CreateRequest{
			IdempotencyKey: deployment, Size: size, Lifetime: time.Hour,
			Labels: map[string]string{
				"bugnet": "1", "bugnet_deployment": deployment, "placement": uuid.NewString(),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}
	blue, green := orphan("blue"), orphan("green")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- serve(ctx, discard, cfg, ln, vkinfo.Read) }()

	base := "http://" + ln.Addr().String()
	pool, err := pgxpool.New(t.Context(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	deadline := time.Now().Add(20 * time.Second)
	for {
		var swept int
		err := pool.QueryRow(t.Context(), `select count(*) from river_job
			where kind = 'sweep' and state = 'completed'`).Scan(&swept)
		if err != nil {
			t.Fatal(err)
		}
		if get(base+"/healthz") == http.StatusOK && get(base+"/readyz") == http.StatusOK &&
			get(base+"/monitor") == http.StatusOK && swept > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("healthz %d, readyz %d, monitor %d, %d sweeps completed",
				get(base+"/healthz"), get(base+"/readyz"), get(base+"/monitor"), swept)
		}
		time.Sleep(50 * time.Millisecond)
	}
	left, err := prov.ListOwned(t.Context(), nil)
	if err != nil || len(left) != 1 || left[0].Ref.ID != green.ID {
		t.Fatalf("sandboxes after the sweep %+v, %v, want only green %s and not blue %s", left,
			err, green.ID, blue.ID)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve after cancel: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("serve did not return after cancel")
	}
	if code := get(base + "/healthz"); code != 0 {
		t.Fatalf("healthz answered %d after shutdown", code)
	}
}

func TestRedriveOutboxNeedsOnlyDatabaseURL(t *testing.T) {
	url := testenv.DatabaseURL(t)
	if err := store.Migrate(t.Context(), url); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := pgxpool.New(t.Context(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	_, err = pool.Exec(t.Context(), `insert into outbox (id, event_type, payload, attempts)
		values ($1, 'placement.failed', '{}', $2)`, uuid.New(), store.MaxOutboxAttempts)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"BUGNET_INTERNAL_TOKEN", "BUGNET_EVENT_SIGNING_KEY"} {
		t.Setenv(name, "")
	}
	t.Setenv("BUGNET_DATABASE_URL", url)
	if err := run(discard, "outbox-redrive"); err != nil {
		t.Fatalf("outbox-redrive: %v", err)
	}
	var stuck int
	if err := pool.QueryRow(t.Context(), `select count(*) from outbox where attempts >= $1`,
		store.MaxOutboxAttempts).Scan(&stuck); err != nil || stuck != 0 {
		t.Fatalf("%d stuck events after the redrive, %v, want none", stuck, err)
	}
	if err := run(discard, "nonsense"); !errors.Is(err, errUsage) {
		t.Fatalf("an unknown argument gave %v, want the usage", err)
	}
}
