package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/oynozan/bugnet-router/internal/config"
	"github.com/oynozan/bugnet-router/internal/store"
	"github.com/oynozan/bugnet-router/internal/testenv"
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

// setEnv sets every variable the worker needs, with the local provider and no events URL
func setEnv(t *testing.T, databaseURL, prefix string) {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	packages := filepath.Join(t.TempDir(), "packages.json")
	if err := os.WriteFile(packages, []byte(packagesJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	valkeyAddr := os.Getenv("BUGNET_TEST_VALKEY_ADDR")
	if valkeyAddr == "" {
		valkeyAddr = "localhost:6379"
	}
	for k, v := range map[string]string{
		"BUGNET_DATABASE_URL":           databaseURL,
		"BUGNET_INTERNAL_TOKEN":         strings.Repeat("t", 32),
		"BUGNET_ORDER_TOKEN_PUBLIC_KEY": base64.StdEncoding.EncodeToString(pub),
		"BUGNET_PACKAGES_FILE":          packages,
		"BUGNET_HANDLE_KEY":             b64(t, 32),
		"BUGNET_EVENT_SIGNING_KEY":      b64(t, ed25519.SeedSize),
		"BUGNET_EVENTS_URL":             "",
		"BUGNET_VALKEY_ADDR":            valkeyAddr,
		"BUGNET_VALKEY_PREFIX":          prefix,
		"BUGNET_PROVIDERS":              "local",
		"BUGNET_ENABLE_LOCAL_PROVIDER":  "1",
		"BUGNET_LOCAL_ROOT":             t.TempDir(),
		"BUGNET_LOCAL_RUNNER":           filepath.Join(t.TempDir(), "bugnet-run"),
		"BUGNET_WORKER_LISTEN_ADDR":     "127.0.0.1:0",
	} {
		t.Setenv(k, v)
	}
}

func TestRunRequiresEventSigningKey(t *testing.T) {
	setEnv(t, "postgres://localhost:1/unused", "bn:unused:")
	t.Setenv("BUGNET_EVENT_SIGNING_KEY", "")
	err := run(discard)
	if err == nil || !strings.Contains(err.Error(), "BUGNET_EVENT_SIGNING_KEY") {
		t.Fatalf("run without a signing key: %v, want an error naming it", err)
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
	setEnv(t, url, prefix)
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- serve(ctx, discard, cfg, ln) }()

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
			swept > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("healthz %d, readyz %d, %d sweeps completed", get(base+"/healthz"),
				get(base+"/readyz"), swept)
		}
		time.Sleep(50 * time.Millisecond)
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
