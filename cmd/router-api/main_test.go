package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/valkey-io/valkey-go"

	"github.com/oynozan/bugnet-router/internal/api"
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

func TestTokensComeFromTheSettings(t *testing.T) {
	got := tokens(config.Config{
		InternalToken: "i", EdgeToken: "e", SecretsToken: "s", SSHToken: "h", EgressToken: "g",
		EgressFetchToken: "f", OperatorToken: "o", PaymentsToken: "p",
	})
	want := api.Tokens{
		Internal: "i", Edge: "e", Secrets: "s", SSH: "h", Egress: "g", Operator: "o",
		Payments: "p",
	}
	if got != want {
		t.Fatalf("tokens %+v, want %+v", got, want)
	}
}

func TestListenSharesThePort(t *testing.T) {
	a, err := listen("127.0.0.1:0", true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()
	b, err := listen(a.Addr().String(), true)
	if err != nil {
		t.Fatalf("a second listener on the same port: %v", err)
	}
	_ = b.Close()
}

func TestListenWithoutReusePortKeepsThePort(t *testing.T) {
	a, err := listen("127.0.0.1:0", false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()
	for _, reuse := range []bool{false, true} {
		if b, err := listen(a.Addr().String(), reuse); err == nil {
			_ = b.Close()
			t.Fatalf("a second listener with reuse_port %v bound a port taken without it", reuse)
		}
	}
}

type closeFunc func()

func (f closeFunc) Close() { f() }

type handoffFunc func()

func (f handoffFunc) Shutdown(context.Context) { f() }

type placed struct{}

func (placed) Wait() {}

// transferServer serves /transfer, which answers once release closes or the request ends
func transferServer(t *testing.T, release <-chan struct{}) (*http.Server, string, <-chan struct{}) {
	t.Helper()
	ln, err := listen("127.0.0.1:0", false)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/transfer", func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		select {
		case <-release:
			_, _ = io.WriteString(w, "done")
		case <-r.Context().Done():
		}
	})
	hs := &http.Server{Handler: mux}
	go func() { _ = hs.Serve(ln) }()
	return hs, "http://" + ln.Addr().String(), entered
}

func TestShutdownDrainsTransferStreams(t *testing.T) {
	release := make(chan struct{})
	hs, url, entered := transferServer(t, release)
	got := make(chan string, 1)
	go func() {
		resp, err := http.Get(url + "/transfer")
		if err != nil {
			got <- err.Error()
			return
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		got <- string(b)
	}()
	<-entered
	var handed, closedAt atomic.Int64
	done := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(done)
		shutdown(discard, hs, closeFunc(func() { closedAt.Store(int64(time.Since(start))) }),
			handoffFunc(func() { handed.Store(int64(time.Since(start))) }), placed{},
			time.Minute)
	}()
	time.Sleep(300 * time.Millisecond)
	if handed.Load() == 0 || time.Duration(handed.Load()) > 200*time.Millisecond {
		t.Fatalf("jobs were handed over after %v, want at once", time.Duration(handed.Load()))
	}
	if _, err := http.Get(url + "/transfer"); err == nil {
		t.Fatal("a new request reached a server that was shutting down")
	}
	if closedAt.Load() != 0 {
		t.Fatal("the API server's streams were closed while a transfer stream was open")
	}
	close(release)
	if body := <-got; body != "done" {
		t.Fatalf("the open transfer stream ended with %q, want it to finish", body)
	}
	<-done
	if closedAt.Load() == 0 {
		t.Fatal("the API server's streams were never closed")
	}
}

func TestShutdownCutsTransferStreamsAtTheDrainsEnd(t *testing.T) {
	hs, url, entered := transferServer(t, nil)
	failed := make(chan error, 1)
	go func() {
		resp, err := http.Get(url + "/transfer")
		if err == nil {
			_, err = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
		}
		failed <- err
	}()
	<-entered
	var closed atomic.Bool
	start := time.Now()
	shutdown(discard, hs, closeFunc(func() { closed.Store(true) }), handoffFunc(func() {}),
		placed{}, 300*time.Millisecond)
	if took := time.Since(start); took < 300*time.Millisecond || took > 2*time.Second {
		t.Fatalf("shutdown took %v, want the 300 ms drain", took)
	}
	if !closed.Load() {
		t.Fatal("the API server's streams were not closed at the drain's end")
	}
	select {
	case <-failed:
	case <-time.After(2 * time.Second):
		t.Fatal("a transfer stream outlived the drain")
	}
}
