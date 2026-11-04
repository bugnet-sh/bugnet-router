// Command router-api serves the Sandbox Router API, and with the migrate argument migrates the
// database and exits.
//
// Serving builds every service from the environment, runs the jobs scanner that takes over
// commands from replicas that went away, and answers Connect calls over HTTP/1.1 and unencrypted
// HTTP/2. On SIGTERM or SIGINT it stops accepting requests, ends long polls and streams, hands
// its jobs to peers, waits a bounded time for placements still being placed, and exits.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/valkey-io/valkey-go"

	"github.com/oynozan/bugnet-router/internal/api"
	"github.com/oynozan/bugnet-router/internal/catalog"
	"github.com/oynozan/bugnet-router/internal/config"
	"github.com/oynozan/bugnet-router/internal/jobs"
	"github.com/oynozan/bugnet-router/internal/placement"
	"github.com/oynozan/bugnet-router/internal/providers"
	"github.com/oynozan/bugnet-router/internal/saga"
	"github.com/oynozan/bugnet-router/internal/store"
	"github.com/oynozan/bugnet-router/internal/streams"
)

const (
	shutdownTimeout   = 20 * time.Second
	readHeaderTimeout = 10 * time.Second
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	slog.SetDefault(log)
	if err := run(log, os.Args[1:]); err != nil {
		log.Error("router-api failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, args []string) error {
	migrate := len(args) == 1 && args[0] == "migrate"
	if len(args) > 0 && !migrate {
		return errors.New("usage: router-api [migrate]")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	// A second signal during shutdown kills the process at once
	context.AfterFunc(ctx, stop)
	if migrate {
		return migrateDatabase(ctx, log)
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	return serve(ctx, log, cfg)
}

// migrateDatabase reads only BUGNET_DATABASE_URL, so a migration job needs no serving settings
func migrateDatabase(ctx context.Context, log *slog.Logger) error {
	url, err := config.DatabaseURL()
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	if err := store.Migrate(ctx, url); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	log.Info("migrations applied")
	return nil
}

func serve(ctx context.Context, log *slog.Logger, cfg config.Config) error {
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("open postgres pool: %w", err)
	}
	defer pool.Close()
	vk, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{cfg.ValkeyAddr}})
	if err != nil {
		return fmt.Errorf("connect to valkey: %w", err)
	}
	defer vk.Close()
	cat, err := catalog.Load(cfg.PackagesFile)
	if err != nil {
		return err
	}
	reg, err := providers.Build(cfg)
	if err != nil {
		return err
	}
	// A provider without its runner build would bill for sandboxes that can never run a job
	runners, err := providers.Runners(cfg, reg)
	if err != nil {
		return err
	}
	// Insert only, the worker runs the jobs
	rc, err := river.NewClient(riverpgxv5.New(pool), &river.Config{})
	if err != nil {
		return fmt.Errorf("river client: %w", err)
	}

	st := store.New(pool)
	tracker := placement.NewTracker(time.Now)
	engine := placement.New(reg, tracker)
	sg := saga.New(saga.Deps{
		Store:        st,
		Engine:       engine,
		Registry:     reg,
		Catalog:      cat,
		River:        rc,
		Valkey:       vk,
		Prefix:       cfg.ValkeyPrefix,
		OrderKey:     cfg.OrderTokenPublicKey,
		HandleKey:    cfg.HandleKey,
		MarginFloor:  cfg.MarginFloor,
		Tracker:      tracker,
		ReplicaID:    cfg.ReplicaID,
		DeploymentID: cfg.DeploymentID,
		Runners:      runners,
		Log:          log,
	})
	js := jobs.New(jobs.Deps{
		Store:     st,
		Bus:       streams.NewBus(vk, cfg.ValkeyPrefix),
		Registry:  reg,
		ReplicaID: cfg.ReplicaID,
		Log:       log,
	})
	var scanner sync.WaitGroup
	// The scanner runs until jobs.Shutdown, so jobs keep being taken over while HTTP drains
	scanner.Go(func() { js.RunScanner(context.Background()) })
	srv := api.New(api.Deps{
		Store:       st,
		Saga:        sg,
		Jobs:        js,
		Engine:      engine,
		Catalog:     cat,
		Registry:    reg,
		Valkey:      vk,
		Prefix:      cfg.ValkeyPrefix,
		MarginFloor: cfg.MarginFloor,
		Log:         log,
	})
	defer srv.Close()

	mux := http.NewServeMux()
	api.Mount(mux, srv, cfg.InternalToken)
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	hs := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           mux,
		Protocols:         &protocols,
		ReadHeaderTimeout: readHeaderTimeout,
	}
	hs.RegisterOnShutdown(srv.Close)
	failed := make(chan error, 1)
	go func() { failed <- hs.ListenAndServe() }()
	log.Info("serving", "addr", cfg.ListenAddr, "replica", cfg.ReplicaID,
		"providers", cfg.Providers)

	var serveErr error
	select {
	case <-ctx.Done():
		log.Info("shutting down")
	case err := <-failed:
		serveErr = fmt.Errorf("serve: %w", err)
	}
	shutdown(log, hs, js, sg)
	scanner.Wait()
	return serveErr
}

// shutdown drains HTTP, hands running jobs to peers and waits a bounded time for placing
func shutdown(log *slog.Logger, hs *http.Server, js *jobs.Service, sg *saga.Saga) {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := hs.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Warn("http shutdown", "err", err)
		_ = hs.Close()
	}

	jctx, jcancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer jcancel()
	js.Shutdown(jctx)

	placed := make(chan struct{})
	go func() {
		sg.Wait()
		close(placed)
	}()
	select {
	case <-placed:
	case <-time.After(shutdownTimeout):
		log.Warn("placements were still being placed at exit")
	}
}
