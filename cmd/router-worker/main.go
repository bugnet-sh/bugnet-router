// Command router-worker works the router's River jobs for placement deadlines, the watch, the
// sweep and the outbox, and with the outbox-redrive argument sends dead outbox events again.
//
// It builds every service from the environment, starts the River client and answers /healthz
// and /readyz over HTTP. On SIGTERM or SIGINT it stops fetching jobs, gives running ones a
// bounded time before cancelling them, waits a bounded time for the saga, and exits.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/valkey-io/valkey-go"

	"github.com/oynozan/bugnet-router/internal/catalog"
	"github.com/oynozan/bugnet-router/internal/config"
	"github.com/oynozan/bugnet-router/internal/placement"
	"github.com/oynozan/bugnet-router/internal/providers"
	"github.com/oynozan/bugnet-router/internal/saga"
	"github.com/oynozan/bugnet-router/internal/store"
	"github.com/oynozan/bugnet-router/internal/worker"
)

const (
	shutdownTimeout   = 20 * time.Second
	cancelTimeout     = 10 * time.Second
	httpStopTimeout   = 5 * time.Second
	readHeaderTimeout = 10 * time.Second
	readyTimeout      = 2 * time.Second
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	slog.SetDefault(log)
	if err := run(log, os.Args[1:]...); err != nil {
		log.Error("router-worker failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, args ...string) error {
	redrive := len(args) == 1 && args[0] == "outbox-redrive"
	if len(args) > 0 && !redrive {
		return errors.New("usage: router-worker [outbox-redrive]")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	// A second signal during shutdown kills the process at once
	context.AfterFunc(ctx, stop)
	if redrive {
		return redriveOutbox(ctx, log)
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if cfg.EventSigningKey == nil {
		return errors.New("config: BUGNET_EVENT_SIGNING_KEY is required by the worker")
	}
	ln, err := net.Listen("tcp", cfg.WorkerListenAddr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	return serve(ctx, log, cfg, ln)
}

// redriveOutbox reads only BUGNET_DATABASE_URL and gives dead events their attempts back
func redriveOutbox(ctx context.Context, log *slog.Logger) error {
	url, err := config.DatabaseURL()
	if err != nil {
		return fmt.Errorf("outbox-redrive: %w", err)
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return fmt.Errorf("outbox-redrive: open postgres pool: %w", err)
	}
	defer pool.Close()
	n, err := store.New(pool).RedriveOutbox(ctx)
	if err != nil {
		return fmt.Errorf("outbox-redrive: %w", err)
	}
	log.Info("dead outbox events will be sent again", "events", n)
	return nil
}

func serve(ctx context.Context, log *slog.Logger, cfg config.Config, ln net.Listener) error {
	defer func() { _ = ln.Close() }()
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

	st := store.New(pool)
	tracker := placement.NewTracker(time.Now)
	w, err := worker.New(pool, saga.Deps{
		Store:        st,
		Engine:       placement.New(reg, tracker),
		Registry:     reg,
		Catalog:      cat,
		Valkey:       vk,
		Prefix:       cfg.ValkeyPrefix,
		OrderKey:     cfg.OrderTokenPublicKey,
		HandleKey:    cfg.HandleKey,
		MarginFloor:  cfg.MarginFloor,
		Tracker:      tracker,
		ReplicaID:    cfg.ReplicaID,
		DeploymentID: cfg.DeploymentID,
		Log:          log,
	}, worker.Deps{EventsURL: cfg.EventsURL, SigningKey: cfg.EventSigningKey})
	if err != nil {
		return err
	}
	// Start gets its own context because cancelling it would skip the soft stop
	if err := w.River().Start(context.WithoutCancel(ctx)); err != nil {
		return fmt.Errorf("start river: %w", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(rw http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(rw, "ok\n")
	})
	mux.HandleFunc("GET /readyz", readyz(log, st, vk))
	hs := &http.Server{Handler: mux, ReadHeaderTimeout: readHeaderTimeout}
	failed := make(chan error, 1)
	go func() { failed <- hs.Serve(ln) }()
	log.Info("working", "addr", ln.Addr().String(), "replica", cfg.ReplicaID,
		"providers", cfg.Providers)

	var serveErr error
	select {
	case <-ctx.Done():
		log.Info("shutting down")
	case err := <-failed:
		serveErr = fmt.Errorf("serve: %w", err)
	}
	shutdown(log, w)
	hctx, cancel := context.WithTimeout(context.Background(), httpStopTimeout)
	defer cancel()
	if err := hs.Shutdown(hctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Warn("http shutdown", "err", err)
	}
	return serveErr
}

// shutdown lets running jobs finish for a bounded time, then cancels them and waits for the saga
func shutdown(log *slog.Logger, w *worker.Worker) {
	sctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := w.River().Stop(sctx); err != nil {
		log.Warn("jobs still running at the stop deadline, cancelling them", "err", err)
		cctx, ccancel := context.WithTimeout(context.Background(), cancelTimeout)
		defer ccancel()
		if err := w.River().StopAndCancel(cctx); err != nil {
			log.Warn("river did not stop", "err", err)
		}
	}

	done := make(chan struct{})
	go func() {
		w.Saga().Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(shutdownTimeout):
		log.Warn("saga work was still running at exit")
	}
}

// readyz reports whether Postgres and Valkey both answer
func readyz(log *slog.Logger, st *store.Store, vk valkey.Client) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
		defer cancel()
		checks := []struct {
			name string
			ping func(context.Context) error
		}{
			{"postgres", st.Ping},
			{"valkey", func(ctx context.Context) error {
				return vk.Do(ctx, vk.B().Ping().Build()).Error()
			}},
		}
		for _, c := range checks {
			if err := c.ping(ctx); err != nil {
				log.Warn("not ready", "dependency", c.name, "err", err)
				http.Error(rw, c.name+" is unreachable", http.StatusServiceUnavailable)
				return
			}
		}
		_, _ = io.WriteString(rw, "ready\n")
	}
}
