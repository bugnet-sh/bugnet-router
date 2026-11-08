// Command router-worker works the router's River jobs for placement deadlines, the watch, the
// sweep and the outbox, and with the outbox-redrive argument sends stuck outbox events at once.
//
// router-worker -config FILE builds every service from the settings in FILE and the secrets in
// the environment, starts the River client and answers /healthz, /readyz and /monitor over HTTP.
// On SIGTERM or SIGINT it stops fetching jobs, gives running ones a bounded time before
// cancelling them, waits a bounded time for the saga, and exits.
package main

import (
	"context"
	"errors"
	"flag"
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

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/edgeevents"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/providers"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/vkclient"
	"github.com/bugnet-sh/bugnet-router/internal/vkinfo"
	"github.com/bugnet-sh/bugnet-router/internal/worker"
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

var errUsage = errors.New("usage: router-worker -config FILE, or router-worker outbox-redrive")

func run(log *slog.Logger, args ...string) error {
	redrive := len(args) == 1 && args[0] == "outbox-redrive"
	var path string
	if !redrive {
		fs := flag.NewFlagSet("router-worker", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		fs.StringVar(&path, "config", "", "settings file")
		if err := fs.Parse(args); err != nil || fs.NArg() > 0 || path == "" {
			return errUsage
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	// A second signal during shutdown kills the process at once
	context.AfterFunc(ctx, stop)
	if redrive {
		return redriveOutbox(ctx, log)
	}
	cfg, err := config.Load(path, os.LookupEnv)
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
	return serve(ctx, log, cfg, ln, vkinfo.Read)
}

// redriveOutbox reads only BUGNET_DATABASE_URL and gives stuck events their attempts back
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
	log.Info("stuck outbox events will be sent at once", "events", n)
	return nil
}

// openStore returns the store, sealing provider credentials under the data-plane keys
func openStore(pool *pgxpool.Pool, cfg config.Config, log *slog.Logger) *store.Store {
	return store.New(pool, store.WithRefSealer(cfg.DataPlaneKeys), store.WithLogger(log))
}

func serve(
	ctx context.Context, log *slog.Logger, cfg config.Config, ln net.Listener, read vkinfo.Reader,
) error {
	defer func() { _ = ln.Close() }()
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("open postgres pool: %w", err)
	}
	defer pool.Close()
	vk, err := vkclient.New(cfg.ValkeyAddr)
	if err != nil {
		return fmt.Errorf("connect to valkey: %w", err)
	}
	defer vk.Close()
	if err := vkinfo.CheckStart(ctx, log, vk, read); err != nil {
		return err
	}
	cat, err := catalog.Load(cfg.PackagesFile)
	if err != nil {
		return err
	}
	reg, err := providers.Build(cfg, vk)
	if err != nil {
		return err
	}

	st := openStore(pool, cfg, log)
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
		Events:       edgeevents.New(vk, cfg.ValkeyPrefix, log),
		Budget:       cfg.EdgeBudget,
		Keys:         cfg.DataPlaneKeys,
	}, worker.Deps{
		EventsURL: cfg.EventsURL, SigningKey: cfg.EventSigningKey,
		DataplaneWorkers: cfg.DataplaneWorkers,
	})
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
	mux.HandleFunc("GET /monitor", monitor(log, st, vk, read, cfg.Hardening.PoolList))
	hs := &http.Server{Handler: mux, ReadHeaderTimeout: readHeaderTimeout}
	failed := make(chan error, 1)
	go func() { failed <- hs.Serve(ln) }()
	log.Info("working", "addr", ln.Addr().String(), "replica", cfg.ReplicaID,
		"providers", cfg.Providers, "dataplane_workers", w.DataplaneWorkers())

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
