// Command router-api serves the Sandbox Router API, and with the migrate argument migrates the
// database and exits.
//
// router-api -config FILE serves with the settings in FILE and the secrets in the environment.
// Serving builds every service from them, runs the jobs scanner that takes over commands from
// replicas that went away, and answers Connect calls over HTTP/1.1 and unencrypted HTTP/2 on a
// listener bound with SO_REUSEPORT when api.reuse_port is set, so a new process binds beside it.
// With stats.listen_addr set it also serves the public stats feed on that loopback listener.
// On SIGTERM or SIGINT it stops accepting requests, ends long polls and watch streams and hands
// its jobs to peers at once. It lets its transfer streams finish for up to transfers.max_duration,
// cuts what is left, waits a bounded time for placements still being placed, and exits.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	stdlog "log"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/valkey-io/valkey-go"
	"golang.org/x/sys/unix"

	"github.com/bugnet-sh/bugnet-router/internal/api"
	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/edgeevents"
	"github.com/bugnet-sh/bugnet-router/internal/health"
	"github.com/bugnet-sh/bugnet-router/internal/hostrules"
	"github.com/bugnet-sh/bugnet-router/internal/jobs"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	poolpkg "github.com/bugnet-sh/bugnet-router/internal/pool"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/providers"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/secrets"
	"github.com/bugnet-sh/bugnet-router/internal/sessions"
	"github.com/bugnet-sh/bugnet-router/internal/stats"
	"github.com/bugnet-sh/bugnet-router/internal/store"
	"github.com/bugnet-sh/bugnet-router/internal/streams"
	"github.com/bugnet-sh/bugnet-router/internal/telemetry"
	"github.com/bugnet-sh/bugnet-router/internal/vkclient"
	"github.com/bugnet-sh/bugnet-router/internal/vkinfo"
)

const (
	shutdownTimeout   = 20 * time.Second
	readHeaderTimeout = 10 * time.Second
)

func main() {
	log := slog.New(telemetry.LogHandler(slog.NewJSONHandler(os.Stderr, nil)))
	slog.SetDefault(log)
	if err := run(log, os.Args[1:]); err != nil {
		log.Error("router-api failed", "err", err)
		os.Exit(1)
	}
}

var errUsage = errors.New("usage: router-api -config FILE, or router-api migrate")

func run(log *slog.Logger, args []string) error {
	migrate := len(args) == 1 && args[0] == "migrate"
	var path string
	if !migrate {
		fs := flag.NewFlagSet("router-api", flag.ContinueOnError)
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
	if migrate {
		return migrateDatabase(ctx, log)
	}
	cfg, err := config.Load(path, os.LookupEnv)
	if err != nil {
		return err
	}
	return serve(ctx, log, cfg, vkinfo.Read)
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

// openStore returns the store, sealing provider credentials under the data-plane keys
func openStore(pool *pgxpool.Pool, cfg config.Config, log *slog.Logger) *store.Store {
	return store.New(pool, store.WithRefSealer(cfg.DataPlaneKeys), store.WithLogger(log),
		store.WithSessionMaxEvents(cfg.Sessions.MaxEvents),
		store.WithSessionMaxJobs(cfg.Sessions.MaxJobs),
		store.WithReserveSlots(cfg.LimitsPoll.ReserveSlots))
}

func serve(ctx context.Context, log *slog.Logger, cfg config.Config, read vkinfo.Reader) error {
	if err := checkWallets(log, cfg); err != nil {
		return err
	}
	boot, err := bootSettings(log, cfg)
	if err != nil {
		return err
	}
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
	if err := checkRestrictedEgress(cfg, cat); err != nil {
		return err
	}
	tel, err := telemetry.FromConfig(cfg, "router-api", log)
	if err != nil {
		return err
	}
	defer tel.Close()
	reg, err := providers.Build(cfg, vk, tel)
	if err != nil {
		return err
	}
	if err := api.CheckHooks(cfg.Hooks, cfg.HookSecrets, reg); err != nil {
		return err
	}
	if err := checkPools(reg, cfg); err != nil {
		return err
	}
	if err := checkImages(reg, cfg); err != nil {
		return err
	}
	if err := checkBoxdRefs(reg, cfg, cat); err != nil {
		return err
	}
	// A provider without its runner build would bill for sandboxes that can never run a job
	runners, err := providers.Runners(cfg, reg)
	if err != nil {
		return err
	}
	// Insert only, the worker runs the jobs
	rc, err := river.NewClient(riverpgxv5.New(pool),
		&river.Config{Middleware: tel.Middleware(false)})
	if err != nil {
		return fmt.Errorf("river client: %w", err)
	}

	events := edgeevents.New(vk, cfg.ValkeyPrefix, log)
	sources, err := secrets.NewSources(secrets.SourcesDeps{
		Settings: secrets.SourceSettingsOf(cfg), Registry: reg, Store: openStore(pool, cfg, log),
		Valkey: vk, Prefix: cfg.ValkeyPrefix, Events: events, River: rc, Log: log,
	})
	if err != nil {
		return err
	}

	// The live view's own client, so a crowd of page viewers never starves tool calls
	var liveVK valkey.Client
	if cfg.Sessions.Live.Enabled {
		if liveVK, err = liveClient(cfg); err != nil {
			return fmt.Errorf("connect to valkey for the live view: %w", err)
		}
		defer liveVK.Close()
	}

	st := openStore(pool, cfg, log)
	writer := sessions.NewWriter(st, cfg.DataPlaneKeys, log)
	tracker := placement.NewTracker(time.Now)
	warmth, warm := poolWarmth(cfg, reg, st)
	shared := health.NewShared(st, tracker, log)
	engine := placement.New(reg, shared, warm)
	// Insert only, so it serves ListPools and DrainPool from Postgres alone
	pools, err := poolpkg.New(poolpkg.Deps{
		Store: st, Registry: reg, River: rc, Settings: cfg.Pools, Replica: cfg.ReplicaID, Log: log,
	})
	if err != nil {
		return err
	}
	sg := saga.New(saga.Deps{
		Telemetry:     tel,
		Store:         st,
		Engine:        engine,
		Registry:      reg,
		Catalog:       cat,
		River:         rc,
		Valkey:        vk,
		Prefix:        cfg.ValkeyPrefix,
		OrderKey:      cfg.OrderTokenPublicKey,
		HandleKey:     cfg.HandleKey,
		MarginFloor:   cfg.MarginFloor,
		Tracker:       tracker,
		ReplicaID:     cfg.ReplicaID,
		DeploymentID:  cfg.DeploymentID,
		Runners:       runners,
		Log:           log,
		Events:        events,
		Budget:        cfg.EdgeBudget,
		Keys:          cfg.DataPlaneKeys,
		Boot:          boot,
		SSH:           saga.SSHSettingsOf(cfg),
		Wallets:       cfg.Wallets,
		Sessions:      writer,
		SessionKeep:   cfg.Sessions.Keep,
		StatsMaxBytes: cfg.Estimator.StatsMaxBytes,
		// The hold of D-b (a), for abuse and operator ends
		SessionAbuseHold: cfg.Sessions.AbuseHold,
		CreatesPerSecond: cfg.CreatesPerSecond,
		Pools:            warmth,
		WarmFirst:        cfg.Pools.Order == config.OrderWarmFirst,
		ImageAccept:      cfg.Images.Accept,
		ImageChannel:     cfg.Images.Channel,
		PoolRoom:         cfg.Pools.ExtensionRoom,
		Reserve:          cfg.Reserve,
		ReserveShare:     cfg.Pools.ReserveShare,
		Hedger: saga.NewHedger(saga.HedgerDeps{
			Settings: cfg.Hedge, Health: shared, Valkey: vk, Prefix: cfg.ValkeyPrefix,
			CreatesPerSecond: cfg.CreatesPerSecond, MarginFloor: cfg.MarginFloor,
		}),
	})
	est, err := buildEstimator(ctx, log, cfg, st, vk, cat, reg)
	if err != nil {
		return err
	}
	jd := jobDeps(cfg, st, streams.NewBus(vk, cfg.ValkeyPrefix), reg, log)
	jd.Sessions = writer
	jd.Telemetry = tel
	js := jobs.New(jd)
	var scanner sync.WaitGroup
	// The scanner runs until jobs.Shutdown, so jobs keep being taken over while HTTP drains
	scanner.Go(func() { js.RunScanner(context.Background()) })
	srv := api.New(api.Deps{
		Telemetry:           tel,
		Store:               st,
		Saga:                sg,
		Jobs:                js,
		Engine:              engine,
		Catalog:             cat,
		Registry:            reg,
		Valkey:              vk,
		Prefix:              cfg.ValkeyPrefix,
		MarginFloor:         cfg.MarginFloor,
		Log:                 log,
		SandboxOpsPerSecond: cfg.SandboxOpsPerSecond,
		SandboxOpsBurst:     cfg.SandboxOpsBurst,
		Budget:              cfg.EdgeBudget,
		Keys:                cfg.DataPlaneKeys,
		Transfers:           cfg.Transfers,
		TransferHost:        cfg.Names.TransferHost,
		PreviewDomain:       cfg.Names.PreviewDomain,
		Previews:            cfg.Previews,
		River:               rc,
		Secrets:             cfg.Secrets,
		Sources:             sources,
		SecretsHost:         cfg.Names.SecretsHost,
		EgressHost:          cfg.Names.EgressHost,
		X402:                cfg.X402,
		HostRules:           hostrules.New(cfg.X402, cfg.Names, cfg.E2B.Domain),
		SSH:                 cfg.SSH,
		SSHHost:             cfg.Names.SSHHost,
		SSHPort:             cfg.Names.SSHPort,
		Wallets:             cfg.Wallets,
		Bench:               cfg.Bench,
		Health:              cfg.Health,
		ProviderOps:         cfg.ProviderOps,
		Estimator:           est,
		Sessions:            cfg.Sessions,
		SessionWriter:       writer,
		LiveValkey:          liveVK,
		Bootstrap:           cfg.Bootstrap,
		Fetch:               cfg.Fetch,
		FetchToken:          cfg.EgressFetchToken,
		Images:              cfg.ImageTools,
		ImageAccept:         cfg.Images.Accept,
		Hooks:               cfg.Hooks,
		HookSecrets:         cfg.HookSecrets,
		Pools:               pools,
	})
	defer srv.Close()

	mux := http.NewServeMux()
	api.Mount(mux, srv.Servers(), tokens(cfg))
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	hs := &http.Server{
		Handler:           mux,
		Protocols:         &protocols,
		ReadHeaderTimeout: readHeaderTimeout,
	}
	hs.RegisterOnShutdown(srv.Drain)
	ln, err := listen(cfg.ListenAddr, cfg.APIReusePort)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.ListenAddr, err)
	}
	feed, feedLn, err := statsServer(cfg, stats.NewHandler(st, cat, &cfg, time.Now, log))
	if err != nil {
		_ = ln.Close()
		return err
	}
	failed := make(chan error, 2)
	go func() { failed <- hs.Serve(ln) }()
	if feed != nil {
		go func() { failed <- feed.Serve(feedLn) }()
		log.Info("serving the stats feed", "addr", cfg.Stats.ListenAddr)
	}
	log.Info("serving", "addr", cfg.ListenAddr, "replica", cfg.ReplicaID,
		"providers", cfg.Providers)

	var serveErr error
	select {
	case <-ctx.Done():
		log.Info("shutting down")
	case err := <-failed:
		serveErr = fmt.Errorf("serve: %w", err)
	}
	if feed != nil {
		// The feed's answers are short, its WriteTimeout bounds what is left
		sctx, cancel := context.WithTimeout(context.Background(), statsWriteTimeout)
		_ = feed.Shutdown(sctx)
		cancel()
	}
	shutdown(log, hs, srv, js, sg, cfg.Transfers.MaxDuration)
	sg.Close()
	scanner.Wait()
	return serveErr
}

// checkPools refuses a pool on a provider that cannot keep one, which would fill nothing and say
// nothing
func checkPools(reg *provider.Registry, cfg config.Config) error {
	return poolpkg.Validate(reg, cfg.Pools)
}

// checkImages refuses images.accept when no provider is accepted or exempt, and says which
// providers it keeps out of placements
func checkImages(reg *provider.Registry, cfg config.Config) error {
	accept := cfg.Images.Accept
	if len(accept) == 0 {
		return nil
	}
	found := false
	for _, p := range reg.Ordered() {
		if reg.ImageAccepted(p, accept) {
			found = true
			continue
		}
		slog.Error("images.accept keeps a provider out of placements", "provider", p.Name(),
			"version", reg.ImageVersion(p))
	}
	if !found {
		return errors.New("images.accept lists no version that a provider has, and " +
			"images.exempt names none of them")
	}
	return nil
}

// checkBoxdRefs refuses images.boxd.refs keys that are not sizes boxd sells, and a refs that
// misses a size the catalog's packages land on
func checkBoxdRefs(reg *provider.Registry, cfg config.Config, cat *catalog.Catalog) error {
	refs := cfg.Images.Boxd.Refs
	p, ok := reg.Get("boxd")
	sizer, isSizer := provider.As[provider.Sizer](p)
	if len(refs) == 0 || !ok || !isSizer {
		return nil
	}
	sold := map[string]bool{}
	for _, s := range sizer.Sizes() {
		sold[s.Name] = true
	}
	for _, size := range slices.Sorted(maps.Keys(refs)) {
		if !sold[size] {
			return fmt.Errorf("images.boxd.refs names %q, which is not a size boxd sells", size)
		}
	}
	for _, pkg := range cat.All() {
		if size, ok := p.SizeFor(pkg.Resources); ok && refs[size.Name] == "" {
			return fmt.Errorf("images.boxd.refs has no artifact for size %q, which package %q "+
				"lands on", size.Name, pkg.ID)
		}
	}
	return nil
}

// poolWarmth is the view of warm members that marks candidates, none while no pool is on
func poolWarmth(
	cfg config.Config, reg *provider.Registry, st *store.Store,
) (*poolpkg.Warmth, placement.Warm) {
	if !cfg.Pools.Enabled || len(cfg.Pools.Pool) == 0 {
		return nil, nil
	}
	w := poolpkg.NewWarmth(st, poolpkg.ConfigsFor(reg, cfg.Pools), nil)
	return w, w
}

// checkWallets refuses the payer claim while Payments can neither ask CheckPayer nor hear blocks
func checkWallets(log *slog.Logger, cfg config.Config) error {
	w := cfg.Wallets
	switch {
	case !w.UsePayer:
		log.Info("wallets.use_payer is false, so every order counts as having no payer")
	case cfg.PaymentsToken == "":
		return errors.New("wallets.use_payer is true while BUGNET_PAYMENTS_ROUTER_TOKEN is " +
			"empty, so Payments cannot call CheckPayer, set the token or turn use_payer off")
	case !w.EmitEvents:
		return errors.New("wallets.use_payer is true while wallets.emit_events is false, so " +
			"Payments never hears of a blocked wallet, turn emit_events on or use_payer off")
	}
	return nil
}

// bootSettings reads the CA and the mining pool list once, so no placement with secrets lacks a CA
func bootSettings(log *slog.Logger, cfg config.Config) (saga.BootSettings, error) {
	return saga.LoadBootSettings(log, cfg)
}

// checkRestrictedEgress refuses a restricted package on E2B, whose allowOut takes IPv4 alone
func checkRestrictedEgress(cfg config.Config, cat *catalog.Catalog) error {
	if !slices.Contains(cfg.Providers, "e2b") ||
		slices.ContainsFunc(cfg.Secrets.EgressIPs, netip.Addr.Is4) {
		return nil
	}
	for _, p := range cat.All() {
		if len(p.EgressAllow) > 0 {
			return fmt.Errorf("package %q has egress.allow and providers lists e2b, while "+
				"secrets.egress_ips holds no IPv4 address, so its E2B sandboxes would reach "+
				"nothing, add the egress proxy's IPv4 address to secrets.egress_ips", p.ID)
		}
	}
	return nil
}

// listen binds addr over TCP, with SO_REUSEPORT when reusePort is set so a new version binds too
func listen(addr string, reusePort bool) (net.Listener, error) {
	var lc net.ListenConfig
	if reusePort {
		lc.Control = func(_, _ string, rc syscall.RawConn) error {
			var serr error
			err := rc.Control(func(fd uintptr) {
				serr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
			})
			return errors.Join(err, serr)
		}
	}
	return lc.Listen(context.Background(), "tcp", addr)
}

const (
	statsReadHeaderTimeout = 5 * time.Second
	statsWriteTimeout      = 10 * time.Second
	statsIdleTimeout       = 60 * time.Second
	statsMaxHeaderBytes    = 8 << 10
)

// statsServer binds the public stats feed's loopback listener, nil when stats.listen_addr is empty
func statsServer(cfg config.Config, h http.Handler) (*http.Server, net.Listener, error) {
	if !cfg.Stats.Enabled() {
		return nil, nil, nil
	}
	ln, err := listen(cfg.Stats.ListenAddr, cfg.APIReusePort)
	if err != nil {
		return nil, nil, fmt.Errorf("listen on stats.listen_addr: %w", err)
	}
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	return &http.Server{
		Handler:           h,
		Protocols:         &protocols,
		ReadHeaderTimeout: statsReadHeaderTimeout,
		WriteTimeout:      statsWriteTimeout,
		IdleTimeout:       statsIdleTimeout,
		MaxHeaderBytes:    statsMaxHeaderBytes,
		// net/http's own error lines name client addresses, which this listener never logs
		ErrorLog: stdlog.New(io.Discard, "", 0),
	}, ln, nil
}

// tokens are the callers' bearer tokens from the environment
func tokens(cfg config.Config) api.Tokens {
	return api.Tokens{
		Internal: cfg.InternalToken, Edge: cfg.EdgeToken, Secrets: cfg.SecretsToken,
		SSH: cfg.SSHToken, Egress: cfg.EgressToken, Operator: cfg.OperatorToken,
		Payments: cfg.PaymentsToken, Sessions: cfg.SessionsToken,
	}
}

// liveClient connects the live view's client, its blocking pool sized for its readers
func liveClient(cfg config.Config) (valkey.Client, error) {
	opt, err := vkclient.Options(cfg.ValkeyAddr)
	if err != nil {
		return nil, err
	}
	opt.BlockingPoolSize = cfg.Sessions.Live.MaxReaders
	return valkey.NewClient(opt)
}

// jobDeps gives the jobs service its dependencies and the configured output limits
func jobDeps(
	cfg config.Config, st *store.Store, bus *streams.Bus, reg *provider.Registry, log *slog.Logger,
) jobs.Deps {
	return jobs.Deps{
		Store:           st,
		Bus:             bus,
		Registry:        reg,
		ReplicaID:       cfg.ReplicaID,
		Log:             log,
		JobBudget:       cfg.JobOutputBudget,
		PlacementBudget: cfg.PlacementOutputBudget,
		TailReserve:     cfg.PlacementTailReserve,
		MaxRunning:      cfg.MaxRunningJobs,
	}
}

// closer ends every stream of the API server
type closer interface{ Close() }

// handoff hands running jobs to peers
type handoff interface{ Shutdown(ctx context.Context) }

// placing waits for placements still being placed
type placing interface{ Wait() }

// shutdown hands jobs over while transfer streams drain for up to drain, then waits on placing
func shutdown(
	log *slog.Logger, hs *http.Server, srv closer, js handoff, sg placing, drain time.Duration,
) {
	served := make(chan struct{})
	go func() {
		defer close(served)
		ctx, cancel := context.WithTimeout(context.Background(), drain)
		defer cancel()
		if err := hs.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Warn("transfer streams still open at the drain's end, cutting them", "err", err)
		}
		srv.Close()
		_ = hs.Close()
	}()

	jctx, jcancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer jcancel()
	js.Shutdown(jctx)
	<-served

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
