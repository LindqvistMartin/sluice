package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/LindqvistMartin/sluice/internal/config"
	"github.com/LindqvistMartin/sluice/internal/deliver"
	"github.com/LindqvistMartin/sluice/internal/dlq"
	"github.com/LindqvistMartin/sluice/internal/ingest"
	"github.com/LindqvistMartin/sluice/internal/metrics"
	"github.com/LindqvistMartin/sluice/internal/observability"
	"github.com/LindqvistMartin/sluice/internal/replay"
	"github.com/LindqvistMartin/sluice/internal/route"
)

// version is overridden via -ldflags at release time.
var version = "dev"

const (
	limiterTTL      = 10 * time.Minute
	shutdownTimeout = 10 * time.Second
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "sluice:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	// dlq is a verb, not a flag: operator subcommands over the queue file run as a
	// one-shot process and never reach the daemon path below. Existing flags start
	// with "-", so this dispatch leaves every other invocation unchanged.
	if len(args) > 0 && args[0] == "dlq" {
		return runDLQ(ctx, args[1:], stdout, stderr)
	}

	fs := flag.NewFlagSet("sluice", flag.ContinueOnError)
	fs.SetOutput(stderr)
	cfgPath := fs.String("c", "sluice.yml", "path to the configuration file")
	check := fs.Bool("t", false, "check the configuration and exit")
	showVersion := fs.Bool("version", false, "print the version and exit")
	logFormat := fs.String("log-format", "text", "log output format: text or json")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *showVersion {
		_, _ = fmt.Fprintln(stdout, "sluice", version)
		return nil
	}

	cfg, cfgErr := config.Load(*cfgPath)
	if *check {
		// Config check is independent of logging, so it runs before log setup:
		// a bad --log-format must not mask a config verdict.
		if cfgErr != nil {
			return fmt.Errorf("config %s is invalid:\n%w", *cfgPath, cfgErr)
		}
		_, _ = fmt.Fprintf(stdout, "config %s: ok\n", *cfgPath)
		return nil
	}

	format, err := observability.ParseLogFormat(*logFormat)
	if err != nil {
		return err
	}
	log := observability.NewLogger(stdout, format)

	if cfgErr != nil {
		return fmt.Errorf("load config: %w", cfgErr)
	}

	matcher := route.New(cfg)
	limiter := ingest.NewIPLimiter(cfg.Limits.Rate(), limiterTTL)

	store, err := dlq.Open(cfg.DLQ.Path, log)
	if err != nil {
		return fmt.Errorf("open dlq: %w", err)
	}
	// Drain order on shutdown: srv.Shutdown (below) stops accepting and waits for
	// in-flight handlers to return BEFORE these defers run, so nothing is mid-Persist
	// when the pipeline closes — keep srv.Shutdown out of the defer chain or that
	// guarantee is lost. Among the defers, LIFO runs worker.Stop, then pool.Close,
	// then store.Close: the worker stops claiming and submitting before the pool drains
	// its in-flight attempts, and the store's writer stops last so every outcome is
	// recorded and the WAL is checkpointed.
	defer func() { _ = store.Close(context.Background()) }()

	pool := deliver.NewPool(deliver.PoolConfig{
		Client:   deliver.NewClient(),
		Reporter: store,
		Logger:   log,
	})
	pool.Start()
	defer pool.Close()

	// wake lets a Persist nudge the worker to drain immediately instead of waiting for
	// the next scan tick; a size-1 buffer coalesces a burst of persists into one nudge.
	// Size the lease to outlast the longest possible attempt (claim, queue, one HTTP
	// try, report) so a slow but healthy target is not re-claimed and delivered twice
	// mid-attempt. The 6x factor leaves headroom for queueing; the worker floors it at
	// its 60s default, so the default 10s timeout leaves the lease at 60s.
	var maxTimeout time.Duration
	for _, r := range cfg.Routes {
		for _, t := range r.Fanout {
			if t.Timeout.Duration > maxTimeout {
				maxTimeout = t.Timeout.Duration
			}
		}
	}

	wake := make(chan struct{}, 1)
	worker := replay.New(store, pool, replay.Config{
		LeaseTTL: 6 * maxTimeout,
		MaxBytes: int64(cfg.DLQ.MaxSizeMB) << 20,
		Wake:     wake,
		Resolver: newTargetResolver(cfg),
		Logger:   log,
	})
	worker.Start()
	defer worker.Stop()

	coord := &coordinator{store: store, wake: wake}
	handler := ingest.New(ingest.Options{
		Matcher:      matcher,
		Limiter:      limiter,
		Persister:    coord,
		MaxBodyBytes: cfg.Limits.MaxBodyBytes,
		Logger:       log,
	})

	runCtx, cancel := context.WithCancel(ctx)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		limiter.Run(runCtx)
	}()
	// Order matters: cancel runs before wg.Wait so the limiter is told to stop
	// before we join it. Defers run last-in-first-out.
	defer wg.Wait()
	defer cancel()

	srv := &http.Server{
		Addr:    cfg.Listen,
		Handler: handler,
		// Bound every phase so a slow or idle client cannot pin a connection:
		// MaxBytesReader caps the body's size, these cap the time to move it.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverErr := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info("sluice starting", "version", version, "addr", srv.Addr, "config", *cfgPath)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	// Metrics listen on a separate, opt-in address (empty disables it) so the
	// exposition is never served on the public webhook port. metricsErr stays nil
	// when disabled, which leaves its select case below inert.
	var metricsSrv *http.Server
	var metricsErr chan error
	if cfg.MetricsListen != "" {
		metricsErr = make(chan error, 1)
		gather := func(ctx context.Context) (metrics.Snapshot, error) {
			st, err := store.Stats(ctx)
			if err != nil {
				return metrics.Snapshot{}, err
			}
			return metrics.Snapshot{
				Version: version,
				Pending: st.Pending,
				Dead:    st.Dead,
				Evicted: worker.EvictedTotal(),
			}, nil
		}
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.NewHandler(gather))
		metricsSrv = &http.Server{
			Addr:              cfg.MetricsListen,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       15 * time.Second,
			WriteTimeout:      15 * time.Second,
			IdleTimeout:       60 * time.Second,
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Info("metrics listening", "addr", metricsSrv.Addr)
			if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				metricsErr <- err
			}
		}()
	}

	// A listener error or a shutdown signal ends the wait. Either way both servers are
	// then shut down so their goroutines return before the deferred wg.Wait — shutting
	// down only one would leave the other's goroutine blocked in ListenAndServe.
	var runErr error
	select {
	case err := <-serverErr:
		runErr = fmt.Errorf("listener failed: %w", err)
	case err := <-metricsErr:
		runErr = fmt.Errorf("metrics listener failed: %w", err)
	case <-runCtx.Done():
		log.Info("shutdown signal received, draining")
	}

	// Stop accepting first; the deferred worker, pool, and store closes then stop
	// claiming, drain in-flight deliveries, and checkpoint the WAL, in that order.
	shutdownCtx, scancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer scancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && runErr == nil {
		runErr = fmt.Errorf("graceful shutdown failed: %w", err)
	}
	if metricsSrv != nil {
		_ = metricsSrv.Shutdown(shutdownCtx)
	}

	if runErr != nil {
		return runErr
	}
	log.Info("sluice stopped")
	return nil
}

// coordinator persists each accepted event durably, then nudges the replay worker to
// deliver it. It is the composition root's glue between the store and the worker's
// wake channel, and it satisfies ingest.Persister.
type coordinator struct {
	store *dlq.Store
	wake  chan<- struct{}
}

// Persist commits the event and its deliveries, then nudges the worker. It returns nil
// only once the write is durable, which is what lets the handler answer 200; the nudge
// is fire-and-forget after the commit, so the ack still means persisted, not delivered.
func (c *coordinator) Persist(ctx context.Context, ev ingest.Event) error {
	urls := make([]string, len(ev.Targets))
	for i, t := range ev.Targets {
		urls[i] = t.URL
	}

	if _, err := c.store.Persist(ctx, dlq.EventRecord{
		Route:      ev.Route,
		Headers:    ev.Headers,
		Body:       ev.Body,
		TargetURLs: urls,
	}); err != nil {
		return fmt.Errorf("persist event: %w", err)
	}

	// A full buffer already means "work waiting", so this non-blocking send never
	// stalls inbound and never grows unbounded.
	select {
	case c.wake <- struct{}{}:
	default:
	}
	return nil
}

// targetResolver recovers per-target delivery settings from the validated config,
// keyed by route path and target URL, so the worker can build a Delivery from a leased
// row that carries only the URL. A target dropped from config resolves to ok=false.
type targetResolver struct {
	byRoute map[string]map[string]config.Target
}

func newTargetResolver(cfg *config.Config) targetResolver {
	byRoute := make(map[string]map[string]config.Target, len(cfg.Routes))
	for _, r := range cfg.Routes {
		targets := make(map[string]config.Target, len(r.Fanout))
		for _, t := range r.Fanout {
			targets[t.URL] = t
		}
		byRoute[r.Path] = targets
	}
	return targetResolver{byRoute: byRoute}
}

func (tr targetResolver) Resolve(route, targetURL string) (time.Duration, int, bool) {
	t, ok := tr.byRoute[route][targetURL]
	if !ok {
		return 0, 0, false
	}
	return t.Timeout.Duration, t.Retry.Max, true
}

// Compile-time checks that the concrete types keep satisfying the interfaces the pool
// and worker consume, without those packages importing one another.
var (
	_ deliver.Reporter      = (*dlq.Store)(nil)
	_ replay.Store          = (*dlq.Store)(nil)
	_ replay.Submitter      = (*deliver.Pool)(nil)
	_ replay.TargetResolver = targetResolver{}
)
