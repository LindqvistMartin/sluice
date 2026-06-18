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
	"github.com/LindqvistMartin/sluice/internal/ingest"
	"github.com/LindqvistMartin/sluice/internal/observability"
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
	handler := ingest.New(ingest.Options{
		Matcher:      matcher,
		Limiter:      limiter,
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

	select {
	case err := <-serverErr:
		return fmt.Errorf("listener failed: %w", err)
	case <-runCtx.Done():
		log.Info("shutdown signal received, draining")
	}

	// The drain order grows as the pipeline lands: after the server stops
	// accepting, stop the replay worker, drain in-flight deliveries, then close the
	// DLQ writer and checkpoint the WAL. For now it is the server then the limiter.
	shutdownCtx, scancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer scancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown failed: %w", err)
	}

	log.Info("sluice stopped")
	return nil
}
