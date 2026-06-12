package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// version is overridden via -ldflags at release time.
var version = "dev"

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("sluice exited with error", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfgPath := flag.String("c", "sluice.yml", "path to the configuration file")
	check := flag.Bool("t", false, "check the configuration and exit")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println("sluice", version)
		return nil
	}

	if *check {
		// Configuration loading and validation arrive in a later iteration; the
		// flag is wired now so the command surface stays stable.
		log.Info("config check not implemented yet", "config", *cfgPath)
		return nil
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := &http.Server{Addr: ":8080"}

	serverErr := make(chan error, 1)
	go func() {
		log.Info("sluice starting", "version", version, "addr", srv.Addr, "config", *cfgPath)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	select {
	case err := <-serverErr:
		return fmt.Errorf("listener failed: %w", err)
	case <-ctx.Done():
		log.Info("shutdown signal received, draining")
	}

	// The drain order grows as the pipeline lands: stop the replay worker, drain
	// in-flight deliveries, then close the DLQ writer and checkpoint the WAL.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown failed: %w", err)
	}

	log.Info("sluice stopped")
	return nil
}
