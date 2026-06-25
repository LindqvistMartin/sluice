package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/LindqvistMartin/sluice/internal/config"
	"github.com/LindqvistMartin/sluice/internal/dlq"
)

// runDLQ handles `sluice dlq <subcommand>`: one-shot operator commands over the DLQ
// file, separate from the daemon. It opens the same database the daemon uses (resolved
// from the config's dlq.path) through a lightweight handle, so it works whether or not
// the daemon is running. Flags follow the subcommand: `sluice dlq retry -c sluice.yml`.
func runDLQ(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("dlq: expected a subcommand: retry or list")
	}

	sub := args[0]
	if sub != "retry" && sub != "list" {
		return fmt.Errorf("dlq: unknown subcommand %q (want retry or list)", sub)
	}

	fs := flag.NewFlagSet("sluice dlq "+sub, flag.ContinueOnError)
	fs.SetOutput(stderr)
	cfgPath := fs.String("c", "sluice.yml", "path to the configuration file")
	var event *string
	if sub == "retry" {
		event = fs.String("event", "", "only retry deliveries for this event id")
	}
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	cli, err := dlq.OpenCLI(cfg.DLQ.Path)
	if err != nil {
		return err
	}
	defer func() { _ = cli.Close() }()

	switch sub {
	case "retry":
		n, err := cli.RetryParked(ctx, *event, time.Now().UnixMilli())
		if err != nil {
			return err
		}
		scope := ""
		if *event != "" {
			scope = fmt.Sprintf(" for event %s", *event)
		}
		_, _ = fmt.Fprintf(stdout, "retried %d parked %s%s\n", n, plural(n, "delivery", "deliveries"), scope)
	case "list":
		dead, err := cli.ListDead(ctx)
		if err != nil {
			return err
		}
		for _, d := range dead {
			_, _ = fmt.Fprintf(stdout, "event=%s delivery=%d route=%s url=%s attempts=%d last_error=%q\n",
				d.EventID, d.DeliveryID, d.Route, d.TargetURL, d.Attempts, d.LastError)
		}
	}
	return nil
}

// plural picks the singular or plural noun for n.
func plural(n int64, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}
