package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LindqvistMartin/sluice/internal/config"
	"github.com/LindqvistMartin/sluice/internal/deliver"
	"github.com/LindqvistMartin/sluice/internal/dlq"
	"github.com/LindqvistMartin/sluice/internal/ingest"
	"github.com/LindqvistMartin/sluice/internal/replay"
)

// seedParkedDLQ creates a DLQ with one parked delivery and writes a minimal config
// pointing at it, returning the config path and the parked event id.
func seedParkedDLQ(t *testing.T) (cfgPath, eventID string) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.ToSlash(filepath.Join(dir, "dlq.db"))

	store, err := dlq.Open(dbPath, nil)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	res, err := store.Persist(context.Background(), dlq.EventRecord{
		Route: "/hook", Headers: http.Header{}, Body: []byte("x"),
		TargetURLs: []string{"http://down.example"},
	})
	if err != nil {
		t.Fatalf("persist: %v", err)
	}
	if err := store.Park(context.Background(), res.DeliveryIDs[0], 6, "exhausted"); err != nil {
		t.Fatalf("park: %v", err)
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	cfgPath = filepath.Join(dir, "sluice.yml")
	cfg := "dlq:\n  path: '" + dbPath + "'\nroutes:\n  - path: /hook\n    fanout:\n      - url: http://down.example\n"
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return cfgPath, res.EventID
}

func TestRun_DLQRetry(t *testing.T) {
	cfgPath, _ := seedParkedDLQ(t)

	var out, errb bytes.Buffer
	if err := run(context.Background(), []string{"dlq", "retry", "-c", cfgPath}, &out, &errb); err != nil {
		t.Fatalf("run: %v (stderr=%q)", err, errb.String())
	}
	if !strings.Contains(out.String(), "retried 1 parked delivery") {
		t.Errorf("stdout = %q, want it to report retrying 1", out.String())
	}
}

func TestRun_DLQRetry_Event(t *testing.T) {
	cfgPath, eventID := seedParkedDLQ(t)

	var out, errb bytes.Buffer
	if err := run(context.Background(), []string{"dlq", "retry", "-c", cfgPath, "-event", eventID}, &out, &errb); err != nil {
		t.Fatalf("run: %v (stderr=%q)", err, errb.String())
	}
	if !strings.Contains(out.String(), "for event "+eventID) {
		t.Errorf("stdout = %q, want it to mention the event id", out.String())
	}
}

func TestRun_DLQList(t *testing.T) {
	cfgPath, eventID := seedParkedDLQ(t)

	var out, errb bytes.Buffer
	if err := run(context.Background(), []string{"dlq", "list", "-c", cfgPath}, &out, &errb); err != nil {
		t.Fatalf("run: %v (stderr=%q)", err, errb.String())
	}
	got := out.String()
	for _, want := range []string{
		"event=" + eventID,
		"route=/hook",
		"url=http://down.example",
		"attempts=6",
		`last_error="exhausted"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("list line missing %q\nstdout = %q", want, got)
		}
	}
}

func TestRun_DLQ_UnknownSubcommand(t *testing.T) {
	var out, errb bytes.Buffer
	err := run(context.Background(), []string{"dlq", "frob"}, &out, &errb)
	if err == nil || !strings.Contains(err.Error(), "unknown subcommand") {
		t.Fatalf("err = %v, want an unknown-subcommand error", err)
	}
}

func TestRun_DLQ_NoSubcommand(t *testing.T) {
	var out, errb bytes.Buffer
	err := run(context.Background(), []string{"dlq"}, &out, &errb)
	if err == nil || !strings.Contains(err.Error(), "subcommand") {
		t.Fatalf("err = %v, want a missing-subcommand error", err)
	}
}

// TestRun_DLQRetry_RedeliversWhileWorkerRuns is the cross-process case: a parked
// delivery is revived through a second handle on the same database file while the
// daemon's store and replay worker are still running, and the worker redelivers it.
func TestRun_DLQRetry_RedeliversWhileWorkerRuns(t *testing.T) {
	body := []byte(`{"alert":"firing"}`)

	type received struct {
		body    string
		eventID string
	}
	got := make(chan received, 4)

	// serving is false until the target "recovers": it 500s first to drive the
	// delivery to dead, then 200s so the revived row can succeed.
	var serving atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !serving.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		b, _ := io.ReadAll(r.Body)
		got <- received{body: string(b), eventID: r.Header.Get("X-Sluice-Event-Id")}
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	dbPath := filepath.ToSlash(filepath.Join(t.TempDir(), "dlq.db"))
	store, err := dlq.Open(dbPath, nil)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close(context.Background()) }()

	pool := deliver.NewPool(deliver.PoolConfig{
		Workers:     2,
		Reporter:    store,
		BackoffBase: time.Millisecond,
		BackoffCap:  5 * time.Millisecond,
	})
	pool.Start()
	defer pool.Close()

	cfg := &config.Config{Routes: []config.Route{{
		Path: "/hook",
		Fanout: []config.Target{
			{URL: target.URL, Timeout: config.Duration{Duration: 2 * time.Second}, Retry: config.Retry{Max: 2}},
		},
	}}}

	wake := make(chan struct{}, 1)
	worker := replay.New(store, pool, replay.Config{
		Interval: 20 * time.Millisecond,
		LeaseTTL: time.Minute,
		Wake:     wake,
		Resolver: newTargetResolver(cfg),
	})
	worker.Start()
	defer worker.Stop()

	coord := &coordinator{store: store, wake: wake}
	ev := ingest.Event{
		Route:   "/hook",
		Headers: http.Header{"Content-Type": {"application/json"}},
		Body:    body,
		Targets: cfg.Routes[0].Fanout,
	}
	if err := coord.Persist(context.Background(), ev); err != nil {
		t.Fatalf("persist: %v", err)
	}

	// Wait until the failing target drives the delivery to dead.
	deadline := time.After(10 * time.Second)
	for {
		st, err := store.Stats(context.Background())
		if err != nil {
			t.Fatalf("stats: %v", err)
		}
		if st.Dead == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("delivery did not park (stats %+v)", st)
		case <-time.After(20 * time.Millisecond):
		}
	}

	// Target recovers; an operator revives the parked row through a second handle on the
	// same file while the daemon's store and worker keep running.
	serving.Store(true)
	cli, err := dlq.OpenCLI(dbPath)
	if err != nil {
		t.Fatalf("open cli: %v", err)
	}
	defer func() { _ = cli.Close() }()
	n, err := cli.RetryParked(context.Background(), "", time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("retry parked: %v", err)
	}
	if n != 1 {
		t.Fatalf("retried %d, want 1", n)
	}

	// The running worker re-claims the revived row on its next scan and delivers it.
	select {
	case r := <-got:
		if r.body != string(body) {
			t.Errorf("delivered body = %q, want %q", r.body, body)
		}
		if r.eventID == "" {
			t.Error("delivered with empty X-Sluice-Event-Id")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("revived delivery was not redelivered")
	}

	// A fully delivered event leaves nothing behind.
	drain := time.After(10 * time.Second)
	for countDeliveries(t, dbPath) != 0 {
		select {
		case <-drain:
			t.Fatal("deliveries did not drain to 0 after retry")
		case <-time.After(20 * time.Millisecond):
		}
	}
}
