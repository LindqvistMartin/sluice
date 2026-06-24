package main

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"net/http"
	"net/http/httptest"
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

func TestRun_Version(t *testing.T) {
	var out, errb bytes.Buffer
	if err := run(context.Background(), []string{"-version"}, &out, &errb); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(out.String(), "sluice ") {
		t.Errorf("version output = %q", out.String())
	}
}

func TestRun_ConfigCheckOK(t *testing.T) {
	var out, errb bytes.Buffer
	err := run(context.Background(), []string{"-t", "-c", "testdata/good.yml"}, &out, &errb)
	if err != nil {
		t.Fatalf("run: %v (stderr=%q)", err, errb.String())
	}
	if !strings.Contains(out.String(), "ok") {
		t.Errorf("stdout = %q, want it to contain ok", out.String())
	}
}

func TestRun_ConfigCheckBad(t *testing.T) {
	var out, errb bytes.Buffer
	err := run(context.Background(), []string{"-t", "-c", "testdata/bad.yml"}, &out, &errb)
	if err == nil {
		t.Fatal("expected an error for an invalid config")
	}
	if !strings.Contains(err.Error(), "invalid") {
		t.Errorf("error = %q, want it to report invalid", err.Error())
	}
	if !strings.Contains(err.Error(), "fanout[0].url") {
		t.Errorf("error = %q, want a field path", err.Error())
	}
}

func TestRun_BadLogFormat(t *testing.T) {
	var out, errb bytes.Buffer
	err := run(context.Background(), []string{"--log-format", "xml", "-c", "testdata/good.yml"}, &out, &errb)
	if err == nil {
		t.Fatal("expected an error for an unsupported log format")
	}
}

func TestRun_ConfigCheckOK_JSON(t *testing.T) {
	var out, errb bytes.Buffer
	err := run(context.Background(), []string{"-t", "--log-format", "json", "-c", "testdata/good.yml"}, &out, &errb)
	if err != nil {
		t.Fatalf("run: %v (stderr=%q)", err, errb.String())
	}
	if !strings.Contains(out.String(), "ok") {
		t.Errorf("stdout = %q, want it to contain ok", out.String())
	}
}

func TestRun_VersionBeforeLogFormat(t *testing.T) {
	var out, errb bytes.Buffer
	// A bad --log-format must not stop -version from printing.
	if err := run(context.Background(), []string{"-version", "--log-format", "xml"}, &out, &errb); err != nil {
		t.Fatalf("version should win over log-format validation: %v", err)
	}
	if !strings.Contains(out.String(), "sluice ") {
		t.Errorf("version output = %q", out.String())
	}
}

// TestPipeline_PersistThenReplayDelivers exercises the whole durable pipeline end to
// end: the coordinator persists and nudges, the replay worker claims and submits, and
// the pool delivers — including a target that fails once and succeeds on the
// rescheduled pass. A fully delivered event drains both tables to empty.
func TestPipeline_PersistThenReplayDelivers(t *testing.T) {
	body := []byte(`{"alert":"firing"}`)

	type received struct {
		body    string
		eventID string
	}
	got := make(chan received, 8)

	okTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got <- received{body: string(b), eventID: r.Header.Get("X-Sluice-Event-Id")}
		w.WriteHeader(http.StatusOK)
	}))
	defer okTarget.Close()

	// Fails once, then succeeds: exercises reschedule-then-deliver across passes.
	var flakyHits atomic.Int32
	flakyTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if flakyHits.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		b, _ := io.ReadAll(r.Body)
		got <- received{body: string(b), eventID: r.Header.Get("X-Sluice-Event-Id")}
		w.WriteHeader(http.StatusOK)
	}))
	defer flakyTarget.Close()

	dbPath := filepath.Join(t.TempDir(), "dlq.db")
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
			{URL: okTarget.URL, Timeout: config.Duration{Duration: 2 * time.Second}, Retry: config.Retry{Max: 3}},
			{URL: flakyTarget.URL, Timeout: config.Duration{Duration: 2 * time.Second}, Retry: config.Retry{Max: 3}},
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

	timeout := time.After(10 * time.Second)
	var eventIDs []string
	for range 2 {
		select {
		case r := <-got:
			if r.body != string(body) {
				t.Errorf("target body = %q, want %q", r.body, body)
			}
			if r.eventID == "" {
				t.Error("target received empty X-Sluice-Event-Id")
			}
			eventIDs = append(eventIDs, r.eventID)
		case <-timeout:
			t.Fatal("timed out waiting for delivery")
		}
	}
	if eventIDs[0] != eventIDs[1] {
		t.Errorf("targets saw different event ids: %q vs %q", eventIDs[0], eventIDs[1])
	}

	// Both targets delivered (the flaky one after a reschedule), so the store drains to
	// empty: a fully delivered event leaves nothing behind.
	deadline := time.After(10 * time.Second)
	for countDeliveries(t, dbPath) != 0 {
		select {
		case <-deadline:
			t.Fatal("deliveries did not drain to 0")
		case <-time.After(20 * time.Millisecond):
		}
	}
	if got := flakyHits.Load(); got != 2 {
		t.Errorf("flaky target hit %d times, want 2 (one 500, one 200)", got)
	}
}

// countDeliveries counts delivery rows over an independent read-only connection.
func countDeliveries(t *testing.T, path string) int {
	t.Helper()
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open read db: %v", err)
	}
	defer func() { _ = db.Close() }()

	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM deliveries").Scan(&n); err != nil {
		t.Fatalf("count deliveries: %v", err)
	}
	return n
}
