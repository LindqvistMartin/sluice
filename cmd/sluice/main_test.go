package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LindqvistMartin/sluice/internal/config"
	"github.com/LindqvistMartin/sluice/internal/deliver"
	"github.com/LindqvistMartin/sluice/internal/dlq"
	"github.com/LindqvistMartin/sluice/internal/ingest"
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

// TestCoordinator_PersistAndFanOut exercises the composition-root glue end to end:
// an event is persisted and every target receives the body and a shared event id.
// It pins the positional contract between the store's returned delivery ids and the
// event's targets, which nothing else covers.
func TestCoordinator_PersistAndFanOut(t *testing.T) {
	body := []byte(`{"alert":"firing"}`)

	type received struct {
		body    string
		eventID string
	}
	got := make(chan received, 2)
	newTarget := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			got <- received{body: string(b), eventID: r.Header.Get("X-Sluice-Event-Id")}
			w.WriteHeader(http.StatusOK)
		}))
	}
	t1, t2 := newTarget(), newTarget()
	defer t1.Close()
	defer t2.Close()

	store, err := dlq.Open(filepath.Join(t.TempDir(), "dlq.db"), nil)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close(context.Background()) }()

	pool := deliver.NewPool(deliver.PoolConfig{Workers: 2, Reporter: store})
	pool.Start()
	defer pool.Close()

	coord := &coordinator{store: store, pool: pool}
	ev := ingest.Event{
		Route:   "/hook",
		Headers: http.Header{"Content-Type": {"application/json"}},
		Body:    body,
		Targets: []config.Target{
			{URL: t1.URL, Timeout: config.Duration{Duration: 2 * time.Second}, Retry: config.Retry{Max: 3}},
			{URL: t2.URL, Timeout: config.Duration{Duration: 2 * time.Second}, Retry: config.Retry{Max: 3}},
		},
	}
	if err := coord.Persist(context.Background(), ev); err != nil {
		t.Fatalf("persist: %v", err)
	}

	timeout := time.After(5 * time.Second)
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
			t.Fatal("timed out waiting for fan-out")
		}
	}
	if eventIDs[0] != eventIDs[1] {
		t.Errorf("targets saw different event ids: %q vs %q", eventIDs[0], eventIDs[1])
	}
}
