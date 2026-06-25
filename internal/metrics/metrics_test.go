package metrics

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandler_Exposition(t *testing.T) {
	h := NewHandler(func(context.Context) (Snapshot, error) {
		return Snapshot{Version: "v1.2.3", Pending: 3, Dead: 2, Evicted: 7}, nil
	})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/plain; version=0.0.4") {
		t.Errorf("content-type = %q, want the prometheus text format", ct)
	}

	body := rec.Body.String()
	wantLines := []string{
		`sluice_build_info{version="v1.2.3"} 1`,
		"# TYPE sluice_dlq_pending gauge",
		"sluice_dlq_pending 3",
		"# TYPE sluice_dlq_dead gauge",
		"sluice_dlq_dead 2",
		"# TYPE sluice_dlq_evicted_total counter",
		"sluice_dlq_evicted_total 7",
	}
	for _, line := range wantLines {
		if !strings.Contains(body, line) {
			t.Errorf("body missing %q\n--- body ---\n%s", line, body)
		}
	}

	// The _total suffix is the counter's alone; the gauges must not carry it.
	for _, bad := range []string{"sluice_dlq_pending_total", "sluice_dlq_dead_total"} {
		if strings.Contains(body, bad) {
			t.Errorf("body has %q; gauges must not use the _total suffix", bad)
		}
	}
}

func TestHandler_GatherError_503(t *testing.T) {
	h := NewHandler(func(context.Context) (Snapshot, error) {
		return Snapshot{}, errors.New("db down")
	})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 (a failed scrape, not stale data)", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "sluice_dlq") {
		t.Errorf("503 body leaked partial metrics: %q", rec.Body.String())
	}
}

func TestHandler_RejectsNonGet(t *testing.T) {
	h := NewHandler(func(context.Context) (Snapshot, error) {
		return Snapshot{}, nil
	})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/metrics", nil))

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", rec.Code)
	}
	if allow := rec.Header().Get("Allow"); allow != "GET" {
		t.Errorf("Allow = %q, want GET", allow)
	}
}

func TestHandler_EscapesVersionLabel(t *testing.T) {
	h := NewHandler(func(context.Context) (Snapshot, error) {
		return Snapshot{Version: "1.0\"\\\n"}, nil
	})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if !strings.Contains(rec.Body.String(), `sluice_build_info{version="1.0\"\\\n"} 1`) {
		t.Errorf("version label not escaped\n--- body ---\n%s", rec.Body.String())
	}
}
