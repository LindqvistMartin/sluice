package metrics

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
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
		// The per-target families always declare themselves, even with no targets.
		"# TYPE sluice_deliveries_total counter",
		"# TYPE sluice_delivery_failures_total counter",
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

	// With no targets the per-target families declare their type but emit no samples.
	for _, sample := range []string{"sluice_deliveries_total{", "sluice_delivery_failures_total{"} {
		if strings.Contains(body, sample) {
			t.Errorf("body has a per-target sample %q with no targets configured", sample)
		}
	}
}

func TestHandler_Exposition_PerTarget(t *testing.T) {
	h := NewHandler(func(context.Context) (Snapshot, error) {
		return Snapshot{
			Version: "v1",
			Targets: []TargetStat{
				{Route: "/gh", Target: "https://a.example/hook", Delivered: 42, Failed: 3},
				{Route: "/gl", Target: "https://a.example/hook", Delivered: 7, Failed: 0},
			},
		}, nil
	})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	body := rec.Body.String()
	wantLines := []string{
		"# TYPE sluice_deliveries_total counter",
		`sluice_deliveries_total{route="/gh",target="https://a.example/hook"} 42`,
		`sluice_deliveries_total{route="/gl",target="https://a.example/hook"} 7`,
		"# TYPE sluice_delivery_failures_total counter",
		`sluice_delivery_failures_total{route="/gh",target="https://a.example/hook"} 3`,
		`sluice_delivery_failures_total{route="/gl",target="https://a.example/hook"} 0`,
	}
	for _, line := range wantLines {
		if !strings.Contains(body, line) {
			t.Errorf("body missing %q\n--- body ---\n%s", line, body)
		}
	}

	// One URL configured under two routes stays two distinct series — the route is
	// part of the key — so the shared URL appears in four samples (delivered+failed x2).
	if got := strings.Count(body, `target="https://a.example/hook"`); got != 4 {
		t.Errorf("samples for the shared URL = %d, want 4", got)
	}
}

func TestCounters_ConcurrentInc(t *testing.T) {
	c := NewCounters()

	const goroutines = 8
	const perGoroutine = 1000
	keys := []struct{ route, target string }{
		{"/a", "https://t1"},
		{"/a", "https://t2"},
		{"/b", "https://t1"},
	}

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			k := keys[g%len(keys)]
			for range perGoroutine {
				c.IncDelivered(k.route, k.target)
				c.IncFailed(k.route, k.target)
			}
		}(g)
	}
	wg.Wait()

	snap := c.Snapshot()

	if !slices.IsSortedFunc(snap, func(a, b TargetStat) int {
		if a.Route != b.Route {
			return strings.Compare(a.Route, b.Route)
		}
		return strings.Compare(a.Target, b.Target)
	}) {
		t.Errorf("snapshot not sorted by (route, target): %+v", snap)
	}
	if len(snap) != len(keys) {
		t.Errorf("distinct targets = %d, want %d", len(snap), len(keys))
	}

	// Grand totals are exact regardless of how goroutines mapped to keys, which is what
	// the race detector guards: a lost update would drop the total below the work done.
	var totalDelivered, totalFailed int64
	for _, ts := range snap {
		totalDelivered += ts.Delivered
		totalFailed += ts.Failed
	}
	if want := int64(goroutines * perGoroutine); totalDelivered != want {
		t.Errorf("total delivered = %d, want %d", totalDelivered, want)
	}
	if want := int64(goroutines * perGoroutine); totalFailed != want {
		t.Errorf("total failed = %d, want %d", totalFailed, want)
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

func TestHandler_EscapesTargetLabels(t *testing.T) {
	h := NewHandler(func(context.Context) (Snapshot, error) {
		return Snapshot{Targets: []TargetStat{
			{Route: "a\"\\b", Target: "https://t.example", Delivered: 1, Failed: 0},
		}}, nil
	})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if !strings.Contains(rec.Body.String(), `sluice_deliveries_total{route="a\"\\b",target="https://t.example"} 1`) {
		t.Errorf("route label not escaped\n--- body ---\n%s", rec.Body.String())
	}
}
