package deliver

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// recordingReporter is a Reporter that records terminal outcomes and signals each
// one on done, so a test can wait for completion without sleeping.
type recordingReporter struct {
	mu          sync.Mutex
	delivered   map[int64]string // deliveryID -> eventID
	rescheduled map[int64]int    // deliveryID -> attempts
	parked      map[int64]int    // deliveryID -> attempts
	done        chan struct{}
}

func newRecordingReporter(expected int) *recordingReporter {
	return &recordingReporter{
		delivered:   make(map[int64]string),
		rescheduled: make(map[int64]int),
		parked:      make(map[int64]int),
		done:        make(chan struct{}, expected),
	}
}

func (r *recordingReporter) MarkDelivered(_ context.Context, eventID string, deliveryID int64) error {
	r.mu.Lock()
	r.delivered[deliveryID] = eventID
	r.mu.Unlock()
	r.done <- struct{}{}
	return nil
}

func (r *recordingReporter) Reschedule(_ context.Context, deliveryID int64, attempts int, _ string, _ int64) error {
	r.mu.Lock()
	r.rescheduled[deliveryID] = attempts
	r.mu.Unlock()
	r.done <- struct{}{}
	return nil
}

func (r *recordingReporter) Park(_ context.Context, deliveryID int64, attempts int, _ string) error {
	r.mu.Lock()
	r.parked[deliveryID] = attempts
	r.mu.Unlock()
	r.done <- struct{}{}
	return nil
}

func (r *recordingReporter) deliveredCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.delivered)
}

func (r *recordingReporter) rescheduledCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.rescheduled)
}

func (r *recordingReporter) parkedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.parked)
}

func (r *recordingReporter) rescheduledAttempts(id int64) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rescheduled[id]
}

func (r *recordingReporter) parkedAttempts(id int64) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.parked[id]
}

// waitFor blocks until n terminal outcomes are reported or the test deadline trips.
func (r *recordingReporter) waitFor(t *testing.T, n int) {
	t.Helper()
	timeout := time.After(5 * time.Second)
	for range n {
		select {
		case <-r.done:
		case <-timeout:
			t.Fatalf("timed out waiting for %d outcomes", n)
		}
	}
}

// TestFanout_MultiTarget_HappyPath is the project's first integration contract:
// one inbound POST fans out to every configured target, and each target receives
// the original body unchanged along with the dedup header.
func TestFanout_MultiTarget_HappyPath(t *testing.T) {
	const eventID = "evt_happy_path"
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

	reporter := newRecordingReporter(2)
	pool := NewPool(PoolConfig{Workers: 2, Reporter: reporter})
	pool.Start()
	defer pool.Close()

	pool.Submit(Delivery{DeliveryID: 1, EventID: eventID, TargetURL: t1.URL, Body: body, Timeout: 2 * time.Second, RetryMax: 3})
	pool.Submit(Delivery{DeliveryID: 2, EventID: eventID, TargetURL: t2.URL, Body: body, Timeout: 2 * time.Second, RetryMax: 3})

	reporter.waitFor(t, 2)

	for range 2 {
		r := <-got
		if r.body != string(body) {
			t.Errorf("target received body %q, want %q", r.body, body)
		}
		if r.eventID != eventID {
			t.Errorf("target received X-Sluice-Event-Id %q, want %q", r.eventID, eventID)
		}
	}
	if reporter.deliveredCount() != 2 {
		t.Errorf("delivered = %d, want 2", reporter.deliveredCount())
	}
	if n := reporter.rescheduledCount() + reporter.parkedCount(); n != 0 {
		t.Errorf("non-delivered outcomes = %d, want 0", n)
	}
}

// TestFanout_RedirectNotFollowed checks that a 3xx is treated as a retryable
// failure, not a delivery: the client must not chase the Location (which would let
// a target bounce the body to another host and report it delivered). With budget
// remaining, the pass is reported as a reschedule.
func TestFanout_RedirectNotFollowed(t *testing.T) {
	var rootHits, finalHits atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/final", func(w http.ResponseWriter, _ *http.Request) {
		finalHits.Add(1)
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		rootHits.Add(1)
		http.Redirect(w, r, "/final", http.StatusFound)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	reporter := newRecordingReporter(1)
	pool := NewPool(PoolConfig{Workers: 1, Reporter: reporter})
	pool.Start()
	defer pool.Close()

	pool.Submit(Delivery{DeliveryID: 5, EventID: "evt_redirect", TargetURL: srv.URL + "/", Body: []byte("x"), Timeout: 2 * time.Second, RetryMax: 1})
	reporter.waitFor(t, 1)

	if reporter.deliveredCount() != 0 {
		t.Errorf("delivered = %d, want 0 (a 3xx must not count as delivered)", reporter.deliveredCount())
	}
	if reporter.rescheduledCount() != 1 {
		t.Errorf("rescheduled = %d, want 1", reporter.rescheduledCount())
	}
	if got := finalHits.Load(); got != 0 {
		t.Errorf("redirect target hit %d times, want 0 (redirects must not be followed)", got)
	}
	if got := rootHits.Load(); got != 1 {
		t.Errorf("target hit %d times, want 1 (one attempt per pass)", got)
	}
}

// TestFanout_StripsSensitiveHeaders checks that an inbound sender's credentials are
// not forwarded to targets, while ordinary headers and the dedup header are.
func TestFanout_StripsSensitiveHeaders(t *testing.T) {
	gotHeaders := make(chan http.Header, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeaders <- r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	reporter := newRecordingReporter(1)
	pool := NewPool(PoolConfig{Workers: 1, Reporter: reporter})
	pool.Start()
	defer pool.Close()

	pool.Submit(Delivery{
		DeliveryID: 3,
		EventID:    "evt_headers",
		TargetURL:  srv.URL,
		Body:       []byte("x"),
		Headers: http.Header{
			"Authorization":   {"Bearer secret"},
			"Cookie":          {"session=abc"},
			"X-Custom-Header": {"keep-me"},
		},
		Timeout:  2 * time.Second,
		RetryMax: 1,
	})
	reporter.waitFor(t, 1)

	h := <-gotHeaders
	if got := h.Get("Authorization"); got != "" {
		t.Errorf("Authorization forwarded = %q, want it stripped", got)
	}
	if got := h.Get("Cookie"); got != "" {
		t.Errorf("Cookie forwarded = %q, want it stripped", got)
	}
	if got := h.Get("X-Custom-Header"); got != "keep-me" {
		t.Errorf("X-Custom-Header = %q, want it forwarded", got)
	}
	if got := h.Get("X-Sluice-Event-Id"); got != "evt_headers" {
		t.Errorf("X-Sluice-Event-Id = %q, want evt_headers", got)
	}
}

// recordingCounter is a Counter that tallies outcomes per (route, target) so a test
// can assert the pool counted a delivery or a failed attempt against the right key.
type recordingCounter struct {
	mu        sync.Mutex
	delivered map[string]int
	failed    map[string]int
}

func newRecordingCounter() *recordingCounter {
	return &recordingCounter{delivered: make(map[string]int), failed: make(map[string]int)}
}

func (c *recordingCounter) IncDelivered(route, target string) {
	c.mu.Lock()
	c.delivered[route+"\x00"+target]++
	c.mu.Unlock()
}

func (c *recordingCounter) IncFailed(route, target string) {
	c.mu.Lock()
	c.failed[route+"\x00"+target]++
	c.mu.Unlock()
}

func (c *recordingCounter) deliveredCount(route, target string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.delivered[route+"\x00"+target]
}

func (c *recordingCounter) failedCount(route, target string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.failed[route+"\x00"+target]
}

// TestFanout_CountsPerTargetOutcomes checks the pool tallies a 2xx as one delivery and
// a non-2xx as one failed attempt, each keyed by the delivery's own (route, target).
func TestFanout_CountsPerTargetOutcomes(t *testing.T) {
	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer okSrv.Close()
	failSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failSrv.Close()

	reporter := newRecordingReporter(2)
	counter := newRecordingCounter()
	pool := NewPool(PoolConfig{Workers: 2, Reporter: reporter, Counter: counter})
	pool.Start()
	defer pool.Close()

	pool.Submit(Delivery{DeliveryID: 1, EventID: "evt_ok", Route: "/gh", TargetURL: okSrv.URL, Body: []byte("x"), Timeout: 2 * time.Second, RetryMax: 3})
	pool.Submit(Delivery{DeliveryID: 2, EventID: "evt_fail", Route: "/gl", TargetURL: failSrv.URL, Body: []byte("x"), Timeout: 2 * time.Second, RetryMax: 3})

	reporter.waitFor(t, 2)

	if got := counter.deliveredCount("/gh", okSrv.URL); got != 1 {
		t.Errorf("delivered count for (/gh, ok target) = %d, want 1", got)
	}
	if got := counter.failedCount("/gl", failSrv.URL); got != 1 {
		t.Errorf("failed count for (/gl, failing target) = %d, want 1", got)
	}
	// A success is not also a failure, and the route is part of the key: the failing
	// target's tally must not land under the delivered target's (route, URL).
	if got := counter.failedCount("/gh", okSrv.URL); got != 0 {
		t.Errorf("failed count for the delivered target = %d, want 0", got)
	}
	if got := counter.deliveredCount("/gl", failSrv.URL); got != 0 {
		t.Errorf("delivered count for the failing target = %d, want 0", got)
	}
}

// TestFanout_CountsAccumulateAcrossAttempts is the ADR's headline case: a target that
// fails twice before succeeding tallies two failures and one delivery. A single worker
// with one in-flight submission at a time keeps the target's 500, 500, 200 sequence
// deterministic, standing in for the replay worker re-submitting a rescheduled row.
func TestFanout_CountsAccumulateAcrossAttempts(t *testing.T) {
	var hits atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	reporter := newRecordingReporter(3)
	counter := newRecordingCounter()
	pool := NewPool(PoolConfig{Workers: 1, Reporter: reporter, Counter: counter})
	pool.Start()
	defer pool.Close()

	// RetryMax 5 keeps the two failures as reschedules, not parks; wait for each pass's
	// outcome before submitting the next so the attempts hit the target in order.
	for attempt := range 3 {
		pool.Submit(Delivery{DeliveryID: 1, EventID: "evt_retry", Route: "/r", TargetURL: target.URL, Body: []byte("x"), Timeout: 2 * time.Second, RetryMax: 5, Attempts: attempt})
		reporter.waitFor(t, 1)
	}

	if got := counter.deliveredCount("/r", target.URL); got != 1 {
		t.Errorf("delivered = %d, want 1", got)
	}
	if got := counter.failedCount("/r", target.URL); got != 2 {
		t.Errorf("failed = %d, want 2 (every failed attempt counts)", got)
	}
}
