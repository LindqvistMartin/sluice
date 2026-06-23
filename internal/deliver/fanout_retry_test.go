package deliver

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestFanout_RetryThenSuccess checks that a target failing once is retried with
// backoff and the eventual 2xx is reported as a single delivery, never a failure.
// Tiny backoff bounds keep the test well under a second; the assertions are on
// outcome and hit count, not timing, so jitter cannot flake it.
func TestFanout_RetryThenSuccess(t *testing.T) {
	const eventID = "evt_retry"

	var hits atomic.Int32
	var mu sync.Mutex
	var seenIDs []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seenIDs = append(seenIDs, r.Header.Get("X-Sluice-Event-Id"))
		mu.Unlock()
		if hits.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	reporter := newRecordingReporter(1)
	pool := NewPool(PoolConfig{
		Workers:     1,
		Reporter:    reporter,
		BackoffBase: time.Millisecond,
		BackoffCap:  5 * time.Millisecond,
	})
	pool.Start()
	defer pool.Close()

	pool.Submit(Delivery{DeliveryID: 7, EventID: eventID, TargetURL: srv.URL, Body: []byte("payload"), Timeout: 2 * time.Second, RetryMax: 3})

	reporter.waitFor(t, 1)

	if got := hits.Load(); got != 2 {
		t.Fatalf("target hit %d times, want 2 (one 500, one 200)", got)
	}
	if reporter.deliveredCount() != 1 {
		t.Errorf("delivered = %d, want 1", reporter.deliveredCount())
	}
	if reporter.failedCount() != 0 {
		t.Errorf("failed = %d, want 0", reporter.failedCount())
	}

	mu.Lock()
	defer mu.Unlock()
	for _, id := range seenIDs {
		if id != eventID {
			t.Errorf("X-Sluice-Event-Id = %q, want %q", id, eventID)
		}
	}
}

// TestFanout_ExhaustsToFailure drives a target that always fails through its full
// retry budget and checks the delivery is reported failed with the total attempt
// count, not delivered. This is the safety-relevant branch the happy/retry tests
// never reach.
func TestFanout_ExhaustsToFailure(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	reporter := newRecordingReporter(1)
	pool := NewPool(PoolConfig{
		Workers:     1,
		Reporter:    reporter,
		BackoffBase: time.Millisecond,
		BackoffCap:  5 * time.Millisecond,
	})
	pool.Start()
	defer pool.Close()

	pool.Submit(Delivery{DeliveryID: 11, EventID: "evt_exhaust", TargetURL: srv.URL, Body: []byte("x"), Timeout: 2 * time.Second, RetryMax: 2})
	reporter.waitFor(t, 1)

	if reporter.deliveredCount() != 0 {
		t.Errorf("delivered = %d, want 0", reporter.deliveredCount())
	}
	if reporter.failedCount() != 1 {
		t.Fatalf("failed = %d, want 1", reporter.failedCount())
	}
	if got := reporter.failedAttempts(11); got != 3 {
		t.Errorf("recorded attempts = %d, want 3 (RetryMax+1)", got)
	}
	if got := hits.Load(); got != 3 {
		t.Errorf("target hit %d times, want 3", got)
	}
}
