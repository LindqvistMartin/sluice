package deliver

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestFanout_RetryAfterPacesReschedule checks the end-to-end honouring of Retry-After: a
// 429 carrying the header reschedules the next attempt at the delay the target asked for,
// not the sub-second jittered backoff a first failure would otherwise draw.
func TestFanout_RetryAfterPacesReschedule(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "2")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	reporter := newRecordingReporter(1)
	pool := NewPool(PoolConfig{Workers: 1, Reporter: reporter})
	pool.Start()
	defer pool.Close()

	before := time.Now()
	pool.Submit(Delivery{DeliveryID: 21, EventID: "evt_ra", TargetURL: srv.URL, Body: []byte("x"), Timeout: 2 * time.Second, RetryMax: 3})
	reporter.waitFor(t, 1)

	if reporter.rescheduledCount() != 1 {
		t.Fatalf("rescheduled = %d, want 1", reporter.rescheduledCount())
	}
	// The first failure's backoff tops out near base (100ms); a delay this far out can only
	// have come from the 2s Retry-After, so a generous window still proves it was honoured.
	delay := time.UnixMilli(reporter.rescheduledNextAt(21)).Sub(before)
	if delay < 1500*time.Millisecond || delay > 3*time.Second {
		t.Errorf("next attempt in %v, want ~2s from the Retry-After header", delay)
	}
}

// TestFanout_RetryAfterClampedToCap checks the bound on a hostile or absurd value: a 503
// asking for a full day is clamped to MaxRetryAfter, so the row is retried within the cap
// rather than parked off the schedule for a day.
func TestFanout_RetryAfterClampedToCap(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "86400") // one day
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	reporter := newRecordingReporter(1)
	pool := NewPool(PoolConfig{Workers: 1, Reporter: reporter, MaxRetryAfter: 30 * time.Second})
	pool.Start()
	defer pool.Close()

	before := time.Now()
	pool.Submit(Delivery{DeliveryID: 22, EventID: "evt_ra_clamp", TargetURL: srv.URL, Body: []byte("x"), Timeout: 2 * time.Second, RetryMax: 3})
	reporter.waitFor(t, 1)

	delay := time.UnixMilli(reporter.rescheduledNextAt(22)).Sub(before)
	// Clamped to the 30s cap: neither the day the target asked for nor the sub-second backoff.
	if delay < 29*time.Second || delay > 31*time.Second {
		t.Errorf("next attempt in %v, want it clamped to the 30s cap", delay)
	}
}

// TestFanout_RetryAfterIgnoredOnOtherStatus checks the scope of the feature: a 500 carries
// no honoured Retry-After even when the header is present, so the next attempt falls back to
// the jittered backoff rather than the target's number. Only 429 and 503 are obeyed.
func TestFanout_RetryAfterIgnoredOnOtherStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "99999")
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	reporter := newRecordingReporter(1)
	pool := NewPool(PoolConfig{Workers: 1, Reporter: reporter})
	pool.Start()
	defer pool.Close()

	before := time.Now()
	pool.Submit(Delivery{DeliveryID: 23, EventID: "evt_ra_ignored", TargetURL: srv.URL, Body: []byte("x"), Timeout: 2 * time.Second, RetryMax: 3})
	reporter.waitFor(t, 1)

	delay := time.UnixMilli(reporter.rescheduledNextAt(23)).Sub(before)
	// Backoff for a first failure is bounded by base (100ms); the 99999s header is ignored,
	// so the delay stays well under a second instead of clamping to the Retry-After cap.
	if delay > time.Second {
		t.Errorf("next attempt in %v, want the sub-second backoff (Retry-After ignored on 500)", delay)
	}
}
