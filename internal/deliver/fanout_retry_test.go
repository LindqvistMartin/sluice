package deliver

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// TestFanout_FailureReschedules checks that a failing attempt with budget remaining is
// reported as a reschedule — not a delivery or a park — and that the pool makes exactly
// one HTTP attempt per submission. Retrying the rescheduled row later is the replay
// worker's job, so the journey across passes is covered by the pipeline test in main.
func TestFanout_FailureReschedules(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	reporter := newRecordingReporter(1)
	pool := NewPool(PoolConfig{Workers: 1, Reporter: reporter})
	pool.Start()
	defer pool.Close()

	pool.Submit(Delivery{DeliveryID: 7, EventID: "evt_retry", TargetURL: srv.URL, Body: []byte("payload"), Timeout: 2 * time.Second, RetryMax: 3})
	reporter.waitFor(t, 1)

	if got := hits.Load(); got != 1 {
		t.Fatalf("target hit %d times, want 1 (one attempt per pass)", got)
	}
	if reporter.deliveredCount() != 0 {
		t.Errorf("delivered = %d, want 0", reporter.deliveredCount())
	}
	if reporter.parkedCount() != 0 {
		t.Errorf("parked = %d, want 0 (budget remains)", reporter.parkedCount())
	}
	if reporter.rescheduledCount() != 1 {
		t.Fatalf("rescheduled = %d, want 1", reporter.rescheduledCount())
	}
	if got := reporter.rescheduledAttempts(7); got != 1 {
		t.Errorf("recorded attempts = %d, want 1", got)
	}
}

// TestFanout_ParkAfterBudget drives the final pass of a delivery whose prior attempts
// already reached the budget: one more failure parks it as dead with the cumulative
// attempt count, rather than rescheduling it forever. This is the safety-relevant
// branch the happy and reschedule tests never reach.
func TestFanout_ParkAfterBudget(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	reporter := newRecordingReporter(1)
	pool := NewPool(PoolConfig{Workers: 1, Reporter: reporter})
	pool.Start()
	defer pool.Close()

	// Attempts:2 with RetryMax:2 makes this pass the (RetryMax+1)-th attempt.
	pool.Submit(Delivery{DeliveryID: 11, EventID: "evt_exhaust", TargetURL: srv.URL, Body: []byte("x"), Timeout: 2 * time.Second, RetryMax: 2, Attempts: 2})
	reporter.waitFor(t, 1)

	if reporter.deliveredCount() != 0 {
		t.Errorf("delivered = %d, want 0", reporter.deliveredCount())
	}
	if reporter.rescheduledCount() != 0 {
		t.Errorf("rescheduled = %d, want 0 (budget spent)", reporter.rescheduledCount())
	}
	if reporter.parkedCount() != 1 {
		t.Fatalf("parked = %d, want 1", reporter.parkedCount())
	}
	if got := reporter.parkedAttempts(11); got != 3 {
		t.Errorf("recorded attempts = %d, want 3 (RetryMax+1)", got)
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("target hit %d times, want 1", got)
	}
}
