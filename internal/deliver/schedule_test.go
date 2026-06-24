package deliver

import (
	"math/rand/v2"
	"testing"
	"time"
)

// TestSchedule pins the reschedule-vs-park boundary and the next-attempt window. The
// boundary must reproduce the pre-durable in-process loop's RetryMax+1 total attempts:
// a delivery reschedules while attempts <= RetryMax and parks once attempts exceed it.
func TestSchedule(t *testing.T) {
	const (
		base = 100 * time.Millisecond
		cap  = 30 * time.Second
	)
	now := time.Unix(0, 0)

	tests := []struct {
		name        string
		attempts    int
		retryMax    int
		wantOutcome Outcome
	}{
		{"first failure with budget", 1, 3, OutcomeReschedule},
		{"last failure within budget", 3, 3, OutcomeReschedule},
		{"one past budget parks", 4, 3, OutcomePark},
		{"zero budget parks on first failure", 1, 0, OutcomePark},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(1, 2))
			outcome, next := schedule(tt.attempts, tt.retryMax, now, base, cap, rng)
			if outcome != tt.wantOutcome {
				t.Fatalf("outcome = %v, want %v", outcome, tt.wantOutcome)
			}
			if outcome == OutcomePark {
				if !next.IsZero() {
					t.Errorf("park next = %v, want zero time", next)
				}
				return
			}
			// Reschedule: next is now plus a full-jitter delay in [0, ceiling).
			hi := now.Add(ceiling(tt.attempts-1, base, cap))
			if next.Before(now) || !next.Before(hi) {
				t.Errorf("next = %v, want in [%v, %v)", next, now, hi)
			}
		})
	}
}
