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
			outcome, next := schedule(tt.attempts, tt.retryMax, now, base, cap, rng, noRetryHint)
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

// TestScheduleHonoursOverride checks that a non-negative override (an honoured Retry-After)
// sets the next-attempt time verbatim rather than drawing a jittered backoff, that the
// budget check still wins so a hint cannot rescue an exhausted delivery, and that a zero
// override schedules an immediate retry.
func TestScheduleHonoursOverride(t *testing.T) {
	const (
		base = 100 * time.Millisecond
		cap  = 30 * time.Second
	)
	now := time.Unix(0, 0)
	rng := rand.New(rand.NewPCG(1, 2))

	// Within budget: the override is the exact offset, not a jittered draw.
	outcome, next := schedule(1, 3, now, base, cap, rng, 2*time.Second)
	if outcome != OutcomeReschedule {
		t.Fatalf("outcome = %v, want %v", outcome, OutcomeReschedule)
	}
	if want := now.Add(2 * time.Second); !next.Equal(want) {
		t.Errorf("next = %v, want %v (the override verbatim)", next, want)
	}

	// Budget still wins: an override does not rescue a delivery past its retry budget.
	outcome, next = schedule(4, 3, now, base, cap, rng, 2*time.Second)
	if outcome != OutcomePark {
		t.Errorf("outcome = %v, want %v (budget spent)", outcome, OutcomePark)
	}
	if !next.IsZero() {
		t.Errorf("park next = %v, want zero time", next)
	}

	// A zero override means retry now — a Retry-After of 0 or an already-past date.
	if _, next := schedule(1, 3, now, base, cap, rng, 0); !next.Equal(now) {
		t.Errorf("next = %v, want %v (immediate retry)", next, now)
	}
}
