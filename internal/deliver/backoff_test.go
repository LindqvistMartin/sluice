package deliver

import (
	"math/rand/v2"
	"testing"
	"time"
)

// TestBackoff_BoundsExponential checks the jittered delay stays below the
// exponential ceiling for attempts whose ceiling is still under cap. The bound
// is computed by an independent shift so it does not lean on the implementation.
func TestBackoff_BoundsExponential(t *testing.T) {
	const base = 100 * time.Millisecond
	const cap = 10 * time.Second
	rng := rand.New(rand.NewPCG(42, 1024))

	for _, attempt := range []int{0, 1, 2, 5} {
		bound := base << attempt // 100ms..3.2s, all below cap
		for range 1000 {
			got := Backoff(attempt, base, cap, rng)
			if got < 0 || got >= bound {
				t.Fatalf("attempt %d: got %v, want within [0,%v)", attempt, got, bound)
			}
		}
	}
}

// TestBackoff_ClampsToCap checks large attempts clamp to cap without overflow.
func TestBackoff_ClampsToCap(t *testing.T) {
	const base = 100 * time.Millisecond
	const cap = 10 * time.Second
	rng := rand.New(rand.NewPCG(99, 7))

	for _, attempt := range []int{10, 20, 63, 1000} {
		for range 1000 {
			got := Backoff(attempt, base, cap, rng)
			if got < 0 || got >= cap {
				t.Fatalf("attempt %d: got %v, want within [0,%v)", attempt, got, cap)
			}
		}
	}
}

// TestBackoff_Degenerate checks a zero base or cap yields no delay rather than
// panicking on a non-positive random bound.
func TestBackoff_Degenerate(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	cases := []struct {
		name      string
		base, cap time.Duration
	}{
		{"zero base", 0, 10 * time.Second},
		{"zero cap", 100 * time.Millisecond, 0},
		{"both zero", 0, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, attempt := range []int{0, 1, 5} {
				if got := Backoff(attempt, c.base, c.cap, rng); got != 0 {
					t.Errorf("attempt %d: got %v, want 0", attempt, got)
				}
			}
		})
	}
}

// TestBackoff_FullJitterSpansLowHalf checks the jitter is full, not equal: draws
// reach below half the ceiling, which an equal-jitter (cap/2 + rand(cap/2)) scheme
// never would.
func TestBackoff_FullJitterSpansLowHalf(t *testing.T) {
	const base = 100 * time.Millisecond
	const cap = 10 * time.Second
	const attempt = 5
	bound := base << attempt // 3.2s, below cap

	rng := rand.New(rand.NewPCG(7, 11))
	var lo, hi time.Duration = bound, 0
	for range 2000 {
		d := Backoff(attempt, base, cap, rng)
		lo = min(lo, d)
		hi = max(hi, d)
	}
	if lo >= bound/2 {
		t.Errorf("min draw %v not below half of %v; jitter is not full", lo, bound)
	}
	if hi <= bound/2 {
		t.Errorf("max draw %v not above half of %v", hi, bound)
	}
}

// TestBackoff_Deterministic checks the injected source makes the delay
// reproducible: identical seeds yield identical sequences.
func TestBackoff_Deterministic(t *testing.T) {
	const base = 100 * time.Millisecond
	const cap = 10 * time.Second
	a := rand.New(rand.NewPCG(5, 5))
	b := rand.New(rand.NewPCG(5, 5))

	for attempt := range 8 {
		if Backoff(attempt, base, cap, a) != Backoff(attempt, base, cap, b) {
			t.Fatalf("same seed produced different delay at attempt %d", attempt)
		}
	}
}
