package deliver

import (
	"math/rand/v2"
	"time"
)

// Backoff returns a randomized delay for a zero-based retry attempt using
// exponential backoff with full jitter: a value drawn uniformly from
// [0, min(cap, base*2^attempt)). The caller supplies the random source so the
// function stays deterministic under test and free of shared global state.
//
// rng must not be shared across goroutines: rand.Rand is not safe for concurrent
// use, so each worker should own its source.
func Backoff(attempt int, base, cap time.Duration, rng *rand.Rand) time.Duration {
	bound := ceiling(attempt, base, cap)
	if bound <= 0 {
		return 0
	}
	return time.Duration(rng.Int64N(int64(bound)))
}

// ceiling computes min(cap, base*2^attempt), doubling iteratively so a large
// attempt count clamps to cap instead of overflowing int64.
func ceiling(attempt int, base, cap time.Duration) time.Duration {
	if base <= 0 || cap <= 0 || attempt < 0 {
		return 0
	}

	delay := base
	for i := 0; i < attempt; i++ {
		if delay >= cap {
			return cap
		}
		delay *= 2
		if delay <= 0 { // wrapped negative: int64 overflow
			return cap
		}
	}
	if delay > cap {
		return cap
	}
	return delay
}
