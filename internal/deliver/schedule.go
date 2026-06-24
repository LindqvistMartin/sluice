package deliver

import (
	"math/rand/v2"
	"time"
)

// Outcome is what the pool does with a delivery after a failed attempt.
type Outcome int

const (
	// OutcomeReschedule means the delivery still has budget and should be retried at a
	// later next-attempt time.
	OutcomeReschedule Outcome = iota
	// OutcomePark means the delivery has spent its budget and should be dead-lettered.
	OutcomePark
)

// schedule decides what happens after a failed attempt. attempts is the new,
// post-increment count (>= 1). It parks once the count exceeds the retry budget;
// otherwise it reschedules at now plus a full-jitter backoff drawn from the new
// count. The boundary reproduces the earlier in-process loop exactly: that loop made
// RetryMax+1 attempts before giving up, so parking on attempts > RetryMax yields the
// same total across passes.
func schedule(attempts, retryMax int, now time.Time, base, cap time.Duration, rng *rand.Rand) (Outcome, time.Time) {
	if attempts > retryMax {
		return OutcomePark, time.Time{}
	}
	return OutcomeReschedule, now.Add(Backoff(attempts-1, base, cap, rng))
}
