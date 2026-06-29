package deliver

import "net/http"

// attemptOutcome is how a single delivery attempt resolved. It drives the pool's next
// move: deliver, retry within budget, or park immediately. It is distinct from Outcome,
// which is only the retry-versus-park choice once a failure is already known retryable.
type attemptOutcome int

const (
	// attemptDelivered is a 2xx: the target accepted the event.
	attemptDelivered attemptOutcome = iota
	// attemptRetryable is a failure that may clear on its own — a 5xx, a 429 or 408, or a
	// transport error — so it is worth another pass while the retry budget lasts.
	attemptRetryable
	// attemptPermanent is a failure an identical retry earns the same answer to — a 3xx
	// we do not follow, or a 4xx other than 408/429 — so retrying only burns budget. It
	// parks.
	attemptPermanent
)

// classify maps an HTTP status code to an attemptOutcome. A failure is permanent when the
// target answered with a status it will keep returning: a 4xx other than the retry-worthy
// 408 and 429 is a rejection that an identical retry gets the same answer to, and a 3xx is
// a redirect the client deliberately does not follow. The transient failures stay
// retryable — server errors (5xx), rate limiting (429), and request timeout (408).
// Transport errors carry no status and are handled by the caller as retryable.
func classify(status int) attemptOutcome {
	switch {
	case status >= 200 && status < 300:
		return attemptDelivered
	case status == http.StatusRequestTimeout, status == http.StatusTooManyRequests:
		return attemptRetryable
	case status >= 500 && status < 600:
		return attemptRetryable
	default:
		return attemptPermanent
	}
}
