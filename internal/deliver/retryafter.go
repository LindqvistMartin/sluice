package deliver

import (
	"net/http"
	"strconv"
	"time"
)

// noRetryHint is what attempt reports when a retryable response carried no usable
// Retry-After, so the scheduler keeps its jittered backoff. A real hint is a delay of
// zero or more, so a negative value is an unambiguous "no hint".
const noRetryHint = time.Duration(-1)

// honoursRetryAfter reports whether a status is one whose Retry-After is worth obeying.
// Only 429 (Too Many Requests) and 503 (Service Unavailable) are: each is the target
// explicitly asking the client to slow down or come back later, the two statuses RFC 9110
// pairs the header with. The other retryable failures — a 5xx that is not 503, a 408, or a
// transport error — make no such promise, so they keep the generic backoff.
func honoursRetryAfter(status int) bool {
	return status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable
}

// parseRetryAfter reads an RFC 9110 Retry-After value relative to now. The header takes one
// of two forms: delta-seconds (a non-negative integer) or an HTTP-date. It returns the
// delay until the target says a retry is worthwhile and ok=true when a valid value was
// present; a missing, malformed, or negative value returns ok=false so the caller falls
// back to its backoff. A date already in the past yields a zero delay — the stated time has
// arrived — rather than a negative one.
func parseRetryAfter(h http.Header, now time.Time) (time.Duration, bool) {
	v := h.Get("Retry-After")
	if v == "" {
		return 0, false
	}
	// delta-seconds is the common form, the one a load balancer emits under pressure.
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			return 0, false
		}
		return time.Duration(secs) * time.Second, true
	}
	// HTTP-date: accept the three formats http.ParseTime handles and measure from now.
	if t, err := http.ParseTime(v); err == nil {
		if d := t.Sub(now); d > 0 {
			return d, true
		}
		return 0, true
	}
	return 0, false
}
