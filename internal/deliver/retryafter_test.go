package deliver

import (
	"net/http"
	"testing"
	"time"
)

// TestParseRetryAfter pins the two RFC 9110 forms and the rejection of everything else: a
// delta-seconds value becomes that many seconds, an HTTP-date becomes the delay until it,
// a date already past becomes zero, and a missing, negative, or unparseable value reports
// no hint so the caller keeps its backoff.
func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	httpDate := func(d time.Duration) string {
		return now.Add(d).UTC().Format(http.TimeFormat)
	}

	tests := []struct {
		name     string
		value    string // the Retry-After header value; "" means the header is absent
		present  bool
		want     time.Duration
		wantOK   bool
		setEmpty bool // build the header without the key at all
	}{
		{name: "delta seconds", value: "120", want: 120 * time.Second, wantOK: true},
		{name: "zero seconds retries now", value: "0", want: 0, wantOK: true},
		{name: "negative seconds is no hint", value: "-5", want: 0, wantOK: false},
		{name: "future http-date", value: httpDate(90 * time.Second), want: 90 * time.Second, wantOK: true},
		{name: "past http-date clamps to zero", value: httpDate(-30 * time.Second), want: 0, wantOK: true},
		{name: "garbage is no hint", value: "soon", want: 0, wantOK: false},
		{name: "trailing junk after number is no hint", value: "120s", want: 0, wantOK: false},
		{name: "absent header is no hint", setEmpty: true, want: 0, wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := http.Header{}
			if !tt.setEmpty {
				h.Set("Retry-After", tt.value)
			}
			got, ok := parseRetryAfter(h, now)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && got != tt.want {
				t.Errorf("delay = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestHonoursRetryAfter pins the small set of statuses whose Retry-After is obeyed: 429 and
// 503 only. The other retryable statuses are retried on the standard backoff even if they
// carry the header, so the hint never overrides where it was not meant to.
func TestHonoursRetryAfter(t *testing.T) {
	tests := []struct {
		status int
		want   bool
	}{
		{http.StatusTooManyRequests, true},
		{http.StatusServiceUnavailable, true},
		{http.StatusRequestTimeout, false},
		{http.StatusInternalServerError, false},
		{http.StatusBadGateway, false},
		{http.StatusGatewayTimeout, false},
		{http.StatusOK, false},
		{http.StatusNotFound, false},
	}
	for _, tt := range tests {
		if got := honoursRetryAfter(tt.status); got != tt.want {
			t.Errorf("honoursRetryAfter(%d) = %v, want %v", tt.status, got, tt.want)
		}
	}
}
