package deliver

import (
	"net/http"
	"testing"
)

// TestClassify pins the status-to-outcome mapping that decides whether a failed delivery
// is retried or parked: a 2xx delivers, 5xx and the transient 408/429 retry, and every
// other status — 3xx and the remaining 4xx — is permanent.
func TestClassify(t *testing.T) {
	tests := []struct {
		name   string
		status int
		want   attemptOutcome
	}{
		{"200 ok", http.StatusOK, attemptDelivered},
		{"201 created", http.StatusCreated, attemptDelivered},
		{"204 no content", http.StatusNoContent, attemptDelivered},
		{"199 below 2xx", 199, attemptPermanent},
		{"300 multiple choices", http.StatusMultipleChoices, attemptPermanent},
		{"301 moved permanently", http.StatusMovedPermanently, attemptPermanent},
		{"302 found", http.StatusFound, attemptPermanent},
		{"307 temporary redirect", http.StatusTemporaryRedirect, attemptPermanent},
		{"400 bad request", http.StatusBadRequest, attemptPermanent},
		{"401 unauthorized", http.StatusUnauthorized, attemptPermanent},
		{"403 forbidden", http.StatusForbidden, attemptPermanent},
		{"404 not found", http.StatusNotFound, attemptPermanent},
		{"409 conflict", http.StatusConflict, attemptPermanent},
		{"410 gone", http.StatusGone, attemptPermanent},
		{"422 unprocessable entity", http.StatusUnprocessableEntity, attemptPermanent},
		{"408 request timeout", http.StatusRequestTimeout, attemptRetryable},
		{"429 too many requests", http.StatusTooManyRequests, attemptRetryable},
		{"500 internal server error", http.StatusInternalServerError, attemptRetryable},
		{"502 bad gateway", http.StatusBadGateway, attemptRetryable},
		{"503 service unavailable", http.StatusServiceUnavailable, attemptRetryable},
		{"504 gateway timeout", http.StatusGatewayTimeout, attemptRetryable},
		{"599 above the known 5xx", 599, attemptRetryable},
		{"600 beyond 5xx", 600, attemptPermanent},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classify(tt.status); got != tt.want {
				t.Errorf("classify(%d) = %d, want %d", tt.status, got, tt.want)
			}
		})
	}
}
