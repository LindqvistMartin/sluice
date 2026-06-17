package ingest

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/LindqvistMartin/sluice/internal/config"
	"github.com/LindqvistMartin/sluice/internal/route"
)

func generousRate() config.Rate {
	return config.Rate{Count: 1000, Window: time.Minute}
}

func testHandler(maxBody int64, r config.Rate) *Handler {
	matcher := route.New(&config.Config{
		Routes: []config.Route{
			{Path: "/hook", Fanout: []config.Target{{URL: "http://flare/in"}}},
		},
	})
	return New(Options{
		Matcher:      matcher,
		Limiter:      NewIPLimiter(r, time.Minute),
		Persister:    nil, // persistence lands in a later iteration
		MaxBodyBytes: maxBody,
	})
}

func postResp(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	return resp
}

func post(t *testing.T, url, body string) int {
	t.Helper()
	resp := postResp(t, url, body)
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func TestHandler_RateLimit429(t *testing.T) {
	srv := httptest.NewServer(testHandler(1<<20, config.Rate{Count: 1, Window: time.Minute}))
	defer srv.Close()

	// The first POST spends the single token and reaches the (unwired) persister,
	// so it gets 503; the second POST from the same client is rate limited.
	if code := post(t, srv.URL+"/hook", "x"); code != http.StatusServiceUnavailable {
		t.Fatalf("first status = %d, want 503", code)
	}
	resp := postResp(t, srv.URL+"/hook", "x")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second status = %d, want 429", resp.StatusCode)
	}
	if got := resp.Header.Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After = %q, want 1", got)
	}
}

func TestHandler_BodyCapBoundary(t *testing.T) {
	srv := httptest.NewServer(testHandler(16, generousRate()))
	defer srv.Close()

	tests := []struct {
		name string
		body string
		want int
	}{
		{"empty", "", http.StatusServiceUnavailable}, // read succeeds, falls through to the nil persister
		{"at the limit", strings.Repeat("a", 16), http.StatusServiceUnavailable},
		{"over the limit", strings.Repeat("a", 17), http.StatusRequestEntityTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if code := post(t, srv.URL+"/hook", tt.body); code != tt.want {
				t.Errorf("body %d bytes: status = %d, want %d", len(tt.body), code, tt.want)
			}
		})
	}
}

func TestHandler_UnknownRoute404(t *testing.T) {
	srv := httptest.NewServer(testHandler(1<<20, generousRate()))
	defer srv.Close()

	if code := post(t, srv.URL+"/nope", "x"); code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", code)
	}
}

func TestHandler_NoPersister503(t *testing.T) {
	srv := httptest.NewServer(testHandler(1<<20, generousRate()))
	defer srv.Close()

	resp := postResp(t, srv.URL+"/hook", "x")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	if got := resp.Header.Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After = %q, want 1", got)
	}
}

func TestHandler_WrongMethod405(t *testing.T) {
	srv := httptest.NewServer(testHandler(1<<20, generousRate()))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/hook")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", resp.StatusCode)
	}
	if got := resp.Header.Get("Allow"); got != "POST" {
		t.Errorf("Allow = %q, want POST", got)
	}
}

func TestClientIP(t *testing.T) {
	tests := []struct {
		remote string
		want   string
	}{
		{"1.2.3.4:55555", "1.2.3.4"},
		{"[2001:db8::1]:443", "2001:db8::1"},
		{"203.0.113.7", "203.0.113.7"}, // no port: fall back to the raw value
		{"", ""},
	}
	for _, tt := range tests {
		if got := clientIP(&http.Request{RemoteAddr: tt.remote}); got != tt.want {
			t.Errorf("clientIP(%q) = %q, want %q", tt.remote, got, tt.want)
		}
	}
}
