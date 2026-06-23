package ingest

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"

	"github.com/LindqvistMartin/sluice/internal/config"
	"github.com/LindqvistMartin/sluice/internal/route"
)

// Persister durably stores an accepted event before the inbound request is
// acknowledged. The DLQ-backed implementation arrives in a later iteration; until
// a persister is wired the handler replies 503 rather than acknowledging data it
// cannot store.
type Persister interface {
	Persist(ctx context.Context, ev Event) error
}

// Event is a webhook accepted for delivery.
type Event struct {
	Route   string
	Headers http.Header
	Body    []byte
	Targets []config.Target
}

// Handler accepts inbound webhooks: it rate-limits by client IP, matches a route,
// enforces the body-size cap, and hands the event to the persister.
type Handler struct {
	matcher      *route.Matcher
	limiter      *IPLimiter
	persister    Persister
	maxBodyBytes int64
	log          *slog.Logger
}

// Options configures a Handler.
type Options struct {
	Matcher      *route.Matcher
	Limiter      *IPLimiter
	Persister    Persister
	MaxBodyBytes int64
	Logger       *slog.Logger
}

// New builds a Handler from opts.
func New(opts Options) *Handler {
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Handler{
		matcher:      opts.Matcher,
		limiter:      opts.Limiter,
		persister:    opts.Persister,
		maxBodyBytes: opts.MaxBodyBytes,
		log:          log,
	}
}

const retryAfter = "1"

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Rate-limit first so every request — including rejected methods and unknown
	// routes — is metered against the per-IP budget.
	if !h.limiter.allow(clientIP(r)) {
		w.Header().Set("Retry-After", retryAfter)
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		return
	}

	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	matched, ok := h.matcher.Match(r)
	if !ok {
		http.Error(w, "no matching route", http.StatusNotFound)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, h.maxBodyBytes)
	defer func() { _ = r.Body.Close() }()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "could not read request body", http.StatusBadRequest)
		return
	}

	if h.persister == nil {
		// Persistence is not wired yet; never acknowledge data we cannot store.
		w.Header().Set("Retry-After", retryAfter)
		http.Error(w, "persistence unavailable", http.StatusServiceUnavailable)
		return
	}

	// r.Header is owned by the server and reused after this returns, and the body
	// fans out on goroutines that outlive the request, so clone the headers here.
	// matched.Fanout is process-lifetime config and needs no copy.
	ev := Event{Route: matched.Path, Headers: r.Header.Clone(), Body: body, Targets: matched.Fanout}
	if err := h.persister.Persist(r.Context(), ev); err != nil {
		h.log.Error("persist failed", "route", matched.Path, "err", err)
		w.Header().Set("Retry-After", retryAfter)
		http.Error(w, "could not persist event", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// clientIP returns the host portion of the request's remote address, used as the
// rate-limit key. Trusted-proxy / X-Forwarded-For handling is out of scope here.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
