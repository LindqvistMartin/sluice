package deliver

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"
)

// DefaultBackoffBase and DefaultBackoffCap bound the retry delay in production.
// Tests pass their own bounds through PoolConfig to keep retry timing sub-millisecond.
const (
	DefaultBackoffBase = 100 * time.Millisecond
	DefaultBackoffCap  = 30 * time.Second

	defaultWorkers = 8
)

// Reporter records the outcome of a delivery so the durable store stays in sync.
// A *dlq.Store satisfies it structurally, so the pool reports results without
// importing the store.
type Reporter interface {
	MarkDelivered(ctx context.Context, eventID string, deliveryID int64) error
	MarkFailed(ctx context.Context, deliveryID int64, attempts int, lastErr string) error
}

// Delivery is one attempt to push one event's body to one target. It is the sole
// input to the pool, produced identically by fresh ingest today and the replay
// worker later, so the pool has one input contract regardless of source.
//
// Body and Headers are read-only to the pool and may be shared across the several
// deliveries of one event; workers must not mutate them.
type Delivery struct {
	DeliveryID int64
	EventID    string
	TargetURL  string
	Body       []byte
	Headers    http.Header
	Timeout    time.Duration
	RetryMax   int
}

// PoolConfig configures a Pool. The zero value of each field falls back to a sane
// default, so only Reporter is required for production use.
type PoolConfig struct {
	Workers     int
	Client      *http.Client
	Reporter    Reporter
	BackoffBase time.Duration
	BackoffCap  time.Duration
	Logger      *slog.Logger
	Seed        uint64
}

// Pool fans deliveries out to downstream targets across a fixed set of workers,
// retrying each target in process with exponential backoff up to its budget.
type Pool struct {
	in       chan Delivery
	quit     chan struct{}
	workers  int
	client   *http.Client
	reporter Reporter

	backoffBase time.Duration
	backoffCap  time.Duration
	log         *slog.Logger
	seed        uint64

	wg        sync.WaitGroup
	closeOnce sync.Once
}

// NewClient builds the shared HTTP client used for every outbound delivery. It
// carries no client-level timeout: each attempt is bounded by its own per-target
// context deadline instead, so one slow target cannot borrow another's budget.
func NewClient() *http.Client {
	return &http.Client{
		// Do not follow redirects: a 3xx from a target is not a delivery, and an
		// attacker-controlled Location would let a target bounce the body and the
		// X-Sluice-Event-Id header to an unintended host. A 3xx then falls through
		// to the retryable-failure path like any other non-2xx.
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Transport: &http.Transport{
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   10,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: time.Second,
		},
	}
}

// NewPool builds a Pool from cfg, applying defaults for any unset field.
func NewPool(cfg PoolConfig) *Pool {
	workers := cfg.Workers
	if workers <= 0 {
		workers = defaultWorkers
	}
	client := cfg.Client
	if client == nil {
		client = NewClient()
	}
	base := cfg.BackoffBase
	if base <= 0 {
		base = DefaultBackoffBase
	}
	bcap := cfg.BackoffCap
	if bcap <= 0 {
		bcap = DefaultBackoffCap
	}
	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Pool{
		in:          make(chan Delivery, workers*4),
		quit:        make(chan struct{}),
		workers:     workers,
		client:      client,
		reporter:    cfg.Reporter,
		backoffBase: base,
		backoffCap:  bcap,
		log:         log,
		seed:        cfg.Seed,
	}
}

// Start launches the worker goroutines. Each owns its own random source so the
// jittered backoff stays deterministic per worker and free of shared state.
func (p *Pool) Start() {
	for i := range p.workers {
		rng := rand.New(rand.NewPCG(p.seed, uint64(i)+1))
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for d := range p.in {
				p.deliver(d, rng)
			}
		}()
	}
}

// Submit enqueues a delivery. It is called after the event is durably persisted,
// so a full queue applies backpressure to inbound rather than dropping work.
func (p *Pool) Submit(d Delivery) {
	p.in <- d
}

// Close stops accepting deliveries, abandons any pending backoff waits, and waits
// for in-flight attempts to finish (each bounded by its per-target timeout).
// Abandoned retries leave their rows pending in the store. Safe to call twice.
func (p *Pool) Close() {
	p.closeOnce.Do(func() {
		close(p.quit)
		close(p.in)
	})
	p.wg.Wait()
}

// deliver attempts one target, retrying with backoff up to RetryMax, and reports
// the terminal outcome. A shutdown signalled on quit abandons the retry wait and
// leaves the row pending for later redelivery.
func (p *Pool) deliver(d Delivery, rng *rand.Rand) {
	var lastErr string
	attempts := 0
	for attempt := 0; attempt <= d.RetryMax; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(Backoff(attempt-1, p.backoffBase, p.backoffCap, rng)):
			case <-p.quit:
				return
			}
		}
		attempts = attempt + 1

		ok, errStr := p.attempt(d)
		if ok {
			// Report on a fresh context, never a shutdown one: the store is closed
			// strictly after the pool, so the final state is always recorded.
			if err := p.reporter.MarkDelivered(context.Background(), d.EventID, d.DeliveryID); err != nil {
				p.log.Error("report delivered", "event", d.EventID, "delivery", d.DeliveryID, "err", err)
			}
			return
		}
		lastErr = errStr
	}

	if err := p.reporter.MarkFailed(context.Background(), d.DeliveryID, attempts, lastErr); err != nil {
		p.log.Error("report failed", "delivery", d.DeliveryID, "err", err)
	}
}

// attempt performs a single POST to the target, bounded by the target's timeout.
// It reports success only on a 2xx; any other status or a transport error is a
// retryable failure (per-status-code classification is a later refinement).
func (p *Pool) attempt(d Delivery) (ok bool, errStr string) {
	ctx, cancel := context.WithTimeout(context.Background(), d.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.TargetURL, bytes.NewReader(d.Body))
	if err != nil {
		return false, fmt.Sprintf("build request: %v", err)
	}
	copyHeaders(req.Header, d.Headers)
	req.Header.Set("X-Sluice-Event-Id", d.EventID)

	resp, err := p.client.Do(req)
	if err != nil {
		return false, err.Error()
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body) // drain so the connection can be reused

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return true, ""
	}
	return false, fmt.Sprintf("status %d", resp.StatusCode)
}

// hopByHop are connection-scoped headers that belong to the inbound hop, not the
// forwarded request. Content-Length and Host are also skipped: the body reader
// sets the length, and the target's host is its own.
var hopByHop = map[string]bool{
	"Connection":          true,
	"Content-Length":      true,
	"Host":                true,
	"Keep-Alive":          true,
	"Proxy-Authenticate":  true,
	"Proxy-Authorization": true,
	"Te":                  true,
	"Trailer":             true,
	"Transfer-Encoding":   true,
	"Upgrade":             true,
}

// sensitive headers carry the inbound sender's credentials. Fan-out copies one
// source's request to several unrelated targets, so forwarding these would hand
// every target a secret meant only for sluice; a target authenticates by its own
// URL or signature instead. A pass-through mode, if ever needed, belongs behind a
// config flag rather than the default.
var sensitive = map[string]bool{
	"Authorization": true,
	"Cookie":        true,
}

func copyHeaders(dst, src http.Header) {
	for k, vs := range src {
		if hopByHop[k] || sensitive[k] {
			continue
		}
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
}
