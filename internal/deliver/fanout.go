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

	// DefaultMaxRetryAfter caps how far an honoured Retry-After can defer a retry. The
	// header is the target's number, not ours, so it is trusted further than the backoff
	// — but only this far, which is what stops a hostile or fat-fingered value
	// (Retry-After: 86400) from parking a row for a day. It is deliberately larger than
	// BackoffCap: a retry is durable, so the row waits on disk rather than holding a
	// worker, and real 429/503 windows run to minutes, not the backoff's seconds.
	DefaultMaxRetryAfter = 5 * time.Minute

	defaultWorkers = 8
)

// Reporter records the outcome of a delivery so the durable store stays in sync.
// A *dlq.Store satisfies it structurally, so the pool reports results without
// importing the store.
type Reporter interface {
	MarkDelivered(ctx context.Context, eventID string, deliveryID int64) error
	Reschedule(ctx context.Context, deliveryID int64, attempts int, lastErr string, nextAttemptAt int64) error
	Park(ctx context.Context, deliveryID int64, attempts int, lastErr string) error
}

// Counter tallies per-target delivery outcomes for the metrics endpoint. A
// *metrics.Counters satisfies it structurally, so the pool counts without importing
// the metrics package, the same way Reporter keeps the store at arm's length.
type Counter interface {
	IncDelivered(route, target string)
	IncFailed(route, target string)
}

// noopCounter is the default when metrics are disabled: the tallies only matter when
// the exposition is served, so an unconfigured pool keeps its delivery path free of them.
type noopCounter struct{}

func (noopCounter) IncDelivered(string, string) {}
func (noopCounter) IncFailed(string, string)    {}

// Delivery is one attempt to push one event's body to one target. It is the sole
// input to the pool, produced by the replay worker from a leased row, so the pool
// has one input contract regardless of why the row became due.
//
// Body and Headers are read-only to the pool and may be shared across the several
// deliveries of one event; workers must not mutate them.
type Delivery struct {
	DeliveryID int64
	EventID    string
	Route      string // owning route path; with TargetURL it is the per-target metric key
	TargetURL  string
	Body       []byte
	Headers    http.Header
	Timeout    time.Duration
	RetryMax   int
	Attempts   int // attempts already recorded before this pass; 0 for a fresh row
}

// PoolConfig configures a Pool. The zero value of each field falls back to a sane
// default, so only Reporter is required for production use.
type PoolConfig struct {
	Workers       int
	Client        *http.Client
	Reporter      Reporter
	Counter       Counter
	BackoffBase   time.Duration
	BackoffCap    time.Duration
	MaxRetryAfter time.Duration
	Logger        *slog.Logger
	Seed          uint64
}

// Pool fans deliveries out to downstream targets across a fixed set of workers. Each
// submission is a single delivery attempt; the worker reports the outcome —
// delivered, rescheduled for a later pass, or parked — and the replay worker drives
// retries by re-submitting rows as they come due.
type Pool struct {
	in       chan Delivery
	workers  int
	client   *http.Client
	reporter Reporter
	counter  Counter

	backoffBase   time.Duration
	backoffCap    time.Duration
	maxRetryAfter time.Duration
	log           *slog.Logger
	seed          uint64

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
		// X-Sluice-Event-Id header to an unintended host. Since we will not follow it,
		// a 3xx is a permanent failure and parks at once rather than retrying.
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
	maxRA := cfg.MaxRetryAfter
	if maxRA <= 0 {
		maxRA = DefaultMaxRetryAfter
	}
	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	counter := cfg.Counter
	if counter == nil {
		counter = noopCounter{}
	}
	return &Pool{
		in:            make(chan Delivery, workers*4),
		workers:       workers,
		client:        client,
		reporter:      cfg.Reporter,
		counter:       counter,
		backoffBase:   base,
		backoffCap:    bcap,
		maxRetryAfter: maxRA,
		log:           log,
		seed:          cfg.Seed,
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

// Close stops accepting deliveries and waits for in-flight attempts to finish (each
// bounded by its per-target timeout), so every worker reports its outcome into the
// store before the caller closes it. Safe to call twice.
func (p *Pool) Close() {
	p.closeOnce.Do(func() {
		close(p.in)
	})
	p.wg.Wait()
}

// deliver makes one attempt at the target and reports the outcome: delivered on a 2xx,
// rescheduled for a later pass on a retryable failure with budget left, or parked —
// either because the budget is spent or because the failure is permanent, a status the
// target will keep returning (classified by attempt). Retries are durable — the replay
// worker re-submits a rescheduled row when it next comes due — so a single submission is
// a single HTTP attempt. Outcomes are reported on a fresh context, never a shutdown one:
// the store closes strictly after the pool, so the final state is always recorded.
func (p *Pool) deliver(d Delivery, rng *rand.Rand) {
	result, retryAfter, errStr := p.attempt(d)
	if result == attemptDelivered {
		p.counter.IncDelivered(d.Route, d.TargetURL)
		if err := p.reporter.MarkDelivered(context.Background(), d.EventID, d.DeliveryID); err != nil {
			p.log.Error("report delivered", "event", d.EventID, "delivery", d.DeliveryID, "err", err)
		}
		return
	}

	// Count every failed attempt, not just the terminal park, so the metric reflects
	// retry pressure: a row that fails twice then succeeds is two failures, one delivery.
	p.counter.IncFailed(d.Route, d.TargetURL)
	attempts := d.Attempts + 1

	// A permanent failure cannot be fixed by waiting — the target rejects the request the
	// same way on every pass — so it parks now instead of spending the rest of the retry
	// budget. Parked, not dropped: the row stays in the DLQ to read.
	if result == attemptPermanent {
		p.park(d.DeliveryID, attempts, errStr)
		return
	}

	// An honoured Retry-After paces this one retry in place of the jittered backoff,
	// bounded by maxRetryAfter so a target cannot defer the row indefinitely. noRetryHint
	// is negative and so never trips the clamp, leaving the backoff in charge.
	override := retryAfter
	if override > p.maxRetryAfter {
		override = p.maxRetryAfter
	}
	switch outcome, nextAt := schedule(attempts, d.RetryMax, time.Now(), p.backoffBase, p.backoffCap, rng, override); outcome {
	case OutcomePark:
		p.park(d.DeliveryID, attempts, errStr)
	default:
		if err := p.reporter.Reschedule(context.Background(), d.DeliveryID, attempts, errStr, nextAt.UnixMilli()); err != nil {
			p.log.Error("report rescheduled", "delivery", d.DeliveryID, "err", err)
		}
	}
}

// park marks a delivery dead — the terminal state for a row that will not be retried,
// whether its budget is spent or its status is permanently rejecting. The row is parked,
// not deleted, so an operator can still read why it died.
func (p *Pool) park(deliveryID int64, attempts int, lastErr string) {
	if err := p.reporter.Park(context.Background(), deliveryID, attempts, lastErr); err != nil {
		p.log.Error("report parked", "delivery", deliveryID, "err", err)
	}
}

// attempt performs a single POST to the target, bounded by the target's timeout, and
// classifies the result. A 2xx is delivered; any other status is a retryable or permanent
// failure (see classify). A transport error carries no status and is treated as
// retryable — the next pass may reach a target that has recovered. The second return is the
// next-retry delay the target asked for: a parsed Retry-After when the status is one that
// carries it (429, 503), or noRetryHint otherwise, which leaves the caller's backoff in
// charge.
func (p *Pool) attempt(d Delivery) (attemptOutcome, time.Duration, string) {
	ctx, cancel := context.WithTimeout(context.Background(), d.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.TargetURL, bytes.NewReader(d.Body))
	if err != nil {
		return attemptRetryable, noRetryHint, fmt.Sprintf("build request: %v", err)
	}
	copyHeaders(req.Header, d.Headers)
	req.Header.Set("X-Sluice-Event-Id", d.EventID)

	resp, err := p.client.Do(req)
	if err != nil {
		return attemptRetryable, noRetryHint, err.Error()
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body) // drain so the connection can be reused

	result := classify(resp.StatusCode)
	if result == attemptDelivered {
		return attemptDelivered, noRetryHint, ""
	}

	// Only 429 and 503 carry a Retry-After worth obeying; for every other retryable status
	// the hint stays absent and the scheduler keeps its backoff.
	hint := noRetryHint
	if result == attemptRetryable && honoursRetryAfter(resp.StatusCode) {
		if ra, ok := parseRetryAfter(resp.Header, time.Now()); ok {
			hint = ra
		}
	}
	return result, hint, fmt.Sprintf("status %d", resp.StatusCode)
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
