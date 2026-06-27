package replay

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LindqvistMartin/sluice/internal/deliver"
	"github.com/LindqvistMartin/sluice/internal/dlq"
)

// Store is the persistence the worker drives: it claims due deliveries under a lease
// and bounds the store by evicting the oldest events. A *dlq.Store satisfies it.
type Store interface {
	ClaimDue(ctx context.Context, now int64, leaseTTL time.Duration, limit int) ([]dlq.Claimed, error)
	Reschedule(ctx context.Context, deliveryID int64, attempts int, lastErr string, nextAttemptAt int64) error
	EvictOldest(ctx context.Context, maxBytes int64) (int, error)
}

// Submitter accepts a delivery for one attempt. A *deliver.Pool satisfies it.
type Submitter interface {
	Submit(d deliver.Delivery)
}

// TargetResolver recovers a claimed row's per-target Timeout and RetryMax from
// config, keyed by the event's route and the target URL. ok is false when the target
// is no longer configured.
type TargetResolver interface {
	Resolve(route, targetURL string) (timeout time.Duration, retryMax int, ok bool)
}

// Defaults applied for an unset Config field.
const (
	defaultInterval      = time.Second
	defaultLeaseTTL      = 60 * time.Second
	defaultEvictInterval = 30 * time.Second
	// defaultBatchSize matches the pool's default input buffer (workers*4) so a
	// claimed batch hands off without the worker blocking on a backlog while holding
	// leases on rows the pool cannot yet take.
	defaultBatchSize = 32

	// unresolvedBackoff is how far a delivery whose target left the config is pushed
	// out before it is reconsidered, so an orphaned row is revisited rarely instead of
	// re-claimed every lease window.
	unresolvedBackoff = 5 * time.Minute
)

// Config tunes the worker. The zero value of each field falls back to a default.
type Config struct {
	Interval      time.Duration   // due-scan cadence; a wake nudge drives fresh events sooner
	LeaseTTL      time.Duration   // how long a claimed row is hidden from re-claim; floored at 60s, sized by the caller to exceed the longest attempt
	EvictInterval time.Duration   // size-bound check cadence
	BatchSize     int             // max rows claimed per scan
	MaxBytes      int64           // store size bound; eviction is skipped when <= 0
	Wake          <-chan struct{} // fresh-event nudge; nil leaves only the ticker
	Resolver      TargetResolver
	Logger        *slog.Logger
}

// Worker is the sole delivery driver. On a ticker — and on a wake nudge for freshly
// persisted events — it claims due deliveries, resolves each target's settings, and
// submits them to the pool; on a slower ticker it evicts the oldest events to keep
// the store under its size bound.
type Worker struct {
	store    Store
	pool     Submitter
	resolver TargetResolver
	log      *slog.Logger

	interval      time.Duration
	leaseTTL      time.Duration
	evictInterval time.Duration
	batchSize     int
	maxBytes      int64
	wake          <-chan struct{}

	quit      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	evicted   atomic.Int64
}

// New builds a Worker, applying defaults for unset Config fields. store, pool, and
// Resolver are required. It panics on a nil dependency, or if LeaseTTL does not
// outlast Interval — both are construction-time programmer errors. LeaseTTL is floored
// at the 60s default and is sized by the caller (see cmd/sluice/main.go) to exceed the
// longest attempt, so a slow delivery is not re-claimed and sent twice mid-flight.
func New(store Store, pool Submitter, cfg Config) *Worker {
	if store == nil || pool == nil || cfg.Resolver == nil {
		panic("replay: store, pool, and Resolver are required")
	}
	if cfg.Interval <= 0 {
		cfg.Interval = defaultInterval
	}
	if cfg.LeaseTTL < defaultLeaseTTL {
		cfg.LeaseTTL = defaultLeaseTTL
	}
	if cfg.EvictInterval <= 0 {
		cfg.EvictInterval = defaultEvictInterval
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultBatchSize
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.LeaseTTL <= cfg.Interval {
		panic("replay: LeaseTTL must be greater than Interval")
	}
	return &Worker{
		store:         store,
		pool:          pool,
		resolver:      cfg.Resolver,
		log:           cfg.Logger,
		interval:      cfg.Interval,
		leaseTTL:      cfg.LeaseTTL,
		evictInterval: cfg.EvictInterval,
		batchSize:     cfg.BatchSize,
		maxBytes:      cfg.MaxBytes,
		wake:          cfg.Wake,
		quit:          make(chan struct{}),
		done:          make(chan struct{}),
	}
}

// Start launches the worker loop in its own goroutine.
func (w *Worker) Start() {
	go w.run()
}

// Stop signals the loop to exit and waits for it to return, so no new claim is issued
// afterwards. It abandons an in-progress batch between submits; a submit already
// blocked on a full pool can delay return until the pool frees a slot, after which the
// pool's own shutdown (which must run after Stop) finishes in-flight deliveries. Safe
// to call more than once.
func (w *Worker) Stop() {
	w.closeOnce.Do(func() { close(w.quit) })
	<-w.done
}

// EvictedTotal is the number of events evicted since start. It backs the
// sluice_dlq_evicted_total metric.
func (w *Worker) EvictedTotal() int64 {
	return w.evicted.Load()
}

func (w *Worker) run() {
	defer close(w.done)

	scan := time.NewTicker(w.interval)
	defer scan.Stop()
	evict := time.NewTicker(w.evictInterval)
	defer evict.Stop()

	// Drain once immediately so a freshly persisted event does not wait a whole tick.
	w.replay()

	for {
		select {
		case <-w.quit:
			return
		case <-scan.C:
			w.replay()
		case <-w.wake:
			w.replay()
		case <-evict.C:
			w.evict()
		}
	}
}

// replay claims due deliveries a batch at a time and submits each to the pool,
// stopping once a batch comes back short (the backlog is drained) or Stop is
// signalled. A target no longer in config is skipped, leaving its row pending for a
// later config fix; because the claim leased it, the skip costs at most one re-claim
// per lease window rather than a busy loop. Claim errors are logged and retried next
// tick.
func (w *Worker) replay() {
	for {
		select {
		case <-w.quit:
			return
		default:
		}

		claimed, err := w.store.ClaimDue(context.Background(), time.Now().UnixMilli(), w.leaseTTL, w.batchSize)
		if err != nil {
			w.log.Error("claim due", "err", err)
			return
		}

		for _, c := range claimed {
			// Stop starting new submits promptly once Stop is signalled; the lease on
			// any row not yet submitted expires and it is reclaimed on a later run.
			select {
			case <-w.quit:
				return
			default:
			}

			timeout, retryMax, ok := w.resolver.Resolve(c.Route, c.TargetURL)
			if !ok {
				// The target left the config. Don't park it (the config may be restored),
				// but push its next attempt out so it is revisited rarely rather than
				// re-claimed every lease window, and record why for an operator.
				next := time.Now().Add(unresolvedBackoff).UnixMilli()
				if err := w.store.Reschedule(context.Background(), c.DeliveryID, c.Attempts, "target not in config: "+c.TargetURL, next); err != nil {
					w.log.Error("defer unresolved delivery", "delivery", c.DeliveryID, "err", err)
				} else {
					w.log.Warn("target not in config, deferred",
						"route", c.Route, "url", c.TargetURL, "delivery", c.DeliveryID)
				}
				continue
			}
			w.pool.Submit(deliver.Delivery{
				DeliveryID: c.DeliveryID,
				EventID:    c.EventID,
				Route:      c.Route,
				TargetURL:  c.TargetURL,
				Body:       c.Body,
				Headers:    c.Headers,
				Timeout:    timeout,
				RetryMax:   retryMax,
				Attempts:   c.Attempts,
			})
		}

		if len(claimed) < w.batchSize {
			return
		}
	}
}

// evict trims the store back under its size bound, counting and loudly logging any
// eviction. It is a no-op when MaxBytes is unset.
func (w *Worker) evict() {
	if w.maxBytes <= 0 {
		return
	}
	n, err := w.store.EvictOldest(context.Background(), w.maxBytes)
	if err != nil {
		w.log.Error("evict oldest", "err", err)
		return
	}
	if n > 0 {
		total := w.evicted.Add(int64(n))
		w.log.Error("dlq over size bound, evicted oldest events",
			"evicted", n, "total", total, "max_bytes", w.maxBytes)
	}
}
