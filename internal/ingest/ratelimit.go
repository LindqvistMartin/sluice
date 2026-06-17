package ingest

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/LindqvistMartin/sluice/internal/config"
)

// maxIPBuckets caps how many per-IP limiters are kept at once. It bounds memory
// against an attacker rotating source addresses (trivial over IPv6) faster than
// idle buckets are reaped.
const maxIPBuckets = 100_000

// IPLimiter applies a per-client-IP token-bucket rate limit. Each IP gets its own
// limiter, created on first use; a background loop prunes idle entries and a hard
// cap bounds the table so the map cannot grow without limit.
type IPLimiter struct {
	mu              sync.Mutex
	buckets         map[string]*ipBucket
	limit           rate.Limit
	burst           int
	ttl             time.Duration
	cleanupInterval time.Duration
	maxBuckets      int
	now             func() time.Time // injectable clock; drives both refill and eviction
}

type ipBucket struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// NewIPLimiter builds a limiter allowing r.Count events per r.Window per IP, with
// a burst equal to r.Count. Buckets idle for longer than ttl are evicted by Run,
// which sweeps at least once a minute regardless of ttl.
func NewIPLimiter(r config.Rate, ttl time.Duration) *IPLimiter {
	return &IPLimiter{
		buckets:         make(map[string]*ipBucket),
		limit:           rate.Limit(r.PerSecond()),
		burst:           r.Count,
		ttl:             ttl,
		cleanupInterval: min(ttl, time.Minute),
		maxBuckets:      maxIPBuckets,
		now:             time.Now,
	}
}

// allow reports whether a request from ip may proceed, consuming one token. The
// limiter is fed the injectable clock so refill is deterministic under test.
func (l *IPLimiter) allow(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	t := l.now()
	b, ok := l.buckets[ip]
	if !ok {
		// Fail closed once the table is full: reject unseen IPs rather than let
		// spoofed source addresses exhaust memory. The reaper frees slots as
		// buckets go idle.
		if len(l.buckets) >= l.maxBuckets {
			return false
		}
		b = &ipBucket{limiter: rate.NewLimiter(l.limit, l.burst)}
		l.buckets[ip] = b
	}
	b.lastSeen = t
	return b.limiter.AllowN(t, 1)
}

// Run prunes idle buckets until ctx is cancelled. It is meant to run in its own
// goroutine owned by the caller, which cancels ctx and joins it on shutdown.
func (l *IPLimiter) Run(ctx context.Context) {
	ticker := time.NewTicker(l.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.cleanup()
		}
	}
}

func (l *IPLimiter) cleanup() {
	l.mu.Lock()
	defer l.mu.Unlock()

	cutoff := l.now().Add(-l.ttl)
	for ip, b := range l.buckets {
		if b.lastSeen.Before(cutoff) {
			delete(l.buckets, ip)
		}
	}
}
