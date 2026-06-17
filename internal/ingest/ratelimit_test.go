package ingest

import (
	"context"
	"testing"
	"time"

	"github.com/LindqvistMartin/sluice/internal/config"
)

func TestIPLimiter_BurstThenRefill(t *testing.T) {
	base := time.Now()
	cur := base
	l := NewIPLimiter(config.Rate{Count: 1, Window: time.Minute}, 10*time.Minute)
	l.now = func() time.Time { return cur }

	if !l.allow("ip") {
		t.Fatal("first request should be allowed (burst 1)")
	}
	if l.allow("ip") {
		t.Fatal("second immediate request should be limited")
	}

	cur = base.Add(time.Minute) // one window later, one token has refilled
	if !l.allow("ip") {
		t.Fatal("request after a full window should be allowed again")
	}
}

func TestIPLimiter_PerIPIsolation(t *testing.T) {
	now := time.Now()
	l := NewIPLimiter(config.Rate{Count: 1, Window: time.Minute}, time.Minute)
	l.now = func() time.Time { return now }

	if !l.allow("a") {
		t.Fatal("first request from a should be allowed")
	}
	if !l.allow("b") {
		t.Fatal("first request from b should be allowed (separate bucket)")
	}
	if l.allow("a") {
		t.Fatal("second request from a should be limited")
	}
}

func TestIPLimiter_CleanupEvictsStale(t *testing.T) {
	base := time.Now()
	cur := base
	l := NewIPLimiter(config.Rate{Count: 1, Window: time.Minute}, 5*time.Minute)
	l.now = func() time.Time { return cur }

	l.allow("stale")
	cur = base.Add(10 * time.Minute) // past the ttl
	l.allow("fresh")
	l.cleanup()

	l.mu.Lock()
	_, staleExists := l.buckets["stale"]
	_, freshExists := l.buckets["fresh"]
	l.mu.Unlock()

	if staleExists {
		t.Error("stale bucket should have been evicted")
	}
	if !freshExists {
		t.Error("fresh bucket should remain")
	}
}

func TestIPLimiter_RunStopsOnContext(t *testing.T) {
	l := NewIPLimiter(config.Rate{Count: 1, Window: time.Minute}, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		l.Run(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancel")
	}
}

func TestIPLimiter_CapRejectsNewWhenFull(t *testing.T) {
	now := time.Now()
	l := NewIPLimiter(config.Rate{Count: 100, Window: time.Minute}, time.Minute)
	l.now = func() time.Time { return now }
	l.maxBuckets = 2

	if !l.allow("a") || !l.allow("b") {
		t.Fatal("the first two IPs should be admitted")
	}
	if l.allow("c") {
		t.Fatal("a new IP must be rejected once the table is full")
	}
	if !l.allow("a") {
		t.Fatal("an already-tracked IP must still be served when the table is full")
	}
}

func TestNewIPLimiter_CleanupIntervalCapped(t *testing.T) {
	if l := NewIPLimiter(config.Rate{Count: 1, Window: time.Minute}, time.Hour); l.cleanupInterval != time.Minute {
		t.Errorf("cleanupInterval = %v, want it capped at 1m", l.cleanupInterval)
	}
	if l := NewIPLimiter(config.Rate{Count: 1, Window: time.Minute}, 30*time.Second); l.cleanupInterval != 30*time.Second {
		t.Errorf("cleanupInterval = %v, want 30s (below the cap)", l.cleanupInterval)
	}
}
