package replay

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/LindqvistMartin/sluice/internal/deliver"
	"github.com/LindqvistMartin/sluice/internal/dlq"
)

// fakeStore returns queued claim batches in order and a fixed eviction count, so the
// worker loop can be tested without a real database.
type fakeStore struct {
	batches  [][]dlq.Claimed
	claimErr error
	evictN   int
}

func (f *fakeStore) ClaimDue(_ context.Context, _ int64, _ time.Duration, _ int) ([]dlq.Claimed, error) {
	if f.claimErr != nil {
		return nil, f.claimErr
	}
	if len(f.batches) == 0 {
		return nil, nil
	}
	b := f.batches[0]
	f.batches = f.batches[1:]
	return b, nil
}

func (f *fakeStore) EvictOldest(_ context.Context, _ int64) (int, error) {
	return f.evictN, nil
}

func (f *fakeStore) Reschedule(_ context.Context, _ int64, _ int, _ string, _ int64) error {
	return nil
}

// fakeSubmitter records every submitted delivery on a channel.
type fakeSubmitter struct {
	got chan deliver.Delivery
}

func (f *fakeSubmitter) Submit(d deliver.Delivery) { f.got <- d }

// fakeResolver resolves any URL except missURL, which it reports as unconfigured.
type fakeResolver struct {
	timeout  time.Duration
	retryMax int
	missURL  string
}

func (f fakeResolver) Resolve(_, url string) (time.Duration, int, bool) {
	if url == f.missURL {
		return 0, 0, false
	}
	return f.timeout, f.retryMax, true
}

func TestWorker_ClaimsAndSubmitsDue(t *testing.T) {
	store := &fakeStore{batches: [][]dlq.Claimed{{
		{DeliveryID: 1, EventID: "e1", Route: "/h", TargetURL: "http://a", Body: []byte("x"), Attempts: 2},
	}}}
	sub := &fakeSubmitter{got: make(chan deliver.Delivery, 1)}
	w := New(store, sub, Config{
		Interval:      50 * time.Millisecond,
		LeaseTTL:      time.Minute,
		EvictInterval: time.Hour,
		Resolver:      fakeResolver{timeout: 3 * time.Second, retryMax: 4},
	})
	w.Start()
	defer w.Stop()

	select {
	case d := <-sub.got:
		if d.DeliveryID != 1 || d.EventID != "e1" || d.TargetURL != "http://a" {
			t.Errorf("submitted = %+v, unexpected identity", d)
		}
		if d.Timeout != 3*time.Second || d.RetryMax != 4 {
			t.Errorf("resolved timeout/retry = %v/%d, want 3s/4", d.Timeout, d.RetryMax)
		}
		if d.Attempts != 2 {
			t.Errorf("attempts = %d, want 2 (carried from the claim)", d.Attempts)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a submit")
	}
}

func TestWorker_ResolverMiss_SkipsSubmit(t *testing.T) {
	store := &fakeStore{batches: [][]dlq.Claimed{{
		{DeliveryID: 1, Route: "/h", TargetURL: "http://gone"},
		{DeliveryID: 2, Route: "/h", TargetURL: "http://ok"},
	}}}
	sub := &fakeSubmitter{got: make(chan deliver.Delivery, 2)}
	w := New(store, sub, Config{
		Interval:      50 * time.Millisecond,
		LeaseTTL:      time.Minute,
		EvictInterval: time.Hour,
		Resolver:      fakeResolver{timeout: time.Second, retryMax: 1, missURL: "http://gone"},
	})
	w.Start()
	defer w.Stop()

	select {
	case d := <-sub.got:
		if d.DeliveryID != 2 {
			t.Errorf("submitted delivery %d, want only 2 (1 is unresolved)", d.DeliveryID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a submit")
	}
	// The unresolved delivery must not be submitted.
	select {
	case d := <-sub.got:
		t.Errorf("unexpected extra submit: delivery %d", d.DeliveryID)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestWorker_Evicts(t *testing.T) {
	store := &fakeStore{evictN: 3}
	sub := &fakeSubmitter{got: make(chan deliver.Delivery, 1)}
	w := New(store, sub, Config{
		Interval:      time.Hour, // keep the replay path idle; only eviction should fire
		LeaseTTL:      2 * time.Hour,
		EvictInterval: 20 * time.Millisecond,
		MaxBytes:      1024,
		Resolver:      fakeResolver{},
	})
	w.Start()
	defer w.Stop()

	deadline := time.After(5 * time.Second)
	for w.EvictedTotal() < 3 {
		select {
		case <-deadline:
			t.Fatalf("EvictedTotal = %d, want >= 3", w.EvictedTotal())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestWorker_StopIsClean(t *testing.T) {
	store := &fakeStore{}
	sub := &fakeSubmitter{got: make(chan deliver.Delivery, 1)}
	w := New(store, sub, Config{
		Interval:      10 * time.Millisecond,
		LeaseTTL:      time.Minute,
		EvictInterval: 10 * time.Millisecond,
		Resolver:      fakeResolver{},
	})
	w.Start()

	done := make(chan struct{})
	go func() {
		w.Stop()
		w.Stop() // second Stop must not panic or block
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return promptly")
	}
}

func TestWorker_WakeTriggersDrain(t *testing.T) {
	// The immediate drain at start consumes the first (empty) batch; the queued
	// delivery is returned only on the next drain, which the hour-long ticker will not
	// trigger, so a submit proves the wake nudge drove it.
	store := &fakeStore{batches: [][]dlq.Claimed{
		{},
		{{DeliveryID: 1, EventID: "e1", Route: "/h", TargetURL: "http://a", Body: []byte("x")}},
	}}
	sub := &fakeSubmitter{got: make(chan deliver.Delivery, 1)}
	wake := make(chan struct{}, 1)
	w := New(store, sub, Config{
		Interval:      time.Hour,
		LeaseTTL:      2 * time.Hour,
		EvictInterval: time.Hour,
		Wake:          wake,
		Resolver:      fakeResolver{timeout: time.Second, retryMax: 1},
	})
	w.Start()
	defer w.Stop()

	select {
	case d := <-sub.got:
		t.Fatalf("submit before wake: %+v", d)
	case <-time.After(150 * time.Millisecond):
	}

	wake <- struct{}{}
	select {
	case d := <-sub.got:
		if d.DeliveryID != 1 {
			t.Errorf("submitted delivery %d, want 1", d.DeliveryID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no submit after wake; the nudge did not drive a drain")
	}
}

func TestWorker_ClaimError_DoesNotSpin(t *testing.T) {
	store := &fakeStore{claimErr: errors.New("boom")}
	sub := &fakeSubmitter{got: make(chan deliver.Delivery, 1)}
	w := New(store, sub, Config{
		Interval:      20 * time.Millisecond,
		LeaseTTL:      time.Minute,
		EvictInterval: time.Hour,
		Resolver:      fakeResolver{},
	})
	w.Start()
	defer w.Stop()

	// On a persistent claim error the worker logs and waits for the next tick rather
	// than busy-looping or submitting anything.
	select {
	case d := <-sub.got:
		t.Errorf("unexpected submit on claim error: %+v", d)
	case <-time.After(200 * time.Millisecond):
	}
}
