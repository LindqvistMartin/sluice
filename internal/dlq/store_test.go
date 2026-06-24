package dlq

import (
	"context"
	"database/sql"
	"net/http"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

// openTestStore opens a store on a fresh temp-file database (not :memory:, so the
// reopen test can close and recover the same file) and closes it on cleanup.
func openTestStore(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "dlq.db")
	s, err := Open(path, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s, path
}

// countRows reads a table count over an independent read-only connection, so the
// assertion never borrows the store's single write connection.
func countRows(t *testing.T, path, table string) int {
	t.Helper()
	db, err := sql.Open("sqlite", dsn(path))
	if err != nil {
		t.Fatalf("open read db: %v", err)
	}
	defer func() { _ = db.Close() }()

	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func TestStore_PersistThenDeliver_EmptiesTables(t *testing.T) {
	s, path := openTestStore(t)

	res, err := s.Persist(context.Background(), EventRecord{
		Route:      "/hook",
		Headers:    http.Header{"X-Test": {"a", "b"}},
		Body:       []byte("payload"),
		TargetURLs: []string{"http://a.example", "http://b.example"},
	})
	if err != nil {
		t.Fatalf("persist: %v", err)
	}
	if res.EventID == "" {
		t.Fatal("persist returned an empty event id")
	}
	if len(res.DeliveryIDs) != 2 {
		t.Fatalf("got %d delivery ids, want 2", len(res.DeliveryIDs))
	}
	if n := countRows(t, path, "events"); n != 1 {
		t.Errorf("events = %d, want 1", n)
	}
	if n := countRows(t, path, "deliveries"); n != 2 {
		t.Errorf("deliveries = %d, want 2", n)
	}

	for _, id := range res.DeliveryIDs {
		if err := s.MarkDelivered(context.Background(), res.EventID, id); err != nil {
			t.Fatalf("mark delivered: %v", err)
		}
	}

	if n := countRows(t, path, "deliveries"); n != 0 {
		t.Errorf("deliveries after delivery = %d, want 0", n)
	}
	if n := countRows(t, path, "events"); n != 0 {
		t.Errorf("events after delivery = %d, want 0", n)
	}
}

func TestStore_Reopen_PreservesRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dlq.db")

	s, err := Open(path, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := s.Persist(context.Background(), EventRecord{
		Route:      "/hook",
		Headers:    http.Header{},
		Body:       []byte("payload"),
		TargetURLs: []string{"http://a.example"},
	}); err != nil {
		t.Fatalf("persist: %v", err)
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2, err := Open(path, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close(context.Background()) })

	if n := countRows(t, path, "events"); n != 1 {
		t.Errorf("events after reopen = %d, want 1", n)
	}
	if n := countRows(t, path, "deliveries"); n != 1 {
		t.Errorf("deliveries after reopen = %d, want 1", n)
	}
}

func TestStore_MarkDelivered_PendingSiblingPinsEvent(t *testing.T) {
	s, path := openTestStore(t)

	res, err := s.Persist(context.Background(), EventRecord{
		Route:      "/hook",
		Headers:    http.Header{},
		Body:       []byte("payload"),
		TargetURLs: []string{"http://a.example", "http://b.example"},
	})
	if err != nil {
		t.Fatalf("persist: %v", err)
	}

	// Deliver only the first target. The second stays pending and must keep the
	// event row alive so the body survives for the remaining delivery.
	if err := s.MarkDelivered(context.Background(), res.EventID, res.DeliveryIDs[0]); err != nil {
		t.Fatalf("mark delivered: %v", err)
	}

	if n := countRows(t, path, "events"); n != 1 {
		t.Errorf("events = %d, want 1 (a pending sibling must pin the event)", n)
	}
	if n := countRows(t, path, "deliveries"); n != 1 {
		t.Errorf("deliveries = %d, want 1", n)
	}
}

// scanDelivery reads one delivery's mutable state over an independent read-only
// connection, so the assertion never borrows the store's single write connection.
func scanDelivery(t *testing.T, path string, id int64) (status string, attempts int, nextAttemptAt, leaseUntil int64, lastErr string) {
	t.Helper()
	db, err := sql.Open("sqlite", dsn(path))
	if err != nil {
		t.Fatalf("open read db: %v", err)
	}
	defer func() { _ = db.Close() }()

	row := db.QueryRow("SELECT status, attempts, next_attempt_at, lease_until, last_error FROM deliveries WHERE id = ?", id)
	if err := row.Scan(&status, &attempts, &nextAttemptAt, &leaseUntil, &lastErr); err != nil {
		t.Fatalf("scan delivery: %v", err)
	}
	return status, attempts, nextAttemptAt, leaseUntil, lastErr
}

// eventCount reports whether an event id is present (1) or gone (0).
func eventCount(t *testing.T, path, id string) int {
	t.Helper()
	db, err := sql.Open("sqlite", dsn(path))
	if err != nil {
		t.Fatalf("open read db: %v", err)
	}
	defer func() { _ = db.Close() }()

	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM events WHERE id = ?", id).Scan(&n); err != nil {
		t.Fatalf("count event: %v", err)
	}
	return n
}

// backdateEvent sets an event's receipt time so eviction order is deterministic.
func backdateEvent(t *testing.T, path, id string, receivedAt int64) {
	t.Helper()
	db, err := sql.Open("sqlite", dsn(path))
	if err != nil {
		t.Fatalf("open write db: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec("UPDATE events SET received_at = ? WHERE id = ?", receivedAt, id); err != nil {
		t.Fatalf("backdate event: %v", err)
	}
}

func TestStore_ClaimDue_LeasesAndReturnsBody(t *testing.T) {
	s, path := openTestStore(t)

	res, err := s.Persist(context.Background(), EventRecord{
		Route:      "/hook",
		Headers:    http.Header{"X-Test": {"a", "b"}},
		Body:       []byte("payload"),
		TargetURLs: []string{"http://a.example", "http://b.example"},
	})
	if err != nil {
		t.Fatalf("persist: %v", err)
	}

	// Persist sets next_attempt_at to receipt time, so any later now is due.
	now := time.Now().UnixMilli() + 1000
	claimed, err := s.ClaimDue(context.Background(), now, time.Minute, 10)
	if err != nil {
		t.Fatalf("claim due: %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("claimed = %d, want 2", len(claimed))
	}
	for _, c := range claimed {
		if string(c.Body) != "payload" {
			t.Errorf("body = %q, want payload", c.Body)
		}
		if c.Route != "/hook" {
			t.Errorf("route = %q, want /hook", c.Route)
		}
		if got := c.Headers.Get("X-Test"); got != "a" {
			t.Errorf("X-Test = %q, want a", got)
		}
		if c.EventID != res.EventID {
			t.Errorf("event id = %q, want %q", c.EventID, res.EventID)
		}
	}

	// A second claim at the same instant returns nothing: the rows are leased.
	again, err := s.ClaimDue(context.Background(), now, time.Minute, 10)
	if err != nil {
		t.Fatalf("re-claim: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("re-claim returned %d, want 0 (rows are leased)", len(again))
	}

	wantLease := now + time.Minute.Milliseconds()
	for _, id := range res.DeliveryIDs {
		status, _, _, leaseUntil, _ := scanDelivery(t, path, id)
		if status != "pending" {
			t.Errorf("status = %q, want pending (lease keeps the row pending)", status)
		}
		if leaseUntil != wantLease {
			t.Errorf("lease_until = %d, want %d", leaseUntil, wantLease)
		}
	}
}

func TestStore_ClaimDue_Atomic_DisjointUnderRace(t *testing.T) {
	s, _ := openTestStore(t)

	const total = 50
	urls := make([]string, total)
	for i := range urls {
		urls[i] = "http://t.example/" + strconv.Itoa(i)
	}
	if _, err := s.Persist(context.Background(), EventRecord{
		Route:      "/hook",
		Headers:    http.Header{},
		Body:       []byte("x"),
		TargetURLs: urls,
	}); err != nil {
		t.Fatalf("persist: %v", err)
	}

	now := time.Now().UnixMilli() + 1000
	const goroutines = 8
	var wg sync.WaitGroup
	results := make(chan []int64, goroutines)
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var got []int64
			for {
				claimed, err := s.ClaimDue(context.Background(), now, time.Minute, 7)
				if err != nil {
					t.Errorf("claim due: %v", err)
					return
				}
				if len(claimed) == 0 {
					break
				}
				for _, c := range claimed {
					got = append(got, c.DeliveryID)
				}
			}
			results <- got
		}()
	}
	wg.Wait()
	close(results)

	seen := make(map[int64]bool)
	count := 0
	for got := range results {
		for _, id := range got {
			if seen[id] {
				t.Errorf("delivery %d claimed by more than one goroutine", id)
			}
			seen[id] = true
			count++
		}
	}
	if count != total {
		t.Errorf("claimed %d distinct rows in total, want %d", count, total)
	}
}

func TestStore_ClaimDue_LeaseExpiryRedelivers(t *testing.T) {
	s, _ := openTestStore(t)
	res, err := s.Persist(context.Background(), EventRecord{
		Route: "/hook", Headers: http.Header{}, Body: []byte("x"),
		TargetURLs: []string{"http://a.example"},
	})
	if err != nil {
		t.Fatalf("persist: %v", err)
	}

	now := time.Now().UnixMilli() + 1000
	if claimed, err := s.ClaimDue(context.Background(), now, time.Minute, 10); err != nil || len(claimed) != 1 {
		t.Fatalf("claim = %d (err %v), want 1", len(claimed), err)
	}

	// Within the lease window: not re-claimable.
	if again, _ := s.ClaimDue(context.Background(), now+30_000, time.Minute, 10); len(again) != 0 {
		t.Fatalf("re-claim within lease returned %d, want 0", len(again))
	}
	// After the lease expires: the crashed-worker row recovers.
	later := now + time.Minute.Milliseconds() + 1000
	again, err := s.ClaimDue(context.Background(), later, time.Minute, 10)
	if err != nil {
		t.Fatalf("re-claim after expiry: %v", err)
	}
	if len(again) != 1 {
		t.Fatalf("re-claim after expiry = %d, want 1", len(again))
	}
	if again[0].DeliveryID != res.DeliveryIDs[0] {
		t.Errorf("redelivered id = %d, want %d", again[0].DeliveryID, res.DeliveryIDs[0])
	}
}

func TestStore_ClaimDue_ExcludesDead(t *testing.T) {
	s, _ := openTestStore(t)
	res, err := s.Persist(context.Background(), EventRecord{
		Route: "/hook", Headers: http.Header{}, Body: []byte("x"),
		TargetURLs: []string{"http://a.example"},
	})
	if err != nil {
		t.Fatalf("persist: %v", err)
	}
	if err := s.Park(context.Background(), res.DeliveryIDs[0], 6, "exhausted"); err != nil {
		t.Fatalf("park: %v", err)
	}

	now := time.Now().UnixMilli() + 1000
	claimed, err := s.ClaimDue(context.Background(), now, time.Minute, 10)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 0 {
		t.Errorf("claimed = %d, want 0 (dead rows are excluded)", len(claimed))
	}
}

func TestStore_Reschedule_AdvancesNextAttempt(t *testing.T) {
	s, path := openTestStore(t)
	res, err := s.Persist(context.Background(), EventRecord{
		Route: "/hook", Headers: http.Header{}, Body: []byte("x"),
		TargetURLs: []string{"http://down.example"},
	})
	if err != nil {
		t.Fatalf("persist: %v", err)
	}
	id := res.DeliveryIDs[0]

	now := time.Now().UnixMilli() + 1000
	if _, err := s.ClaimDue(context.Background(), now, time.Minute, 10); err != nil {
		t.Fatalf("claim: %v", err)
	}
	next := now + 5000
	if err := s.Reschedule(context.Background(), id, 2, "status 500", next); err != nil {
		t.Fatalf("reschedule: %v", err)
	}

	status, attempts, nextAt, leaseUntil, lastErr := scanDelivery(t, path, id)
	if status != "pending" {
		t.Errorf("status = %q, want pending", status)
	}
	if attempts != 2 {
		t.Errorf("attempts = %d, want 2", attempts)
	}
	if lastErr != "status 500" {
		t.Errorf("last_error = %q, want %q", lastErr, "status 500")
	}
	if nextAt != next {
		t.Errorf("next_attempt_at = %d, want %d", nextAt, next)
	}
	if leaseUntil != 0 {
		t.Errorf("lease_until = %d, want 0 (reschedule clears the lease)", leaseUntil)
	}

	// Not due before next; due at or after it.
	if c, _ := s.ClaimDue(context.Background(), next-1, time.Minute, 10); len(c) != 0 {
		t.Errorf("claim before next_attempt_at returned %d, want 0", len(c))
	}
	if c, _ := s.ClaimDue(context.Background(), next, time.Minute, 10); len(c) != 1 {
		t.Errorf("claim at next_attempt_at returned %d, want 1", len(c))
	}
}

func TestStore_Park_SetsDeadAndPinsEvent(t *testing.T) {
	s, path := openTestStore(t)
	res, err := s.Persist(context.Background(), EventRecord{
		Route: "/hook", Headers: http.Header{}, Body: []byte("x"),
		TargetURLs: []string{"http://down.example"},
	})
	if err != nil {
		t.Fatalf("persist: %v", err)
	}
	if err := s.Park(context.Background(), res.DeliveryIDs[0], 6, "exhausted"); err != nil {
		t.Fatalf("park: %v", err)
	}

	status, attempts, _, leaseUntil, lastErr := scanDelivery(t, path, res.DeliveryIDs[0])
	if status != "dead" {
		t.Errorf("status = %q, want dead", status)
	}
	if attempts != 6 {
		t.Errorf("attempts = %d, want 6", attempts)
	}
	if lastErr != "exhausted" {
		t.Errorf("last_error = %q, want %q", lastErr, "exhausted")
	}
	if leaseUntil != 0 {
		t.Errorf("lease_until = %d, want 0", leaseUntil)
	}
	// Parking is not deletion: the dead delivery pins its event so the body survives.
	if n := countRows(t, path, "events"); n != 1 {
		t.Errorf("events = %d, want 1 (a dead delivery pins the event)", n)
	}
	if n := countRows(t, path, "deliveries"); n != 1 {
		t.Errorf("deliveries = %d, want 1", n)
	}
}

func TestStore_Transitions_TolerateMissingRow(t *testing.T) {
	s, _ := openTestStore(t)
	const ghost = 999 // never persisted; models a concurrently evicted row

	if err := s.MarkDelivered(context.Background(), "evt_missing", ghost); err != nil {
		t.Errorf("MarkDelivered on missing row = %v, want nil", err)
	}
	if err := s.Reschedule(context.Background(), ghost, 1, "x", 123); err != nil {
		t.Errorf("Reschedule on missing row = %v, want nil", err)
	}
	if err := s.Park(context.Background(), ghost, 1, "x"); err != nil {
		t.Errorf("Park on missing row = %v, want nil", err)
	}
}

func TestStore_EvictOldest_DropsOldestAndCascades(t *testing.T) {
	s, path := openTestStore(t)

	const n = 20
	body := make([]byte, 50*1024) // big enough that 20 events clear the size bound
	ids := make([]string, n)
	for i := range n {
		res, err := s.Persist(context.Background(), EventRecord{
			Route: "/hook", Headers: http.Header{}, Body: body,
			TargetURLs: []string{"http://a.example"},
		})
		if err != nil {
			t.Fatalf("persist %d: %v", i, err)
		}
		ids[i] = res.EventID
		backdateEvent(t, path, res.EventID, int64(i)) // i ascending => event 0 oldest
	}

	evicted, err := s.EvictOldest(context.Background(), 512*1024)
	if err != nil {
		t.Fatalf("evict: %v", err)
	}
	if evicted == 0 || evicted >= n {
		t.Fatalf("evicted = %d, want some but not all of %d", evicted, n)
	}

	survivors := n - evicted
	if got := countRows(t, path, "events"); got != survivors {
		t.Errorf("events = %d, want %d", got, survivors)
	}
	if got := countRows(t, path, "deliveries"); got != survivors {
		t.Errorf("deliveries = %d, want %d (deliveries cascade with their event)", got, survivors)
	}
	if eventCount(t, path, ids[0]) != 0 {
		t.Errorf("oldest event survived, want it evicted first")
	}
	if eventCount(t, path, ids[n-1]) != 1 {
		t.Errorf("newest event evicted, want it kept")
	}
}

func TestStore_EvictOldest_UnderBudget_NoOp(t *testing.T) {
	s, path := openTestStore(t)
	if _, err := s.Persist(context.Background(), EventRecord{
		Route: "/hook", Headers: http.Header{}, Body: []byte("small"),
		TargetURLs: []string{"http://a.example"},
	}); err != nil {
		t.Fatalf("persist: %v", err)
	}

	evicted, err := s.EvictOldest(context.Background(), 1<<30) // 1 GiB, far above use
	if err != nil {
		t.Fatalf("evict: %v", err)
	}
	if evicted != 0 {
		t.Errorf("evicted = %d, want 0 (under budget)", evicted)
	}
	if got := countRows(t, path, "events"); got != 1 {
		t.Errorf("events = %d, want 1 (nothing evicted under budget)", got)
	}
}

func TestStore_EvictOldest_ToleratesLeasedInFlight(t *testing.T) {
	s, path := openTestStore(t)

	const n = 20
	body := make([]byte, 50*1024)
	var oldest int64
	var oldestEvent string
	for i := range n {
		res, err := s.Persist(context.Background(), EventRecord{
			Route: "/hook", Headers: http.Header{}, Body: body,
			TargetURLs: []string{"http://a.example"},
		})
		if err != nil {
			t.Fatalf("persist %d: %v", i, err)
		}
		backdateEvent(t, path, res.EventID, int64(i))
		if i == 0 {
			oldest = res.DeliveryIDs[0]
			oldestEvent = res.EventID
		}
	}

	// Lease a row as the worker would, then evict under pressure: the oldest event
	// (which the leased row belongs to) is among those dropped.
	now := time.Now().UnixMilli() + 1_000_000
	if _, err := s.ClaimDue(context.Background(), now, time.Minute, 1); err != nil {
		t.Fatalf("claim: %v", err)
	}
	evicted, err := s.EvictOldest(context.Background(), 512*1024)
	if err != nil {
		t.Fatalf("evict: %v", err)
	}
	if evicted == 0 {
		t.Fatal("evicted 0, want the oldest leased event dropped under pressure")
	}
	if eventCount(t, path, oldestEvent) != 0 {
		t.Error("oldest (leased) event survived eviction, want it dropped")
	}

	// Reporting the gone row's outcome must be a no-op, not an error.
	if err := s.MarkDelivered(context.Background(), "gone", oldest); err != nil {
		t.Errorf("MarkDelivered on evicted leased row = %v, want nil", err)
	}
	if err := s.Park(context.Background(), oldest, 1, "x"); err != nil {
		t.Errorf("Park on evicted leased row = %v, want nil", err)
	}
}
