package dlq

import (
	"context"
	"database/sql"
	"net/http"
	"path/filepath"
	"testing"
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

func TestStore_MarkFailed_KeepsRowPending(t *testing.T) {
	s, path := openTestStore(t)

	res, err := s.Persist(context.Background(), EventRecord{
		Route:      "/hook",
		Headers:    http.Header{},
		Body:       []byte("payload"),
		TargetURLs: []string{"http://down.example"},
	})
	if err != nil {
		t.Fatalf("persist: %v", err)
	}

	if err := s.MarkFailed(context.Background(), res.DeliveryIDs[0], 3, "status 500"); err != nil {
		t.Fatalf("mark failed: %v", err)
	}

	db, err := sql.Open("sqlite", dsn(path))
	if err != nil {
		t.Fatalf("open read db: %v", err)
	}
	defer func() { _ = db.Close() }()

	var (
		status   string
		attempts int
		lastErr  string
	)
	row := db.QueryRow("SELECT status, attempts, last_error FROM deliveries WHERE id = ?", res.DeliveryIDs[0])
	if err := row.Scan(&status, &attempts, &lastErr); err != nil {
		t.Fatalf("scan delivery: %v", err)
	}
	if status != "pending" {
		t.Errorf("status = %q, want pending", status)
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3", attempts)
	}
	if lastErr != "status 500" {
		t.Errorf("last_error = %q, want %q", lastErr, "status 500")
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
