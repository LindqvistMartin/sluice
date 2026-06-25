package dlq

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCLI_RetryParked_UnparksAndResetsBudget(t *testing.T) {
	s, path := openTestStore(t)
	res, err := s.Persist(context.Background(), EventRecord{
		Route: "/hook", Headers: http.Header{}, Body: []byte("x"),
		TargetURLs: []string{"http://down.example"},
	})
	if err != nil {
		t.Fatalf("persist: %v", err)
	}
	id := res.DeliveryIDs[0]
	if err := s.Park(context.Background(), id, 6, "exhausted"); err != nil {
		t.Fatalf("park: %v", err)
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	cli, err := OpenCLI(path)
	if err != nil {
		t.Fatalf("open cli: %v", err)
	}
	defer func() { _ = cli.Close() }()

	now := time.Now().UnixMilli()
	n, err := cli.RetryParked(context.Background(), "", now)
	if err != nil {
		t.Fatalf("retry parked: %v", err)
	}
	if n != 1 {
		t.Fatalf("retried %d, want 1", n)
	}

	status, attempts, nextAt, leaseUntil, lastErr := scanDelivery(t, path, id)
	if status != "pending" {
		t.Errorf("status = %q, want pending", status)
	}
	if attempts != 0 {
		t.Errorf("attempts = %d, want 0 (a fresh budget)", attempts)
	}
	if leaseUntil != 0 {
		t.Errorf("lease_until = %d, want 0", leaseUntil)
	}
	if lastErr != "" {
		t.Errorf("last_error = %q, want empty", lastErr)
	}
	if nextAt != now {
		t.Errorf("next_attempt_at = %d, want %d (due now)", nextAt, now)
	}

	// A revived row must be claimable again — the inverse of TestStore_ClaimDue_ExcludesDead.
	s2, err := Open(path, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.Close(context.Background()) }()
	claimed, err := s2.ClaimDue(context.Background(), now+1, time.Minute, 10)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d, want 1 (a revived row is due)", len(claimed))
	}
}

func TestCLI_RetryParked_ByEvent(t *testing.T) {
	s, path := openTestStore(t)
	a, err := s.Persist(context.Background(), EventRecord{
		Route: "/hook", Headers: http.Header{}, Body: []byte("x"),
		TargetURLs: []string{"http://a.example"},
	})
	if err != nil {
		t.Fatalf("persist a: %v", err)
	}
	b, err := s.Persist(context.Background(), EventRecord{
		Route: "/hook", Headers: http.Header{}, Body: []byte("y"),
		TargetURLs: []string{"http://b.example"},
	})
	if err != nil {
		t.Fatalf("persist b: %v", err)
	}
	if err := s.Park(context.Background(), a.DeliveryIDs[0], 6, "boom"); err != nil {
		t.Fatalf("park a: %v", err)
	}
	if err := s.Park(context.Background(), b.DeliveryIDs[0], 6, "boom"); err != nil {
		t.Fatalf("park b: %v", err)
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	cli, err := OpenCLI(path)
	if err != nil {
		t.Fatalf("open cli: %v", err)
	}
	defer func() { _ = cli.Close() }()

	n, err := cli.RetryParked(context.Background(), a.EventID, time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("retry parked: %v", err)
	}
	if n != 1 {
		t.Fatalf("retried %d, want 1 (only event a)", n)
	}

	if status, _, _, _, _ := scanDelivery(t, path, a.DeliveryIDs[0]); status != "pending" {
		t.Errorf("event a status = %q, want pending", status)
	}
	if status, _, _, _, _ := scanDelivery(t, path, b.DeliveryIDs[0]); status != "dead" {
		t.Errorf("event b status = %q, want dead (untouched)", status)
	}
}

func TestCLI_RetryParked_OnlyTouchesDead(t *testing.T) {
	s, path := openTestStore(t)
	res, err := s.Persist(context.Background(), EventRecord{
		Route: "/hook", Headers: http.Header{}, Body: []byte("x"),
		TargetURLs: []string{"http://dead.example", "http://live.example"},
	})
	if err != nil {
		t.Fatalf("persist: %v", err)
	}
	dead, live := res.DeliveryIDs[0], res.DeliveryIDs[1]

	// Give the live (pending) sibling a recognizable state so we can prove the un-park
	// leaves it untouched rather than clobbering every pending row.
	future := time.Now().UnixMilli() + 1_000_000
	if err := s.Reschedule(context.Background(), live, 3, "transient", future); err != nil {
		t.Fatalf("reschedule live: %v", err)
	}
	if err := s.Park(context.Background(), dead, 6, "exhausted"); err != nil {
		t.Fatalf("park dead: %v", err)
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	cli, err := OpenCLI(path)
	if err != nil {
		t.Fatalf("open cli: %v", err)
	}
	defer func() { _ = cli.Close() }()

	n, err := cli.RetryParked(context.Background(), "", time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("retry parked: %v", err)
	}
	if n != 1 {
		t.Fatalf("retried %d, want 1 (only the dead row)", n)
	}

	if status, attempts, _, _, _ := scanDelivery(t, path, dead); status != "pending" || attempts != 0 {
		t.Errorf("dead row = (%q, attempts %d), want (pending, 0)", status, attempts)
	}
	status, attempts, nextAt, _, lastErr := scanDelivery(t, path, live)
	if status != "pending" || attempts != 3 || nextAt != future || lastErr != "transient" {
		t.Errorf("live row = (%q, attempts %d, next %d, err %q), want it untouched (pending, 3, %d, transient)",
			status, attempts, nextAt, lastErr, future)
	}
}

func TestCLI_RetryParked_NoDead_ZeroNoError(t *testing.T) {
	s, path := openTestStore(t)
	if _, err := s.Persist(context.Background(), EventRecord{
		Route: "/hook", Headers: http.Header{}, Body: []byte("x"),
		TargetURLs: []string{"http://a.example"},
	}); err != nil {
		t.Fatalf("persist: %v", err)
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	cli, err := OpenCLI(path)
	if err != nil {
		t.Fatalf("open cli: %v", err)
	}
	defer func() { _ = cli.Close() }()

	n, err := cli.RetryParked(context.Background(), "", time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("retry parked: %v", err)
	}
	if n != 0 {
		t.Errorf("retried %d, want 0 (nothing parked)", n)
	}
}

func TestCLI_OpenCLI_MissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent.db")
	if _, err := OpenCLI(path); err == nil {
		t.Fatal("OpenCLI on a missing file = nil, want an error")
	}
	// It must not lazily create the database on a mistyped path.
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("OpenCLI created the file; want it left absent (stat err = %v)", err)
	}
}

func TestCLI_ListDead(t *testing.T) {
	s, path := openTestStore(t)
	res, err := s.Persist(context.Background(), EventRecord{
		Route: "/hook", Headers: http.Header{}, Body: []byte("x"),
		TargetURLs: []string{"http://a.example"},
	})
	if err != nil {
		t.Fatalf("persist: %v", err)
	}

	cli, err := OpenCLI(path)
	if err != nil {
		t.Fatalf("open cli: %v", err)
	}
	defer func() { _ = cli.Close() }()

	// Nothing parked yet.
	if dead, err := cli.ListDead(context.Background()); err != nil || len(dead) != 0 {
		t.Fatalf("ListDead on a clean store = %v (err %v), want empty", dead, err)
	}

	if err := s.Park(context.Background(), res.DeliveryIDs[0], 4, "boom"); err != nil {
		t.Fatalf("park: %v", err)
	}

	dead, err := cli.ListDead(context.Background())
	if err != nil {
		t.Fatalf("list dead: %v", err)
	}
	if len(dead) != 1 {
		t.Fatalf("listed %d, want 1", len(dead))
	}
	d := dead[0]
	if d.EventID != res.EventID || d.Route != "/hook" || d.TargetURL != "http://a.example" || d.Attempts != 4 || d.LastError != "boom" {
		t.Errorf("dead delivery = %+v, want event=%s route=/hook url=http://a.example attempts=4 err=boom", d, res.EventID)
	}
	if d.DeliveryID != res.DeliveryIDs[0] {
		t.Errorf("delivery id = %d, want %d", d.DeliveryID, res.DeliveryIDs[0])
	}
}

func TestCLI_RetryParked_UnknownEvent(t *testing.T) {
	s, path := openTestStore(t)
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
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	cli, err := OpenCLI(path)
	if err != nil {
		t.Fatalf("open cli: %v", err)
	}
	defer func() { _ = cli.Close() }()

	n, err := cli.RetryParked(context.Background(), "no-such-event", time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("retry parked: %v", err)
	}
	if n != 0 {
		t.Errorf("retried %d, want 0 (no matching event)", n)
	}
	if status, _, _, _, _ := scanDelivery(t, path, res.DeliveryIDs[0]); status != "dead" {
		t.Errorf("status = %q, want dead (an unknown-event retry must not touch it)", status)
	}
}

func TestCLI_ListDead_OrderedAndOnlyDead(t *testing.T) {
	s, path := openTestStore(t)

	var parked []int64
	for range 3 {
		res, err := s.Persist(context.Background(), EventRecord{
			Route: "/hook", Headers: http.Header{}, Body: []byte("x"),
			TargetURLs: []string{"http://a.example"},
		})
		if err != nil {
			t.Fatalf("persist: %v", err)
		}
		if err := s.Park(context.Background(), res.DeliveryIDs[0], 6, "boom"); err != nil {
			t.Fatalf("park: %v", err)
		}
		parked = append(parked, res.DeliveryIDs[0])
	}
	// A pending sibling that must not appear in the dead list.
	pending, err := s.Persist(context.Background(), EventRecord{
		Route: "/hook", Headers: http.Header{}, Body: []byte("x"),
		TargetURLs: []string{"http://b.example"},
	})
	if err != nil {
		t.Fatalf("persist pending: %v", err)
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	cli, err := OpenCLI(path)
	if err != nil {
		t.Fatalf("open cli: %v", err)
	}
	defer func() { _ = cli.Close() }()

	dead, err := cli.ListDead(context.Background())
	if err != nil {
		t.Fatalf("list dead: %v", err)
	}
	if len(dead) != 3 {
		t.Fatalf("listed %d, want 3 (only the parked rows)", len(dead))
	}
	for i, d := range dead {
		if d.DeliveryID != parked[i] {
			t.Errorf("row %d id = %d, want %d (oldest-first by id)", i, d.DeliveryID, parked[i])
		}
		if d.DeliveryID == pending.DeliveryIDs[0] {
			t.Errorf("pending delivery %d appeared in the dead list", d.DeliveryID)
		}
	}
}
