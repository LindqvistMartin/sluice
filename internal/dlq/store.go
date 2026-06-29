package dlq

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite" // registers the pure-Go "sqlite" driver (CGO_ENABLED=0)
)

// schemaDDL is the durable store: events holds each payload once, deliveries holds
// one row per target with a foreign key that cascades on delete. A row is 'pending'
// until it is leased and driven to a terminal state — deleted on success, or 'dead'
// once it is parked, whether its retry budget was exhausted or its failure was
// permanent. lease_until keeps an in-flight row from being claimed twice and lets a
// crashed worker's row recover once the lease expires; next_attempt_at schedules the
// next due time. The full column set was created up front so this needs no migration.
const schemaDDL = `
CREATE TABLE IF NOT EXISTS events (
    id           TEXT    PRIMARY KEY,
    route        TEXT    NOT NULL,
    headers_json BLOB    NOT NULL,
    body         BLOB    NOT NULL,
    received_at  INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS deliveries (
    id              INTEGER PRIMARY KEY,
    event_id        TEXT    NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    target_url      TEXT    NOT NULL,
    status          TEXT    NOT NULL DEFAULT 'pending',
    attempts        INTEGER NOT NULL DEFAULT 0,
    next_attempt_at INTEGER NOT NULL DEFAULT 0,
    lease_until     INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT    NOT NULL DEFAULT '',
    created_at      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_deliveries_due      ON deliveries(status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_deliveries_event_id ON deliveries(event_id);
`

// dsn builds the connection string. The pragmas are set per connection by the
// driver: WAL lets readers run alongside the single writer, synchronous=NORMAL is
// the durability boundary (survives process death, not power loss), busy_timeout
// turns a momentary lock into a short wait, and foreign_keys is load-bearing so
// the events->deliveries cascade actually fires. _txlock=immediate makes the
// writer take the write lock at BEGIN rather than on first write.
func dsn(path string) string {
	return path + "?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)" +
		"&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)&_txlock=immediate"
}

// EventRecord is an accepted event handed to the store for durable persistence.
// One deliveries row is written per entry in TargetURLs, in order.
type EventRecord struct {
	Route      string
	Headers    http.Header
	Body       []byte
	TargetURLs []string
}

// PersistResult reports the identifiers assigned by a successful Persist. EventID
// is the random event id; DeliveryIDs are the per-target row ids in TargetURLs order.
type PersistResult struct {
	EventID     string
	DeliveryIDs []int64
}

// Claimed is a leased delivery returned by ClaimDue. It carries everything the
// delivery pool needs to attempt the target except the per-target Timeout and
// RetryMax, which live in config and the caller resolves by Route and TargetURL.
type Claimed struct {
	DeliveryID int64
	EventID    string
	Route      string
	TargetURL  string
	Headers    http.Header
	Body       []byte
	Attempts   int // attempts already recorded before this lease
}

// Store is the SQLite-backed durable queue. All writes go through a single
// goroutine that owns the connection, so write serialization is a property of the
// architecture rather than a lock every caller must remember.
type Store struct {
	db     *sql.DB
	reader *sql.DB
	ops    chan writeOp
	done   chan struct{}
	log    *slog.Logger

	closeOnce sync.Once
	closeErr  error
}

// writeOp is a unit of work for the writer goroutine: a closure to run against the
// owned connection and a buffered channel to reply on once it returns.
type writeOp struct {
	ctx   context.Context
	fn    func(context.Context, *sql.DB) error
	reply chan error
}

// Open opens (creating if needed) the store at path, applies the schema, and
// starts the writer goroutine. The returned Store must be closed to checkpoint
// the WAL and release the file.
func Open(path string, log *slog.Logger) (*Store, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	db, err := sql.Open("sqlite", dsn(path))
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// One connection: the writer owns it, and it backs the single-writer invariant
	// physically as well as by goroutine ownership.
	db.SetMaxOpenConns(1)

	if _, err := db.ExecContext(context.Background(), schemaDDL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}

	// A separate connection for reads (Stats). WAL lets it run concurrently with the
	// writer, so a metrics scrape never queues behind the delivery write path.
	reader, err := sql.Open("sqlite", dsn(path))
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("open sqlite reader: %w", err)
	}

	s := &Store{
		db:     db,
		reader: reader,
		ops:    make(chan writeOp),
		done:   make(chan struct{}),
		log:    log,
	}
	go s.writer()
	return s, nil
}

// writer is the sole owner of the write connection. It runs each queued closure
// and replies, in arrival order, until the ops channel is closed.
func (s *Store) writer() {
	defer close(s.done)
	for op := range s.ops {
		op.reply <- op.fn(op.ctx, s.db)
	}
}

// do submits fn to the writer and waits for the commit, honouring ctx on both the
// send and the reply so a cancelled caller is not pinned to a slow write.
func (s *Store) do(ctx context.Context, fn func(context.Context, *sql.DB) error) error {
	op := writeOp{ctx: ctx, fn: fn, reply: make(chan error, 1)}
	select {
	case s.ops <- op:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-op.reply:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Persist writes the event and one pending delivery per target in a single
// transaction, returning only once it is committed. The inbound 200 is sent on
// the strength of this commit: it means persisted, not yet delivered. A caller
// whose context cancels exactly as the commit lands may see an error for a write
// that did succeed; delivery is at-least-once and the dedup header covers it.
func (s *Store) Persist(ctx context.Context, rec EventRecord) (PersistResult, error) {
	var res PersistResult
	err := s.do(ctx, func(ctx context.Context, db *sql.DB) error {
		headers, err := json.Marshal(rec.Headers)
		if err != nil {
			return fmt.Errorf("marshal headers: %w", err)
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin: %w", err)
		}
		defer func() { _ = tx.Rollback() }()

		id := rand.Text()
		now := time.Now().UnixMilli()
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO events (id, route, headers_json, body, received_at) VALUES (?, ?, ?, ?, ?)`,
			id, rec.Route, headers, rec.Body, now); err != nil {
			return fmt.Errorf("insert event: %w", err)
		}

		ids := make([]int64, len(rec.TargetURLs))
		for i, url := range rec.TargetURLs {
			result, err := tx.ExecContext(ctx,
				`INSERT INTO deliveries (event_id, target_url, next_attempt_at, created_at)
				 VALUES (?, ?, ?, ?)`,
				id, url, now, now)
			if err != nil {
				return fmt.Errorf("insert delivery: %w", err)
			}
			if ids[i], err = result.LastInsertId(); err != nil {
				return fmt.Errorf("delivery id: %w", err)
			}
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit: %w", err)
		}
		res = PersistResult{EventID: id, DeliveryIDs: ids}
		return nil
	})
	return res, err
}

// ClaimDue atomically leases up to limit deliveries that are pending, due
// (next_attempt_at <= now), and not already under a live lease (lease_until <= now),
// returning each joined to its event's body and headers. The select-then-lease runs
// in one transaction on the single write connection, so two concurrent callers
// cannot claim the same row: the writer goroutine serializes closures, and a row the
// first call leased is excluded from the second by its advanced lease_until. now and
// leaseTTL are supplied by the caller so the worker and tests control time directly.
func (s *Store) ClaimDue(ctx context.Context, now int64, leaseTTL time.Duration, limit int) ([]Claimed, error) {
	var claimed []Claimed
	err := s.do(ctx, func(ctx context.Context, db *sql.DB) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin: %w", err)
		}
		defer func() { _ = tx.Rollback() }()

		rows, err := tx.QueryContext(ctx,
			`SELECT d.id, d.event_id, e.route, d.target_url, e.headers_json, e.body, d.attempts
			 FROM deliveries d JOIN events e ON e.id = d.event_id
			 WHERE d.status = 'pending' AND d.next_attempt_at <= ? AND d.lease_until <= ?
			 ORDER BY d.next_attempt_at ASC LIMIT ?`,
			now, now, limit)
		if err != nil {
			return fmt.Errorf("select due: %w", err)
		}

		var batch []Claimed
		for rows.Next() {
			var (
				c           Claimed
				headersJSON []byte
			)
			if err := rows.Scan(&c.DeliveryID, &c.EventID, &c.Route, &c.TargetURL, &headersJSON, &c.Body, &c.Attempts); err != nil {
				_ = rows.Close()
				return fmt.Errorf("scan due: %w", err)
			}
			if err := json.Unmarshal(headersJSON, &c.Headers); err != nil {
				_ = rows.Close()
				return fmt.Errorf("unmarshal headers: %w", err)
			}
			batch = append(batch, c)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return fmt.Errorf("iterate due: %w", err)
		}
		// The result set must be fully closed before the UPDATE runs: the store holds a
		// single connection, and a query in progress would block the next statement.
		if err := rows.Close(); err != nil {
			return fmt.Errorf("close due: %w", err)
		}

		if len(batch) == 0 {
			return tx.Commit()
		}

		args := make([]any, 0, len(batch)+2)
		args = append(args, now+leaseTTL.Milliseconds())
		placeholders := make([]string, len(batch))
		for i := range batch {
			placeholders[i] = "?"
			args = append(args, batch[i].DeliveryID)
		}
		args = append(args, now)
		if _, err := tx.ExecContext(ctx,
			`UPDATE deliveries SET lease_until = ? WHERE id IN (`+strings.Join(placeholders, ",")+
				`) AND status = 'pending' AND lease_until <= ?`,
			args...); err != nil {
			return fmt.Errorf("lease due: %w", err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit: %w", err)
		}
		claimed = batch
		return nil
	})
	return claimed, err
}

// MarkDelivered removes a delivered target and, once an event has no deliveries
// left in any state, removes the event so a fully delivered event leaves nothing
// behind: in the happy path both tables end empty. A 0-row match (the row was
// concurrently evicted) is a no-op success, not an error.
func (s *Store) MarkDelivered(ctx context.Context, eventID string, deliveryID int64) error {
	return s.do(ctx, func(ctx context.Context, db *sql.DB) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin: %w", err)
		}
		defer func() { _ = tx.Rollback() }()

		if _, err := tx.ExecContext(ctx, `DELETE FROM deliveries WHERE id = ?`, deliveryID); err != nil {
			return fmt.Errorf("delete delivery: %w", err)
		}
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM events WHERE id = ? AND NOT EXISTS (SELECT 1 FROM deliveries WHERE event_id = ?)`,
			eventID, eventID); err != nil {
			return fmt.Errorf("collect event: %w", err)
		}
		return tx.Commit()
	})
}

// Reschedule records a failed attempt that still has budget left: it stores the new
// attempt count and error, advances next_attempt_at to the next due time, and clears
// the lease so the row is eligible for a later scan. A single guarded UPDATE needs no
// transaction — the single-writer connection makes it atomic. The status='pending'
// guard prevents resurrecting a row another pass already parked, and a 0-row match
// (the row was concurrently evicted) is a no-op success.
func (s *Store) Reschedule(ctx context.Context, deliveryID int64, attempts int, lastErr string, nextAttemptAt int64) error {
	return s.do(ctx, func(ctx context.Context, db *sql.DB) error {
		if _, err := db.ExecContext(ctx,
			`UPDATE deliveries SET attempts = ?, last_error = ?, next_attempt_at = ?, lease_until = 0
			 WHERE id = ? AND status = 'pending'`,
			attempts, lastErr, nextAttemptAt, deliveryID); err != nil {
			return fmt.Errorf("reschedule delivery: %w", err)
		}
		return nil
	})
}

// Park moves a delivery to status='dead', a terminal and inspectable state, once it
// will not be retried — its retry budget is spent or its failure is permanent. The row
// is kept, not deleted, so it pins its event body for a later manual replay —
// dead-lettering is parking, not deletion. As with Reschedule, the status='pending'
// guard makes it idempotent and a 0-row match is a no-op.
func (s *Store) Park(ctx context.Context, deliveryID int64, attempts int, lastErr string) error {
	return s.do(ctx, func(ctx context.Context, db *sql.DB) error {
		if _, err := db.ExecContext(ctx,
			`UPDATE deliveries SET status = 'dead', attempts = ?, last_error = ?, lease_until = 0
			 WHERE id = ? AND status = 'pending'`,
			attempts, lastErr, deliveryID); err != nil {
			return fmt.Errorf("park delivery: %w", err)
		}
		return nil
	})
}

// Stats is a point-in-time count of deliveries by status.
type Stats struct {
	Pending int64
	Dead    int64
}

// Stats counts deliveries by status for the metrics endpoint. It reads on a separate
// connection rather than the writer, so a scrape neither blocks nor is blocked by the
// delivery write path; WAL keeps the count consistent with committed writes.
func (s *Store) Stats(ctx context.Context) (Stats, error) {
	return statsQuery(ctx, s.reader)
}

// statsQuery counts deliveries by status on db. It is connection-agnostic so the
// daemon's read connection and the operator CLI's connection share it. COALESCE keeps
// an empty table returning zeros rather than NULL.
func statsQuery(ctx context.Context, db *sql.DB) (Stats, error) {
	var s Stats
	err := db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END), 0),
		       COALESCE(SUM(CASE WHEN status = 'dead'    THEN 1 ELSE 0 END), 0)
		FROM deliveries`).Scan(&s.Pending, &s.Dead)
	if err != nil {
		return Stats{}, fmt.Errorf("count by status: %w", err)
	}
	return s, nil
}

// DeadDelivery is a parked delivery as listed for an operator. It carries enough to
// identify and triage the row but deliberately omits the event body, which can be
// large or hold secrets.
type DeadDelivery struct {
	DeliveryID int64
	EventID    string
	Route      string
	TargetURL  string
	Attempts   int
	LastError  string
}

// listDeadQuery returns every parked (dead) delivery on db, oldest first by id. It is
// connection-agnostic for the same reason as statsQuery.
func listDeadQuery(ctx context.Context, db *sql.DB) ([]DeadDelivery, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT d.id, d.event_id, e.route, d.target_url, d.attempts, d.last_error
		FROM deliveries d JOIN events e ON e.id = d.event_id
		WHERE d.status = 'dead'
		ORDER BY d.id`)
	if err != nil {
		return nil, fmt.Errorf("select dead: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var dead []DeadDelivery
	for rows.Next() {
		var d DeadDelivery
		if err := rows.Scan(&d.DeliveryID, &d.EventID, &d.Route, &d.TargetURL, &d.Attempts, &d.LastError); err != nil {
			return nil, fmt.Errorf("scan dead: %w", err)
		}
		dead = append(dead, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dead: %w", err)
	}
	return dead, nil
}

// evictBatch is how many oldest events are deleted per pass while bringing the store
// back under its size bound.
const evictBatch = 16

// EvictOldest enforces a logical-size bound, deleting whole events oldest-first
// (their deliveries cascade via the foreign key) until the store is back under
// maxBytes or empty, and returns the number of events evicted. Size is the logical
// occupancy (page_count - freelist_count) * page_size, not the file size: SQLite
// does not shrink the file on delete, so a file-size trigger would never fall once
// the high-water mark is reached. A checkpoint runs first so WAL pages are counted,
// gated by a cheap upper-bound pre-check so a store well under budget pays nothing.
func (s *Store) EvictOldest(ctx context.Context, maxBytes int64) (int, error) {
	var evicted int
	err := s.do(ctx, func(ctx context.Context, db *sql.DB) error {
		// page_count*page_size ignores the freelist, so if even that upper bound is
		// under maxBytes the real occupancy certainly is — skip the heavier checkpoint.
		var pageCount, pageSize int64
		if err := db.QueryRowContext(ctx, `PRAGMA page_count`).Scan(&pageCount); err != nil {
			return fmt.Errorf("page_count: %w", err)
		}
		if err := db.QueryRowContext(ctx, `PRAGMA page_size`).Scan(&pageSize); err != nil {
			return fmt.Errorf("page_size: %w", err)
		}
		if pageCount*pageSize <= maxBytes {
			return nil
		}

		if _, err := db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
			return fmt.Errorf("checkpoint: %w", err)
		}

		for {
			size, err := logicalSize(ctx, db)
			if err != nil {
				return err
			}
			if size <= maxBytes {
				return nil
			}

			ids, err := oldestEventIDs(ctx, db, evictBatch)
			if err != nil {
				return err
			}
			if len(ids) == 0 {
				return nil // nothing left to evict
			}

			placeholders := make([]string, len(ids))
			args := make([]any, len(ids))
			for i, id := range ids {
				placeholders[i] = "?"
				args[i] = id
			}
			if _, err := db.ExecContext(ctx,
				`DELETE FROM events WHERE id IN (`+strings.Join(placeholders, ",")+`)`,
				args...); err != nil {
				return fmt.Errorf("delete events: %w", err)
			}
			evicted += len(ids)
		}
	})
	return evicted, err
}

// logicalSize is the committed occupancy in bytes: allocated pages minus free pages,
// times page size. Deleting rows frees pages onto the freelist immediately on this
// connection, so the figure falls as the eviction loop deletes without needing a
// fresh checkpoint each pass.
func logicalSize(ctx context.Context, db *sql.DB) (int64, error) {
	var pageCount, freelist, pageSize int64
	if err := db.QueryRowContext(ctx, `PRAGMA page_count`).Scan(&pageCount); err != nil {
		return 0, fmt.Errorf("page_count: %w", err)
	}
	if err := db.QueryRowContext(ctx, `PRAGMA freelist_count`).Scan(&freelist); err != nil {
		return 0, fmt.Errorf("freelist_count: %w", err)
	}
	if err := db.QueryRowContext(ctx, `PRAGMA page_size`).Scan(&pageSize); err != nil {
		return 0, fmt.Errorf("page_size: %w", err)
	}
	return (pageCount - freelist) * pageSize, nil
}

// oldestEventIDs returns up to limit event ids ordered by receipt time, oldest first.
func oldestEventIDs(ctx context.Context, db *sql.DB, limit int) ([]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT id FROM events ORDER BY received_at ASC LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("select oldest: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan oldest: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate oldest: %w", err)
	}
	return ids, nil
}

// Close stops the writer, checkpoints the WAL into the main database file so a
// clean shutdown leaves no work behind the journal, and closes the connection. It
// is safe to call more than once.
func (s *Store) Close(ctx context.Context) error {
	s.closeOnce.Do(func() {
		close(s.ops)
		<-s.done // writer drains queued ops and exits before we touch the connection

		// Close the reader first so no idle read connection blocks the truncating
		// checkpoint, which reclaims the WAL only as the last connection standing.
		readerErr := s.reader.Close()

		var checkpointErr error
		if _, err := s.db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
			checkpointErr = fmt.Errorf("wal checkpoint: %w", err)
		}
		s.closeErr = errors.Join(readerErr, checkpointErr, s.db.Close())
	})
	return s.closeErr
}
