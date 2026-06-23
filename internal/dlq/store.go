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
	"sync"
	"time"

	_ "modernc.org/sqlite" // registers the pure-Go "sqlite" driver (CGO_ENABLED=0)
)

// schemaDDL is the durable store: events holds each payload once, deliveries holds
// one row per target with a foreign key that cascades on delete. The full column
// set is created up front so later schema needs no migration; this iteration only
// ever writes status='pending' and never leases.
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

// Store is the SQLite-backed durable queue. All writes go through a single
// goroutine that owns the connection, so write serialization is a property of the
// architecture rather than a lock every caller must remember.
type Store struct {
	db   *sql.DB
	ops  chan writeOp
	done chan struct{}
	log  *slog.Logger

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

	s := &Store{
		db:   db,
		ops:  make(chan writeOp),
		done: make(chan struct{}),
		log:  log,
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

// MarkDelivered removes a delivered target and, once an event has no deliveries
// left in any state, removes the event so a fully delivered event leaves nothing
// behind: in the happy path both tables end empty.
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

// MarkFailed records a delivery that exhausted its in-process retries. A single
// UPDATE needs no transaction: the single-writer connection makes it atomic. The
// row is left 'pending' (status untouched) this iteration; terminal dead-lettering
// and accumulating attempts across passes arrive with the replay worker.
func (s *Store) MarkFailed(ctx context.Context, deliveryID int64, attempts int, lastErr string) error {
	return s.do(ctx, func(ctx context.Context, db *sql.DB) error {
		if _, err := db.ExecContext(ctx,
			`UPDATE deliveries SET attempts = ?, last_error = ? WHERE id = ?`,
			attempts, lastErr, deliveryID); err != nil {
			return fmt.Errorf("update delivery: %w", err)
		}
		return nil
	})
}

// Close stops the writer, checkpoints the WAL into the main database file so a
// clean shutdown leaves no work behind the journal, and closes the connection. It
// is safe to call more than once.
func (s *Store) Close(ctx context.Context) error {
	s.closeOnce.Do(func() {
		close(s.ops)
		<-s.done // writer drains queued ops and exits before we touch the connection

		var checkpointErr error
		if _, err := s.db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
			checkpointErr = fmt.Errorf("wal checkpoint: %w", err)
		}
		s.closeErr = errors.Join(checkpointErr, s.db.Close())
	})
	return s.closeErr
}
