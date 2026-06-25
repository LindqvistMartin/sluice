package dlq

import (
	"context"
	"database/sql"
	"fmt"
	"os"
)

// CLI is a short-lived operator handle over the DLQ file, used by `sluice dlq ...`.
// Unlike Store it starts no writer goroutine, applies no schema, and does not
// checkpoint on close, so it can share the file with a running daemon: WAL plus the
// busy_timeout and _txlock=immediate pragmas in dsn let SQLite's file locks serialize
// its single guarded UPDATE against the daemon's writer, and the daemon's own
// connection owns WAL checkpointing. It is the write analogue of inspecting the queue
// with `sqlite3 dlq.db 'select ...'`, and it works whether the daemon is up or down —
// the latter being the common case, just after an incident.
type CLI struct {
	db *sql.DB
}

// OpenCLI opens an existing DLQ at path for operator commands. It refuses a missing
// file rather than letting sql.Open lazily create an empty database: a typo in the
// configured path should fail loudly, not silently report zero parked deliveries.
func OpenCLI(path string) (*CLI, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("no DLQ at %s: %w", path, err)
	}
	db, err := sql.Open("sqlite", dsn(path))
	if err != nil {
		return nil, fmt.Errorf("open dlq: %w", err)
	}
	db.SetMaxOpenConns(1)
	return &CLI{db: db}, nil
}

// RetryParked revives parked deliveries so the worker attempts them again. It is the
// only dead->pending transition in the store: the deliberate inverse of the
// status='pending' guard every other write keeps to refuse resurrecting a parked row.
// It resets attempts to 0 — the pool parks once attempts exceed the retry budget, so a
// row revived at its exhausted count would re-park after a single failed attempt — and
// makes the row immediately due. With no eventID it revives all parked rows; with one
// it scopes to that event. Returns the number of rows revived; a row that is not dead
// (already pending, in flight, or delivered) is a clean no-op.
func (c *CLI) RetryParked(ctx context.Context, eventID string, now int64) (int64, error) {
	q := `UPDATE deliveries
	      SET status = 'pending', attempts = 0, next_attempt_at = ?, lease_until = 0, last_error = ''
	      WHERE status = 'dead'`
	args := []any{now}
	if eventID != "" {
		q += ` AND event_id = ?`
		args = append(args, eventID)
	}

	res, err := c.db.ExecContext(ctx, q, args...)
	if err != nil {
		return 0, fmt.Errorf("retry parked: %w", err)
	}
	return res.RowsAffected()
}

// ListDead returns every parked delivery for an operator to triage, oldest first.
func (c *CLI) ListDead(ctx context.Context) ([]DeadDelivery, error) {
	return listDeadQuery(ctx, c.db)
}

// Close releases the connection. It does not checkpoint the WAL: that belongs to the
// daemon's Store, which owns the file's lifecycle.
func (c *CLI) Close() error {
	return c.db.Close()
}
