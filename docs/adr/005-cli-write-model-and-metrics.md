# ADR-005: Operator CLI write model and the metrics endpoint

## Status

Accepted.

## Context

ADR-004 left two things deferred: a way to replay parked deliveries, and a metrics
endpoint to publish the eviction counter the worker already keeps. Both are operator
surfaces on a queue that, by ADR-003, has exactly one writer — a goroutine inside the
daemon that owns the only write connection. ADR-003 also blessed reading the database
from outside the daemon (`sqlite3 dlq.db 'select ...'`), but said nothing about an
external process *writing* it.

Replaying a parked delivery is a write: it moves a row from `dead` back to `pending`.
So the open question is how an operator command issues that write without breaking the
single-writer invariant — and whether it should reach the database directly or go
through the running daemon. Two facts shape the answer. Parking happens during an
outage, exactly when an operator may have stopped the daemon to investigate; and the
daemon's inbound listener is a bare handler that rate-limits and rejects non-POST
traffic, with no admin surface to extend.

## Decision

**`sluice dlq` writes the database directly, as a short-lived process.** It is not an
admin HTTP endpoint. An endpoint cannot run when the daemon is down, which is the case
the command most needs to serve, and adding a mutating route to the public inbound
listener would mean putting an operator control on the same port as untrusted webhook
traffic. A direct-DB command is the write analogue of the already-blessed external
read.

This does not violate the single-writer invariant: that invariant is per-process —
one writer goroutine owning one connection inside the daemon. A second OS process is a
different writer, and SQLite, not the daemon, arbitrates between them. The connection
string (ADR-003) already carries what makes that safe across processes: WAL so the
command's transaction does not block the daemon's readers, `busy_timeout` so a momentary
lock becomes a short wait rather than an error, and `_txlock=immediate` so the writer
takes the lock at `BEGIN`. The command's write is a single statement; it never
interleaves with the daemon's writer.

**The command uses a dedicated, lightweight handle, not the daemon's `Store`.**
`Store.Open` starts the writer goroutine and applies the schema, and `Store.Close`
checkpoints the WAL — all wrong for a one-shot process sharing the file with a live
daemon, which owns checkpointing. The handle reuses the same `dsn` (so it inherits the
pragmas, including `foreign_keys`, whose omission ADR-003 warns silently breaks the
cascade), opens with no goroutine and no schema step, refuses a missing file rather
than lazily creating an empty one, and closes with a plain connection close.

**Un-parking is one guarded, idempotent statement.** It is the only `dead -> pending`
transition in the store: the deliberate inverse of the `status = 'pending'` guard every
other write keeps to refuse resurrecting a parked row. It resets `attempts` to zero —
the pool parks once attempts exceed the retry budget, so a row revived at its exhausted
count would re-park after a single failed attempt — clears the lease, blanks the last
error, and sets the next attempt to now so the row is immediately due. A row that is
not `dead` (already pending, in flight, or delivered) matches nothing and is a no-op;
the command reports how many rows it revived. The running worker, which rescans on its
own interval, claims the revived row on its next pass with no nudge or restart.

The command ships two subcommands: `retry` (all parked rows, or one event with
`-event`) and `list` (the parked rows for triage, omitting the event body, which can be
large or hold secrets). Counts are not a third subcommand; they belong to metrics.

**Metrics are a separate, opt-in listener.** A new `metrics_listen` address enables the
endpoint; empty — the default — disables it. It never shares the inbound port, and
validation rejects a metrics address equal to `listen` so an operator cannot expose it
there by mistake. The exposition is hand-rolled Prometheus text: a counter
(`sluice_dlq_evicted_total`, from the worker's existing counter), two gauges
(`sluice_dlq_pending`, `sluice_dlq_dead`, counted at scrape time), and build info.
Pulling a metrics client for a handful of values is not worth a dependency in a project
that ships as one small static binary. A gather error returns 503 — a failed scrape,
not stale numbers.

## Consequences

**Plus**

- Replay works with the daemon down, which is when an operator usually reaches for it,
  and needs no broker, no admin port, and no new dependency.
- The single-writer invariant is untouched: the command is a separate process, and
  SQLite's file locks plus `busy_timeout` are the cross-process arbiter ADR-003 relies
  on. The handle never checkpoints, leaving the file's lifecycle to the daemon.
- Metrics are off by default and can never land on the public webhook port, matching the
  fail-closed posture of the rest of the daemon.

**Minus**

- Cross-process safety rests on `busy_timeout`, not an advisory lock: under sustained
  daemon write load a command could wait out the timeout and fail with a busy error.
  At webhook volumes this is far from the single-writer ceiling, and the guarded,
  idempotent statement is safe to simply re-run.
- A retry issued against a live daemon can produce one duplicate delivery per revived
  row if the target had in fact already received it — the same at-least-once window as a
  lease-expiry redelivery, covered by the `X-Sluice-Event-Id` dedup header (ADR-001).
- The pending and dead gauges are counted per scrape rather than tracked incrementally;
  a single indexed count is cheap, and it keeps the delivery path free of instrumentation.
