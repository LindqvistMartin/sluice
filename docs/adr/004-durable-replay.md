# ADR-004: Durable replay, leasing, and eviction

## Status

Accepted.

## Context

Persisting an event before acknowledging it (ADR-001) is only half a durable queue.
Until now retries lived in memory: a worker attempted a target, and on failure looped
in process with backoff up to the budget. Two gaps followed from that. A delivery that
exhausted its in-process retries was left `pending` forever — never retried again,
never dead-lettered — so the store accumulated rows nothing would ever drive. And a
process that died mid-attempt lost the in-memory retry state entirely; the row stayed
`pending`, but nothing re-attempted it after restart. The store was written but never
read: there was no path back out of it.

The schema was provisioned for the fix from the start — `status`, `attempts`,
`next_attempt_at`, `lease_until`, and an `idx_deliveries_due(status, next_attempt_at)`
index — but none of it was used. This decision puts it to work.

## Decision

A **replay worker** becomes the sole delivery driver. Ingest persists an event and
nudges the worker; the worker scans for due deliveries, claims them, and submits each
to the pool. The pool makes **one attempt per submission** and reports the outcome:
delivered (the row is removed), rescheduled (a later `next_attempt_at`, still pending),
or parked (`status = 'dead'`). Retries are durable rows, not in-memory loops, so a
restart resumes them and an exhausted delivery has a terminal state.

**Claiming leases by timestamp.** A claim sets `lease_until = now + leaseTTL` and
leaves `status = 'pending'`; the due scan excludes any row whose lease has not expired.
The select-and-lease runs in one transaction on the single write connection (ADR-003),
so two concurrent claims cannot take the same row. Keeping the status `pending` rather
than introducing a `leased` state has two payoffs: a crashed worker's row recovers for
free once its lease expires, with no startup sweep to reset it; and the constant
`status = 'pending'` predicate keeps the due index usable. Dead rows sit in a separate
status partition and the scan never visits them.

**The pool owns the reschedule-versus-park decision**, because the retry budget
(`retry.max`) is per-target config, not a stored column. A claimed row carries only its
target URL, so the worker resolves the target's timeout and budget from the current
config, keyed by route and URL, when it builds the delivery. Resolving from config
(rather than denormalizing the budget into the row) keeps the schema migration-free and
means an operator who widens a timeout during an outage sees it apply to already-queued
work. A target removed from config resolves to "unknown"; rather than park the row
(the config may be restored), the worker pushes its next attempt well out, so it is
revisited rarely instead of re-claimed every lease window, and remains recoverable.

**Eviction bounds the store** per ADR-002: a periodic check measures logical size
(`(page_count - freelist_count) * page_size`, after a checkpoint) and, when it exceeds
`max_size_mb`, deletes the oldest events first, their deliveries cascading via the
foreign key. Each eviction is logged at `ERROR` and counted. Because eviction can drop
a row another worker is mid-delivery on, the terminal transitions (`MarkDelivered`,
`Reschedule`, `Park`) treat a zero-row match as a no-op rather than an error.

`sluice_dlq_evicted_total` is, for now, an `ERROR` log line plus an in-process counter
the worker exposes; a metrics endpoint to publish it is left for a later change, as is
the `sluice dlq retry` command for manually replaying parked rows.

## Consequences

**Plus**

- Retries survive process death: an undelivered row is simply a due row after restart,
  and leasing makes crash recovery automatic with no reconciliation step.
- One retry path, one source of attempt counts, one input contract into the pool —
  there is no in-memory retry state to disagree with the durable record.
- A delivery that can never succeed parks as `dead` instead of lingering `pending`
  forever, and the store can no longer grow without bound.

**Minus**

- A freshly persisted event waits up to one scan interval before its first attempt; the
  wake nudge keeps that close to immediate in practice, but the ack still means
  persisted, not delivered (ADR-001), so this only shifts when delivery starts.
- At-least-once gains a new duplicate window: a worker that succeeds at the target but
  dies before recording it re-delivers after the lease expires. The `X-Sluice-Event-Id`
  dedup header (ADR-001) covers it; `leaseTTL` is derived from the longest configured
  target timeout (6x, floored at 60s) so a healthy-but-slow attempt keeps its lease and
  a duplicate stays a crash-only event rather than a routine one.
- Each transient failure now costs a database write to reschedule, where before it was
  an in-memory sleep. At webhook volumes this is far below the single-writer ceiling
  (ADR-003), and durability of retry state is the point of the queue.
