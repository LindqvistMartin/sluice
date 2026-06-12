# ADR-002: Body lifecycle and bounded DLQ

## Status

Accepted.

## Context

Fan-out means one inbound event becomes N outbound deliveries. The naive shape —
one row per delivery, each carrying its own copy of the body — duplicates the
payload N times and makes "is this event fully delivered?" a query over scattered
rows. It also leaves two questions unanswered that decide whether the DLQ can be
trusted: what happens when the queue grows without bound, and what happens to an
event that can never be delivered.

A DLQ that drops data silently under pressure is worse than no DLQ, because it
looks healthy while losing events. A DLQ in which dead events simply disappear is
not a dead-letter queue at all.

## Decision

The body is stored once. The schema is two tables: `events` holds the payload,
headers, and receipt time keyed by a random event id; `deliveries` holds one row
per target, referencing `events.id` with `ON DELETE CASCADE`.

A successful delivery deletes its `deliveries` row. An event is deleted only once
it has no `deliveries` rows left in any state, so a fully delivered event leaves
nothing behind. **In the happy path both tables are empty** — the database size
tracks the real backlog, and `sqlite3 dlq.db 'select count(*) from deliveries'`
honestly reports `0` in the demo.

The DLQ is bounded by `max_size_mb`. When the bound is exceeded, the oldest events
are evicted first, each eviction logged at `ERROR` and counted in
`sluice_dlq_evicted_total`. Data loss under sustained pressure is possible, but it
is **loud** — never masked.

Size is measured **logically**, as `(page_count - freelist_count) * page_size`,
not as the file size on disk. SQLite does not shrink the file when rows are
deleted — freed pages go on a freelist and are reused — so a file-size trigger
would never fall once the high-water mark is reached and would keep evicting live
events even when real occupancy is low. The logical measure counts allocated pages
in the main database file (whole pages, not live bytes, and after checkpoint),
which is the right granularity for bounding what the DLQ has committed to disk.

An event that exhausts its retry budget is not deleted. Its `deliveries` row moves
to `status = 'dead'`, where it stays — visible to `sqlite3`, and to the `sluice
dlq retry` command planned for a later release. Because that row stays, so does its
parent `events` row: a dead delivery pins the event that carries its body, which is
what makes parking durable rather than a dangling reference. Dead-lettering is
parking, not deletion.

## Consequences

**Plus**

- The payload exists once regardless of fan-out width; storage tracks events, not
  events times targets.
- "Fully delivered" is the absence of rows, which is trivial to observe and makes
  the empty-queue demo truthful rather than staged.
- Both failure modes that erode trust — unbounded growth and vanishing dead
  events — have explicit, observable answers.

**Minus**

- Drop-oldest is still data loss when downstream is down long enough to blow the
  bound. The design makes the loss loud (ERROR + metric); it does not make it
  impossible. Sizing `max_size_mb` for the expected outage window is the
  operator's call.
- Logical-size accounting depends on `foreign_keys = ON` so that evicting an
  `events` row cascades to its `deliveries`; without the pragma the cascade is
  decorative and eviction would orphan delivery rows. ADR-003 makes that pragma
  load-bearing.
- The on-disk file does not shrink after eviction or drain. This is expected
  SQLite behaviour, not a leak; `VACUUM` is left to the operator and kept off the
  hot path.
