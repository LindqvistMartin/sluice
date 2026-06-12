# ADR-003: SQLite concurrency

## Status

Accepted.

## Context

The DLQ is written from several places at once: the ingest handler persisting a
new event, the delivery workers deleting rows on success or rescheduling them on
failure, and the replay worker leasing due rows. SQLite permits exactly one
writer at a time. The question is how to honour that without scattering lock
discipline across every caller, and without giving up the single static binary.

There is also a driver choice hiding in here. The cgo SQLite driver is the default
reflex, but cgo means the binary is no longer static and the build needs a C
toolchain — both at odds with "one binary you copy to a box".

## Decision

Writes go through a single writer goroutine that owns the write connection.
Callers send a request — and a reply channel — over a channel; the writer performs
the `INSERT` / `UPDATE` / `DELETE`, commits, and replies. This is the canonical Go
pattern of ownership through a channel rather than a mutex shared by everyone, and
it matches SQLite's one-writer reality exactly instead of fighting it. Reads use
separate read-only connections; WAL mode lets readers proceed concurrently with
the writer, each seeing a consistent snapshot up to the last commit.

The connection is opened with `journal_mode = WAL`, `synchronous = NORMAL` (the
durability boundary argued in ADR-001), a `busy_timeout` so a momentary lock is a
short wait rather than an immediate error, and `foreign_keys = ON`. The last is
not optional: SQLite disables foreign keys per connection by default, and the
DLQ's `ON DELETE CASCADE` from `events` to `deliveries` (ADR-002) is decorative
without it — eviction would leave orphaned delivery rows that the replay worker
would try to deliver with no body to send.

The driver is `modernc.org/sqlite`, the pure-Go implementation. No cgo, so
`CGO_ENABLED=0` still produces a static binary that runs on `distroless/static`.

## Consequences

**Plus**

- Write serialization is a property of the architecture, not a convention every
  caller has to remember. There is one place that writes, and it is obvious where.
- The static binary survives. `go install` and a distroless image both work with
  no C toolchain in sight, which is half the pitch of the project.
- The DLQ is plain SQLite, so an operator can point `sqlite3 dlq.db 'select ...'`
  at a stuck queue and read it directly. That inspectability is an operational
  feature, and it is the main reason to prefer SQLite over an embedded key-value
  store like BoltDB or Badger, whose stored values are not human-readable or
  directly queryable without writing your own decoding tooling.

**Minus**

- A single writer is a throughput ceiling. For inbound webhook volumes — alerts,
  CI events, deploy hooks — it is nowhere near binding, but Sluice is explicitly
  not a high-write datastore, and that boundary is intentional.
- The pure-Go driver is generally a little slower than the cgo one. The static
  binary and toolchain-free build are judged the better trade for this use case.
- Every connection must set `foreign_keys = ON` itself; a future read path that
  opens a connection and forgets the pragma would silently lose cascade semantics.
  The pragma set is centralized in one place to contain that risk.
