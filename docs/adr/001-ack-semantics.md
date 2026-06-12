# ADR-001: Ack semantics

## Status

Accepted.

## Context

A webhook receiver makes exactly one promise to its senders: the meaning of a
`200`. Get that promise wrong and every reliability claim downstream of it is
built on sand.

There are three places to send the `200`:

- **On receive** — reply as soon as the request is read. Fast, and a lie: if the
  process dies between the reply and the write, the event is gone but the sender
  believes it was accepted. Senders that treat `2xx` as "delivered" never retry it.
- **On all delivered** — hold the inbound request open until every downstream
  target has accepted. Honest about delivery, but it couples inbound latency to
  the slowest target and turns one stuck downstream into inbound timeouts at the
  sender. A fan-out daemon that blocks the source on a flaky Slack webhook has
  failed at its one job.
- **On durable receive** — reply only once the event is on disk and the
  transaction is committed, then fan out asynchronously.

## Decision

Sluice acks on durable receive. The inbound `200` means **persisted**, not
**seen** and not **delivered**.

The handler writes the event into the SQLite DLQ and waits for the writer to
confirm the commit before it sends the response. Only then does fan-out begin, on
its own goroutines. Inbound latency is therefore the cost of one local disk
commit, independent of how many targets a route has or how healthy they are.

If the event cannot be persisted — disk full, DLQ unavailable — the handler
returns `503` with `Retry-After`. It never returns `200` without durability. A
5xx is an honest "try again", and a sender like Alertmanager will.

The durability boundary is explicit, and it is **process death, not power loss**.
With `journal_mode=WAL` and `synchronous=NORMAL`, a committed event survives
`kill -9`, OOM, and panic. `NORMAL` writes the commit frame to the WAL but
deliberately skips the per-commit `fsync`: the bytes reach the operating system's
page cache via `write()`, so a process that dies leaves them intact for the kernel
to flush, but they are not yet forced to stable storage. What `NORMAL` therefore
does not guarantee is survival of an OS crash or power cut in the window before the
kernel flushes that cache; only `synchronous=FULL`, which fsyncs every commit,
closes that window — at a throughput cost. Sluice defaults to `NORMAL` and exposes
`FULL` as a config option for operators who want it.

A clean shutdown checkpoints the WAL into the main database file; after an unclean
stop, SQLite replays the WAL automatically on next open, so a committed event is
recovered with no operator action (absent power loss). The storage mechanics are
in ADR-003.

## Consequences

**Plus**

- Inbound latency is decoupled from downstream health. One stuck target cannot
  become inbound timeouts at the sender.
- The `200` is a guarantee that holds across a daemon crash: a persisted event is
  redelivered after restart from the same `dlq.db`.
- The contract is small enough to state in one sentence in the README, which is
  exactly the kind of claim a reviewer checks and then trusts.

**Minus**

- Delivery is **at-least-once**, never exactly-once. Retries can duplicate a
  delivery, so every outbound request carries an `X-Sluice-Event-Id` header for
  receiver-side dedup. The README will not claim exactly-once in any wording.
- `synchronous=NORMAL` accepts a real, narrow data-loss window on OS or power
  death. This is a deliberate default, not an oversight; `FULL` is one config
  line away for those who want it.
- The honesty of the `503` depends on the sender retrying 5xx. Alertmanager does.
  **GitHub does not** auto-resubmit a failed delivery — as of 2026 it offers a
  manual redeliver in a roughly three-day window, or a script against its REST
  API. That is a property of the sender, not something Sluice can fix; it is
  stated here so it is not silently assumed away.

## Verification

The guarantee is pinned by the crash-recovery integration tests — the
`TestFanout_MultiTarget_HappyPath` contract committed now, extended once the
delivery pipeline lands: an event acked with `200` is still delivered after the
daemon object is destroyed and recreated over the same `dlq.db`, the in-process
equivalent of `kill -9`.
