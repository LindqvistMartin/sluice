# ADR-006: Per-target delivery counters

## Status

Accepted.

## Context

ADR-005 closed the metrics endpoint with deliberately narrow scope: one counter the
worker already kept (`sluice_dlq_evicted_total`) and two gauges (`sluice_dlq_pending`,
`sluice_dlq_dead`) counted at scrape time. It noted that counting the gauges per scrape
"keeps the delivery path free of instrumentation." The remaining roadmap item was
per-target delivery and failure counters — how many deliveries each target took and how
many failed — which the queue-wide series cannot answer: they say nothing about which
target is healthy and which is absorbing retries.

Two facts make this more than another scrape-time series. First, a target has no name or
ID in config — only a URL — and validation dedups URLs per route, not globally, so the
same URL may be configured under two routes. The stable identity is the `(route, target)`
pair. Second, a successful delivery removes its row (the store's `MarkDelivered`), so
unlike pending and dead, a delivered count cannot be recovered from the database at scrape
time — the rows are gone. A monotonic delivered count has to be kept as deliveries happen.

## Decision

**The counters live in process and are incremented on the delivery path.** This is the
instrumentation ADR-005 avoided for the gauges, and here it is unavoidable: a delivered
row is removed, so no scrape-time query can reconstruct how many landed. The pool already
decides each outcome — delivered, rescheduled, or parked — so it is the one place that
sees every event exactly once, and the counts ride those same branches.

**The key is the `(route, target)` pair, not the URL alone.** A target URL is unique only
within its route, so keying on the URL would silently merge two distinct targets that
happen to share an endpoint into one series. The route is carried on the leased row but was
dropped when the worker built a `Delivery`; it is now threaded through so the pool can
label each outcome. Both labels come from static config, so the label set is bounded —
there is no request-derived cardinality to fear.

**`sluice_delivery_failures_total` counts every failed attempt, not just the terminal
park.** A non-2xx or transport error increments it whether the row is then rescheduled or
parked. This mirrors the standard success/failure-rate pair and surfaces retry pressure: a
target that fails twice before succeeding is two failures and one delivery, which a
terminal-only count would hide. The final give-up is already visible through the `dead`
gauge.

**Counting is gated on the endpoint being enabled.** When `metrics_listen` is empty the
pool is handed a no-op counter, so the default configuration keeps its delivery path free
of even this much work, consistent with ADR-005's opt-in posture. The exposition reuses the
hand-rolled renderer: two new counter families, one labelled line per target, HELP and TYPE
written once ahead of their samples, with the same label escaping as the build-info version.

**The worker's "target left config" deferral is not counted.** When a leased row names a
target no longer in config, the worker pushes it out rather than attempting it — there is
no HTTP attempt, so it is neither a delivery nor a failure. Counting it would conflate a
config-drift bookkeeping move with a target's real success rate.

## Consequences

**Plus**

- An operator can see per-target health — which target is failing or absorbing retries —
  instead of only the queue-wide pending and dead totals.
- The `(route, target)` key keeps two routes that share a URL as separate series, so the
  numbers stay attributable; the labels are config-bounded, so the series count cannot
  blow up.
- With metrics disabled the delivery path is unchanged: a no-op counter, no map, no lock.

**Minus**

- The delivered and failure counts are in-process and reset on restart, unlike the DLQ
  gauges recomputed from the database. They are rate-and-trend signals, not durable totals;
  a restart shows as a counter reset, which is the normal Prometheus counter contract.
- A counter is held per `(route, target)` for the life of the process, even after a target
  is removed from config, until the daemon restarts. At config-scale cardinality this is
  negligible.
- Per-target instrumentation now sits on the hot path when metrics are enabled — one
  mutex-guarded map increment per outcome. At webhook volumes this is far below the HTTP
  cost of the delivery it accompanies.
