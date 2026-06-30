# Changelog

All notable changes to this project are documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). Being pre-1.0, the
configuration contract may change in a minor release; such changes are called out here.

## [Unreleased]

## [0.1.0] - 2026-06-30

First tagged release: a single-binary webhook fan-out daemon with a durable, on-disk queue
and no broker to run alongside it.

### Added

- **Ingestion.** One inbound endpoint, routed by path and gated by optional header match,
  behind per-IP rate limiting and a body-size cap.
- **Durable queue.** Every accepted event is written to a local SQLite DLQ before it is
  acknowledged, so an ack means persisted, not merely received.
- **Fan-out.** Each event is delivered to every configured target, with its own timeout,
  retry budget, and full-jitter exponential backoff.
- **Durable retries.** Failed deliveries are rescheduled on disk and survive a restart,
  leased so a crashed worker's in-flight work is recovered rather than lost or duplicated.
- **Bounded, no silent drops.** Deliveries that exhaust their budget are parked, not
  dropped; the DLQ is bounded by size with oldest-first eviction.
- **Per-status retry classification.** 5xx, 429, 408, and transport errors are retried;
  other 4xx and 3xx are parked at once instead of spending the retry budget (ADR-007).
- **Retry-After.** A 429 or 503 carrying `Retry-After` is retried at the target's stated
  delay rather than the generic backoff, bounded against a hostile value (ADR-008).
- **Operator CLI.** `sluice dlq list` and `sluice dlq retry` inspect and replay parked
  deliveries, with the daemon up or down.
- **Optional metrics.** An opt-in Prometheus endpoint on a separate address (never the
  inbound port), exposing queue depth and per-target delivery and failure counters.
- **Operability.** Structured logging in text or JSON, config-as-code with strict parsing,
  a `-t` config check, and a `-version` flag.
- **Distribution.** A distroless, non-root container image of around 15 MB.

[Unreleased]: https://github.com/LindqvistMartin/sluice/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/LindqvistMartin/sluice/releases/tag/v0.1.0
