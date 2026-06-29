# Sluice

Inbound webhook fan-out for self-hosted incident stacks.
One binary, persistent DLQ.

[![CI](https://github.com/LindqvistMartin/sluice/actions/workflows/ci.yml/badge.svg)](https://github.com/LindqvistMartin/sluice/actions)
[![Go](https://img.shields.io/badge/Go-1.26-00add8.svg)](https://go.dev)
[![MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> A small self-hosted SRE stack, built to chain: **pulsewatch** to detect, **sluice** to route webhooks, **flare** to run the incident.

> Early development — a small daemon I built while running my Flare instance.
> Configuration and interfaces are not stable yet.

## Why

Most incident tools expose one inbound webhook URL. Once a second system needs to reach it, you either run a queue in front or start dropping events when a target is down. Sluice is that queue. It takes webhooks in, writes them to a local store before acknowledging, and fans them out to every configured target, retrying the ones that fail.

## What works today

- One endpoint in, matched by path and headers
- Per-IP rate limiting and body-size caps in front of it
- Every accepted event persisted to a local SQLite DLQ before it is acknowledged
- Fan-out to many targets, each delivered with its own timeout, retries, and backoff
- Durable retries: failed deliveries are rescheduled on disk and survive a restart, leased so a crashed worker's in-flight work recovers
- Deliveries that exhaust their budget are parked, not dropped, and the DLQ is bounded by size with oldest-first eviction
- Failed deliveries are classified by response: 5xx, 429, 408, and transport errors are retried; other 4xx and 3xx are parked at once rather than spending the retry budget
- `sluice dlq list` and `sluice dlq retry` to inspect parked deliveries and replay them, with the daemon up or down
- An optional Prometheus metrics endpoint, off by default and never on the inbound port (`metrics_listen`)
- Per-target delivery and failure counters on that endpoint, labelled by route and target
- Structured logging, config-as-code, and a `-t` config check

## Planned

- Honouring a target's `Retry-After` on 429 and 503 to pace the next retry, instead of the standard backoff

One small static binary in a ~15 MB image, with no message broker to run alongside it. Design notes are in the [ADRs](docs/adr).

## License

MIT
