# Sluice

Inbound webhook fan-out for self-hosted incident stacks.
One binary, persistent DLQ.

[![CI](https://github.com/LindqvistMartin/sluice/actions/workflows/ci.yml/badge.svg)](https://github.com/LindqvistMartin/sluice/actions)
[![Release](https://img.shields.io/github/v/release/LindqvistMartin/sluice?sort=semver)](https://github.com/LindqvistMartin/sluice/releases)
[![Go](https://img.shields.io/badge/Go-1.26-00add8.svg)](https://go.dev)
[![MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> A small self-hosted SRE stack, built to chain: **pulsewatch** to detect, **sluice** to route webhooks, **flare** to run the incident.

> `v0.1.0` — a small daemon I built while running my own Flare instance. The pipeline is
> durable end to end; the config in [`sluice.example.yml`](sluice.example.yml) is the v0.1
> contract, which may still change before v1.0.

## Why

Most incident tools expose one inbound webhook URL. Once a second system needs to reach it, you either run a queue in front or start dropping events when a target is down. Sluice is that queue. It takes webhooks in, writes them to a local store before acknowledging, and fans them out to every configured target, retrying the ones that fail.

## Quickstart

```sh
go build -o sluice ./cmd/sluice
cp sluice.example.yml sluice.yml     # edit routes and targets
./sluice -t -c sluice.yml            # validate the config before starting
./sluice -c sluice.yml               # run the daemon (listens on :8080)
```

Post a webhook to a configured route; sluice persists it, acknowledges, then fans it out to every target:

```sh
curl -i -X POST localhost:8080/prometheus -d '{"alert":"firing"}'
```

A target that is down does not lose the event — it is parked in the DLQ and replayed on demand, with the daemon up or down:

```sh
./sluice dlq list                    # triage parked deliveries
./sluice dlq retry                   # revive all of them (or -event <id> to scope)
```

Or run the distroless image (~15 MB) instead of a local binary:

```sh
docker build -t sluice .
docker run --rm -p 8080:8080 -v "$PWD/sluice.yml:/sluice.yml:ro" sluice -c /sluice.yml
```

## What works today

- One endpoint in, matched by path and headers
- Per-IP rate limiting and body-size caps in front of it
- Every accepted event persisted to a local SQLite DLQ before it is acknowledged
- Fan-out to many targets, each delivered with its own timeout, retries, and backoff
- Durable retries: failed deliveries are rescheduled on disk and survive a restart, leased so a crashed worker's in-flight work recovers
- Deliveries that exhaust their budget are parked, not dropped, and the DLQ is bounded by size with oldest-first eviction
- Failed deliveries are classified by response: 5xx, 429, 408, and transport errors are retried; other 4xx and 3xx are parked at once rather than spending the retry budget
- A `Retry-After` on a 429 or 503 is honoured to pace the next retry at the target's stated delay, bounded so a target cannot defer a delivery past a cap
- `sluice dlq list` and `sluice dlq retry` to inspect parked deliveries and replay them, with the daemon up or down
- An optional Prometheus metrics endpoint, off by default and never on the inbound port (`metrics_listen`)
- Per-target delivery and failure counters on that endpoint, labelled by route and target
- Structured logging, config-as-code, and a `-t` config check

## Non-goals

Sluice is one node doing one job, so some things are deliberately out of scope:

- **No message broker.** The durable queue is a local SQLite file; there is nothing to run alongside it.
- **No clustering or HA.** A single instance owns its queue, sited next to the stack it feeds; a restart recovers in-flight work from disk rather than failing over.
- **At-least-once, not exactly-once.** A crash mid-report can show a target a duplicate; every delivery carries `X-Sluice-Event-Id` so targets can dedupe.
- **No inbound auth beyond rate-limit and header match.** Put sluice behind a reverse proxy or mTLS if the endpoint needs authentication.
- **No per-target retry knobs.** The retryable status set and the timing bounds are sound defaults, not configuration — the reasoning is in the [ADRs](docs/adr).

One small static binary in a ~15 MB image, with no message broker to run alongside it. Design notes are in the [ADRs](docs/adr).

## License

MIT
