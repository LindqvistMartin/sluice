# ADR-007: Per-status-code retry classification

## Status

Accepted.

## Context

The pool retries a failed delivery on the durable replay path (ADR-004): a failure is
rescheduled until the per-target budget (`retry.max`) is spent, then parked as `dead`.
Until now every failure was treated the same — any non-2xx response and any transport
error was retryable. That is the safe default a first version should ship, but it spends
the whole retry budget on responses that will never change. A target that returns 401
because a token is wrong, 404 because the path is gone, or 422 because the body is
malformed returns it again on the next identical request; retrying five times and waiting
out the backoff only delays the inevitable park and buries a misconfigured target under
retry noise.

## Decision

**A failure is permanent when the target answers with a status it will keep returning.**
A 4xx other than 408 and 429 is the target rejecting this exact request — wrong path,
wrong credentials, unprocessable body — and an identical retry earns the same answer. A
3xx is a redirect the client deliberately does not follow, so it cannot resolve itself
either. Both are classified permanent. The transient failures stay retryable: server
errors (5xx), rate limiting (429), and request timeout (408), as do transport errors and
timeouts, which carry no status at all — the next pass may reach a target that has
recovered. The mapping is a small pure function, so the boundary is tested on its own.

**A permanent failure parks immediately, through the same path as an exhausted budget.**
It does not consult the retry budget: spending four more passes on a request that fails
identically helps no one. It parks as `dead` — parked, not dropped — so the row stays in
the DLQ for an operator to read and replay, the same no-silent-drop rule the queue is
built on.

**The classification is code, not configuration.** As with the backoff bounds, the right
default beats a knob every operator has to understand: the retry-worthy statuses are a
small, well-understood set, and a per-target override would have to thread through config,
validation, and the leased-row resolver for a need no one has raised. If that need
appears, a `retry.on` list is a natural later addition; it is not paid for up front.

**A target's `Retry-After` is not honoured yet.** A 429 or 503 is retried on the standard
jittered backoff rather than the delay the target asks for. Reading the header and feeding
it to the next-attempt time is a larger change — it bypasses the backoff function and
needs its own bounds against a hostile or absurd value — and is left to its own decision.

## Consequences

**Plus**

- The retry budget is spent only where another pass can plausibly help, so a flaky target
  is retried while a misconfigured one is dead-lettered fast instead of after the full
  budget.
- A parked permanent failure surfaces a real problem — a bad URL, a revoked token — quickly
  and visibly in the DLQ, rather than being buried under several scheduled retries.
- The decision is a pure function of the status code, independent of the durable
  scheduling, so it is exhaustively unit-tested without standing up a pool.

**Minus**

- A target that returns a 4xx only transiently — a strict gateway mid-deploy, say — is now
  parked on the first failure rather than riding out the blip on retries. The retryable set
  is kept deliberately conservative to limit this, but it is a behaviour change from the
  retry-everything default, most visibly for 3xx, which used to be retried.
- Without `Retry-After`, a 429 is retried on generic backoff rather than the target's
  requested pace; the backoff cap keeps it bounded, but it is not the target's number.
