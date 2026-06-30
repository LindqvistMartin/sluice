# ADR-008: Honouring Retry-After on 429 and 503

## Status

Accepted. Resolves the deferral recorded in ADR-007.

## Context

ADR-007 split failures into retryable and permanent, but every retryable failure was
paced the same way: the jittered exponential backoff of ADR-004, drawn from the attempt
count. That ignores the one case where the target tells us exactly how long to wait. A 429
(Too Many Requests) and a 503 (Service Unavailable) often carry a `Retry-After` header —
the target asking the client to come back after a stated delay. Retrying a rate-limited
target on our own backoff instead of its number is the wrong move twice over: too soon
wastes an attempt the target will reject again, and too late leaves the event sitting when
the target was ready. ADR-007 left this open precisely because reading the header and
feeding it to the next-attempt time bypasses the backoff function and needs its own bounds
against a hostile or absurd value.

## Decision

**Retry-After is honoured on 429 and 503, and nowhere else.** These are the two statuses
RFC 9110 pairs the header with, and each is an explicit "come back later" — a rate limit or
a stated outage window. The other retryable failures make no such promise: a 500 or 502 is
the target failing, not pacing us, and a transport error carries no header at all. They
keep the generic backoff even if a `Retry-After` happens to be present, so the hint never
takes over where it was not meant to. The status set is a small predicate, tested on its
own.

**Both header forms are parsed, and only a sane value wins.** RFC 9110 allows either
delta-seconds (a non-negative integer) or an HTTP-date. Both are read; an HTTP-date already
in the past becomes a zero delay — its moment has arrived — rather than a negative one. A
missing, negative, or unparseable value yields no hint, and the delivery falls back to its
backoff. Parsing cannot fail the delivery: the worst a bad header does is leave the existing
behaviour in place.

**An honoured delay is bounded by a cap that is deliberately larger than the backoff cap.**
A target could send `Retry-After: 86400` and pin a row for a day, so the honoured value is
clamped to `DefaultMaxRetryAfter` (5 minutes). That cap is larger than the 30-second backoff
cap on purpose. The backoff cap bounds *our guess* at how long to wait; Retry-After is the
*target's own number*, which earns more latitude. It is affordable because a retry is
durable (ADR-004): the row waits on disk with a next-attempt time, not in a held worker, so
a longer honoured delay costs a queue slot, not a goroutine — and real 429/503 windows run
to minutes, not the backoff's seconds.

**The retry budget still decides parking; the hint only paces a retry that will happen
anyway.** The schedule checks `attempts > retry.max` first, exactly as before, so a
Retry-After never rescues a delivery that has run out of budget — it only sets *when* the
next attempt lands, never *whether* one does. The header is parsed in the attempt, clamped
in the pool where the cap lives, and applied in the scheduler as an override of the backoff
offset; a negative sentinel keeps the backoff in charge when there is no hint.

**The cap is code, not configuration**, as with the backoff bounds (ADR-007): a single
sound default beats a knob every operator has to reason about. A per-target
`retry.max_retry_after` is a natural later addition if the need appears; it is not paid for
up front.

## Consequences

**Plus**

- A rate-limited target is retried at the pace it asked for, so sluice stops hammering a 429
  on a backoff that is too eager and stops sitting on an event past the window the target
  named.
- The behaviour is bounded against a hostile or fat-fingered header: the worst a target can
  do is defer its own row to the cap, never longer, and never affect another target's.
- Parsing is a pure function of the header and the clock, unit-tested across both forms and
  every rejection, and the end-to-end pacing and clamp are covered without sleeping.

**Minus**

- A target can still push its next retry out to the cap, longer than the backoff would have.
  That is the point — it is the target's stated window — but it does widen the delay a single
  misbehaving-but-not-hostile target can introduce, up to the bound.
- The cap is one global default, not per-target. A deployment with one target that needs a
  longer honoured window and another that should be held tighter cannot express that yet;
  the knob is deferred until asked for.
- The delivery hot path grows one parse and one branch on the 429/503 failures. It is off
  the success path entirely and trivial on the failure path, but it is not free.
