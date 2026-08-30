# ADR-0020: correlationId is not traceId — neither is derived from the other

## Context

Constitution item 9 asks for this distinction explicitly, noting "interviewers conflate these
constantly." Both `correlationId` and the OTel trace ID identify "everything related to this one
logical operation," which is exactly why they're easy to confuse — and exactly why conflating them
is a real design mistake, not just a naming quibble.

## The distinction

**`correlationId`** is a field on `EventEnvelope` (ADR-0002), set once by the root event in a
causal chain (`OrderCreated` sets `correlationId = its own eventId`) and propagated unchanged by
every downstream event in that saga. It is:

- **Business-durable.** It lives in the event payload contract, stored permanently in every
  service's `outbox_events`/`processed_events` rows, queryable in SQL forever. It is part of what
  this project's data model *means* — "which events belong to the same order's saga" is a business
  question `correlationId` answers, not an observability concern.
- **Always present.** Every event has one, unconditionally — there is no sampling, no possible
  absence. `processed_events` dedupe, saga step audit trails (`saga_step`), and any future
  cross-service business query all depend on this being reliably there.

**Trace context** (`traceparent`/`tracestate`, and the trace ID inside it) is:

- **Observability tooling.** It exists so a human debugging a slow or broken request can see the
  causal chain of *spans* — HTTP call, DB write, relay publish, Kafka consume, next DB write — with
  timing, in Jaeger. Nothing in this project's business logic reads a trace ID to make a decision.
- **Can be sampled away.** Under `Sampler.parentBased(Sampler.traceIdRatioBased(p))` with `p < 1.0`,
  most traces are never exported at all — the spans simply don't exist in the backend. A trace ID
  that was never sampled is not recoverable from Jaeger after the fact. `correlationId`, stored in
  Postgres, always is.

## Why neither is derived from the other

They could look derivable — both are UUID-shaped (well, `correlationId` is a UUID;
`traceparent`'s trace ID is a 32-hex-char value with a different format entirely) and both span the
same causal chain in this project's specific case. But deriving one from the other would be a
category error: `correlationId` needs to survive `eventforge.tracing.enabled=false` (tracing
disabled entirely — a legitimate ops posture, not a corner case) and survive sampling-away under any
probability below 1.0. If a future engineer "simplified" the schema by dropping `correlationId` and
reconstructing saga membership from trace IDs instead, every unsampled trace would silently lose
the ability to answer "which events belong together" — a correctness regression hiding behind an
observability feature that was never meant to carry business-load-bearing meaning.

## Decision

They stay two entirely separate mechanisms, never cross-referenced in code: `correlationId` is
written and read only via `EventEnvelope`/`OutboxEventRow`; trace context is written and read only
via `EventForgeTracer`. No method in this codebase accepts one and produces the other.

## The one sentence

*`correlationId` is how this project answers "which events belong to the same business
transaction" — forever, in SQL, whether or not anyone is watching; trace context is how a human
answers "what actually happened, in what order, and how long did each step take" — for exactly as
long as the sampling policy decided to keep a copy, and not one moment longer.*

## Consequences

- `saga_step`/`processed_events` queries for "reconstruct this saga's history" always use
  `correlationId`/`order_id`, never a trace ID — SQL-inspectability (M3's saga design point)
  doesn't depend on whether tracing was even enabled when the saga ran.
- A test disabling tracing entirely (`eventforge.tracing.enabled=false`) must still pass every
  saga/outbox/dedupe test unmodified — nothing business-critical may depend on `EventForgeTracer`
  producing non-null values.

## Revisit if

A future requirement genuinely needs trace data to be as durable/queryable as `correlationId` (e.g.
compliance retention of full request timelines) — at that point the right answer is a durable trace
store with its own retention policy, not conflating `correlationId` with a trace ID to get there.
