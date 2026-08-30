# ADR-0019: Always-sample policy, and why a parent's sampling decision must survive propagation

## Context

Constitution item 8: state the sampling policy, and confirm a sampling decision made at the HTTP
entry point actually propagates correctly through the outbox and relay — "a dropped sampling flag
that silently breaks downstream traces is a real bug."

## The policy

`eventforge.tracing.sampling-probability=1.0` — always sample, in every service, by default. This
is a deliberate, stated choice for what this project currently is: a portfolio/demonstration system
whose entire point is having a trace to look at, not a production service under real load where
head-based sampling exists to control export volume and backend cost. There is no traffic here to
trim. Stating 1.0 as policy rather than leaving the default unexamined is the point of this ADR —
R2 forbids presenting an untuned number as if it were measured, and this one is explicitly not a
measurement, just a policy for a system with no load to sample away.

## Why the mechanism matters more than the number

The number (1.0) is easy. What's easy to get wrong — and what the work order calls out by name —
is whether a sampling decision, once made, actually **survives** every hop between where it's made
and where a consumer of that decision needs to respect it. Two things make this hold here:

1. **The sampled/not-sampled decision lives inside the propagated `traceparent` string itself** (the
   trailing two hex digits, per the W3C spec — `01` sampled, `00` not). Because M4 uses the *real*
   W3C propagator (`W3CTraceContextPropagator`) to produce every `traceparent` this project stores
   and transports — not hand-rolled string surgery, unlike M1 — that flag byte round-trips
   correctly through every hop for free: DB column → Kafka header → next DB column → next Kafka
   header, byte-for-byte, because it's genuinely part of the string being carried, not a separate
   piece of state something could forget to also propagate.
2. **Every span in this system uses `Sampler.parentBased(...)`** (`TracingAutoConfiguration`), not
   an independent ratio sampler at each hop. A `ParentBased` sampler's rule is simple and absolute:
   if a parent context says "sampled," every child inherits that and is sampled too, regardless of
   what that child's own ratio would have decided in isolation; if a parent says "not sampled," no
   child re-rolls the dice and second-guesses it. This is what makes "a sampling decision made at
   the HTTP entry point propagates correctly through the outbox and relay" actually true rather
   than aspirational — the relay's child span (ADR-0005's M4 note, ADR-0017) and every consumer
   span downstream are children of whatever was extracted from the stored/transported context, so
   they inherit the origin's decision unconditionally.

## The bug this guards against

**Without `ParentBased`**, each hop's `Sampler.traceIdRatioBased(p)` would make its OWN independent
decision using the trace ID's hash — which is stable per trace ID, so in practice a single ratio
sampler at every hop usually agrees with itself. The real danger is a hop that has NO sampler
context to inherit from at all (e.g. a span accidentally started with `setNoParent()` instead of
extracting the stored context) — that span would silently start a *new* decision, and worse, a new
trace ID, orphaning everything downstream of it into a second, disconnected trace. This is exactly
the shape of bug M1's crash-window testing discipline exists to catch by proof rather than
inspection: `SamplingPropagationIntegrationTest` forces a specific inbound `traceparent` with the
NOT-sampled flag (`...-00`) at the HTTP entry, and asserts the stored outbox row and the relayed
Kafka header both still carry `-00` end to end, while a parallel sampled trace (`-01`) in the same
test produces real exported spans and the unsampled one produces none — proving the flag isn't just
present in the string, but actually respected.

## Consequences

- The one clock-driven exception is `SagaOrchestrator.handleTimeout` (ADR-0015): a timeout has no
  inbound context to inherit a sampling decision from at all, so it starts a fresh root span,
  which — under the always-1.0 policy — is sampled anyway. If this project's sampling probability
  is ever lowered below 1.0, timeout-triggered traces would independently re-roll under the ratio
  sampler, which is correct behavior for a genuinely new trace, not a bug.

## Revisit if

Sampling probability is ever lowered below 1.0 for real (e.g. this project starts generating
enough load to matter) — at that point, re-verify `SamplingPropagationIntegrationTest` still holds
under a fractional ratio, not just the always-on case this ADR was written against.

## Amendment: the bug this ADR predicted, found by actually running the test

`SamplingPropagationIntegrationTest` was written before it was ever executed (see the M4
stop-and-report's verification transcript). Running it for the first time failed immediately: a
`...-00` (not-sampled) inbound `traceparent` came back as `...-01` (sampled) after the outbox write
and relay publish — the exact failure mode this ADR's "bug this guards against" section describes,
just one layer further out than expected. The production code (`TracingAutoConfiguration`,
`EventForgeTracer`) was correct throughout. The actual defect was in the test's own
`SdkTracerProvider`, in `common-testing`'s `TracingTestConfiguration`: it configured
`Sampler.alwaysOn()` directly, not `Sampler.parentBased(Sampler.alwaysOn())`. A bare
`AlwaysOnSampler` does not consult its parent at all — it forces `RECORD_AND_SAMPLE` unconditionally,
which silently overrides *any* propagated not-sampled decision, in every test that imports it, not
just this one. It happened to be harmless everywhere else, because no other test exercises a
deliberately-not-sampled inbound context; this one does, by design, which is exactly why it caught
it. Fixed to `Sampler.parentBased(Sampler.alwaysOn())`: root spans (no parent) are still always
sampled — preserving full trace-structure recording for every other test that imports this class —
but a span with a real propagated parent now defers to that parent's own decision, matching
production's `Sampler.parentBased(Sampler.traceIdRatioBased(p))` shape exactly. This is the concrete
instance of R10 (verify, don't recall): the mechanism was correct on paper and in review; only
actually running the assertion against real infrastructure surfaced the gap, one layer removed from
where the ADR's own reasoning was aimed.
