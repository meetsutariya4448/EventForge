# Runbook: saga compensation failure (`COMPENSATION_FAILED`)

Constitution item 6, "the hard case": what an operator does when a saga's compensation itself has
failed permanently. See ADR-0015 for the design this runbook is the operational half of.

## What this state means

A saga reached `COMPENSATION_FAILED` because `RefundPayment` was dispatched
`eventforge.saga.max-compensation-attempts` times (default 3) and payment-service never answered
any of them with `PaymentRefunded` before each attempt's deadline. Concretely, in this project's
current scope, the only way this happens is a payment-service consumer that is down, wedged, or
permanently failing for the whole duration of every retry window — not a transient blip, which the
bounded retry already absorbs.

**What is and is not true when you see this state:**

- The order's payment **was authorized** (money was taken, or the local-ledger equivalent — see
  ADR-0013 on what this means once a real payment provider is involved).
- The refund **was never confirmed**. It may not have happened at all (payment-service never
  processed any `RefundPayment` attempt), or it may have happened but the confirmation never
  reached the orchestrator — layer 2 of ADR-0016's compensation-idempotency design exists
  specifically to make the second case safe to resolve without double-refunding, but resolving it
  is still a manual step, not automatic.
- The saga will **not** retry further. This state is terminal by design (ADR-0015) — leaving it
  alone does not resolve it.

## How to find affected orders

```sql
-- On order-service's database.
SELECT order_id, status, updated_at FROM orders WHERE status = 'CANCELLATION_FAILED';

SELECT saga_id, order_id, state, compensation_attempts, updated_at
FROM saga_instance WHERE state = 'COMPENSATION_FAILED';

-- Full step history for one saga - what was actually dispatched, and when.
SELECT step_name, status, dispatched_at, completed_at, detail
FROM saga_step WHERE saga_id = '<saga_id>' ORDER BY dispatched_at;
```

The alert surface (ADR-0015): the `saga.compensation.failed` Micrometer counter
(`/actuator/metrics/saga.compensation.failed` on order-service) increments once per saga that
reaches this state — in a real deployment this is what paging would be wired to. This runbook is
what the page should link to.

## Manual remediation steps

1. **Confirm payment-service is actually the problem, not still just slow.** Check
   payment-service's own health/logs (`docker logs`, or `/actuator/health` if it's up at all). If
   it's genuinely down, bring it back up first — a saga in `COMPENSATION_FAILED` does not
   automatically resume once the dependency recovers (see "Why this doesn't auto-resume" below).

2. **Check the actual payment state on payment-service's own database**, not just the saga's
   record of what it dispatched:
   ```sql
   -- On payment-service's database.
   SELECT payment_id, order_id, amount_cents, status FROM payments WHERE order_id = '<order_id>';
   ```
   - If `status = 'AUTHORIZED'`: the refund genuinely never happened. Proceed to step 3.
   - If `status = 'REFUNDED'`: the refund DID happen — only the confirmation back to the
     orchestrator was lost. Proceed to step 4 (no financial action needed, only closing out the
     saga's own bookkeeping).

3. **If the refund never happened**: manually trigger it through whatever payment-service exposes
   for this (there is no dedicated remediation API in this project as of M3 — this is a deliberate
   scope boundary, not an oversight: an unauthenticated "retry refund" endpoint is a bigger risk
   than a documented manual step an operator takes deliberately). At minimum this means directly
   invoking `PaymentAuthorizationService.handleRefundPayment` with a fresh, manually-constructed
   `RefundPayment` envelope for this order — through a REPL/admin tool, not through the normal
   Kafka path (which the saga's own automation already exhausted).

4. **Close out the saga record** once the money side is confirmed resolved (refunded now, or
   already was). There is no automated "acknowledge and close" action in this project as of M3;
   the direct, honest action is updating the row yourself once you've verified the underlying
   state, e.g.:
   ```sql
   UPDATE saga_instance SET state = 'COMPENSATED' WHERE saga_id = '<saga_id>';
   UPDATE orders SET status = 'CANCELLED' WHERE order_id = '<order_id>';
   ```
   Do this only after step 2/3 has confirmed the refund is real — this update does not itself
   refund anything, it only stops the order from showing up in the `CANCELLATION_FAILED` query
   above.

## Why this doesn't auto-resume

A saga that reached `COMPENSATION_FAILED` is, by construction, one where the automated retry
budget was already spent. Auto-resuming on "payment-service looks healthy again" risks firing a
`RefundPayment` for an order that was ALSO manually remediated in the meantime by an operator who
didn't know the saga would resume on its own — a double-refund, exactly the failure mode the
three-layer idempotency design (ADR-0016) works hard to prevent elsewhere. Requiring a human to
confirm the payment-service-side state before touching the saga record is the deliberate choice
here, not a missing feature.
