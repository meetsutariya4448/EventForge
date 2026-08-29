-- payment-service's own business entity: a local payment ledger row, authorized in response to a
-- consumed OrderCreated event. One payment per order in M2's scope (no partial/multi-attempt
-- authorization flows yet - that's saga/M3 territory). The unique constraint on order_id is a
-- second, DB-level idempotency backstop beyond processed_events' dedup - defensive, not load-bearing
-- on its own (see the dedupe-key ADR).
CREATE TABLE payments (
    payment_id    UUID         PRIMARY KEY,
    order_id      VARCHAR(64)  NOT NULL,
    amount_cents  BIGINT       NOT NULL,
    status        VARCHAR(32)  NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uq_payments_order_id UNIQUE (order_id)
);
