-- The first real business entity in the project (order-service's own concern, not shared
-- infrastructure like outbox_events/processed_events). Minimal by design: the domain is
-- scaffolding for proving the outbox/saga/tracing claims, not something to enrich.
CREATE TABLE orders (
    order_id      UUID PRIMARY KEY,
    status        VARCHAR(32)  NOT NULL,
    amount_cents  BIGINT       NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);
