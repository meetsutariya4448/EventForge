-- Orchestrated saga state (M3, constitution item 1): persisted, SQL-inspectable mid-flight, not
-- choreographed and not in-memory. The saga gets its own identifier (saga_id) — correlationId
-- stays what it always was, the causal-chain id carried on every event, never overloaded as the
-- saga's own primary key (see ADR-0016).
CREATE TABLE saga_instance (
    saga_id                 UUID PRIMARY KEY,
    order_id                UUID         NOT NULL UNIQUE,
    -- The root correlationId for this saga's whole causal chain (OrderCreated's own eventId) -
    -- stored here, not just read from each inbound envelope, because a timeout-triggered dispatch
    -- has no inbound envelope to read it from at all.
    correlation_id          UUID         NOT NULL,
    state                   VARCHAR(32)  NOT NULL,
    amount_cents            BIGINT       NOT NULL,
    sku                     VARCHAR(64)  NOT NULL,
    quantity                BIGINT       NOT NULL,
    -- This order_id's next free outbox_events.aggregate_sequence value (OrderCreated already
    -- consumed sequence 1 before this row exists).
    next_sequence           BIGINT       NOT NULL DEFAULT 2,
    -- Persisted timeout deadline for whatever step is currently in flight (constitution item 5):
    -- the source of truth is this column, not an in-memory timer that dies with the process. NULL
    -- once the saga reaches a terminal state.
    deadline_at             TIMESTAMPTZ,
    -- Bounded compensation retry count (constitution item 6): how many times RefundPayment has
    -- been (re)dispatched for this saga. COMPENSATION_FAILED is reached, not retried forever, once
    -- this hits eventforge.saga.max-compensation-attempts.
    compensation_attempts   INT          NOT NULL DEFAULT 0,
    created_at              TIMESTAMPTZ  NOT NULL,
    updated_at              TIMESTAMPTZ  NOT NULL
);

-- Sweep query shape: find in-flight sagas whose deadline has passed.
CREATE INDEX idx_saga_instance_deadline ON saga_instance (deadline_at) WHERE deadline_at IS NOT NULL;

-- One row per dispatched step — an audit trail inspectable via SQL mid-flight or after the fact,
-- not just the current state (constitution item 1's whole point in choosing orchestration).
CREATE TABLE saga_step (
    saga_step_id   UUID PRIMARY KEY,
    saga_id        UUID         NOT NULL REFERENCES saga_instance (saga_id),
    step_name      VARCHAR(64)  NOT NULL,
    status         VARCHAR(32)  NOT NULL,
    dispatched_at  TIMESTAMPTZ  NOT NULL,
    completed_at   TIMESTAMPTZ,
    detail         TEXT
);

CREATE INDEX idx_saga_step_saga_id ON saga_step (saga_id);
