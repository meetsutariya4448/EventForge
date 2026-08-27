-- Transactional outbox. No relay/claiming logic exists yet (M1) — this migration establishes the
-- schema only, shaped so the future relay can (a) claim work per-aggregate in creation order
-- without reordering a single aggregate's events across parallel workers (trap T1), and
-- (b) scan only unpublished rows without the table's growth degrading that scan (trap T3).
CREATE TABLE outbox_events (
    event_id            UUID PRIMARY KEY,
    aggregate_type      VARCHAR(64)  NOT NULL,
    aggregate_id        VARCHAR(64)  NOT NULL,
    -- Monotonic per-aggregate sequence (trap T1): lets a future relay claim and publish
    -- an aggregate's events in creation order even under parallel workers.
    aggregate_sequence  BIGINT       NOT NULL,
    event_type          VARCHAR(128) NOT NULL,
    schema_version      INT          NOT NULL,
    correlation_id      UUID         NOT NULL,
    causation_id        UUID,
    -- W3C trace context, durable copy for restoration by the relay (never in payload).
    traceparent         VARCHAR(64),
    tracestate          VARCHAR(512),
    payload             JSONB        NOT NULL,
    occurred_at         TIMESTAMPTZ  NOT NULL,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at        TIMESTAMPTZ,
    publish_attempts    INT          NOT NULL DEFAULT 0,
    last_attempt_at     TIMESTAMPTZ,
    last_error          TEXT
);

-- Enforces monotonicity per aggregate; also the natural claim-order index for T1.
CREATE UNIQUE INDEX uq_outbox_events_aggregate_sequence
    ON outbox_events (aggregate_id, aggregate_sequence);

-- Partial index: serves "find unpublished, in per-aggregate order" without indexing (and
-- bloating on) published rows (trap T3). Already ordered for future aggregate-level claiming (T1).
CREATE INDEX idx_outbox_events_unpublished
    ON outbox_events (aggregate_id, aggregate_sequence)
    WHERE published_at IS NULL;
