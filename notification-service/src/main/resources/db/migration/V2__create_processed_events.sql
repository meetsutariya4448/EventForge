-- Consumer-side idempotency ledger. No dedupe-check-before-insert logic exists yet (M2) — this
-- migration establishes the schema only. consumer_group is part of the primary key so a service
-- with more than one logical listener over the same topic gets independent idempotency scopes.
CREATE TABLE processed_events (
    consumer_group   VARCHAR(128) NOT NULL,
    event_id         UUID         NOT NULL,
    aggregate_id     VARCHAR(64)  NOT NULL,
    processed_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_group, event_id)
);

CREATE INDEX idx_processed_events_aggregate_id ON processed_events (aggregate_id);

-- Supports a future archival/delete-by-age path (a T3-style concern for this table too);
-- not implemented in M0.
CREATE INDEX idx_processed_events_processed_at ON processed_events (processed_at);
