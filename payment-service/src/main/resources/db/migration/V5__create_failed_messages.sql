-- Durable capture of a record that exhausted its retries (v2 WS3). Until now such a record was
-- logged and skipped: visible in a log line, and then gone. This is the difference between
-- "an operator can see that something failed" and "an operator can do something about it".
--
-- The ordering that matters is not in this file but around it: the row below is committed BEFORE
-- the consumer offset advances past the record. If that order were reversed, a failure to write
-- here would lose the message permanently — the offset would have moved on and nothing would
-- remember what was skipped. Verified against the real framework by
-- RecovererFailureLeavesOffsetUncommittedIntegrationTest before any of this was built.
CREATE TABLE failed_messages (
    failed_message_id UUID PRIMARY KEY,
    consumer_group    VARCHAR(128) NOT NULL,
    topic             VARCHAR(255) NOT NULL,
    partition_id      INT          NOT NULL,
    record_offset     BIGINT       NOT NULL,
    message_key       VARCHAR(255),
    -- Verbatim, and TEXT rather than JSONB deliberately: a poison record may not be valid JSON
    -- at all (that is often exactly why it failed), and even when it is, jsonb would normalize
    -- key order and whitespace — so what gets replayed would no longer be the bytes that were
    -- received. Replay has to reproduce the original message, not an equivalent of it.
    payload           TEXT         NOT NULL,
    -- Includes traceparent/tracestate, so a replayed message continues the trace it belonged to
    -- rather than starting a new one.
    headers           JSONB,
    -- NULL when the envelope could not be parsed. That is a real case, not a defensive one: an
    -- unparseable record is precisely the kind that lands here.
    event_id          UUID,
    event_type        VARCHAR(128),
    failure_reason    TEXT         NOT NULL,
    status            VARCHAR(24)  NOT NULL,
    captured_at       TIMESTAMPTZ  NOT NULL,
    replay_attempts   INT          NOT NULL DEFAULT 0,
    last_replay_at    TIMESTAMPTZ,
    last_replay_error TEXT
);

-- Capture idempotency, keyed on PHYSICAL COORDINATES rather than event_id: a poison record has
-- no parseable event_id to key on, and the same record redelivered after a failed capture must
-- collapse onto one row rather than accumulating duplicates. This is what makes the capture
-- itself safe to retry, using the same INSERT ... ON CONFLICT DO NOTHING idiom the outbox and
-- the consumer dedupe ledger already rely on.
CREATE UNIQUE INDEX uq_failed_messages_coordinates
    ON failed_messages (consumer_group, topic, partition_id, record_offset);

-- The operator's list is "what is still outstanding", so the index covers only those rows and
-- stays sized to the backlog rather than to everything ever captured (the same shape as
-- outbox_events' unpublished-only index, and for the same reason).
CREATE INDEX idx_failed_messages_open ON failed_messages (status) WHERE status <> 'REPLAYED';
