-- HTTP idempotency for POST /orders (v2 WS1). A client that retries a create — because its
-- socket timed out, not because it wants a second order — must not get a second order.
--
-- The claim on this table is the FIRST statement inside the same transaction as the order
-- write, not a separate short transaction that marks IN_PROGRESS first. That is what makes a
-- crashed or rolled-back attempt leave nothing behind: the claim and the effect commit
-- together, or neither does. It is the outbox pattern's own argument applied to an inbound
-- request instead of an outbound event, and it is why there is no status column here and no
-- reaper for stuck claims -- only one for expired ones. See ADR-0021.
CREATE TABLE idempotency_keys (
    idempotency_key     VARCHAR(255) PRIMARY KEY,
    -- sha256 hex of the canonical (amountCents, sku, quantity) triple, taken AFTER
    -- CreateOrderRequest's defaults are applied. Reusing a key with a different body is a
    -- client bug, not a retry, and is rejected rather than served a response describing an
    -- order the caller never asked for.
    request_fingerprint CHAR(64)     NOT NULL,
    order_id            UUID         NOT NULL,
    -- The winner's response, stored verbatim so a later retry replays byte-for-byte what the
    -- first caller got, rather than a freshly-rendered approximation of it.
    --
    -- TEXT, deliberately, not JSONB. jsonb is a parsed document type: it discards the original
    -- text, reorders keys and re-renders whitespace, so a body written as
    -- {"orderId":...,"status":...} reads back as {"status": ..., "orderId": ...}. Semantically
    -- identical, textually different — which is precisely what "verbatim" rules out, and what a
    -- client doing a byte comparison would notice. Nothing here ever queries inside this value,
    -- so jsonb's only real advantage does not apply.
    response_status     INT          NOT NULL,
    response_body       TEXT         NOT NULL,
    created_at          TIMESTAMPTZ  NOT NULL,
    expires_at          TIMESTAMPTZ  NOT NULL
);

-- Serves the reaper's only query ("which rows are past their TTL"). Deliberately not a partial
-- index: unlike outbox_events' unpublished-rows index, every row here is eventually eligible,
-- so there is no stable predicate to exclude the majority by.
CREATE INDEX idx_idempotency_keys_expires_at ON idempotency_keys (expires_at);
