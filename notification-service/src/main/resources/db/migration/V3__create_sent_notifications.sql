-- Stands in for an EXTERNAL side effect (e.g. an email/SMS provider call) - deliberately, since
-- item 6 of the M2 work order is specifically about what happens when a consumer's effect is
-- external: local effects (a payment ledger row, an inventory log row) are naturally re-derivable
-- and cheap to make idempotent even beyond processed_events' own protection, but an external send
-- has no "undo," so processed_events' dedupe is the ONLY thing standing between this service and a
-- customer receiving duplicate notifications. See the dedupe-key/retention-window ADR.
CREATE TABLE sent_notifications (
    notification_id  UUID         PRIMARY KEY,
    order_id         VARCHAR(64)  NOT NULL,
    channel          VARCHAR(32)  NOT NULL,
    sent_at          TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_sent_notifications_order_id ON sent_notifications (order_id);
