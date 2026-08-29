-- M3: payment-service now writes more than one outbox event per order over the payment row's
-- lifetime (PaymentAuthorized, later possibly PaymentRefunded) - aggregate_sequence can no longer
-- be hardcoded to 1 for every write. next_sequence tracks this order's next free value, mirroring
-- order-service's saga_instance.next_sequence (same reasoning: outbox_events' uniqueness is on
-- (aggregate_id, aggregate_sequence), and a hardcoded constant collides the moment a second event
-- for the same order needs writing).
ALTER TABLE payments ADD COLUMN next_sequence BIGINT NOT NULL DEFAULT 2;
