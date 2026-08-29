-- inventory-service's business logic stays deliberately minimal in M2 (reservation semantics are
-- M3 - see the constitution's scope for this milestone). This table only records that
-- inventory-service saw an OrderCreated for a given order, giving the M2 consumer transaction
-- invariant (dedupe -> business mutation -> [next outbox event]) a real, if trivial, mutation to
-- exercise. No stock/reservation columns exist yet - inventing them now would be M3's work done
-- early.
CREATE TABLE inventory_order_events (
    inventory_event_id  UUID         PRIMARY KEY,
    order_id            VARCHAR(64)  NOT NULL,
    received_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uq_inventory_order_events_order_id UNIQUE (order_id)
);
