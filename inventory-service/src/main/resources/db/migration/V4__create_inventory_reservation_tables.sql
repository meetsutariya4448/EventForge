-- M3: real reservation semantics, replacing the M2 stub (inventory_order_events, which just
-- recorded that an OrderCreated was seen — still present, no longer written to; Flyway migrations
-- are never edited or dropped after the fact).
CREATE TABLE inventory_items (
    sku                 VARCHAR(64) PRIMARY KEY,
    available_quantity  BIGINT NOT NULL
);

-- A generous default stock level for the placeholder SKU every order uses unless a test
-- deliberately asks for scarcity (constitution item 8f) by inserting/updating its own row.
INSERT INTO inventory_items (sku, available_quantity) VALUES ('DEFAULT-SKU', 1000000);

CREATE TABLE inventory_reservations (
    reservation_id  UUID PRIMARY KEY,
    order_id        VARCHAR(64)  NOT NULL UNIQUE,
    sku             VARCHAR(64)  NOT NULL,
    quantity        BIGINT       NOT NULL,
    status          VARCHAR(32)  NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL,
    updated_at      TIMESTAMPTZ  NOT NULL
);
