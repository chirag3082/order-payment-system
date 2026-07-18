-- Payment Service schema: payments + idempotency ledger for consumed order events.

CREATE TABLE payments (
    id         VARCHAR(36)   PRIMARY KEY,
    order_id   VARCHAR(36)   NOT NULL,
    amount     NUMERIC(14,2) NOT NULL,
    currency   CHAR(3)       NOT NULL,
    status     VARCHAR(16)   NOT NULL,
    reason     VARCHAR(255),
    created_at TIMESTAMPTZ   NOT NULL
);

CREATE INDEX idx_payments_order_id ON payments (order_id);

CREATE TABLE processed_events (
    event_id     VARCHAR(36) PRIMARY KEY,
    order_id     VARCHAR(36) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL
);
