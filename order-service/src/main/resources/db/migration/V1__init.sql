-- Order Service schema: business table + transactional outbox + idempotency ledger.

CREATE TABLE orders (
    id            VARCHAR(36)   PRIMARY KEY,
    customer_id   VARCHAR(255)  NOT NULL,
    amount        NUMERIC(14,2) NOT NULL,
    currency      CHAR(3)       NOT NULL,
    status        VARCHAR(16)   NOT NULL,
    status_reason VARCHAR(255),
    created_at    TIMESTAMPTZ   NOT NULL,
    updated_at    TIMESTAMPTZ   NOT NULL,
    version       BIGINT        NOT NULL DEFAULT 0
);

CREATE TABLE outbox_events (
    id             VARCHAR(36)  PRIMARY KEY,
    aggregate_type VARCHAR(64)  NOT NULL,
    aggregate_id   VARCHAR(64)  NOT NULL,
    event_type     VARCHAR(64)  NOT NULL,
    topic          VARCHAR(128) NOT NULL,
    payload        TEXT         NOT NULL,
    correlation_id VARCHAR(64)  NOT NULL,
    created_at     TIMESTAMPTZ  NOT NULL,
    published      BOOLEAN      NOT NULL DEFAULT FALSE,
    published_at   TIMESTAMPTZ
);

-- The poller scans for unpublished rows oldest-first; this index keeps that cheap.
CREATE INDEX idx_outbox_unpublished ON outbox_events (created_at) WHERE published = FALSE;

CREATE TABLE processed_payment_results (
    event_id     VARCHAR(36) PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL
);
