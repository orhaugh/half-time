-- The lakehouse: the order stream as a Delta table.
--
-- Every change to every order, appended as typed Parquet under a Delta
-- transaction log, one commit per checkpoint, at a path (or an object store
-- prefix) any Delta reader can open. At least once: a restart can append
-- an interval again, and a reader collapses the copies by order and time.

CREATE TABLE order_changes (
    order_id      VARCHAR,
    cell          VARCHAR,
    zone          VARCHAR,
    restaurant_id VARCHAR,
    customer_id   VARCHAR,
    cx            BIGINT,
    cy            BIGINT,
    placed_at     BIGINT,
    promised_at   BIGINT,
    fee_cents     BIGINT,
    total_cents   BIGINT,
    status        VARCHAR,
    updated_at    BIGINT,
    op            VARCHAR,
    version       BIGINT
) WITH (
    connector         = 'kafka',
    format            = 'json',
    brokers           = 'kafka:9092',
    topic             = 'orders.changes',
    group_id          = 'lake',
    auto_offset_reset = 'earliest'
);

CREATE TABLE lake_orders (
    order_id      VARCHAR,
    cell          VARCHAR,
    zone          VARCHAR,
    restaurant_id VARCHAR,
    customer_id   VARCHAR,
    cx            BIGINT,
    cy            BIGINT,
    placed_at     BIGINT,
    promised_at   BIGINT,
    fee_cents     BIGINT,
    total_cents   BIGINT,
    status        VARCHAR,
    updated_at    BIGINT,
    op            VARCHAR,
    version       BIGINT
) WITH (
    connector = 'delta',
    format    = 'json',
    path      = '/archive/lake/orders.changes'
);

INSERT INTO lake_orders SELECT * FROM order_changes;
