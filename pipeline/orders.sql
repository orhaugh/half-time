-- The order stream: every change the order store commits, announced on Kafka
-- exactly once, in the order it was committed.
--
-- The order service writes PostgreSQL and nothing else; this job reads the
-- table's replication stream (one slot, one publication) and writes each
-- change as one record, with what changed and the row as it is after. The
-- sink is transactional, committed with the job's checkpoints, so a consumer
-- reading committed records sees each change once however the job restarts.
-- Every other job reads this topic, never the table.

CREATE TABLE order_store (
    order_id      VARCHAR PRIMARY KEY,
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
    __op          VARCHAR,
    __version     BIGINT
) WITH (
    connector         = 'postgres',
    format            = 'json',
    mode              = 'cdc',
    changelog_mode    = 'append',
    snapshot_mode     = 'initial',
    on_decode_error   = 'fail',
    conninfo          = 'host=postgres port=5432 dbname=halftime user=halftime password=halftime',
    slot_name         = 'halftime_orders',
    publication_names = 'halftime_orders',
    "table"           = 'public.orders'
);

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
    connector          = 'kafka',
    format             = 'json',
    brokers            = 'kafka:9092',
    topic              = 'orders.changes',
    delivery_guarantee = 'exactly_once',
    transactional_id   = 'orders-changes'
);

INSERT INTO order_changes
SELECT order_id, cell, zone, restaurant_id, customer_id, cx, cy, placed_at, promised_at,
       fee_cents, total_cents, status, updated_at, __op AS op, __version AS version
FROM order_store;
