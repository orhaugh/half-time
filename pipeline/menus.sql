-- The menu stream: every change to a restaurant's menu, from the legacy
-- menu system's binary log, announced on Kafka exactly once.
--
-- The point-of-sale system is MySQL and will stay MySQL. clink reads its
-- binary log, the existing rows first as a snapshot taken at a consistent
-- position and then every change, and writes each as one record, with what
-- changed. The sink is transactional, committed with the job's checkpoints;
-- pipeline/search.sql indexes the stream.

CREATE TABLE menu_store (
    restaurant_id VARCHAR,
    item_id       VARCHAR,
    name          VARCHAR,
    price_cents   BIGINT,
    available     BIGINT,
    updated_at    BIGINT,
    __op          VARCHAR
) WITH (
    connector        = 'mysql',
    mode             = 'cdc',
    host             = 'mysql',
    port             = '3306',
    user             = 'repl',
    password         = 'repl',
    server_id        = '4242',
    tables           = 'halftime.menus',
    initial_snapshot = 'true',
    snapshot_lock    = 'true'
);

CREATE TABLE menu_changes (
    restaurant_id VARCHAR,
    item_id       VARCHAR,
    name          VARCHAR,
    price_cents   BIGINT,
    available     BIGINT,
    updated_at    BIGINT,
    op            VARCHAR
) WITH (
    connector          = 'kafka',
    format             = 'json',
    brokers            = 'kafka:9092',
    topic              = 'menus.changes',
    delivery_guarantee = 'exactly_once',
    transactional_id   = 'menus-changes'
);

INSERT INTO menu_changes
SELECT restaurant_id, item_id, name, price_cents, available, updated_at, __op AS op
FROM menu_store;
