-- The live tracker: where every order stands, in the stores the apps read.
--
-- Redis holds one key per order with its latest state, kept by a top-1 per
-- order over the order stream: the stream's partitions can hand a later
-- change over before an earlier one, so the latest is chosen by the
-- change's version, not by arrival, and the key is replaced or restored as
-- the ranking changes. The rider is a second key, written when the
-- dispatcher decides. Cassandra holds every change as a row of the order's
-- history, keyed by the order and the time, so a change written twice is
-- the same row. All three are kept by key, so a restart that writes a state
-- again changes nothing.

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
    group_id          = 'tracker',
    auto_offset_reset = 'earliest',
    shared_reader     = 'true'
);

CREATE TABLE decisions (
    order_id       VARCHAR,
    cell           VARCHAR,
    zone           VARCHAR,
    restaurant_id  VARCHAR,
    placed_at      BIGINT,
    decide_at      BIGINT,
    fee_cents      BIGINT,
    multiplier_pct BIGINT,
    kind           VARCHAR,
    rider_id       BIGINT,
    candidates     BIGINT,
    score          BIGINT,
    decided_at     BIGINT
) WITH (
    connector         = 'kafka',
    format            = 'json',
    brokers           = 'kafka:9092',
    topic             = 'dispatch.decisions',
    group_id          = 'tracker',
    auto_offset_reset = 'earliest'
);

CREATE TABLE tracker (
    order_id   VARCHAR PRIMARY KEY,
    cell       VARCHAR,
    status     VARCHAR,
    updated_at BIGINT
) WITH (
    connector          = 'redis',
    mode               = 'upsert',
    host               = 'redis',
    port               = '6379',
    key_prefix         = 'order:',
    delivery_guarantee = 'at_least_once'
);

CREATE TABLE rider_of (
    order_id   VARCHAR PRIMARY KEY,
    rider_id   BIGINT,
    decided_at BIGINT
) WITH (
    connector          = 'redis',
    mode               = 'upsert',
    host               = 'redis',
    port               = '6379',
    key_prefix         = 'rider-of:',
    delivery_guarantee = 'at_least_once'
);

CREATE TABLE order_history (
    order_id   VARCHAR,
    updated_at BIGINT,
    cell       VARCHAR,
    status     VARCHAR
) WITH (
    connector          = 'cassandra',
    format             = 'json',
    contact_points     = 'cassandra',
    port               = '9042',
    keyspace           = 'halftime',
    table              = 'order_history',
    delivery_guarantee = 'at_least_once'
);

EXECUTE STATEMENT SET BEGIN
INSERT INTO tracker
SELECT order_id, cell, status, updated_at
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY version DESC) AS rn
    FROM order_changes
) latest
WHERE rn = 1;

INSERT INTO rider_of
SELECT order_id, rider_id, decided_at FROM decisions WHERE kind = 'assigned';

INSERT INTO order_history
SELECT order_id, updated_at, cell, status FROM order_changes;
END;
