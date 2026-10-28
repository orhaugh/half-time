-- The ledgers: a rider's pay for each delivery and a customer's credit for
-- each failed promise, each recorded exactly once.
--
-- A payout is the delivery fee at the multiplier the order was quoted under,
-- recorded when the order store says the order was delivered, for the rider
-- the dispatcher assigned. A credit is a fifth of the basket when a delivery
-- came more than ten minutes after the promise, or the whole basket when the
-- order was cancelled. Both are written in PostgreSQL transactions prepared at
-- each checkpoint barrier and committed when the checkpoint completes, so a
-- restart from a checkpoint records nothing twice. The payouts are also
-- published exactly once, for the running totals in the analytics store.

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
    group_id          = 'ledgers',
    auto_offset_reset = 'earliest',
    shared_reader     = 'true',
    event_time_column = 'updated_at',
    watermark_lag_ms  = '60000',
    idle_timeout_ms   = '90000'
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
    group_id          = 'ledgers',
    auto_offset_reset = 'earliest',
    shared_reader     = 'true',
    event_time_column = 'placed_at',
    watermark_lag_ms  = '60000',
    idle_timeout_ms   = '90000'
);

CREATE TABLE payouts (
    order_id        VARCHAR,
    rider_id        BIGINT,
    cell            VARCHAR,
    zone            VARCHAR,
    fee_cents       BIGINT,
    multiplier_pct  BIGINT,
    amount_cents    BIGINT,
    delivered_at_ms BIGINT
) WITH (
    connector          = 'postgres',
    conninfo           = 'host=postgres port=5432 dbname=halftime user=halftime password=halftime',
    "table"            = 'public.payouts',
    delivery_guarantee = 'exactly_once'
);

CREATE TABLE payouts_published (
    order_id        VARCHAR,
    rider_id        BIGINT,
    cell            VARCHAR,
    zone            VARCHAR,
    fee_cents       BIGINT,
    multiplier_pct  BIGINT,
    amount_cents    BIGINT,
    delivered_at_ms BIGINT
) WITH (
    connector          = 'kafka',
    format             = 'json',
    brokers            = 'kafka:9092',
    topic              = 'ledger.payouts',
    delivery_guarantee = 'exactly_once',
    transactional_id   = 'ledger-payouts'
);

CREATE TABLE credits (
    order_id     VARCHAR,
    customer_id  VARCHAR,
    cell         VARCHAR,
    reason       VARCHAR,
    amount_cents BIGINT,
    at_ms        BIGINT
) WITH (
    connector          = 'postgres',
    conninfo           = 'host=postgres port=5432 dbname=halftime user=halftime password=halftime',
    "table"            = 'public.credits',
    delivery_guarantee = 'exactly_once'
);

EXECUTE STATEMENT SET BEGIN
INSERT INTO payouts
WITH delivered AS (SELECT * FROM order_changes WHERE status = 'delivered'),
     assigned AS (SELECT * FROM decisions WHERE kind = 'assigned')
SELECT d.order_id, a.rider_id, a.cell, a.zone, a.fee_cents, a.multiplier_pct,
       a.fee_cents * a.multiplier_pct / 100 AS amount_cents, d.updated_at AS delivered_at_ms
FROM delivered d JOIN assigned a
  ON d.order_id = a.order_id
 AND d.updated_at BETWEEN a.placed_at + 0 AND a.placed_at + 14400000;

INSERT INTO payouts_published
WITH delivered AS (SELECT * FROM order_changes WHERE status = 'delivered'),
     assigned AS (SELECT * FROM decisions WHERE kind = 'assigned')
SELECT d.order_id, a.rider_id, a.cell, a.zone, a.fee_cents, a.multiplier_pct,
       a.fee_cents * a.multiplier_pct / 100 AS amount_cents, d.updated_at AS delivered_at_ms
FROM delivered d JOIN assigned a
  ON d.order_id = a.order_id
 AND d.updated_at BETWEEN a.placed_at + 0 AND a.placed_at + 14400000;

INSERT INTO credits
SELECT order_id, customer_id, cell, 'late_delivery' AS reason,
       total_cents * 20 / 100 AS amount_cents, updated_at AS at_ms
FROM order_changes
WHERE status = 'delivered' AND updated_at > promised_at + 600000
UNION ALL
SELECT order_id, customer_id, cell, 'cancelled' AS reason,
       total_cents AS amount_cents, updated_at AS at_ms
FROM order_changes
WHERE status = 'cancelled';
END;
