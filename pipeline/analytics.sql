-- The analytics store: every order change, every dispatch decision, every
-- pricing window, every payout and every customer's running spend, in
-- ClickHouse.
--
-- Written through the native protocol, at least once: after a restart a row
-- can arrive twice, identically, and each table's key collapses the copies
-- (clickhouse/schema.sql). The running spend is an event-time window per
-- customer over the order stream, which arrives in the order the order store
-- committed it. The payouts land as rows: a payout is recorded when the later
-- of the delivery and the dispatch decision arrives, so they do not arrive in
-- delivery order, and ClickHouse orders them for the rider_earnings view.

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
    group_id          = 'analytics',
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
    group_id          = 'analytics',
    auto_offset_reset = 'earliest'
);

CREATE TABLE multipliers (
    cell           VARCHAR,
    zone           VARCHAR,
    demand         BIGINT,
    supply         BIGINT,
    multiplier_pct BIGINT,
    window_start   BIGINT,
    window_end     BIGINT
) WITH (
    connector         = 'kafka',
    format            = 'json',
    brokers           = 'kafka:9092',
    topic             = 'pricing.multipliers',
    group_id          = 'analytics',
    auto_offset_reset = 'earliest'
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
    connector         = 'kafka',
    format            = 'json',
    brokers           = 'kafka:9092',
    topic             = 'ledger.payouts',
    group_id          = 'analytics',
    auto_offset_reset = 'earliest'
);

CREATE TABLE order_events (
    order_id       VARCHAR,
    cell           VARCHAR,
    zone           VARCHAR,
    restaurant_id  VARCHAR,
    customer_id    VARCHAR,
    status         VARCHAR,
    fee_cents      BIGINT,
    total_cents    BIGINT,
    placed_at_ms   BIGINT,
    promised_at_ms BIGINT,
    updated_at_ms  BIGINT,
    op             VARCHAR,
    version        BIGINT
) WITH (
    connector          = 'clickhouse',
    insert_format      = 'native',
    host               = 'clickhouse',
    port               = '9000',
    database           = 'halftime',
    "table"            = 'order_events',
    user               = 'halftime',
    password           = 'halftime',
    batch_interval_ms  = '500',
    delivery_guarantee = 'at_least_once'
);

CREATE TABLE dispatch_decisions (
    order_id       VARCHAR,
    kind           VARCHAR,
    rider_id       BIGINT,
    candidates     BIGINT,
    score          BIGINT,
    multiplier_pct BIGINT,
    fee_cents      BIGINT,
    cell           VARCHAR,
    zone           VARCHAR,
    restaurant_id  VARCHAR,
    placed_at_ms   BIGINT
) WITH (
    connector          = 'clickhouse',
    insert_format      = 'native',
    host               = 'clickhouse',
    port               = '9000',
    database           = 'halftime',
    "table"            = 'dispatch_decisions',
    user               = 'halftime',
    password           = 'halftime',
    batch_interval_ms  = '500',
    delivery_guarantee = 'at_least_once'
);

CREATE TABLE zone_pricing_windows (
    cell           VARCHAR,
    zone           VARCHAR,
    window_end_ms  BIGINT,
    demand         BIGINT,
    supply         BIGINT,
    multiplier_pct BIGINT
) WITH (
    connector          = 'clickhouse',
    insert_format      = 'native',
    host               = 'clickhouse',
    port               = '9000',
    database           = 'halftime',
    "table"            = 'zone_pricing_windows',
    user               = 'halftime',
    password           = 'halftime',
    batch_interval_ms  = '500',
    delivery_guarantee = 'at_least_once'
);

CREATE TABLE rider_payouts (
    order_id        VARCHAR,
    rider_id        BIGINT,
    cell            VARCHAR,
    zone            VARCHAR,
    fee_cents       BIGINT,
    multiplier_pct  BIGINT,
    amount_cents    BIGINT,
    delivered_at_ms BIGINT
) WITH (
    connector          = 'clickhouse',
    insert_format      = 'native',
    host               = 'clickhouse',
    port               = '9000',
    database           = 'halftime',
    "table"            = 'rider_payouts',
    user               = 'halftime',
    password           = 'halftime',
    batch_interval_ms  = '500',
    delivery_guarantee = 'at_least_once'
);

CREATE TABLE customer_spend (
    customer_id     VARCHAR,
    cell            VARCHAR,
    order_id        VARCHAR,
    total_cents     BIGINT,
    spent_cents     BIGINT,
    delivered_at_ms BIGINT
) WITH (
    connector          = 'clickhouse',
    insert_format      = 'native',
    host               = 'clickhouse',
    port               = '9000',
    database           = 'halftime',
    "table"            = 'customer_spend',
    user               = 'halftime',
    password           = 'halftime',
    batch_interval_ms  = '500',
    delivery_guarantee = 'at_least_once'
);

EXECUTE STATEMENT SET BEGIN
INSERT INTO order_events
SELECT order_id, cell, zone, restaurant_id, customer_id, status, fee_cents, total_cents,
       placed_at AS placed_at_ms, promised_at AS promised_at_ms, updated_at AS updated_at_ms,
       op, version
FROM order_changes;

INSERT INTO dispatch_decisions
SELECT order_id, kind, rider_id, candidates, score, multiplier_pct, fee_cents, cell, zone,
       restaurant_id, placed_at AS placed_at_ms
FROM decisions;

INSERT INTO zone_pricing_windows
SELECT cell, zone, window_end AS window_end_ms, demand, supply, multiplier_pct
FROM multipliers;

INSERT INTO rider_payouts
SELECT order_id, rider_id, cell, zone, fee_cents, multiplier_pct, amount_cents, delivered_at_ms
FROM payouts_published;

INSERT INTO customer_spend
SELECT customer_id, cell, order_id, total_cents, spent_cents, updated_at AS delivered_at_ms
FROM (
  SELECT *, SUM(total_cents) OVER (PARTITION BY customer_id ORDER BY updated_at) AS spent_cents
  FROM order_changes
  WHERE status = 'delivered'
) AS spend;
END;
