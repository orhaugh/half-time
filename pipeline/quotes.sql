-- The quote: each order with the multiplier it was placed under, exactly once.
--
-- An order placed in the minute after a pricing window closed is priced by
-- that window: the latest price the zone had when the customer ordered. A
-- zone with no window to show (nothing happened there for five minutes) is
-- priced at 1.00x. The quote also carries decide_at, two minutes after the
-- order, which is when the dispatcher stops waiting for riders to report in.

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
    group_id          = 'quotes',
    auto_offset_reset = 'earliest',
    event_time_column = 'updated_at',
    watermark_lag_ms  = '60000',
    idle_timeout_ms   = '90000'
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
    group_id          = 'quotes',
    auto_offset_reset = 'earliest',
    event_time_column = 'window_end',
    watermark_lag_ms  = '60000',
    idle_timeout_ms   = '90000'
);

CREATE TABLE priced_orders (
    order_id       VARCHAR,
    cell           VARCHAR,
    zone           VARCHAR,
    restaurant_id  VARCHAR,
    customer_id    VARCHAR,
    cx             BIGINT,
    cy             BIGINT,
    placed_at      BIGINT,
    promised_at    BIGINT,
    fee_cents      BIGINT,
    total_cents    BIGINT,
    multiplier_pct BIGINT,
    decide_at      BIGINT
) WITH (
    connector          = 'kafka',
    format             = 'json',
    brokers            = 'kafka:9092',
    topic              = 'orders.priced',
    delivery_guarantee = 'exactly_once',
    transactional_id   = 'orders-priced'
);

INSERT INTO priced_orders
WITH placed AS (
  SELECT order_id, cell, zone, restaurant_id, customer_id, cx, cy, placed_at, promised_at,
         fee_cents, total_cents, updated_at
  FROM order_changes WHERE op = 'insert'
),
quoted AS (
  SELECT o.order_id AS order_id, o.cell AS cell, o.zone AS zone, o.restaurant_id AS restaurant_id,
         o.customer_id AS customer_id, o.cx AS cx, o.cy AS cy, o.placed_at AS placed_at,
         o.promised_at AS promised_at, o.fee_cents AS fee_cents, o.total_cents AS total_cents,
         o.updated_at AS updated_at, m.multiplier_pct AS multiplier_pct
  FROM placed o JOIN multipliers m
    ON o.zone = m.zone
   AND o.updated_at BETWEEN m.window_end + 0 AND m.window_end + 59999
  UNION ALL
  SELECT order_id, cell, zone, restaurant_id, customer_id, cx, cy, placed_at, promised_at,
         fee_cents, total_cents, updated_at, CAST(100 AS BIGINT) AS multiplier_pct
  FROM placed
)
SELECT order_id, cell, zone, restaurant_id, customer_id, cx, cy, placed_at, promised_at,
       fee_cents, total_cents, MAX(multiplier_pct) AS multiplier_pct,
       placed_at + 120000 AS decide_at
FROM quoted
GROUP BY order_id, cell, zone, restaurant_id, customer_id, cx, cy, placed_at, promised_at,
         fee_cents, total_cents, TUMBLE(updated_at, INTERVAL '1' MINUTE);
