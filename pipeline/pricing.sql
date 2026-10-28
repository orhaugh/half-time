-- Surge pricing: each zone's multiplier, once a minute, from the last five
-- minutes of demand and supply.
--
-- Demand is the orders placed in the zone, supply the riders who reported a
-- position in it, both counted over a five-minute window that slides every
-- minute, in event time. The multiplier is a step function of their ratio:
-- 1.00x below one order per rider, 1.20x from one, 1.50x from two, 1.80x from
-- three, and 2.00x when no rider is in the zone at all. Each window's row is
-- published exactly once (pipeline/quotes.sql prices orders from it) and kept
-- as the zone's current price in PostgreSQL, one row per zone, replaced in
-- place: an upsert is idempotent, so that table declares at least once and
-- the set is still accepted.

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
    group_id          = 'pricing',
    auto_offset_reset = 'earliest',
    shared_reader     = 'true',
    event_time_column = 'updated_at',
    watermark_lag_ms  = '60000',
    idle_timeout_ms   = '90000'
);

CREATE TABLE rider_positions (
    ts       BIGINT,
    rider_id BIGINT,
    cell     VARCHAR,
    zone     VARCHAR,
    x        BIGINT,
    y        BIGINT,
    sent_at  BIGINT
) WITH (
    connector         = 'kafka',
    format            = 'json',
    brokers           = 'kafka:9092',
    topic             = 'riders.positions',
    group_id          = 'pricing',
    auto_offset_reset = 'earliest',
    shared_reader     = 'true',
    event_time_column = 'ts',
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
    connector          = 'kafka',
    format             = 'json',
    brokers            = 'kafka:9092',
    topic              = 'pricing.multipliers',
    delivery_guarantee = 'exactly_once',
    transactional_id   = 'pricing-multipliers'
);

CREATE TABLE zone_pricing (
    zone           VARCHAR PRIMARY KEY,
    cell           VARCHAR,
    multiplier_pct BIGINT,
    demand         BIGINT,
    supply         BIGINT,
    window_end_ms  BIGINT
) WITH (
    connector          = 'postgres',
    mode               = 'upsert',
    conninfo           = 'host=postgres port=5432 dbname=halftime user=halftime password=halftime',
    "table"            = 'public.zone_pricing',
    delivery_guarantee = 'at_least_once'
);

EXECUTE STATEMENT SET BEGIN
INSERT INTO multipliers
WITH activity AS (
  SELECT cell, zone, updated_at AS ts, CAST(1 AS BIGINT) AS is_order, CAST(NULL AS BIGINT) AS rider_id
  FROM order_changes WHERE op = 'insert'
  UNION ALL
  SELECT cell, zone, ts, CAST(0 AS BIGINT) AS is_order, rider_id
  FROM rider_positions
)
SELECT cell, zone,
       SUM(is_order) AS demand,
       COUNT(DISTINCT rider_id) AS supply,
       CASE WHEN COUNT(DISTINCT rider_id) = 0 THEN 200
            WHEN SUM(is_order) * 100 / COUNT(DISTINCT rider_id) >= 300 THEN 180
            WHEN SUM(is_order) * 100 / COUNT(DISTINCT rider_id) >= 200 THEN 150
            WHEN SUM(is_order) * 100 / COUNT(DISTINCT rider_id) >= 100 THEN 120
            ELSE 100 END AS multiplier_pct,
       window_start, window_end
FROM activity
GROUP BY cell, zone, HOP(ts, INTERVAL '5' MINUTE, INTERVAL '1' MINUTE);

INSERT INTO zone_pricing
WITH activity AS (
  SELECT cell, zone, updated_at AS ts, CAST(1 AS BIGINT) AS is_order, CAST(NULL AS BIGINT) AS rider_id
  FROM order_changes WHERE op = 'insert'
  UNION ALL
  SELECT cell, zone, ts, CAST(0 AS BIGINT) AS is_order, rider_id
  FROM rider_positions
)
SELECT zone, cell,
       CASE WHEN COUNT(DISTINCT rider_id) = 0 THEN 200
            WHEN SUM(is_order) * 100 / COUNT(DISTINCT rider_id) >= 300 THEN 180
            WHEN SUM(is_order) * 100 / COUNT(DISTINCT rider_id) >= 200 THEN 150
            WHEN SUM(is_order) * 100 / COUNT(DISTINCT rider_id) >= 100 THEN 120
            ELSE 100 END AS multiplier_pct,
       SUM(is_order) AS demand,
       COUNT(DISTINCT rider_id) AS supply,
       window_end AS window_end_ms
FROM activity
GROUP BY cell, zone, HOP(ts, INTERVAL '5' MINUTE, INTERVAL '1' MINUTE);
END;
