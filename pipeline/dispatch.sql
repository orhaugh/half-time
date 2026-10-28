-- Dispatch: every order assigned to the nearest rider, or escalated, exactly
-- once.
--
-- The graph says where the restaurant is and which zones touch its own; the
-- riders' reports say where every rider was from three minutes before the
-- order to two minutes after it. The nearest report in those zones within
-- 1,500 m of the restaurant wins, the lower rider id on a tie, and an order
-- no rider was near is escalated. The
-- decision is made at decide_at, two minutes after the order, once the
-- riders' reports up to then are in, and is published exactly once.
-- pipeline/ledgers.sql pays the rider from it when the order is delivered.

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
    connector         = 'kafka',
    format            = 'json',
    brokers           = 'kafka:9092',
    topic             = 'orders.priced',
    group_id          = 'dispatch',
    auto_offset_reset = 'earliest',
    event_time_column = 'decide_at',
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
    group_id          = 'dispatch',
    auto_offset_reset = 'earliest',
    event_time_column = 'ts',
    watermark_lag_ms  = '60000',
    idle_timeout_ms   = '90000'
);

-- Where the restaurant is, its zone, and the zones that touch it. Exactly one
-- row per restaurant; the zones beyond the grid's edge are NULL.
CREATE TABLE restaurant_context (
    restaurant_id VARCHAR,
    rx            BIGINT,
    ry            BIGINT,
    zone0         VARCHAR,
    zone1         VARCHAR,
    zone2         VARCHAR,
    zone3         VARCHAR,
    zone4         VARCHAR
) WITH (
    connector          = 'lookup',
    function           = 'neo4j',
    endpoint           = 'http://neo4j:7474',
    username           = 'neo4j',
    password           = 'env://NEO4J_PASSWORD',
    request_timeout_ms = '10000',
    server_timeout_s   = '5',
    max_in_flight      = '256',
    max_concurrent_requests = '8',
    statement = 'MATCH (r:Restaurant {restaurantId: $key})-[:IN_ZONE]->(z:Zone)
                 OPTIONAL MATCH (z)-[:TOUCHES]->(n:Zone)
                 WITH r, z, n ORDER BY n.zoneId
                 WITH r, z, collect(n.zoneId) AS near
                 RETURN r.x AS rx, r.y AS ry, z.zoneId AS zone0,
                        near[0] AS zone1, near[1] AS zone2, near[2] AS zone3, near[3] AS zone4'
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
    connector          = 'kafka',
    format             = 'json',
    brokers            = 'kafka:9092',
    topic              = 'dispatch.decisions',
    delivery_guarantee = 'exactly_once',
    transactional_id   = 'dispatch-decisions'
);

-- score = squared distance from the rider's report to the restaurant, in
-- square metres, times 100000, plus the rider id: the smallest score is the
-- nearest rider, the lowest id among equals, and the id is read back from it.
-- A report from further than 1,500 m scores as if no rider had reported.
INSERT INTO decisions
WITH placed AS (
  SELECT o.order_id AS order_id, o.cell AS cell, o.zone AS zone, o.restaurant_id AS restaurant_id,
         o.placed_at AS placed_at, o.decide_at AS decide_at, o.fee_cents AS fee_cents,
         o.multiplier_pct AS multiplier_pct, r.rx AS rx, r.ry AS ry,
         r.zone0 AS zone0, r.zone1 AS zone1, r.zone2 AS zone2, r.zone3 AS zone3, r.zone4 AS zone4
  FROM priced_orders o JOIN restaurant_context r ON o.restaurant_id = r.restaurant_id
),
near AS (
  SELECT order_id, cell, zone, restaurant_id, placed_at, decide_at, fee_cents, multiplier_pct, rx, ry,
         zone0 AS search_zone FROM placed
  UNION ALL
  SELECT order_id, cell, zone, restaurant_id, placed_at, decide_at, fee_cents, multiplier_pct, rx, ry,
         zone1 AS search_zone FROM placed
  UNION ALL
  SELECT order_id, cell, zone, restaurant_id, placed_at, decide_at, fee_cents, multiplier_pct, rx, ry,
         zone2 AS search_zone FROM placed
  UNION ALL
  SELECT order_id, cell, zone, restaurant_id, placed_at, decide_at, fee_cents, multiplier_pct, rx, ry,
         zone3 AS search_zone FROM placed
  UNION ALL
  SELECT order_id, cell, zone, restaurant_id, placed_at, decide_at, fee_cents, multiplier_pct, rx, ry,
         zone4 AS search_zone FROM placed
),
cand AS (
  SELECT n.order_id AS order_id, n.cell AS cell, n.zone AS zone, n.restaurant_id AS restaurant_id,
         n.placed_at AS placed_at, n.decide_at AS decide_at, n.fee_cents AS fee_cents,
         n.multiplier_pct AS multiplier_pct,
         CASE WHEN (p.x - n.rx) * (p.x - n.rx) + (p.y - n.ry) * (p.y - n.ry) <= 2250000
              THEN p.rider_id ELSE CAST(NULL AS BIGINT) END AS rider_id,
         CASE WHEN (p.x - n.rx) * (p.x - n.rx) + (p.y - n.ry) * (p.y - n.ry) <= 2250000
              THEN ((p.x - n.rx) * (p.x - n.rx) + (p.y - n.ry) * (p.y - n.ry)) * 100000 + p.rider_id
              ELSE CAST(9223372036854775807 AS BIGINT) END AS score
  FROM near n JOIN rider_positions p
    ON n.search_zone = p.zone
   AND n.decide_at BETWEEN p.ts + 0 AND p.ts + 300000
  UNION ALL
  SELECT order_id, cell, zone, restaurant_id, placed_at, decide_at, fee_cents, multiplier_pct,
         CAST(NULL AS BIGINT) AS rider_id, CAST(9223372036854775807 AS BIGINT) AS score
  FROM placed
)
SELECT order_id, cell, zone, restaurant_id, placed_at, decide_at, fee_cents, multiplier_pct,
       CASE WHEN COUNT(DISTINCT rider_id) = 0 THEN 'escalated' ELSE 'assigned' END AS kind,
       CASE WHEN COUNT(DISTINCT rider_id) = 0 THEN CAST(NULL AS BIGINT)
            ELSE MIN(score) % 100000 END AS rider_id,
       COUNT(DISTINCT rider_id) AS candidates,
       MIN(score) AS score,
       window_end AS decided_at
FROM cand
GROUP BY order_id, cell, zone, restaurant_id, placed_at, decide_at, fee_cents, multiplier_pct,
         TUMBLE(decide_at, INTERVAL '1' MINUTE);
