-- The kitchen watch: a restaurant that bounces three orders in a row is
-- overloaded, and the floor hears about it.
--
-- Every tablet event arrives on Pulsar, which delivers at least once: the
-- bridge publishes an event again after a restart, and the vendor's own
-- producer retries. A repeated event would count as a bounce of its own, so
-- the events are first made distinct, which drops a copy whatever brought
-- it. Per restaurant, in event-time order, three consecutive bounces (a
-- declined order with no acceptance between) raise one alert naming the
-- three orders and the span, and rows are held until the watermark passes
-- them, which is forty-five minutes of match behind the newest row (see
-- the table). The alert is published at least once: Pulsar hands clink no position
-- to replay from, only redelivery of what was not acknowledged, so the
-- planner holds a job over it to at least once, and a repeated alert names
-- the same restaurant and the same span.

CREATE TABLE tablet_bus (
    event_id      VARCHAR,
    order_id      VARCHAR,
    restaurant_id VARCHAR,
    cell          VARCHAR,
    status        VARCHAR,
    ts            BIGINT
) WITH (
    connector         = 'pulsar',
    format            = 'json',
    service_url       = 'pulsar://pulsar:6650',
    topic             = 'persistent://public/default/kitchen.events',
    subscription      = 'kitchen',
    event_time_column = 'ts',
    -- Forty-five minutes of lateness, not one: after a restart the bus hands
    -- a consumer the messages its predecessor had not acknowledged after the
    -- newer ones, and at thirty times real time a restart's seconds are
    -- minutes of match. A bus clink replays from a position of its own
    -- needs a minute here, as the Kafka tables have.
    watermark_lag_ms  = '2700000',
    idle_timeout_ms   = '90000'
);

CREATE TABLE kitchen_alerts (
    restaurant_id VARCHAR,
    cell          VARCHAR,
    first_order   VARCHAR,
    last_order    VARCHAR,
    first_at      BIGINT,
    last_at       BIGINT
) WITH (
    connector          = 'kafka',
    format             = 'json',
    brokers            = 'kafka:9092',
    topic              = 'kitchen.alerts',
    delivery_guarantee = 'at_least_once'
);

INSERT INTO kitchen_alerts
WITH tablet_events AS (
    SELECT DISTINCT event_id, order_id, restaurant_id, cell, status, ts FROM tablet_bus
)
SELECT restaurant_id, cell, first_order, last_order, first_at, last_at
FROM tablet_events MATCH_RECOGNIZE (
    PARTITION BY restaurant_id
    ORDER BY ts
    MEASURES FIRST(b.cell)     AS cell,
             FIRST(b.order_id) AS first_order,
             LAST(b.order_id)  AS last_order,
             FIRST(b.ts)       AS first_at,
             LAST(b.ts)        AS last_at
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (b{3})
    DEFINE b AS status = 'bounced'
);
