-- The tablet bus: the restaurants' kitchen events, carried onto Pulsar.
--
-- The tablet vendor delivers its events on Pulsar. In this project the
-- generator writes them to a Kafka topic, and this job publishes each onto
-- the Pulsar topic the kitchen job subscribes to, which is also how the
-- Pulsar sink is exercised. At least once: a restart can publish an event
-- again, and the kitchen job's pattern tolerates a repeated event.

CREATE TABLE kitchen_events (
    event_id      VARCHAR,
    order_id      VARCHAR,
    restaurant_id VARCHAR,
    cell          VARCHAR,
    status        VARCHAR,
    ts            BIGINT
) WITH (
    connector         = 'kafka',
    format            = 'json',
    brokers           = 'kafka:9092',
    topic             = 'kitchen.events',
    group_id          = 'bridge',
    auto_offset_reset = 'earliest'
);

CREATE TABLE tablet_bus (
    event_id      VARCHAR,
    order_id      VARCHAR,
    restaurant_id VARCHAR,
    cell          VARCHAR,
    status        VARCHAR,
    ts            BIGINT
) WITH (
    connector          = 'pulsar',
    format             = 'json',
    service_url        = 'pulsar://pulsar:6650',
    topic              = 'persistent://public/default/kitchen.events',
    delivery_guarantee = 'at_least_once'
);

INSERT INTO tablet_bus SELECT * FROM kitchen_events;
