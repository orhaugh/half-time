-- The operations desk and the floor: escalations and alerts, onto the
-- systems the people watch.
--
-- An order no rider was near goes to the operations desk's RabbitMQ queue
-- and to the partners' Pulsar topic; a kitchen alert goes to the dispatch
-- floor's NATS subject. Each at least once: a restart can deliver a message
-- again, and each carries its own id.

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
    group_id          = 'ops',
    auto_offset_reset = 'earliest',
    shared_reader     = 'true'
);

CREATE TABLE kitchen_alerts (
    restaurant_id VARCHAR,
    cell          VARCHAR,
    first_order   VARCHAR,
    last_order    VARCHAR,
    first_at      BIGINT,
    last_at       BIGINT
) WITH (
    connector         = 'kafka',
    format            = 'json',
    brokers           = 'kafka:9092',
    topic             = 'kitchen.alerts',
    group_id          = 'ops',
    auto_offset_reset = 'earliest'
);

CREATE TABLE desk_escalations (
    order_id      VARCHAR,
    cell          VARCHAR,
    zone          VARCHAR,
    restaurant_id VARCHAR,
    placed_at     BIGINT
) WITH (
    connector          = 'rabbitmq',
    format             = 'json',
    host               = 'rabbitmq',
    port               = '5672',
    user               = 'halftime',
    password           = 'halftime',
    routing_key        = 'ops.escalations',
    persistent         = 'true',
    delivery_guarantee = 'at_least_once'
);

CREATE TABLE partner_escalations (
    order_id      VARCHAR,
    cell          VARCHAR,
    zone          VARCHAR,
    restaurant_id VARCHAR,
    placed_at     BIGINT
) WITH (
    connector          = 'pulsar',
    format             = 'json',
    service_url        = 'pulsar://pulsar:6650',
    topic              = 'persistent://public/default/partners.escalations',
    delivery_guarantee = 'at_least_once'
);

CREATE TABLE floor_alerts (
    restaurant_id VARCHAR,
    cell          VARCHAR,
    first_order   VARCHAR,
    last_order    VARCHAR,
    first_at      BIGINT,
    last_at       BIGINT
) WITH (
    connector          = 'nats',
    format             = 'json',
    url                = 'nats://nats:4222',
    subject            = 'ops.alerts',
    delivery_guarantee = 'at_least_once'
);

EXECUTE STATEMENT SET BEGIN
INSERT INTO desk_escalations
SELECT order_id, cell, zone, restaurant_id, placed_at FROM decisions WHERE kind = 'escalated';

INSERT INTO partner_escalations
SELECT order_id, cell, zone, restaurant_id, placed_at FROM decisions WHERE kind = 'escalated';

INSERT INTO floor_alerts
SELECT restaurant_id, cell, first_order, last_order, first_at, last_at FROM kitchen_alerts;
END;
