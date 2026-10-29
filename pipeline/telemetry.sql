-- The telemetry store: every rider's position as a point in a time series.
--
-- Each report becomes one point, tagged by rider, city and zone, at the
-- report's own time. At least once: a report written again is the same
-- point, since the store keeps one value per series and time.

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
    group_id          = 'telemetry',
    auto_offset_reset = 'earliest'
);

CREATE TABLE rider_track (
    rider_id BIGINT,
    cell     VARCHAR,
    zone     VARCHAR,
    x        BIGINT,
    y        BIGINT,
    ts       BIGINT
) WITH (
    connector          = 'influxdb',
    format             = 'json',
    url                = 'http://influxdb:8086',
    org                = 'halftime',
    bucket             = 'halftime',
    token              = 'halftime-token',
    measurement        = 'rider_position',
    tags               = 'rider_id,cell,zone',
    timestamp_field    = 'ts',
    precision          = 'ms',
    delivery_guarantee = 'at_least_once'
);

INSERT INTO rider_track SELECT rider_id, cell, zone, x, y, ts FROM rider_positions;
