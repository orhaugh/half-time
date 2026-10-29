-- The gauges: each zone's surge multiplier, as a metric the operations
-- dashboards scrape.
--
-- Each window the pricing job publishes becomes one gauge sample on the
-- Pushgateway, labelled by the zone and the window, and Prometheus scrapes
-- the gateway (deploy/prometheus.yml). The gateway keeps the latest push,
-- so what the dashboards show is each zone's latest window. At least once:
-- a sample pushed again is the same sample.

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
    group_id          = 'gauges',
    auto_offset_reset = 'earliest'
);

CREATE TABLE zone_gauges (
    cell       VARCHAR,
    zone       VARCHAR,
    window_end BIGINT,
    value      BIGINT
) WITH (
    connector          = 'prometheus',
    format             = 'json',
    url                = 'http://pushgateway:9091',
    job                = 'halftime',
    metric_name        = 'halftime_zone_multiplier_pct',
    value_field        = 'value',
    help               = 'each zone surge multiplier in percent at its latest pricing window',
    delivery_guarantee = 'at_least_once'
);

INSERT INTO zone_gauges SELECT cell, zone, window_end, multiplier_pct AS value FROM multipliers;
