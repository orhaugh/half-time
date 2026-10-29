-- The backfill: the dispatch archive read back into the analytics store.
--
-- A bounded job, run after the night: it reads the Parquet files the archive
-- job committed and writes each decision to a table beside the live one, so
-- the archive can be shown to hold what the stream held. The source is a
-- directory of complete files, so the job ends when it has read them.

CREATE TABLE archived_decisions (
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
    connector = 'parquet',
    format    = 'json',
    path      = '/archive/dispatch.decisions/committed'
);

CREATE TABLE decisions_backfill (
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
    "table"            = 'dispatch_decisions_backfill',
    user               = 'halftime',
    password           = 'halftime',
    batch_interval_ms  = '500',
    delivery_guarantee = 'at_least_once'
);

INSERT INTO decisions_backfill
SELECT order_id, kind, rider_id, candidates, score, multiplier_pct, fee_cents, cell, zone, restaurant_id,
       placed_at AS placed_at_ms
FROM archived_decisions;
