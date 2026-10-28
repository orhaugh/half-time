-- The archive: every order change, every rider report and every dispatch
-- decision, as Parquet, exactly once.
--
-- Each stream lands in its own directory. Rows are staged as the stream
-- arrives and the staged file is published, by one atomic rename, only when
-- the checkpoint that holds it completes, so a restart from a checkpoint
-- republishes nothing and loses nothing.

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
    group_id          = 'archive',
    auto_offset_reset = 'earliest'
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
    group_id          = 'archive',
    auto_offset_reset = 'earliest'
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
    group_id          = 'archive',
    auto_offset_reset = 'earliest'
);

CREATE TABLE archived_changes (
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
    connector          = 'parquet',
    path               = '/archive/orders.changes',
    delivery_guarantee = 'exactly_once'
);

CREATE TABLE archived_positions (
    ts       BIGINT,
    rider_id BIGINT,
    cell     VARCHAR,
    zone     VARCHAR,
    x        BIGINT,
    y        BIGINT,
    sent_at  BIGINT
) WITH (
    connector          = 'parquet',
    path               = '/archive/riders.positions',
    delivery_guarantee = 'exactly_once'
);

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
    connector          = 'parquet',
    path               = '/archive/dispatch.decisions',
    delivery_guarantee = 'exactly_once'
);

EXECUTE STATEMENT SET BEGIN
INSERT INTO archived_changes SELECT * FROM order_changes;
INSERT INTO archived_positions SELECT * FROM rider_positions;
INSERT INTO archived_decisions SELECT * FROM decisions;
END;
